#!/usr/bin/env python

"""
Created by mebitek in 2026.

Inspired by:
 - https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py (Template)

https://github.com/mebitek/JkMonitor
Reading information from JK BMS bluetooth via aiobmsble libraries and puts the info on dbus as battery.
"""

import os
import sys
import json
import dbus
import threading
import subprocess
from datetime import datetime, timedelta
import utils
from time import sleep
from typing import Optional

import asyncio
import logging

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError

from aiobmsble import BMSSample
from aiobmsble.bms.jikong_bms import BMS

sys.path.insert(1, "/data/SetupHelper/velib_python")

from vedbus import VeDbusService, VeDbusItemImport
from gi.repository import GLib
from vreg_link_item import VregLinkItem, GenericReg, JkReg

from jk_config import JkConfig

HISTORY_FILE = "/data/conf/jk_history.json"


class JkBms:
    def __init__(self, name, soc, voltage, current, power, temperature):
        self.name        = name
        self.voltage     = voltage
        self.current     = current
        self.power       = power
        self.temperature = temperature
        self.soc         = soc
        self.bms_soc     = None
        self.design_capacity = None
        # fields provided directly by the JK BMS
        self.cycle_charge   = 0.0
        self.cycles         = 0
        self.automatic_syncs = 0
        self.battery_health = 0
        self.delta_voltage  = 0.02
        # calculated / persistent history
        self.hist_last_discharge:    float      = 0.0
        self.hist_deepest_discharge: float      = 0.0
        self.hist_min_voltage: Optional[float] = None
        self.hist_max_voltage: Optional[float] = None
        # energy tracking (Wh)
        self.hist_discharged_energy: float      = 0.0
        self.hist_charged_energy:    float      = 0.0
        # full discharge counter
        self.hist_full_discharges:   int        = 0
        self._soc_was_high:          bool       = False
        # last full charge timestamp
        self.hist_last_full_charge: Optional[datetime] = None
        self.last_update: Optional[datetime] = None
        self.last_sync_time: Optional[datetime] = None
        self.missing_updates = 0
        # BLE device and adapter cache
        self.device       = None
        self.adapter: str = "hci0"   # cached adapter, updated when device is found
        self.low_soc_alarm = 0
        self.low_voltage_alarm = 0
        self.high_voltage_alarm = 0

        self.hist_low_voltage_alarms = 0
        self.hist_high_voltage_alarms = 0


class JkMonitorService:
    def __init__(
        self,
        servicename,
        deviceinstance,
        paths,
        productname="JkBms",
        connection="Bluetooth",
        config=None,
    ):
        self.config = config or JkConfig()

        self.jk = JkBms(config.get_device_name(), 0, 12.8, 0, 0, 0)
        logging.debug("BMS device name: %s", self.jk.name)

        self.jk.device = None

        # _ble_lock: ensures only one BLE update runs at a time.
        # acquire() with blocking=False fails immediately if already taken.
        self._ble_lock = threading.Lock()

        # dedicated asyncio event loop in a daemon thread —
        # BLE operations never block the GLib/dbus thread
        self._async_loop   = asyncio.new_event_loop()
        self._async_thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True,
            name="jk-ble-loop",
        )
        self._async_thread.start()

        # dbus service
        self._dbusservice = VeDbusService(servicename, register=False)
        self._paths = paths

        vregtype = lambda *args, **kwargs: VregLinkItem(
            *args, **kwargs,
            getvreg=self.vreg_link_get,
            setvreg=self.vreg_link_set,
        )

        logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

        productname = "Jk BMS " + config.get_model()
        logging.debug("Product name: %s", productname)

        self._dbusservice.add_path("/Mgmt/ProcessName",    __file__)
        self._dbusservice.add_path("/Mgmt/ProcessVersion", config.get_version())
        self._dbusservice.add_path("/Mgmt/Connection",     connection)

        self._dbusservice.add_path("/DeviceInstance",  deviceinstance)
        self._dbusservice.add_path("/ProductId",       0xA383)
        self._dbusservice.add_path("/ProductName",     productname)
        self._dbusservice.add_path("/DeviceName",      productname)
        self._dbusservice.add_path("/FirmwareVersion", 0x0419)
        self._dbusservice.add_path("/HardwareVersion", 8)
        self._dbusservice.add_path("/Connected",       1)
        self._dbusservice.add_path("/Serial",          config.get_serial())

        self._dbusservice.add_path('/Devices/0/CustomName',      productname)
        self._dbusservice.add_path('/Devices/0/DeviceInstance',  deviceinstance)
        self._dbusservice.add_path('/Devices/0/FirmwareVersion', 0x0419)
        self._dbusservice.add_path('/Devices/0/ProductId',       0xA383)
        self._dbusservice.add_path('/Devices/0/ProductName',     productname)
        self._dbusservice.add_path('/Devices/0/ServiceName',     servicename)
        self._dbusservice.add_path('/Devices/0/Serial',          config.get_serial())
        self._dbusservice.add_path('/Devices/0/VregLink',        None, itemtype=vregtype)

        for path, settings in self._paths.items():
            self._dbusservice.add_path(
                path,
                settings["initial"],
                writeable=True,
                onchangecallback=self._handlechangedvalue,
            )

        self._dbusservice.register()
        self._load_history()

        GLib.timeout_add(1000, self._update)

    # ------------------------------------------------------------------
    # Async loop thread
    # ------------------------------------------------------------------
    def _run_async_loop(self):
        asyncio.set_event_loop(self._async_loop)
        self._async_loop.run_forever()

    # ------------------------------------------------------------------
    # GLib callback — returns immediately, schedules work in BLE thread
    # ------------------------------------------------------------------
    def _update(self):
        if not self._ble_lock.acquire(blocking=False):
            logging.warning("BLE update already in progress, skipping cycle.")
            return True

        future = asyncio.run_coroutine_threadsafe(
            self._async_update_logic(), self._async_loop
        )
        future.add_done_callback(self._on_update_done)
        return True

    def _on_update_done(self, future):
        try:
            future.result()
        except Exception:
            logging.exception("Unhandled exception in async BLE update")
        finally:
            self._ble_lock.release()

    # ------------------------------------------------------------------
    # Async BLE logic
    # All dbus writes go via GLib.idle_add() because this method runs
    # in the asyncio thread, not the GLib thread that owns the bus.
    # ------------------------------------------------------------------
    async def _async_update_logic(self):

        # 1. Scan for device if not already found
        if self.jk.device is None:
            logging.info("Searching for device: %s", self.config.get_device_name())
            try:
                device = await BleakScanner.find_device_by_name(
                    self.config.get_device_name(), timeout=10.0
                )
                if device:
                    self.jk.device  = device
                    self.jk.adapter = self._detect_adapter(device)
                    logging.info("Found device: %s (adapter: %s)", device.address, self.jk.adapter)
                else:
                    logging.warning("Device not found yet...")
                    return
            except Exception as e:
                logging.error("Error during scan: %s", e)
                await self.restart_ble_hardware_and_bluez_driver()
                return

        # 2. Handle alarms — uses cached adapter, never reads a None device
        if self.jk.missing_updates > 10:
            current_alarm = self._dbusservice["/Alarms/InternalFailure"]
            if self.jk.missing_updates > 20:
                if current_alarm != 2:
                    GLib.idle_add(self._dbus_set, "/Alarms/InternalFailure", 2)
                    await self.restart_ble_hardware_and_bluez_driver()
            else:
                if current_alarm != 1:
                    GLib.idle_add(self._dbus_set, "/Alarms/InternalFailure", 1)
                    await self.restart_bluetooth_service()

        # 3. Respect the configured read interval
        if self.jk.last_update is not None and datetime.now() <= self.jk.last_update + timedelta(
            minutes=self.config.get_interval()
        ):
            return

        # 4. Read BMS data
        try:
            async with BMS(ble_device=self.jk.device) as bms:
                data: BMSSample = await bms.async_update()

                if not self.jk.design_capacity:
                    self.jk.design_capacity = data['design_capacity']
                    if self.jk.design_capacity != self.config.get_battery_capacity():
                        self.config.write_to_config(self.jk.design_capacity, "Setup", "BatteryCapacity")
                        self.config = JkConfig()
                capacityAh = self.config.get_battery_capacity()
                self.jk.voltage     = data['voltage']
                self.jk.current     = data['current']
                self.jk.power       = data['power']
                self.jk.soc         = min(100.0, round((data['cycle_charge'] * 100) / self.config.get_battery_capacity(), 2))

                if self.jk.soc < self.config.get_low_soc_alarm_set():
                    self.jk.low_soc_alarm = 1
                if self.jk.soc > self.config.get_low_soc_alarm_clear() and self.jk.low_soc_alarm == 1:
                    self.jk.low_soc_alarm = 0

                if self.jk.voltage < 10.8:
                    if self.jk.low_voltage_alarm == 0:
                        self.jk.hist_low_voltage_alarms += 1
                    self.jk.low_voltage_alarm = 1
                elif self.jk.voltage > 14.6:
                    if self.jk.high_voltage_alarm == 0:
                        self.jk.hist_high_voltage_alarms += 1
                    self.jk.high_voltage_alarm = 1
                else:
                    self.jk.low_voltage_alarm = 0
                    self.jk.high_voltage_alarm = 0
                
                self.jk.bms_soc     = data['battery_level']
                if self.jk.voltage >= self.config.get_soc_detection_voltage():
                    self.jk.soc = 100
                self.jk.temperature = data['temperature']
               

                # native JK fields — .get() for safety on older firmware
                self.jk.cycles         = int(data.get('cycles',         0))
                self.jk.cycle_charge   = float(data.get('cycle_charge', 0.0))
                self.jk.battery_health = int(data.get('battery_health', 0))
                self.jk.delta_voltage  = float(data.get('delta_voltage',0.0))

                consumed   = capacityAh * (100 - self.jk.soc) / 100
                if self.jk.soc == 100 and self.jk.current >= 0:
                    consumed = 0
                    if self.jk.last_sync_time is None:
                        self.jk.automatic_syncs += 1
                        self.jk.last_sync_time = datetime.now()
                    else:
                        if self.jk.last_sync_time < datetime.now() - timedelta(minutes=60):
                            self.jk.automatic_syncs += 1
                            self.jk.last_sync_time = datetime.now()
                else:
                    self.jk.last_sync_time = None

                ttg        = self.remaining_time_seconds(capacityAh, self.jk.soc, self.jk.current)

                # -- Energy integration (Wh) using elapsed time since last update --
                if self.jk.last_update is not None:
                    dt_h = (datetime.now() - self.jk.last_update).total_seconds() / 3600.0
                    wh   = abs(self.jk.power) * dt_h
                    if self.jk.current < 0:
                        self.jk.hist_discharged_energy += wh
                    elif self.jk.current > 0:
                        self.jk.hist_charged_energy += wh

                self.jk.last_update     = datetime.now()
                self.jk.missing_updates = 0

                # -- TimeSinceLastFullCharge --
                if self.jk.soc >= 100:
                    self.jk.hist_last_full_charge = datetime.now()
                time_since_full = 0
                if self.jk.hist_last_full_charge is not None:
                    time_since_full = int(
                        (datetime.now() - self.jk.hist_last_full_charge).total_seconds()
                    )

                # -- FullDischarges: state machine SOC high → SOC low --
                SOC_HIGH = 80
                SOC_LOW  = 20
                if self.jk.soc >= SOC_HIGH:
                    self.jk._soc_was_high = True
                if self.jk._soc_was_high and self.jk.soc <= SOC_LOW:
                    self.jk.hist_full_discharges += 1
                    self.jk._soc_was_high = False
                    logging.info("Full discharge detected (#%d)", self.jk.hist_full_discharges)

                # -- LastDischarge / DeepestDischarge --
                if consumed > 0:
                    self.jk.hist_last_discharge = consumed
                if consumed > self.jk.hist_deepest_discharge:
                    self.jk.hist_deepest_discharge = consumed

                # -- MinimumVoltage / MaximumVoltage --
                if self.jk.hist_min_voltage is None or self.jk.voltage < self.jk.hist_min_voltage:
                    self.jk.hist_min_voltage = self.jk.voltage
                if self.jk.hist_max_voltage is None or self.jk.voltage > self.jk.hist_max_voltage:
                    self.jk.hist_max_voltage = self.jk.voltage

                # total ah drawn
                avg_voltage = (self.jk.hist_min_voltage + self.jk.hist_max_voltage) / 2 if (self.jk.hist_min_voltage and self.jk.hist_max_voltage) else 12.8
                total_drawn = self.jk.hist_discharged_energy / avg_voltage

                # -- AverageDischarge: cycle_charge / cycles (native from BMS) --
                avg_discharge = (
                    total_drawn / self.jk.automatic_syncs if self.jk.automatic_syncs > 0 else 0.0
                )

                last_sync_time_str = '-'
                if self.jk.last_sync_time is not None:
                    last_sync_time_str = self.jk.last_sync_time.strftime("%m/%d/%Y, %H:%M:%S")

                # Push all values to dbus in the GLib thread
                GLib.idle_add(self._dbus_commit, {
                    "/Alarms/InternalFailure":          0,
                    "/Dc/0/Voltage":                    self.jk.voltage,
                    "/Dc/0/Power":                      self.jk.power,
                    "/Dc/0/Current":                    self.jk.current,
                    "/Dc/0/Temperature":                self.jk.temperature,
                    "/Soc":                             self.jk.soc,
                    "/TimeToGo":                        ttg,
                    "/ConsumedAmphours":                consumed,
                    # calculated history
                    "/History/LastDischarge":           self.jk.hist_last_discharge,
                    "/History/DeepestDischarge":        self.jk.hist_deepest_discharge,
                    "/History/MinimumVoltage":          self.jk.hist_min_voltage,
                    "/History/MaximumVoltage":          self.jk.hist_max_voltage,
                    "/History/DischargedEnergy":        round(self.jk.hist_discharged_energy/1000, 3),
                    "/History/ChargedEnergy":           round(self.jk.hist_charged_energy/1000, 3),
                    "/History/FullDischarges":          self.jk.hist_full_discharges,
                    "/History/TimeSinceLastFullCharge": time_since_full,
                    "/History/AverageDischarge":        round(avg_discharge, 3),
                    "/History/AutomaticSyncs":          self.jk.automatic_syncs,
                    "/History/TotalAhDrawn":            total_drawn,
                    "/History/LowVoltageAlarms":        self.jk.hist_low_voltage_alarms,
                    "/History/HighVoltageAlarms":       self.jk.hist_high_voltage_alarms,
                    # native history from BMS
                    "/History/ChargeCycles":            self.jk.cycles,
                    #debug
                    "/RemainingCapacity":               self.jk.cycle_charge,
                    "/BmsSoc":                          self.jk.bms_soc,
                    "/Alarms/LowSoc":                   self.jk.low_soc_alarm,
                    "/Alarms/LowVoltage":               self.jk.low_voltage_alarm,
                    "/Alarms/HighVoltage":              self.jk.high_voltage_alarm,
                    "/LastSyncTime":                    last_sync_time_str
                })
                GLib.idle_add(self._increment_update_index)

                self._save_history()

                logging.debug(
                    "BATTERY UPDATED: SOC %s, V %s",
                    self.jk.soc, self.jk.voltage,
                )

        except Exception as e:
            logging.error("Failed to update BMS: %s", e)
            self.jk.missing_updates += 1
            if self.jk.missing_updates > 5:
                self.jk.device = None

    # ------------------------------------------------------------------
    # History persistence
    # ------------------------------------------------------------------
    def _load_history(self):
        """Load history from JSON file at startup. Starts from zero if missing or corrupt."""
        try:
            with open(HISTORY_FILE, "r") as f:
                data = json.load(f)
            self.jk.hist_last_discharge    = float(data.get("last_discharge",    0.0))
            self.jk.hist_deepest_discharge = float(data.get("deepest_discharge", 0.0))
            self.jk.hist_discharged_energy = float(data.get("discharged_energy", 0.0))
            self.jk.hist_charged_energy    = float(data.get("charged_energy",    0.0))
            self.jk.hist_full_discharges   = int(data.get("full_discharges",     0))
            self.jk._soc_was_high          = bool(data.get("soc_was_high",       False))
            min_v = data.get("min_voltage")
            max_v = data.get("max_voltage")
            self.jk.hist_min_voltage = float(min_v) if min_v is not None else None
            self.jk.hist_max_voltage = float(max_v) if max_v is not None else None
            last_fc = data.get("last_full_charge")
            self.jk.hist_last_full_charge = (
                datetime.fromisoformat(last_fc) if last_fc else None
            )
            self.jk.automatic_syncs = int(data.get("automatic_syncs", 0))
            # native BMS fields — used as fallback until first BMS update
            self.jk.cycles         = int(data.get("cycles",         0))
            self.jk.cycle_charge   = float(data.get("cycle_charge", 0.0))
            self.jk.battery_health = int(data.get("battery_health", 0))
            self.jk.hist_low_voltage_alarms = int(data.get("low_voltage_alarms", 0))
            self.jk.hist_high_voltage_alarms = int(data.get("high_voltage_alarms", 0))
            logging.info(
                "History loaded: last=%.2fAh deepest=%.2fAh "
                "minV=%s maxV=%s cycles=%d discharged=%.1fWh charged=%.1fWh "
                "full_discharges=%d health=%d%%",
                self.jk.hist_last_discharge, self.jk.hist_deepest_discharge,
                self.jk.hist_min_voltage, self.jk.hist_max_voltage,
                self.jk.cycles, self.jk.hist_discharged_energy,
                self.jk.hist_charged_energy, self.jk.hist_full_discharges,
                self.jk.battery_health,
            )
        except FileNotFoundError:
            logging.info("No history file found, starting from scratch.")
        except Exception:
            logging.exception("Error reading history file, starting from scratch.")

    def _save_history(self):
        """Save history atomically: write to .tmp then rename to avoid corruption on crash."""
        tmp = HISTORY_FILE + ".tmp"
        try:
            data = {
                "last_discharge":    self.jk.hist_last_discharge,
                "deepest_discharge": self.jk.hist_deepest_discharge,
                "min_voltage":       self.jk.hist_min_voltage,
                "max_voltage":       self.jk.hist_max_voltage,
                "discharged_energy": self.jk.hist_discharged_energy,
                "charged_energy":    self.jk.hist_charged_energy,
                "full_discharges":   self.jk.hist_full_discharges,
                "soc_was_high":      self.jk._soc_was_high,
                "last_full_charge":  (
                    self.jk.hist_last_full_charge.isoformat()
                    if self.jk.hist_last_full_charge else None
                ),
                # native BMS fields (cached for restart)
                "cycles":            self.jk.cycles,
                "automatic_syncs":   self.jk.automatic_syncs,
                "cycle_charge":      self.jk.cycle_charge,
                "battery_health":    self.jk.battery_health,
                "last_saved":        datetime.now().isoformat(),
                "low_voltage_alarms": self.jk.hist_low_voltage_alarms,
                "high_voltage_alarms": self.jk.hist_high_voltage_alarms
            }
            with open(tmp, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, HISTORY_FILE)
            logging.debug("History saved to %s", HISTORY_FILE)
        except Exception:
            logging.exception("Error saving history file")
            try:
                os.remove(tmp)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # dbus helpers — executed in GLib thread via idle_add
    # ------------------------------------------------------------------
    def _dbus_set(self, path, value):
        self._dbusservice[path] = value
        return False

    def _dbus_commit(self, values: dict):
        for path, value in values.items():
            self._dbusservice[path] = value
        return False

    def _increment_update_index(self):
        index = self._dbusservice["/UpdateIndex"] + 1
        self._dbusservice["/UpdateIndex"] = index if index <= 255 else 0
        return False

    def _handlechangedvalue(self, path, value):
        logging.debug("someone else updated %s to %s" % (path, value))
        return True

    # ------------------------------------------------------------------
    # VregLink GET
    # ------------------------------------------------------------------
    def vreg_link_get(self, reg_id):
        if reg_id == JkReg.DC_MONITOR_MODE.value:
            return GenericReg.OK.value, [0xFE]
        elif reg_id == JkReg.VE_REG_BATTERY_CAPACITY.value:
            capacityAh = float(self.config.get_battery_capacity()/100)
            return GenericReg.OK.value, utils.convert_decimal(capacityAh)
        elif reg_id == JkReg.VE_REG_CHARGED_VOLTAGE.value:
            return GenericReg.OK.value, utils.convert_decimal(1.36)
        elif reg_id == JkReg.VE_REG_PEUKERT_COEFFICIENT.value:
            return GenericReg.OK.value, utils.convert_decimal(1.01)
        elif reg_id == JkReg.VE_REG_CHARGE_DETECTION_TIME.value:
            return GenericReg.OK.value, utils.convert_decimal(0.03)
        elif reg_id == JkReg.VE_REG_CHARGE_EFFICIENCY.value:
            return GenericReg.OK.value, utils.convert_decimal(0.98)
        elif reg_id == JkReg.VE_REG_CURRENT_THRESHOLD.value:
            return GenericReg.OK.value, utils.convert_decimal(0.1)
        elif reg_id == JkReg.VE_REG_CHARGED_CURRENT.value:
            return GenericReg.OK.value, utils.convert_decimal(0.02)
        elif reg_id == JkReg.VE_REG_LOW_SOC.value:
            return GenericReg.OK.value, utils.convert_decimal(1)
        elif reg_id == JkReg.VE_REG_HIST_LAST_DISCHARGE.value:
            return GenericReg.OK.value, utils.convert_decimal(self.jk.hist_last_discharge)
        else:
            logging.debug("GET REG_ID %s" % reg_id)
            return GenericReg.OK.value, []

    # ------------------------------------------------------------------
    # VregLink SET
    # ------------------------------------------------------------------
    def vreg_link_set(self, reg_id, data):
        if reg_id == JkReg.VE_REG_BATTERY_CAPACITY.value:
            decimal = utils.convert_to_decimal(bytearray(data))
            self.config.write_to_config(decimal, "Setup", "BatteryCapacity")
        elif reg_id == JkReg.VE_REG_LOW_SOC.value:
            decimal = utils.convert_to_decimal(bytearray(data))
            self.config.write_to_config(decimal, "Setup", "LowSocAlarmSet")
        elif reg_id == JkReg.VE_REG_LOW_SOC_CLEAR.value:
            decimal = utils.convert_to_decimal(bytearray(data))
            self.config.write_to_config(decimal, "Setup", "LowSocAlarmClear")
        self.config = JkConfig()
        return GenericReg.OK.value, data

    def remaining_time_seconds(self, capacity, soc, current_a):
        MIN_CURRENT = 0.1
        if current_a >= -MIN_CURRENT:
            return 864000
        remaining_ah = capacity * (soc / 100.0)
        hours = remaining_ah / abs(current_a)
        return int(hours * 3600)

    # ------------------------------------------------------------------
    # Bluetooth restart utilities
    # ------------------------------------------------------------------
    @staticmethod
    def _detect_adapter(device) -> str:
        """
        Extract the BLE adapter (e.g. hci0, hci1) from the device DBus path.
        On Linux, device.details contains the full DBus path:
          /org/bluez/hci1/dev_AA_BB_CC_DD_EE_FF  →  hci1
        Falls back to hci0 if detection fails.
        """
        try:
            path = device.details.get("path", "") or str(device.details)
            for part in path.split("/"):
                if part.startswith("hci"):
                    logging.info("BLE adapter detected: %s", part)
                    return part
        except Exception as e:
            logging.warning("Could not determine BLE adapter: %s", e)
        logging.warning("BLE adapter could not be determined, falling back to hci0")
        return "hci0"

    def _restart_ble_hardware_sync(self, adapter: str):
        """Blocking — must only be called via run_in_executor."""
        logging.info("*** Restarting BLE hardware on %s ***", adapter)
        for cmd, label in [
            (["bluetoothctl", "--adapter", adapter, "power", "off"], "power off"),
            (["bluetoothctl", "--adapter", adapter, "power", "on"],  "power on"),
        ]:
            result = subprocess.run(cmd, capture_output=True, text=True)
            logging.info("%s exit code: %d  output: %s", label, result.returncode, result.stdout.strip())
            if label == "power off":
                sleep(5)

    async def restart_ble_hardware_and_bluez_driver(self):
        """Non-blocking: delegates to a thread via run_in_executor."""
        await self._async_loop.run_in_executor(
            None, self._restart_ble_hardware_sync, self.jk.adapter
        )

    def _restart_bluetooth_sync(self, adapter: str):
        """Blocking — must only be called via run_in_executor."""
        logging.warning("*** Attempting Bluetooth daemon restart on %s ***", adapter)
        try:
            subprocess.run(['rfkill', 'unblock', 'bluetooth'], timeout=5)
            sleep(5)
            subprocess.run(['hciconfig', adapter, 'reset'], timeout=5)
            sleep(5)
            result = subprocess.run(['bluetoothctl', '--adapter', adapter, 'power', 'on'], timeout=5)
            if result.returncode == 0:
                logging.info("Bluetooth successfully restarted on %s.", adapter)
                sleep(3)
                return True
            else:
                logging.error("Bluetooth restart error: %s", result.stderr)
                return False
        except Exception as e:
            logging.exception("Exception during Bluetooth restart: %s", e)
            return False

    async def restart_bluetooth_service(self):
        """Non-blocking: delegates to a thread via run_in_executor."""
        await self._async_loop.run_in_executor(
            None, self._restart_bluetooth_sync, self.jk.adapter
        )


def main():
    config = JkConfig()

    level = logging.INFO
    if config.get_debug():
        level = logging.DEBUG
    logging.basicConfig(level=level)
    logging.info(">>>>>>>>>>>>>>>> Jk Monitor Starting <<<<<<<<<<<<<<<<")

    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)

    pvac_output = JkMonitorService(
        servicename="com.victronenergy.battery.jkbms",
        deviceinstance=295,
        paths={
            "/Dc/0/Voltage":                    {"initial": 0},
            "/Dc/0/Current":                    {"initial": 0},
            "/Dc/0/Power":                      {"initial": 0},
            "/Soc":                             {"initial": 0},
            "/UpdateIndex":                     {"initial": 0},
            "/Capacity":                        {"initial": config.get_battery_capacity()},
            "/TimeToGo":                        {"initial": 0},
            "/ConsumedAmphours":                {"initial": 0},
            "/Dc/0/Temperature":                {"initial": 0},
            "/Settings/HasTemperature":         {"initial": 1},
            "/Settings/MonitorMode":            {"initial": 0},
            "/Alarms/LowSoc":                   {"initial": 0},
            "/Alarms/InternalFailure":          {"initial": 0},
            "/Alarms/LowVoltage":               {"initial": 0},
            "/Alarms/HighVoltage":              {"initial": 0},
            "/History/DeepestDischarge":        {"initial": 0},
            "/History/LastDischarge":           {"initial": 0},
            "/History/AverageDischarge":        {"initial": 0},
            "/History/ChargeCycles":            {"initial": 0},
            "/History/FullDischarges":          {"initial": 0},
            "/History/TotalAhDrawn":            {"initial": 0},
            "/History/MinimumVoltage":          {"initial": 0},
            "/History/MaximumVoltage":          {"initial": 0},
            "/History/TimeSinceLastFullCharge": {"initial": 0},
            "/History/AutomaticSyncs":          {"initial": 0},
            "/History/DischargedEnergy":        {"initial": 0},
            "/History/ChargedEnergy":           {"initial": 0},
            "/History/LowVoltageAlarms":        {"initial": 0},
            "/History/HighVoltageAlarms":       {"initial": 0},
            "/RemainingCapacity":               {"initial": 0},
            "/BmsSoc":                          {"initial": 0},
            "/LastSyncTime":                    {"initial": ""}
        },
        config=config,
    )

    logging.info("Connected to dbus, and switching over to GLib.MainLoop() (= event based)")
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()