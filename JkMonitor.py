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
import _thread as thread
import threading
import subprocess
from datetime import datetime, timedelta
import utils
from time import sleep

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
from settingsdevice import SettingsDevice

from jk_config import JkConfig


class JkBms:
    def __init__(self, name, soc, voltage, current, power, temperature):
        self.name = name
        self.voltage = voltage
        self.current = current
        self.power = power
        self.temperature = temperature
        self.soc = soc
        self.hist_last_discharge = None
        self.last_update = None
        self.missing_updates = 0
        self.device = None


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
        logging.debug("* * * MAC %s", self.jk.name)

        self.jk.device = None
        self._ble_lock = threading.Lock()

        self._async_loop = asyncio.new_event_loop()
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
        logging.debug("* * * Product name is %s", productname)

        self._dbusservice.add_path("/Mgmt/ProcessName", __file__)
        self._dbusservice.add_path("/Mgmt/ProcessVersion", config.get_version())
        self._dbusservice.add_path("/Mgmt/Connection", connection)

        self._dbusservice.add_path("/DeviceInstance", deviceinstance)
        self._dbusservice.add_path("/ProductId", 0xA383)
        self._dbusservice.add_path("/ProductName", productname)
        self._dbusservice.add_path("/DeviceName", productname)
        self._dbusservice.add_path("/FirmwareVersion", 0x0419)
        self._dbusservice.add_path("/HardwareVersion", 8)
        self._dbusservice.add_path("/Connected", 1)
        self._dbusservice.add_path("/Serial", config.get_serial())

        self._dbusservice.add_path('/Devices/0/CustomName', productname)
        self._dbusservice.add_path('/Devices/0/DeviceInstance', deviceinstance)
        self._dbusservice.add_path('/Devices/0/FirmwareVersion', 0x0419)
        self._dbusservice.add_path('/Devices/0/ProductId', 0xA383)
        self._dbusservice.add_path('/Devices/0/ProductName', productname)
        self._dbusservice.add_path('/Devices/0/ServiceName', servicename)
        self._dbusservice.add_path('/Devices/0/Serial', config.get_serial())
        self._dbusservice.add_path('/Devices/0/VregLink', None, itemtype=vregtype)

        for path, settings in self._paths.items():
            self._dbusservice.add_path(
                path,
                settings["initial"],
                writeable=True,
                onchangecallback=self._handlechangedvalue,
            )

        self._dbusservice.register()

        GLib.timeout_add(1000, self._update)

    def _run_async_loop(self):
        asyncio.set_event_loop(self._async_loop)
        self._async_loop.run_forever()

    def _update(self):

        if not self._ble_lock.acquire(blocking=False):
            logging.warning("BLE updating, skipping....")
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
            logging.exception("unhandled exception updating BLE")
        finally:
            self._ble_lock.release()


    async def _async_update_logic(self):
        if self.jk.device is None:
            logging.info("Searching for device: %s", self.config.get_device_name())
            try:
                device = await BleakScanner.find_device_by_name(
                    self.config.get_device_name(), timeout=10.0
                )
                if device:
                    self.jk.device = device
                    logging.info("Found device: %s", device.address)
                else:
                    logging.warning("Device not found yet...")
                    return
            except Exception as e:
                self.restart_ble_hardware_and_bluez_driver()
                logging.error(f"Error during scan: {e}")
                return

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

        if self.jk.last_update is None or datetime.now() > self.jk.last_update + timedelta(
            minutes=self.config.get_interval()
        ):
            try:
                async with BMS(ble_device=self.jk.device) as bms:
                    data: BMSSample = await bms.async_update()

                    self.jk.voltage     = data['voltage']
                    self.jk.current     = data['current']
                    self.jk.power       = data['power']
                    self.jk.soc         = data['battery_level']
                    self.jk.temperature = data['temperature']

                    self.jk.last_update      = datetime.now()
                    self.jk.missing_updates  = 0

                    capacityAh = self.config.get_battery_capacity()
                    consumed   = capacityAh * (100 - self.jk.soc) / 100
                    ttg        = self.remaining_time_seconds(
                        capacityAh, self.jk.soc, self.jk.current
                    )
                    if consumed > 0:
                        self.jk.hist_last_discharge = consumed

                    GLib.idle_add(self._dbus_commit, {
                        "/Alarms/InternalFailure": 0,
                        "/Dc/0/Voltage":           self.jk.voltage,
                        "/Dc/0/Power":             self.jk.power,
                        "/Dc/0/Current":           self.jk.current,
                        "/Dc/0/Temperature":       self.jk.temperature,
                        "/Soc":                    self.jk.soc,
                        "/TimeToGo":               ttg,
                        "/ConsumedAmphours":       consumed,
                        "/History/LastDischarge":  self.jk.hist_last_discharge,
                    })
                    GLib.idle_add(self._increment_update_index)

                    logging.debug(
                        "BATTERY UPDATED: SOC %s, V %s",
                        self.jk.soc, self.jk.voltage,
                    )

            except Exception as e:
                logging.error(f"Failed to update BMS: {e}")
                self.jk.missing_updates += 1
                if self.jk.missing_updates > 5:
                    self.jk.device = None

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

    def vreg_link_get(self, reg_id):
        if reg_id == JkReg.DC_MONITOR_MODE.value:
            return GenericReg.OK.value, [0xFE]
        elif reg_id == JkReg.VE_REG_BATTERY_CAPACITY.value:
            capacityAh = float(self.config.get_battery_capacity())
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
        return GenericReg.OK.value, data

    def remaining_time_seconds(self, capacity, soc, current_a):
        MIN_CURRENT = 0.1
        if current_a >= -MIN_CURRENT:
            return 864000
        remaining_ah = capacity * (soc / 100.0)
        hours = remaining_ah / abs(current_a)
        return int(hours * 3600)

    def _get_adapter(self) -> str:
        try:
            if self.jk.device is not None:
                path = self.jk.device.details.get("path", "") or str(self.jk.device.details)
                # estrai hciN dal path DBus
                for part in path.split("/"):
                    if part.startswith("hci"):
                        logging.info("BLE adapter found: %s", part)
                        return part
        except Exception as e:
            logging.warning("cannot find BLE adapter: %s", e)
        logging.warning("cannot find BLE adapter, using fallback hci0")
        return "hci0"

    def _restart_ble_hardware_sync(self, adapter: str):
        logging.info("*** Restarting BLE hardware on %s ***", adapter)
        for cmd, label in [
            (["bluetoothctl", "--adapter", adapter, "power", "off"], "power off"),
            (["bluetoothctl", "--adapter", adapter, "power", "on"],  "power on"),
        ]:
            result = subprocess.run(cmd, capture_output=True, text=True)
            logging.info(f"{label} exit code: {result.returncode}")
            logging.info(f"{label} output: {result.stdout}")
            if label == "power off":
                sleep(5)

    async def restart_ble_hardware_and_bluez_driver(self):
        adapter = self._get_adapter()
        await self._async_loop.run_in_executor(
            None, self._restart_ble_hardware_sync, adapter
        )

    def _restart_bluetooth_sync(self, adapter: str):
        logging.warning("*** Tentativo riavvio demone Bluetooth su %s ***", adapter)
        try:
            subprocess.run(['pkill', 'unblock'], timeout=5)
            sleep(5)
            subprocess.run(['hciconfig', adapter, 'reset'], timeout=5)
            sleep(5)
            result = subprocess.run(['bluetoothctl', '--adapter', adapter, 'power', 'on'], timeout=5)
            if result.returncode == 0:
                logging.info("Bluetooth successfuly restarted for %s.", adapter)
                sleep(3)
                return True
            else:
                logging.error(f"Error restarting Bluetooth: {result.stderr}")
                return False
        except Exception as e:
            logging.exception(f"Bluetooth restart exception: {e}")
            return False

    async def restart_bluetooth_service(self):
        adapter = self._get_adapter()
        await self._async_loop.run_in_executor(
            None, self._restart_bluetooth_sync, adapter
        )


def main():
    config = JkConfig()

    level = logging.INFO
    if config.get_debug():
        level = logging.DEBUG
    logging.basicConfig(level=level)
    logging.info(">>>>>>>>>>>>>>>> Jk Monitor Starting <<<<<<<<<<<<<<<<")

    thread.daemon = True

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
            "/History/DeepestDischarge":        {"initial": None},
            "/History/LastDischarge":           {"initial": None},
            "/History/AverageDischarge":        {"initial": None},
            "/History/ChargeCycles":            {"initial": None},
            "/History/FullDischarges":          {"initial": None},
            "/History/TotalAhDrawn":            {"initial": None},
            "/History/MinimumVoltage":          {"initial": None},
            "/History/MaximumVoltage":          {"initial": None},
            "/History/TimeSinceLastFullCharge": {"initial": None},
            "/History/AutomaticSyncs":          {"initial": None},
            "/History/DischargedEnergy":        {"initial": None},
            "/History/ChargedEnergy":           {"initial": None},
        },
        config=config,
    )

    logging.info("Connected to dbus, and switching over to GLib.MainLoop() (= event based)")
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()