#!/usr/bin/env python

"""
Created by mebitek in 2026.

Inspired by:
 - https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py (Template)


This code and its documentation can be found on: https://github.com/mebitek/JkMonitor
Used https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py as basis for this service.
Reading information from jk  bms bluetooth via aiobmsble librasries and puts the info on dbus as battery.

"""

import os
import sys
import json
import logging
import dbus
import requests
import _thread as thread
import subprocess
from datetime import datetime, timedelta
import utils
import random
from time import sleep

import asyncio
import logging
from typing import Final

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError

from aiobmsble import BMSSample
from aiobmsble.bms.jikong_bms import BMS 

# add the path to our own packages for import
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

        # jk class
        self.jk = JkBms(config.get_device_name(), 0, 12.8, 0, 0, 0)
        logging.debug("* * * MAC %s", self.jk.name)

        self.jk.device = None


        # dbus service
        self._dbusservice = VeDbusService(servicename, register=False)
        self._paths = paths

        vregtype = lambda *args, **kwargs: VregLinkItem(*args, **kwargs, getvreg=self.vreg_link_get, setvreg=self.vreg_link_set)

        logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

        productname = "Jk BMS " + config.get_model()
        logging.debug("* * * Product name is %s", productname)

        # Create the management objects, as specified in the ccgx dbus-api document
        self._dbusservice.add_path("/Mgmt/ProcessName", __file__)
        self._dbusservice.add_path("/Mgmt/ProcessVersion", config.get_version())
        self._dbusservice.add_path("/Mgmt/Connection", connection)

        # Create the mandatory objects
        self._dbusservice.add_path("/DeviceInstance", deviceinstance)
        # value used in ac_sensor_bridge.cpp of dbus-cgwacs
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


    def _update(self):
        # Eseguiamo la parte asincrona in modo sincrono per compatibilità con GLib
        try:
            asyncio.run(self._async_update_logic())
        except Exception:
            logging.exception("Exception while getting jk bms status")
        
        # Gestione indice dbus
        index = self._dbusservice["/UpdateIndex"] + 1
        self._dbusservice["/UpdateIndex"] = index if index <= 255 else 0
        return True


    async def _async_update_logic(self):
        # 1. Cerca il dispositivo se non lo abbiamo ancora
        if self.jk.device is None:
            logging.info(f"Ricerca BMS con nome: '{target_name}'...")
            
            # Non usare find_device_by_name (è meno affidabile su RPi)
            # Usiamo discover per vedere tutto quello che c'è intorno
            devices = await BleakScanner.discover(timeout=10.0)
            
            for d in devices:
                # Controllo sia sul nome che sul MAC (per sicurezza)
                if d.name and target_name.upper() in d.name.upper():
                    self.jk.device = d
                    logging.info(f"BMS Trovato: {d.name} [{d.address}]")
                    break
            
            if self.jk.device is None:
                logging.warning(f"BMS '{target_name}' non trovato. Dispositivi visti: {[d.name for d in devices if d.name]}")
                return

        # 2. Controllo intervallo
        if self.jk.last_update is None or datetime.now() > self.jk.last_update + timedelta(minutes=self.config.get_interval()):
            try:
                # Qui usiamo la libreria aiobmsble
                async with BMS(ble_device=self.jk.device) as bms:
                    data: BMSSample = await bms.async_update()
                    
                    # Aggiorna i dati locali
                    self.jk.voltage = data.voltage
                    self.jk.current = data.current
                    self.jk.power = data.power
                    self.jk.soc = (data.cycle_charge * 100) / self.config.get_battery_capacity()
                    self.jk.temperature = data.temperatures[0] if data.temperatures else 0
                    
                    self.jk.last_update = datetime.now()
                    self.jk.missing_updates = 0
                    
                    if self.jk.soc < self.config.get_low_soc_alarm_set():
                        self._dbusservice["/Alarms/LowSoc"] = 1
                    if self.jk.soc > self.config.get_low_soc_alarm_clear():
                        self._dbusservice["/Alarms/LowSoc"] = 0

                    self._dbusservice["/Dc/0/Voltage"] = self.jk.voltage  
                    self._dbusservice["/Dc/0/Power"] = -self.jk.power
                    self._dbusservice["/Dc/0/Current"] = self.jk.current
                    time_to_go = self.remaining_time_seconds(self.config.get_battery_capacity(), self.jk.soc, self.jk.current)
                    self._dbusservice["/TimeToGo"] = time_to_go
                    self._dbusservice["/Dc/0/Temperature"] = self.jk.temperature

                    consumed = capacityAh * (100 - self.jk.soc) / 100
                    self._dbusservice["/ConsumedAmphours"] = consumed
                    if consumed > 0:
                        self._dbusservice["/History/LastDischarge"] = consumed
                        self.jk.hist_last_discharge = consumed
                    #     deepest_discharge = VeDbusItemImport(dbus_conn, "com.victronenergy.battery.jkbms", '/History/DeepestDischarge')
                    #     if deepest_discharge.get_value() and deepest_discharge.get_value() < consumed:
                    #         self._dbusservice["/History/DeepestDischarge"] = consumed


                    logging.debug("* * * BATTERY SOC %s", self.jk.soc)
                    logging.debug("* * * BATTERY VOLTAGE %s", self.jk.voltage)
                    logging.debug("* * * CURRENT %s", self.jk.current)
                    logging.debug("* * * DC POWER %s", self.jk.power)
                    
            except Exception as e:
                logging.error(f"Failed to update BMS: {e}")
                self.jk.missing_updates += 1



    def _handlechangedvalue(self, path, value):
        logging.debug("someone else updated %s to %s" % (path, value))
        return True  # accept the change

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
            return GenericReg.OK.value, utils.convert_decimal(0.02) #tail current
        elif reg_id == JkReg.VE_REG_LOW_SOC.value:
            return GenericReg.OK.value, utils.convert_decimal(1) #discharge threshold
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

        seconds = int(hours * 3600)

        return seconds

    def restart_ble_hardware_and_bluez_driver(self):

        logging.info("*** Restarting BLE hardware and Bluez driver ***")

        result = subprocess.run(["bluetoothctl", "power", "off"], capture_output=True, text=True)
        logging.info(f"power off exit code: {result.returncode}")
        logging.info(f"power off output: {result.stdout}")

        result = subprocess.run(["bluetoothctl", "power", "on"], capture_output=True, text=True)
        logging.info(f"power on exit code: {result.returncode}")
        logging.info(f"power on output: {result.stdout}")


    def restart_bluetooth_service(self):
        logging.warning("*** Tentativo riavvio demone Bluetooth ***")
        try:
            # 1. Killiamo eventuali processi bluetti rimasti appesi ( zombie )
            # Questo è importante perché potrebbero trattenere il socket
            subprocess.run(['pkill', 'unblock', 'all'], timeout=5)

            sleep(5)

            subprocess.run(['hciconfig','hci0', 'reset'], timeout=5)

            sleep(5)
            
            # 2. Riavviamo il servizio bluetooth usando systemctl
            # Venus OS su RPi usa systemd
            result = subprocess.run(["systemctl", "restart", "bluetooth"], capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                logging.info("Servizio Bluetooth riavviato con successo.")
                # Diamo un attimo al driver per reinsediarsi
                sleep(3)
                return True
            else:
                logging.error(f"Errore systemctl: {result.stderr}")
                return False
                
        except Exception as e:
            logging.exception(f"Eccezione durante riavvio bluetooth: {e}")
            return False
        

def main():
    config = JkConfig()


    # set logging level to include info level entries
    level = logging.INFO
    if config.get_debug():
        level = logging.DEBUG
    logging.basicConfig(level=level)
    logging.info(">>>>>>>>>>>>>>>> Jk Monitor Starting <<<<<<<<<<<<<<<<")

    thread.daemon = True  # allow the program to quit

    from dbus.mainloop.glib import DBusGMainLoop

    # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
    DBusGMainLoop(set_as_default=True)

    pvac_output = JkMonitorService(
        servicename="com.victronenergy.battery.jkbms",
        deviceinstance=295,
        paths={
            "/Dc/0/Voltage": {"initial": 0},
            "/Dc/0/Current": {"initial": 0},
            "/Dc/0/Power": {"initial": 0},
            "/Soc": {"initial": 0},
            "/UpdateIndex": {"initial": 0},
            "/Capacity": {"initial": config.get_battery_capacity()},
            "/TimeToGo": {"initial": 0},
            "/ConsumedAmphours": {"initial": 0},
            "/Dc/0/Temperature": {"initial": 0},
            "/Settings/HasTemperature": {"initial": 1},

            "/Settings/MonitorMode": {"initial": 0},
            "/Alarms/LowSoc": {"initial": 0},
            "/Alarms/InternalFailure": {"initial": 0},

            "/History/DeepestDischarge": {"initial": None}, 
            "/History/LastDischarge": {"initial": None}, 
            "/History/AverageDischarge": {"initial": None}, 
            "/History/ChargeCycles": {"initial": None}, 
            "/History/FullDischarges": {"initial": None}, 
            "/History/TotalAhDrawn": {"initial": None}, 
            "/History/MinimumVoltage": {"initial": None}, 
            "/History/MaximumVoltage": {"initial": None}, 
            "/History/TimeSinceLastFullCharge": {"initial": None}, 
            "/History/AutomaticSyncs": {"initial": None}, 
            "/History/DischargedEnergy": {"initial": None}, 
            "/History/ChargedEnergy": {"initial": None}

        },
        config=config,
    )

    logging.info(
        "Connected to dbus, and switching over to GLib.MainLoop() (= event based)"
    )
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
