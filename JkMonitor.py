#!/usr/bin/env python

"""
Created by mebitek in 2026.

Inspired by:
 - https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py

https://github.com/mebitek/JkMonitor
Legge dati dal JK BMS via Bluetooth (aiobmsble) e li pubblica su dbus come batteria Venus OS.
"""

import os
import sys
import logging
import subprocess
import asyncio
import threading
import _thread as thread
from datetime import datetime, timedelta
from time import sleep

import dbus
from bleak import BleakScanner
from bleak.exc import BleakError
from aiobmsble import BMSSample
from aiobmsble.bms.jikong_bms import BMS

import utils

sys.path.insert(1, "/data/SetupHelper/velib_python")

from vedbus import VeDbusService, VeDbusItemImport
from gi.repository import GLib
from vreg_link_item import VregLinkItem, GenericReg, JkReg
from jk_config import JkConfig


# ---------------------------------------------------------------------------
# Costanti di configurazione
# ---------------------------------------------------------------------------
UPDATE_INTERVAL_MS       = 1000   # ms tra un polling GLib e il successivo
BLE_SCAN_TIMEOUT_S       = 10.0   # timeout scansione BLE
BLE_READ_TIMEOUT_S       = 10.0   # timeout lettura dati BMS
MISSING_WARN_THRESHOLD   = 5      # reset device dopo N fallimenti consecutivi
MISSING_ALARM1_THRESHOLD = 10     # allarme livello 1 + restart bluetooth
MISSING_ALARM2_THRESHOLD = 20     # allarme livello 2 + restart hardware BLE
MIN_DISCHARGE_CURRENT    = -0.1   # A — soglia per considerarsi in scarica
TTG_NO_DISCHARGE         = 864000 # secondi TimeToGo quando non in scarica (~10 giorni)


# ---------------------------------------------------------------------------
# Stato BMS
# ---------------------------------------------------------------------------
class JkBms:
    """Contenitore dati BMS. Nessuna logica, solo stato."""

    def __init__(self, name: str):
        self.name                              = name
        self.voltage                           = 12.8
        self.current                           = 0.0
        self.power                             = 0.0
        self.temperature                       = 0.0
        self.soc                               = 0        # %
        self.hist_last_discharge: float | None = None
        self.last_update: datetime | None      = None
        self.missing_updates                   = 0
        self.device                            = None     # BLEDevice


# ---------------------------------------------------------------------------
# Servizio Venus OS
# ---------------------------------------------------------------------------
class JkMonitorService:

    def __init__(
        self,
        servicename: str,
        deviceinstance: int,
        paths: dict,
        connection: str = "Bluetooth",
        config: JkConfig | None = None,
    ):
        self.config = config or JkConfig()

        # stato BMS
        self.jk = JkBms(name=self.config.get_device_name())
        logging.debug("BMS device name: %s", self.jk.name)

        # ----------------------------------------------------------------
        # Setup dbus
        # ----------------------------------------------------------------
        self._dbusservice = VeDbusService(servicename, register=False)
        self._paths       = paths

        vregtype = lambda *a, **kw: VregLinkItem(
            *a, **kw,
            getvreg=self.vreg_link_get,
            setvreg=self.vreg_link_set,
        )

        productname = f"Jk BMS {self.config.get_model()}"
        logging.info("Product name: %s  DeviceInstance: %d", productname, deviceinstance)

        # oggetti di gestione
        self._dbusservice.add_path("/Mgmt/ProcessName",    __file__)
        self._dbusservice.add_path("/Mgmt/ProcessVersion", self.config.get_version())
        self._dbusservice.add_path("/Mgmt/Connection",     connection)

        # oggetti obbligatori
        self._dbusservice.add_path("/DeviceInstance",  deviceinstance)
        self._dbusservice.add_path("/ProductId",       0xA383)
        self._dbusservice.add_path("/ProductName",     productname)
        self._dbusservice.add_path("/DeviceName",      productname)
        self._dbusservice.add_path("/FirmwareVersion", 0x0419)
        self._dbusservice.add_path("/HardwareVersion", 8)
        self._dbusservice.add_path("/Connected",       1)
        self._dbusservice.add_path("/Serial",          self.config.get_serial())

        # sub-device
        self._dbusservice.add_path("/Devices/0/CustomName",      productname)
        self._dbusservice.add_path("/Devices/0/DeviceInstance",  deviceinstance)
        self._dbusservice.add_path("/Devices/0/FirmwareVersion", 0x0419)
        self._dbusservice.add_path("/Devices/0/ProductId",       0xA383)
        self._dbusservice.add_path("/Devices/0/ProductName",     productname)
        self._dbusservice.add_path("/Devices/0/ServiceName",     servicename)
        self._dbusservice.add_path("/Devices/0/Serial",          self.config.get_serial())
        self._dbusservice.add_path("/Devices/0/VregLink",        None, itemtype=vregtype)

        # path dinamici
        for path, settings in self._paths.items():
            self._dbusservice.add_path(
                path,
                settings["initial"],
                writeable=True,
                onchangecallback=self._handlechangedvalue,
            )

        self._dbusservice.register()

        # ----------------------------------------------------------------
        # Thread-safety:
        # _is_updating  → flag atomico, impedisce sovrapposizioni di update.
        # _async_loop   → event loop asyncio dedicato nel thread _loop_thread.
        #                 Tutto il codice BLE vive qui, mai nel thread GLib.
        # Scritture dbus → sempre via GLib.idle_add() per restare nel thread
        #                  GLib che possiede il bus dbus.
        # ----------------------------------------------------------------
        self._is_updating = False
        self._async_loop  = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_async_loop, daemon=True, name="jk-async-loop"
        )
        self._loop_thread.start()

        # avvia il polling GLib (solo scheduling, non blocca mai)
        GLib.timeout_add(UPDATE_INTERVAL_MS, self._schedule_update)

    # ------------------------------------------------------------------
    # Thread loop asyncio dedicato
    # ------------------------------------------------------------------
    def _run_async_loop(self):
        """Gira per sempre nel thread dedicato. Mai chiamare direttamente."""
        asyncio.set_event_loop(self._async_loop)
        self._async_loop.run_forever()

    # ------------------------------------------------------------------
    # Scheduling update (chiamato dal thread GLib ogni UPDATE_INTERVAL_MS)
    # ------------------------------------------------------------------
    def _schedule_update(self) -> bool:
        """
        Chiamato da GLib ogni secondo nel thread GLib.
        Non fa nulla di asincrono: si limita a schedulare la coroutine
        sull'event loop del thread dedicato e torna immediatamente,
        lasciando GLib libero.
        Se un update è già in corso (flag _is_updating), salta il ciclo.
        """
        if self._is_updating:
            logging.debug("Update già in corso, ciclo saltato.")
            return True  # mantieni il timer

        self._is_updating = True
        future = asyncio.run_coroutine_threadsafe(
            self._async_update(), self._async_loop
        )
        # Callback eseguito nel thread del loop quando la coroutine finisce.
        # Resetta il flag così il prossimo ciclo GLib potrà partire.
        future.add_done_callback(self._on_update_done)
        return True  # mantieni il timer

    def _on_update_done(self, future):
        """Chiamato nel thread asyncio al termine della coroutine."""
        try:
            future.result()  # rilancia eventuali eccezioni non gestite
        except Exception:
            logging.exception("Eccezione non gestita nell'update asincrono")
        finally:
            self._is_updating = False

    # ------------------------------------------------------------------
    # Logica asincrona principale
    # ------------------------------------------------------------------
    async def _async_update(self):
        # 1. Cerca il device BLE solo se non già trovato
        if self.jk.device is None:
            await self._scan_device()
            if self.jk.device is None:
                return  # riprova al prossimo ciclo

        # 2. Gestione allarmi progressiva in base ai fallimenti
        self._handle_alarms()

        # 3. Rispetta l'intervallo configurato tra le letture
        interval = timedelta(minutes=self.config.get_interval())
        if self.jk.last_update is not None and datetime.now() < self.jk.last_update + interval:
            return  # non è ancora il momento di leggere

        # 4. Lettura dati BMS
        await self._read_bms()

    # ------------------------------------------------------------------
    # Scansione BLE
    # ------------------------------------------------------------------
    async def _scan_device(self):
        logging.info("Ricerca device BLE: '%s' ...", self.jk.name)
        try:
            device = await BleakScanner.find_device_by_name(
                self.jk.name, timeout=BLE_SCAN_TIMEOUT_S
            )
            if device:
                self.jk.device = device
                logging.info("Device trovato: %s", device.address)
            else:
                logging.warning("Device '%s' non trovato, riprovo al prossimo ciclo.", self.jk.name)
        except BleakError as e:
            logging.error("Errore BleakScanner: %s — riavvio HW BLE", e)
            self.restart_ble_hardware_and_bluez_driver()
        except Exception as e:
            logging.error("Errore imprevisto durante la scansione: %s", e)

    # ------------------------------------------------------------------
    # Helper thread-safe per scrivere sul dbus
    # ------------------------------------------------------------------
    def _dbus_set(self, path: str, value) -> None:
        """
        Schedula la scrittura sul dbus nel thread GLib tramite idle_add.
        Può essere chiamato da qualsiasi thread in sicurezza.
        """
        GLib.idle_add(self._dbus_set_now, path, value)

    def _dbus_set_now(self, path: str, value) -> bool:
        """Eseguito nel thread GLib. Ritorna False per idle_add (one-shot)."""
        self._dbusservice[path] = value
        return False

    # ------------------------------------------------------------------
    # Gestione allarmi progressiva
    # ------------------------------------------------------------------
    def _handle_alarms(self):
        n = self.jk.missing_updates

        if n > MISSING_ALARM2_THRESHOLD:
            logging.error("Troppi aggiornamenti mancanti (%d): allarme 2 + restart HW BLE", n)
            self._dbus_set("/Alarms/InternalFailure", 2)
            self.restart_ble_hardware_and_bluez_driver()

        elif n > MISSING_ALARM1_THRESHOLD:
            logging.warning("Aggiornamenti mancanti (%d): allarme 1 + restart Bluetooth", n)
            self._dbus_set("/Alarms/InternalFailure", 1)
            self.restart_bluetooth_service()

        # dopo molti fallimenti, azzera il device per forzare una nuova scansione
        if n > MISSING_WARN_THRESHOLD:
            logging.warning("Reset device BLE dopo %d fallimenti consecutivi.", n)
            self.jk.device = None

    # ------------------------------------------------------------------
    # Lettura dati BMS
    # ------------------------------------------------------------------
    async def _read_bms(self):
        try:
            async with BMS(ble_device=self.jk.device) as bms:
                data: BMSSample = await asyncio.wait_for(
                    bms.async_update(), timeout=BLE_READ_TIMEOUT_S
                )

            # aggiornamento stato interno
            self.jk.voltage     = float(data["voltage"])
            self.jk.current     = float(data["current"])
            self.jk.power       = float(data["power"])
            self.jk.soc         = int(data["battery_level"])
            self.jk.temperature = float(data["temperature"])
            self.jk.last_update = datetime.now()
            self.jk.missing_updates = 0

            # calcoli derivati
            capacity_ah = float(self.config.get_battery_capacity())
            consumed_ah = capacity_ah * (100 - self.jk.soc) / 100.0
            ttg         = self._calc_time_to_go(capacity_ah, self.jk.soc, self.jk.current)

            # traccia l'ultimo consumo positivo per lo storico
            if consumed_ah > 0:
                self.jk.hist_last_discharge = consumed_ah

            # aggiornamento dbus (thread-safe: via GLib.idle_add)
            self._dbus_set("/Alarms/InternalFailure", 0)
            self._dbus_set("/Dc/0/Voltage",           self.jk.voltage)
            self._dbus_set("/Dc/0/Current",           self.jk.current)
            self._dbus_set("/Dc/0/Power",             self.jk.power)
            self._dbus_set("/Dc/0/Temperature",       self.jk.temperature)
            self._dbus_set("/Soc",                    self.jk.soc)
            self._dbus_set("/TimeToGo",               ttg)
            self._dbus_set("/ConsumedAmphours",       consumed_ah)
            self._dbus_set("/History/LastDischarge",  self.jk.hist_last_discharge)

            # UpdateIndex: contatore ciclico 0-255, letto e scritto nel thread GLib
            GLib.idle_add(self._increment_update_index)

            logging.debug(
                "BMS aggiornato — SOC: %d%%  V: %.2fV  I: %.2fA  T: %.1f°C  TTG: %ds",
                self.jk.soc, self.jk.voltage, self.jk.current,
                self.jk.temperature, ttg,
            )

        except asyncio.TimeoutError:
            logging.error("Timeout durante la lettura del BMS.")
            self._on_read_failure()
        except BleakError as e:
            logging.error("Errore BLE durante la lettura: %s", e)
            self._on_read_failure()
        except KeyError as e:
            logging.error("Campo mancante nei dati BMS: %s", e)
            self._on_read_failure()
        except Exception as e:
            logging.exception("Errore imprevisto durante la lettura BMS: %s", e)
            self._on_read_failure()

    def _on_read_failure(self):
        """Incrementa il contatore fallimenti e invalida il device se necessario."""
        self.jk.missing_updates += 1
        logging.warning("Fallimenti consecutivi: %d", self.jk.missing_updates)
        if self.jk.missing_updates > MISSING_WARN_THRESHOLD:
            self.jk.device = None  # forza nuova scansione al prossimo ciclo

    def _increment_update_index(self) -> bool:
        """Eseguito nel thread GLib (one-shot via idle_add). Legge e scrive UpdateIndex."""
        idx = self._dbusservice["/UpdateIndex"]
        self._dbusservice["/UpdateIndex"] = (idx + 1) if idx < 255 else 0
        return False  # one-shot

    # ------------------------------------------------------------------
    # Calcolo TimeToGo
    # ------------------------------------------------------------------
    @staticmethod
    def _calc_time_to_go(capacity_ah: float, soc: int, current_a: float) -> int:
        """
        Ritorna i secondi rimanenti alla scarica completa.
        Se la corrente e' sopra la soglia (carica o idle), ritorna TTG_NO_DISCHARGE.
        """
        if current_a >= MIN_DISCHARGE_CURRENT:
            return TTG_NO_DISCHARGE
        remaining_ah = capacity_ah * (soc / 100.0)
        hours        = remaining_ah / abs(current_a)
        return int(hours * 3600)

    # ------------------------------------------------------------------
    # Callback dbus
    # ------------------------------------------------------------------
    def _handlechangedvalue(self, path, value):
        logging.debug("Valore aggiornato da esterno: %s = %s", path, value)
        return True

    # ------------------------------------------------------------------
    # VregLink GET
    # ------------------------------------------------------------------
    def vreg_link_get(self, reg_id):
        capacity_ah = float(self.config.get_battery_capacity())

        reg_map = {
            JkReg.DC_MONITOR_MODE.value:             (GenericReg.OK.value, [0xFE]),
            JkReg.VE_REG_BATTERY_CAPACITY.value:     (GenericReg.OK.value, utils.convert_decimal(capacity_ah)),
            JkReg.VE_REG_CHARGED_VOLTAGE.value:      (GenericReg.OK.value, utils.convert_decimal(1.36)),
            JkReg.VE_REG_PEUKERT_COEFFICIENT.value:  (GenericReg.OK.value, utils.convert_decimal(1.01)),
            JkReg.VE_REG_CHARGE_DETECTION_TIME.value:(GenericReg.OK.value, utils.convert_decimal(0.03)),
            JkReg.VE_REG_CHARGE_EFFICIENCY.value:    (GenericReg.OK.value, utils.convert_decimal(0.98)),
            JkReg.VE_REG_CURRENT_THRESHOLD.value:    (GenericReg.OK.value, utils.convert_decimal(0.1)),
            JkReg.VE_REG_CHARGED_CURRENT.value:      (GenericReg.OK.value, utils.convert_decimal(0.02)),
            JkReg.VE_REG_LOW_SOC.value:              (GenericReg.OK.value, utils.convert_decimal(1)),
            JkReg.VE_REG_HIST_LAST_DISCHARGE.value:  (GenericReg.OK.value, utils.convert_decimal(self.jk.hist_last_discharge or 0.0)),
        }

        result = reg_map.get(reg_id)
        if result:
            return result

        logging.debug("vreg_link_get: reg_id sconosciuto %s", reg_id)
        return GenericReg.OK.value, []

    # ------------------------------------------------------------------
    # VregLink SET
    # ------------------------------------------------------------------
    def vreg_link_set(self, reg_id, data):
        handlers = {
            JkReg.VE_REG_BATTERY_CAPACITY.value: ("Setup", "BatteryCapacity"),
            JkReg.VE_REG_LOW_SOC.value:           ("Setup", "LowSocAlarmSet"),
            JkReg.VE_REG_LOW_SOC_CLEAR.value:     ("Setup", "LowSocAlarmClear"),
        }
        if reg_id in handlers:
            section, key = handlers[reg_id]
            decimal = utils.convert_to_decimal(bytearray(data))
            self.config.write_to_config(decimal, section, key)
            logging.debug("vreg_link_set: %s/%s = %s", section, key, decimal)
        else:
            logging.debug("vreg_link_set: reg_id sconosciuto %s", reg_id)
        return GenericReg.OK.value, data

    # ------------------------------------------------------------------
    # Utilities Bluetooth
    # ------------------------------------------------------------------
    def restart_ble_hardware_and_bluez_driver(self):
        """Spegne e riaccende l'hardware BLE tramite bluetoothctl."""
        logging.info("Riavvio hardware BLE...")
        for cmd, label in [
            (["bluetoothctl", "power", "off"], "power off"),
            (["bluetoothctl", "power", "on"],  "power on"),
        ]:
            result = subprocess.run(cmd, capture_output=True, text=True)
            logging.info("%s — exit: %d  output: %s", label, result.returncode, result.stdout.strip())
            if label == "power off":
                sleep(5)

    def restart_bluetooth_service(self):
        """Resetta il driver hci0 e riabilita il Bluetooth."""
        logging.warning("Riavvio demone Bluetooth...")
        try:
            subprocess.run(["pkill", "unblock"], timeout=5)
            sleep(5)
            subprocess.run(["hciconfig", "hci0", "reset"], timeout=5)
            sleep(5)
            result = subprocess.run(["bluetoothctl", "power", "on"], timeout=5)
            if result.returncode == 0:
                logging.info("Bluetooth riavviato con successo.")
                sleep(3)
                return True
            else:
                logging.error("Errore riavvio Bluetooth: %s", result.stderr)
                return False
        except Exception:
            logging.exception("Eccezione durante riavvio Bluetooth")
            return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    config = JkConfig()

    logging.basicConfig(level=logging.DEBUG if config.get_debug() else logging.INFO)
    logging.info(">>>>>>>>>>>>>>>> Jk Monitor Starting <<<<<<<<<<<<<<<<")

    thread.daemon = True

    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)

    JkMonitorService(
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

    logging.info("Connesso al dbus — avvio GLib.MainLoop()")
    GLib.MainLoop().run()


if __name__ == "__main__":
    main()