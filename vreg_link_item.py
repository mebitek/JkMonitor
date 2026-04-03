from enum import Enum

from vedbus import VeDbusItemExport
import dbus


class VregLinkItem(VeDbusItemExport):
    def __init__(self, *args, getvreg=None, setvreg=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.getvreg = getvreg
        self.setvreg = setvreg

    @dbus.service.method('com.victronenergy.VregLink',
                         in_signature='q', out_signature='qay')
    def GetVreg(self, regid):
        return self.getvreg(int(regid))

    @dbus.service.method('com.victronenergy.VregLink',
                         in_signature='qay', out_signature='qay')
    def SetVreg(self, regid, data):
        return self.setvreg(int(regid), bytes(data))


class GenericReg(Enum):
    OK = 0x0000

class JkReg(Enum):
    DC_MONITOR_MODE = 0xEEB8
    VE_REG_BATTERY_CAPACITY = 0x1000
    VE_REG_CHARGED_VOLTAGE = 0x1001
    VE_REG_CHARGED_CURRENT = 0x1002
    VE_REG_CHARGE_DETECTION_TIME = 0x1003
    VE_REG_CHARGE_EFFICIENCY = 0x1004
    VE_REG_PEUKERT_COEFFICIENT = 0x1005
    VE_REG_CURRENT_THRESHOLD = 0x1006

    VE_REG_LOW_SOC = 0x1008

    VE_REG_HIST_DEEPEST_DISCHARGE = 0x0300
    VE_REG_HIST_LAST_DISCHARGE = 0x0301
