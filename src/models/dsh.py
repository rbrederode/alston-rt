import enum

class PointingState(enum.IntEnum):
    READY = 0
    SLEW = 1
    TRACK = 2
    SCAN = 3
    UNKNOWN = 4

class DishMode(enum.IntEnum):
    STARTUP = 0
    SHUTDOWN = 1
    STANDBY_LP = 2
    STANDBY_FP = 3
    MAINTENANCE = 4
    STOW = 5
    CONFIG = 6
    OPERATE = 7
    UNKNOWN = 8

class CapabilityStates(enum.IntEnum):
    UNAVAILABLE = 0
    STANDBY = 1
    CONFIGURING = 2
    OPERATE_DEGRADED = 3
    OPERATE_FULL = 4
    UNKNOWN = 5

class Feed(enum.IntEnum):
    NONE = 0
    F1420_H3T = 1 # 1420 MHz 3 Turn Helical Feed
    F1420_H7T = 2 # 1420 MHz 7 Turn Helical Feed
    F400_L = 3 # 400 MHz Loop Feed