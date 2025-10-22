import enum

class ObsState(enum.IntEnum):
    """Python enumerated type for observing state."""

    EMPTY = 0
    """The sub-array has no resources allocated and is unconfigured."""

    RESOURCING = 1
    """
    Resources are being allocated to, or deallocated from, the subarray.

    In normal science operations these will be the resources required
    for the upcoming SBI execution.

    This may be a complete de/allocation, or it may be incremental. In
    both cases it is a transient state; when the resourcing operation
    completes, the subarray will automatically transition to EMPTY or
    IDLE, according to whether the subarray ended up having resources or
    not.

    For some subsystems this may be a very brief state if resourcing is
    a quick activity.
    """

    IDLE = 2
    """The subarray has resources allocated but is unconfigured."""

    CONFIGURING = 3
    """
    The subarray is being configured for an observation.

    This is a transient state; the subarray will automatically
    transition to READY when configuring completes normally.
    """

    READY = 4
    """
    The subarray is fully prepared to scan, but is not scanning.

    It may be tracked, but it is not moving in the observed coordinate
    system, nor is it taking data.
    """

    SCANNING = 5
    """
    The subarray is scanning.

    It is taking data and, if needed, all components are synchronously
    moving in the observed coordinate system.

    Any changes to the sub-systems are happening automatically (this
    allows for a scan to cover the case where the phase centre is moved
    in a pre-defined pattern).
    """

    ABORTING = 6
    """The subarray has been interrupted and is aborting what it was doing."""

    ABORTED = 7
    """The subarray is in an aborted state."""

    RESETTING = 8
    """The subarray device is resetting to a base (EMPTY or IDLE) state."""

    FAULT = 9
    """The subarray has detected an error in its observing state."""

    RESTARTING = 10
    """
    The subarray device is restarting.

    After restarting, the subarray will return to EMPTY state, with no
    allocated resources and no configuration defined.
    """
