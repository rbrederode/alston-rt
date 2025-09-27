from ipc.message import APIMessage
from util.timer import Timer
from util.xbase import XSoftwareFailure

import logging
logger = logging.getLogger(__name__)

class Action:

    class Comms:
            
        CONNECT = -2
        DISCONNECT = -3        

        def __init__(self, endpoint_name: str, comms_action: int):
            if endpoint_name is None:
                raise XSoftwareFailure("Action.Comms endpoint_name cannot be None")

            if comms_action is None:
                raise XSoftwareFailure("Action.Comms comms_action cannot be None")

            self.endpoint_name = endpoint_name
            self.comms_action = comms_action
        
        def get_endpoint_name(self):
            return self.endpoint_name
        
        def get_comms_action(self):
            return self.comms_action

        def __str__(self):
            return f"Action.Comms(endpoint_name={self.endpoint_name}, comms_action={self.comms_action})"

    class Timer:

        TIMER_STOP = -1
        TIMER_START = 0  # and above are valid timer actions

        def __init__(self, name:str, timer_action:int, echo_data=None):

            if name is None:
                raise XSoftwareFailure("Action.Timer name cannot be None")

            if timer_action is None:
                raise XSoftwareFailure("Action.Timer timer_action cannot be None")

            self.name = name
            self.timer_action = timer_action
            self.echo_data = echo_data

        def get_name(self):
            return self.name

        def get_timer_action(self):
            return self.timer_action

        def get_echo_data(self):
            return self.echo_data

        def __str__(self):
            return f"Action.Timer(name={self.name}, timer_action={self.timer_action}, echo_data={self.echo_data})"

    #--------------------------
    # Main Action class methods
    #--------------------------

    def __init__(self):
        self.cause = None
        self.msgs_to_remote = []
        self.timer_actions = []
        self.connection_actions = []

    def set_cause(self, cause):
        self.cause = cause

    def set_msg_to_remote(self, msg: APIMessage):
        if msg is not None:
            self.msgs_to_remote.append(msg)
        return self

    def set_connection_action(self, comms_action: "Action.Comms"):
        if comms_action is not None and comms_action.get_comms_action() is not None:
            self.connection_actions.append(comms_action)
        return self

    def set_timer_action(self, timer_action: "Action.Timer"):
        if timer_action is not None and timer_action.get_timer_action() is not None:
            self.timer_actions.append(timer_action)
        return self

    def clear_msgs_to_remote(self):
        self.msgs_to_remote.clear()
        return self

    def clear_timer_actions(self):
        self.timer_actions.clear()
        return self

    def clear_connection_actions(self):
        self.connection_actions.clear()
        return self

    def is_empty(self) -> bool:
        return (len(self.msgs_to_remote) == 0 and
                len(self.timer_actions) == 0 and
                len(self.connection_actions) == 0)

    def __str__(self):
        return f"Action(cause={self.cause}, msgs_to_remote={self.msgs_to_remote}, timer_actions={self.timer_actions}, connection_actions={self.connection_actions})"

        