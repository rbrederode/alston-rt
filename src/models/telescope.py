# -*- coding: utf-8 -*-
from datetime import datetime

from models.app import AppModel
from models.comms import CommunicationStatus
from models.dsh import DishModel, DishMode, PointingState, Feed, CapabilityStates
from models.dig import DigitiserModel
from models.health import HealthState
from models.sdp import ScienceDataProcessorModel
from models.proc import ProcessorModel
from models.tm import TelescopeManagerModel

# The Telescope Model
class TelescopeModel:
    """A class representing the telescope model, which includes:
        - Telescope Manager, 
        - Dish, 
        - Digitiser, and the
        - Science Data Processor
    """
    def __init__(self):

        self.tm = TelescopeManagerModel(id="tm001")
        self.dsh = DishModel(id="dsh001")
        self.dig = DigitiserModel(id="dig001")
        self.sdp = ScienceDataProcessorModel(id="sdp001")

    def save_to_disk(self):
        # Implement disk saving logic here
        pass

    def load_from_disk(self):
        # Implement disk loading logic here
        pass

    def to_dict(self):
        return {
            "tm": self.tm.to_dict(),
            "dsh": self.dsh.to_dict(),
            "dig": self.dig.to_dict(),
            "sdp": self.sdp.to_dict()
        }

if __name__ == "__main__":
    telescope = TelescopeModel()

    telescope.dsh.mode = DishMode.STARTUP
    telescope.dsh.pointing_state = PointingState.READY
    telescope.dsh.app.health = HealthState.OK
    telescope.dig.gain = 10
    telescope.dig.sample_rate = 240000
    telescope.dig.app.health = HealthState.OK

    telescope.sdp.app.health = HealthState.OK
    telescope.sdp.channels = 1024

    import pprint
    print("="*40)
    print("Telescope Model Initialized")
    print("="*40)
    pprint.pprint(telescope.to_dict())