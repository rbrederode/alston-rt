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
        self.dishes = [DishModel(id="dsh001")]  # List of dish models
        self.digitisers = [DigitiserModel(id="dig001")]  # List of digitiser models
        self.sdp = ScienceDataProcessorModel(id="sdp001")
    
    # Backward-compatible properties for single dish/digitiser access
    @property
    def dsh(self):
        """Returns the first dish (for backward compatibility)."""
        return self.dishes[0] if self.dishes else None
    
    @dsh.setter
    def dsh(self, value):
        """Sets the first dish (for backward compatibility)."""
        if self.dishes:
            self.dishes[0] = value
        else:
            self.dishes.append(value)
    
    @property
    def dig(self):
        """Returns the first digitiser (for backward compatibility)."""
        return self.digitisers[0] if self.digitisers else None
    
    @dig.setter
    def dig(self, value):
        """Sets the first digitiser (for backward compatibility)."""
        if self.digitisers:
            self.digitisers[0] = value
        else:
            self.digitisers.append(value)
    
    def add_dish(self, dish_id: str = None) -> DishModel:
        """Add a new dish to the telescope and return it."""
        dish_id = dish_id or f"dsh{len(self.dishes)+1:03d}"
        dish = DishModel(id=dish_id)
        self.dishes.append(dish)
        return dish
    
    def add_digitiser(self, dig_id: str = None) -> DigitiserModel:
        """Add a new digitiser to the telescope and return it."""
        dig_id = dig_id or f"dig{len(self.digitisers)+1:03d}"
        digitiser = DigitiserModel(id=dig_id)
        self.digitisers.append(digitiser)
        return digitiser
    
    def get_dish(self, dish_id: str) -> DishModel:
        """Get a dish by its ID."""
        for dish in self.dishes:
            if dish.id == dish_id:
                return dish
        return None
    
    def get_digitiser(self, dig_id: str) -> DigitiserModel:
        """Get a digitiser by its ID."""
        for dig in self.digitisers:
            if dig.id == dig_id:
                return dig
        return None

    def save_to_disk(self):
        # Implement disk saving logic here
        pass

    def load_from_disk(self):
        # Implement disk loading logic here
        pass

    def to_dict(self):
        return {
            "tm": self.tm.to_dict(),
            "dishes": [dish.to_dict() for dish in self.dishes],
            "digitisers": [dig.to_dict() for dig in self.digitisers],
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