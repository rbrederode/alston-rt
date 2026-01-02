import logging
from typing import Any

from api import tm_dig, tm_sdp
from models import dsh

logger = logging.getLogger(__name__)

# Map Configuration items to attribute names
_config_to_property = {
    "load":             tm_dig.PROPERTY_LOAD,
    "sample_rate":      tm_dig.PROPERTY_SAMPLE_RATE,
    "center_freq":      tm_dig.PROPERTY_CENTER_FREQ,
    "bandwidth":        tm_dig.PROPERTY_BANDWIDTH,
    "freq_correction":  tm_dig.PROPERTY_FREQ_CORRECTION,
    "gain":             tm_dig.PROPERTY_GAIN,
    "scanning":         tm_dig.PROPERTY_SCANNING,
    "channels":         tm_dig.PROPERTY_CHANNELS,
    "scan_duration":    tm_dig.PROPERTY_SCAN_DURATION,
}

def get_property_name_value(config_item: str, value) -> (str, Any):
    """ Get the property name for a given configuration item.
    """
    property = _config_to_property.get(config_item, None)

    # If property is found, map the value accordingly
    if property:
        if property == tm_dig.PROPERTY_LOAD:

            if isinstance(value, bool):
                return property, value
            elif str(value).upper() in ["TRUE", "1", "YES", "ON"]:
                return property, True
            elif str(value).upper() in ["FALSE", "0", "NO", "OFF"]:
                return property, False
            else:
                logger.error(f"Telescope Manager map: invalid LOAD value {value} for property {property}")
                return property, None

        elif property == tm_dig.PROPERTY_GAIN:
            if str(value).upper() == "AUTO":
                return property, {"time_in_secs": 0.5}
            else:
                try:
                    return property, float(value)
                except ValueError:
                    logger.error(f"Telescope Manager map: invalid GAIN value {value} for property {property}")
                return property, None

        elif property == tm_dig.PROPERTY_SCANNING:
            if isinstance(value, bool):
                return property, value
            elif isinstance(value, dict):
                return property, value
            elif str(value).upper() in ["TRUE", "1", "YES", "ON"]:
                return property, True
            elif str(value).upper() in ["FALSE", "0", "NO", "OFF"]:
                return property, False
            else:
                logger.error(f"Telescope Manager map: invalid SCANNING value {value} for property {property}")
                return property, None

        elif property in [
            tm_dig.PROPERTY_SAMPLE_RATE,
            tm_dig.PROPERTY_CENTER_FREQ,
            tm_dig.PROPERTY_BANDWIDTH,
            tm_dig.PROPERTY_FREQ_CORRECTION,
            tm_dig.PROPERTY_CHANNELS,
            tm_dig.PROPERTY_SCAN_DURATION
        ]:
            try:
                # These properties expect numeric values
                if property in [tm_dig.PROPERTY_FREQ_CORRECTION, tm_dig.PROPERTY_CHANNELS, tm_dig.PROPERTY_SCAN_DURATION]:
                    return property, int(value)
                else:
                    return property, float(value)
            except ValueError:
                logger.error(f"Telescope Manager map: invalid numeric value {value} for property {property}")
                return property, None
  
    return property, value

def get_method_name_value(config_item: str, value) -> (str, Any):
    """ Get the method name for a given configuration item.
    """
    if config_item is None or value is None:
        return None, None

    if config_item == "gain" and str(value).upper() == "AUTO":
        return tm_dig.METHOD_GET_AUTO_GAIN, {"time_in_secs": 0.5}
    
    return None, None

if __name__ == "__main__":
    # Test the mapping
    test_items = [
        "Load",
        "Sample Rate",
        "Center Frequency",
        "Bandwidth",
        "Frequency Correction",
        "Gain",
        "Unknown Item"
    ]

    for item in test_items:
        prop_name = get_property_name(item, 10)
        print(f"Config Item: {item} -> Property Name: {prop_name}")
