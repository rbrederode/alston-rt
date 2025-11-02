from typing import Any

from api import tm_dig
from models import dsh


# Map Configuration items to attribute names
_config_to_property = {
    "Feed": tm_dig.PROPERTY_FEED,
    "Sample_Rate": tm_dig.PROPERTY_SAMPLE_RATE,
    "Center_Freq": tm_dig.PROPERTY_CENTER_FREQ,
    "Bandwidth": tm_dig.PROPERTY_BANDWIDTH,
    "Freq_Correction": tm_dig.PROPERTY_FREQ_CORRECTION,
    "Gain": tm_dig.PROPERTY_GAIN,
    "Streaming": tm_dig.PROPERTY_STREAMING,
}

_config_to_feed = {
    "NONE": dsh.Feed.NONE,
    "H3T_1420": dsh.Feed.H3T_1420,
    "H7T_1420": dsh.Feed.H7T_1420,
    "LF_400": dsh.Feed.LF_400,
    "LOAD": dsh.Feed.LOAD,
}

def get_property_name(config_item: str, value) -> str:
    """ Get the property name for a given configuration item.
    """
    property = _config_to_property.get(config_item, None)
    if property:
        if property == tm_dig.PROPERTY_FEED:

            feed_id = _config_to_feed.get(value, None)
            if feed_id is not None:
                return property, feed_id
            else:
                logger.error(f"Telescope Manager map: invalid FEED value {value} for property {property}")
                return property, None

        elif property == tm_dig.PROPERTY_GAIN:
            if str(value).upper() == "AUTO":
                return property, {"time_in_secs": 0.5}
            else:
                try:
                    return property, int(value)
                except ValueError:
                    logger.error(f"Telescope Manager map: invalid GAIN value {value} for property {property}")
                return property, None

        elif property == tm_dig.PROPERTY_STREAMING:
            if isinstance(value, bool):
                return property, value
            elif str(value).upper() in ["TRUE", "1", "YES", "ON"]:
                return property, True
            elif str(value).upper() in ["FALSE", "0", "NO", "OFF"]:
                return property, False
            else:
                logger.error(f"Telescope Manager map: invalid STREAMING value {value} for property {property}")
                return property, None
  
    return property, value

def get_method_name(config_item: str, value) -> (str, Any):
    """ Get the method name for a given configuration item.
    """
    if config_item is None or value is None:
        return None, None

    if config_item == "Gain" and str(value).upper() == "AUTO":
        return tm_dig.METHOD_GET_AUTO_GAIN, {"time_in_secs": 0.5}
    
    return None, None

def get_feed_id(config_item: str) -> str:
    """ Get the feed ID for a given configuration item.
    """
    return _config_to_feed.get(config_item, None)

if __name__ == "__main__":
    # Test the mapping
    test_items = [
        "Feed",
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
