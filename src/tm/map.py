from api import tm_dig
from models import dsh

# Map Configuration items to attribute names
_config_to_property = {
    "Feed": tm_dig.PROPERTY_FEED,
    "Sample Rate": tm_dig.PROPERTY_SAMPLE_RATE,
    "Center Frequency": tm_dig.PROPERTY_CENTER_FREQ,
    "Bandwidth": tm_dig.PROPERTY_BANDWIDTH,
    "Frequency Correction": tm_dig.PROPERTY_FREQ_CORRECTION,
    "Gain": tm_dig.PROPERTY_GAIN,
}

_config_to_feed = {
    "NONE": dsh.Feed.NONE,
    "H3T_1420": dsh.Feed.H3T_1420,
    "H7T_1420": dsh.Feed.H7T_1420,
    "LF_400": dsh.Feed.LF_400,
    "LOAD": dsh.Feed.LOAD,
}

def get_property_name(config_item: str) -> str:
    """ Get the property name for a given configuration item.
    """
    return _config_to_property.get(config_item, None)

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
        prop_name = get_property_name(item)
        print(f"Config Item: {item} -> Property Name: {prop_name}")
