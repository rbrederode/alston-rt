from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING

import logging
logger = logging.getLogger(__name__)

def dict_diff(old_dict, new_dict):
    """
    Compare two dictionaries and determine which keys are:
      - added
      - removed
      - updated (same key exists, but value changed)

    Handles old_dict = None gracefully.
    """

    # If old_dict is None, treat it as empty
    if old_dict is None:
        old_dict = {}

    if new_dict is None:
        new_dict = {}

    # Compute key sets
    old_keys = set(old_dict.keys())
    new_keys = set(new_dict.keys())

    added = new_keys - old_keys
    removed = old_keys - new_keys

    updated = {
        key for key in (old_keys & new_keys)
        if old_dict[key] != new_dict[key]
    }

    return {
        "added": {key: new_dict[key] for key in added},
        "removed": {key: old_dict[key] for key in removed},
        "updated": {
            key: {"old": old_dict[key], "new": new_dict[key]}
            for key in updated
        }
    }

def gen_file_prefix(
    dt:datetime,
    load:bool,
    gain:float,
    duration:int,
    sample_rate:float,
    center_freq:float,
    channels:int,
    entity_id:int = None,
    filetype: str = None) -> str:

    """ Generate a filename prefix based on metadata parameters.
        :param dt: The datetime object representing the entity start time
        :param load: The load flag e.g. True or False
        :param gain: The gain setting e.g. 39.6 dB
        :param duration: The duration in seconds
        :param sample_rate: The sample rate e.g. 2.4e6 Hz
        :param center_freq: The center frequency e.g. 1.42e9 Hz
        :param channels: The number of channels e.g. 1024
        :param entity_id: The entity ID 
        :param filetype: The type of file being generated (e.g., "raw", "spr", "meta")
        :returns: A string representing the file prefix
    """

    return dt.strftime("%Y-%m-%dT%H%M%S") + \
        "-l" + str(load) + \
        "-g" + str(gain) + \
        "-du" + str(duration) + \
        "-bw" + str(round(sample_rate/1e6,2)) + \
        "-cf" + str(round(center_freq/1e6,2)) + \
        "-ch" + str(channels) + \
        ("-id" + str(entity_id) if entity_id is not None else "") + \
        ("-" + filetype if filetype is not None else '')

def find_json_object_end(data:bytes) -> int:
    """ Finds the end index of the first complete JSON object in the byte stream.
        Returns the index of the byte after the closing brace of the JSON object,
        or -1 if no complete JSON object is found.
    """
    # data is bytes
    open_braces = 0
    in_string = False
    escape = False
    for i, b in enumerate(data):
        c = chr(b)
        if c == '"' and not escape:
            in_string = not in_string
        if not in_string:
            if c == '{':
                open_braces += 1
            elif c == '}':
                open_braces -= 1
                if open_braces == 0:
                    return i + 1  # End index is after this brace
        escape = (c == '\\' and not escape)
    return -1  # Not found


# --- Pytest test functions below ---

def test_find_json_object_end_simple():
    test_data = b'{"key1": "value1"}{"key2": "value2"}'
    end_index = find_json_object_end(test_data)
    assert end_index == 18
    assert test_data[:end_index].decode('utf-8') == '{"key1": "value1"}'

def test_find_json_object_end_empty():
    test_data = b''
    end_index = find_json_object_end(test_data)
    assert end_index == -1

def test_find_json_object_end_invalid():
    test_data = b'{"key1": "value1", "key2": {"nestedKey": "nestedValue" "missingComma" "oops"}} extra data'
    end_index = find_json_object_end(test_data)
    assert end_index == 78

def test_find_json_object_end_with_strings():
    test_data = b'{"key1": "value with } brace", "key2": {"nestedKey": "nestedValue"}} extra data'
    end_index = find_json_object_end(test_data)
    assert end_index == 68
    assert test_data[:end_index].decode('utf-8') == '{"key1": "value with } brace", "key2": {"nestedKey": "nestedValue"}}'

def test_find_json_object_end_with_sdp_msg():
    test_data = b'{"api_version": "1.0", "payload_length": 38400000, "to": "sdp", "from": "dig", "api_call": {"msg_type": "adv", "action_code": "samples", "metadata": [{"property": "center_freq", "value": 1420400000.0}, {"property": "sample_rate", "value": 2400000.0}, {"property": "bandwidth", "value": 2000000.0}, {"property": "gain", "value": 40}, {"property": "timestamp", "value": "2025-09-20T12:30:18.035666+00:00"}]}}RRRRRR'
    end_index = find_json_object_end(test_data)
    assert end_index == 406
    assert test_data[:end_index].decode('utf-8') == '{"api_version": "1.0", "payload_length": 38400000, "to": "sdp", "from": "dig", "api_call": {"msg_type": "adv", "action_code": "samples", "metadata": [{"property": "center_freq", "value": 1420400000.0}, {"property": "sample_rate", "value": 2400000.0}, {"property": "bandwidth", "value": 2000000.0}, {"property": "gain", "value": 40}, {"property": "timestamp", "value": "2025-09-20T12:30:18.035666+00:00"}]}}'
    assert test_data[end_index:] == b'RRRRRR'