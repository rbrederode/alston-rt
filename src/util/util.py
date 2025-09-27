import pytest

import logging
logger = logging.getLogger(__name__)

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