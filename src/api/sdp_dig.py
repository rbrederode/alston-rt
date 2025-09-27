import json
import re
import datetime
from datetime import timezone
from typing import Any, Dict
from api.api import API
from ipc.message import Message, AppMessage, APIMessage
from util.xbase import XBase, XStreamUnableToExtract, XStreamUnableToEncode, XAPIValidationFailed, XAPIUnsupportedVersion

import logging
logger = logging.getLogger(__name__)

API_VERSION = "1.0" # Version of the API implemented in this module.
LEGACY_SUPPORTED_VERSIONS = [] # Requires translator methods to/from API_VERSION

# Allowable api msg types 
MSG_TYPE_REQ = "req"  # Request an action to be taken e.g. get or set a property that either succeeds or fails
MSG_TYPE_ADV = "adv"  # Advise that an action must be taken e.g. system is shutting down, so shutdown (no ifs or buts)
MSG_TYPE_RSP = "rsp"  # Response to a request or advice message

MSG_TYPES =  (
    MSG_TYPE_REQ,   # Request an action to be taken e.g. get or set a property that either succeeds or fails
    MSG_TYPE_ADV,   # Advise that an action must be taken e.g. system is shutting down, so shutdown (no ifs or buts)
    MSG_TYPE_RSP    # Response to a request or advice message
)

# Allowable api msg actions 
ACTION_CODE_SAMPLES = "samples"     # Sending IQ data samples

ACTION_CODES = (
    ACTION_CODE_SAMPLES,      # Sending IQ data samples
)

# Allowable origins (from) and destinations (to) of api msg calls
DIG = "dig"  # Digitiser 
SDP = "sdp"  # Science Data Processor

FROM = (
    DIG,
    SDP
)

TO = (
    DIG,
    SDP
)

# Allowable status codes for responses
STATUS_SUCCESS   = "success"
STATUS_ERROR     = "error"

STATUS = (
    STATUS_SUCCESS,
    STATUS_ERROR
)

# Meta data properties 
PROPERTY_CENTER_FREQ    = 'center_freq'      # Center frequency in Hz
PROPERTY_SAMPLE_RATE    = 'sample_rate'      # Sample rate in samples per second
PROPERTY_BANDWIDTH      = 'bandwidth'        # Bandwidth in Hz
PROPERTY_SDR_GAIN       = 'gain'             # Gain in dB
PROPERTY_TIMESTAMP      = 'timestamp'        # Timestamp of samples in ISO 8601 format with timezone info

PROPERTIES = (
    PROPERTY_CENTER_FREQ,
    PROPERTY_SAMPLE_RATE,
    PROPERTY_BANDWIDTH,
    PROPERTY_SDR_GAIN,
    PROPERTY_TIMESTAMP
)

# Allowable msg fields and types defining their format     
#   "field_name": "regex_pattern" | {"type": "type_name", "pattern": "regex_pattern", "enum": [...]} 
#   type_name is one of int, float, str, bool, list, dict, tuple

# Examples:
# "field_name": {"type": "int", "pattern": r"^\d{1,5}$"}  # Integer between 0 and 99999
# "field_name": {"type": "float", "pattern": r"^\d{1,5}\.\d{1,5}$"}  # Float between 0.0 and 99999.99999
# "field_name": {"type": "str", "pattern": r"^[A-Za-z0-9 _\-.,!?]+$"}  # String with certain allowed characters

# FIELD             TYPE                                            DESCRIPTION
METADATA_FIELD = {
    "property":     {"type": "str", "enum": PROPERTIES},           # Property name (one of PROPERTIES)
    "value":        {"type": Any},                                 # Value of the property (type depends on property)
}

# FIELD             TYPE                                            DESCRIPTION
MSG_FIELDS = {
    "msg_type":     {"enum": MSG_TYPES},                            # Message type (one of MSG_TYPES)
    "action_code":  {"enum": ACTION_CODES},                         # Action to be taken (one of ACTION_CODES)
    "metadata":     {
        "type": "list",                                             # Metadata is a list
        "value_type": "dict",                                       # Each value is a dict
        "value_schema": METADATA_FIELD},                            # Each value should match METADATA_FIELD
    "status":       {"enum": STATUS},                               # Status of response (e.g. success, error)
    "message":      {"type": "str"},                                # Additional information about the status
}

# Definition of required, conditional and optional fields for each api msg type
MSG_FIELDS_DEFINITIONS = {
    "adv": {
        "required": {"msg_type", "action_code", "metadata"},
        "conditional": {},
        "optional": {"status", "message"},   
    },
    "rsp": {
        "required": {"msg_type", "action_code", "status"},
        "conditional": {},
        "optional": {"message"},
    },
}

class SDP_DIG(API):
    """
    Class responsible for enforcing the Science Data Processor-Digitiser API.

    The API defines the structure and rules for API messages between these systems.

    API calls are wrapped within an API message before being exchanged. This enforces that 
    each API call is associated with metadata such as the source and destination of the call,
    the version of the API being used, and a timestamp.

    This class provides methods to validate and translate API messages.
    """

    def __init__(self):
        super().__init__()

    def get_api_version(self) -> str:
        """ Returns the API version implemented by this class.
        """
        return API_VERSION

    def get_legacy_supported_versions(self) -> list:
        """ Returns a list of legacy API versions supported by this class.
        """
        return LEGACY_SUPPORTED_VERSIONS

    def validate(self, api_msg: Dict[str, Any]):
        """
        Validates that the api_msg dictionary contains the required fields and that the fields
        conform to the implementation's API_VERSION types and formats.
            :param api_msg: APIMessage dictionary containing the API call in API_VERSION format
            :raises XAPIValidationFailed: If the message fails validation
        """

        logger.debug(f"Validating API message: {json.dumps(api_msg, indent=4)}")

        if 'api_version' not in api_msg:
            raise XAPIValidationFailed("Message missing required field 'api_version'")

        if api_msg['api_version'] != API_VERSION:
            if api_msg['api_version'] not in LEGACY_SUPPORTED_VERSIONS:
                raise XAPIValidationFailed(f"Unsupported API version {api_msg['api_version']}")

        # API call is contained within the API message
        api_call = api_msg['api_call'] 

        if 'msg_type' not in api_call:
            raise XAPIValidationFailed("Message missing required field 'msg_type'")

        msg_type = api_call['msg_type']
        if msg_type not in MSG_FIELDS_DEFINITIONS:
            raise XAPIValidationFailed(f"Unsupported message type '{msg_type}'")

        # Check for required fields
        required_fields = MSG_FIELDS_DEFINITIONS[msg_type].get('required', set())
        for field in required_fields:
            if field not in api_call:
                raise XAPIValidationFailed(f"Message of type '{msg_type}' missing required field '{field}'")

        # Check for conditional fields
        conditional_fields = MSG_FIELDS_DEFINITIONS[msg_type].get('conditional', set())
        for field in conditional_fields:
            pass # No conditional fields defined yet

        # Validate each field's value against its expected type and format
        for field, value in api_call.items():
            if field in MSG_FIELDS:
                if isinstance(MSG_FIELDS[field], str):
                    if not re.fullmatch(MSG_FIELDS[field], str(value)):
                        raise XAPIValidationFailed(f"Invalid value for field '{field}': {value}")
                elif isinstance(MSG_FIELDS[field], dict):
                    if 'type' in MSG_FIELDS[field]:
                        expected_type = MSG_FIELDS[field]['type']
                        if not isinstance(value, eval(expected_type)):
                            raise XAPIValidationFailed(f"Invalid type for field '{field}': expected {expected_type}, got {type(value).__name__}")
                        # Check pattern if present
                        if 'pattern' in MSG_FIELDS[field]:
                            if not re.fullmatch(MSG_FIELDS[field]['pattern'], str(value)):
                                raise XAPIValidationFailed(f"Invalid pattern for field '{field}': {value}")
                elif isinstance(MSG_FIELDS[field], list):
                    if 'value_type' in MSG_FIELDS[field] and 'value_schema' in MSG_FIELDS[field]:
                        value_type = MSG_FIELDS[field]['value_type']
                        value_schema = MSG_FIELDS[field]['value_schema']
                        if not isinstance(value, dict):
                                raise XAPIValidationFailed(f"Invalid type for field '{field}': expected dict, got {type(value).__name__}")
                        for k, v in value.items():
                            if not isinstance(v, eval(value_type)):
                                raise XAPIValidationFailed(f"Invalid type for value in field '{field}': expected {value_type}, got {type(v).__name__}")
                            # Validate each value against the schema
                            for schema_field, schema_rules in value_schema.items():
                                if schema_field not in v:
                                    raise XAPIValidationFailed(f"Value in field '{field}' missing required sub-field '{schema_field}'")
                                schema_value = v[schema_field]
                                if 'type' in schema_rules:
                                    expected_schema_type = schema_rules['type']
                                    if not isinstance(schema_value, eval(expected_schema_type)):
                                        raise XAPIValidationFailed(f"Invalid type for sub-field '{schema_field}' in field '{field}': expected {expected_schema_type}, got {type(schema_value).__name__}")
                                    if 'pattern' in schema_rules:
                                        if not re.fullmatch(schema_rules['pattern'], str(schema_value)):
                                            raise XAPIValidationFailed(f"Invalid pattern for sub-field '{schema_field}' in field '{field}': {schema_value}")
                                    if 'enum' in schema_rules:
                                        if schema_value not in schema_rules['enum']:
                                            raise XAPIValidationFailed(f"Invalid value for sub-field '{schema_field}' in field '{field}': {schema_value}")
                    if 'enum' in MSG_FIELDS[field]:
                        if value not in MSG_FIELDS[field]['enum']:
                            raise XAPIValidationFailed(f"Invalid value for field '{field}': {value}")

    def translate(self, api_msg: Dict[str, Any], target_version: str=API_VERSION) -> Dict[str, Any]:
        """
        Translates an api_msg dictionary to a target_version.
        Must support translation between an API implementation's API_VERSION and LEGACY_SUPPORTED_VERSIONS.
        Must support bi-directional translation i.e. to and from versions.
            :param api_msg: Dictionary containing an APIMessage dictionary
            :param target_version: Target version of the API
            :return: Translated api message dictionary
            :raises XAPIValidationFailed: If the api message fails validation
        """

        if 'api_version' not in api_msg:
            raise XAPIValidationFailed("Message missing required field 'api_version'")

        if api_msg['api_version'] not in LEGACY_SUPPORTED_VERSIONS + [API_VERSION]:
            raise XAPIValidationFailed(f"Unsupported API version {api_msg['api_version']}")

        source_version = api_msg['api_version']

        if target_version is None or source_version == target_version:
            return api_msg

        # Example translation logic (expand as needed for real differences)
        translated_msg = api_msg.copy()

        if source_version == "1.0" and target_version == "2.0":
            logger.debug("Translating from 1.0 to 2.0")
            # Translation logic here
            translated_msg['api_version'] = "2.0"

        elif source_version == "2.0" and target_version == "1.0":
            logger.debug("Translating from 2.0 to 1.0")
            
            # Translation logic here

            translated_msg['api_version'] = "1.0"

        else:
            raise XAPIUnsupportedVersion(f"Translation from version {source_version} to {target_version} not supported")

        return translated_msg

def main():
    
    # Convert the message dictionary to a byte array 
    send_msg = APIMessage()

    api_call = {
        "msg_type":     "adv",
        "action_code":  "samples",
        "metadata": [   
            {"property": "center_freq", "value": 1420.40e6},  # Hz
            {"property": "sample_rate", "value": 2.4e6},      # Hz
            {"property": "bandwidth", "value": 2.0e6},        # Hz
            {"property": "gain", "value": 40},                # dB
            {"property": "timestamp", "value": datetime.datetime.now(timezone.utc).isoformat()}
        ],
    }

    send_msg.set_json_api_header(
        api_version="1.0",
        dt=datetime.datetime.now(timezone.utc),
        from_system="dig",
        to_system="sdp",
        api_call=api_call
    )

    send_msg.set_payload_data(b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09')  # Example byte array payload depicting IQ samples

    api = SDP_DIG()
    api.translate(send_msg.get_json_api_header(), target_version="1.0")
    api.validate(send_msg.get_json_api_header())

    sent_data = send_msg.to_data()

    print("Sent message")
    print(send_msg)

    # Simulate sending the data over a network
    received_data = sent_data

    # Convert the received data back to a dictionary
    receive_msg = APIMessage()
    receive_msg.from_data(received_data)

    print("Received message")
    print(receive_msg)

    api_call = api.translate(receive_msg.get_json_api_header(), target_version="1.0")
    api.validate(api_call)

    print("Translated message to v1.0")
    print(json.dumps(api_call, indent=4))

if __name__ == "__main__":
    main()