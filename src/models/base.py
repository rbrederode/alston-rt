from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Dict, Set
from schema import Schema, And, Or, Use, SchemaError
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.comms import CommunicationStatus
    from models.health import HealthState

# Runtime imports used for type conversion in from_dict()
from models.comms import CommunicationStatus
from models.health import HealthState

from util.xbase import XInvalidTransition, XAPIValidationFailed, XSoftwareFailure

# Base class to model any telescope construct
class BaseModel:
    """
    Base class that provides:
      - schema validation using `schema` library
      - allowed state transition enforcement
      - dictionary-style attribute management
    """
    schema: Schema
    allowed_transitions: Dict[str, Dict[enum.IntEnum, Set[enum.IntEnum]]] = {}

    def __init__(self, **kwargs):

        # store component state here
        self._data: Dict[str, Any] = {}
        # initialise with default or provided values
        for field in self.schema.schema.keys():
            self._data[field] = kwargs.get(field, None)
        # Validate initial structure
        self._validate_schema()

    def _validate_schema(self):
        try:
            self.schema.validate(self._data)
        except SchemaError as e:
            raise XAPIValidationFailed(f"Component model schema error: validate failed: {e}")

    def _validate_transition(self, name: str, new_value: Any):
        if name in self.allowed_transitions:
            old_value = self._data.get(name)
            allowed = self.allowed_transitions[name].get(old_value, set())
            if old_value is not None and new_value not in allowed:
                raise XInvalidTransition(
                    f"Component model attempting invalid transition for name: {name}: {old_value.name} → {new_value.name}"
                )

    def __getattr__(self, name):
        if name in self._data:
            return self._data[name]
        raise XSoftwareFailure(f"Component model attribute name: {name} not found")

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
            return
        if name not in self.schema.schema:
            raise AttributeError(f"Invalid attribute name: {name} for {type(self).__name__}")
        self._validate_transition(name, value)
        self._data[name] = value
        self._validate_schema()  # enforce schema after update

    def from_dict(self, data: Dict[str, Any]):

        from models.app import AppModel
        from models.comms import CommunicationStatus
        from models.dsh import Feed
        from models.health import HealthState
        from models.scan import ScanModel, ScanState
        from models.tm import ScanStoreModel

        for key, value in data.items():
            if key in self.schema.schema:

                # Attempt to infer an expected type from the schema; if that
                # isn't available (complex Schema constructs), fall back to
                # key-based heuristics below.
                try:
                    expected_type = self.schema.schema[key].args[0]
                except Exception:
                    expected_type = None

                # If we determined an actual Python type, use isinstance checks
                if isinstance(expected_type, type):
                    # Special handling for processing_scans: convert list of dicts to list of ScanModel instances
                    # This must happen before the isinstance check because value is already a list
                    if key == "processing_scans" and expected_type == list and isinstance(value, list):
                        converted_scans = []
                        for scan_dict in value:
                            if isinstance(scan_dict, dict):
                                scan_instance = ScanModel()
                                scan_instance.from_dict(scan_dict)
                                converted_scans.append(scan_instance)
                            elif isinstance(scan_dict, ScanModel):
                                converted_scans.append(scan_dict)
                        value = converted_scans
                    elif not isinstance(value, expected_type):
                        if expected_type == int:
                            value = int(value)
                        elif expected_type == float:
                            value = float(value)
                        elif expected_type == bool:
                            value = bool(value)
                        elif expected_type == list:
                            value = list(value)
                        elif expected_type == dict:
                            value = dict(value)
                        elif expected_type == ScanStoreModel:
                            # Build a default instance then update via from_dict to
                            # allow nested conversion of enum/string fields without
                            # tripping schema validation in the constructor.
                            instance = ScanStoreModel()
                            instance.from_dict(value)
                            value = instance
                        elif expected_type == CommunicationStatus:
                            # Accept enum instance, numeric value, numeric string or name
                            if isinstance(value, CommunicationStatus):
                                pass
                            elif isinstance(value, str):
                                try:
                                    # try by name first
                                    value = CommunicationStatus[value]
                                except KeyError:
                                    # try numeric string
                                    value = CommunicationStatus(int(value))
                            else:
                                value = CommunicationStatus(int(value))
                        elif expected_type == HealthState:
                            if isinstance(value, HealthState):
                                pass
                            elif isinstance(value, str):
                                try:
                                    value = HealthState[value]
                                except KeyError:
                                    value = HealthState(int(value))
                            else:
                                value = HealthState(int(value))
                        elif expected_type == ScanState:
                            if isinstance(value, ScanState):
                                pass
                            elif isinstance(value, str):
                                try:
                                    value = ScanState[value]
                                except KeyError:
                                    value = ScanState(int(value))
                            else:
                                value = ScanState(int(value))
                        elif expected_type == AppModel:
                            # Build a default instance then update via from_dict to
                            # allow nested conversion of enum/string fields without
                            # tripping schema validation in the constructor.
                            instance = AppModel()
                            instance.from_dict(value)
                            value = instance
                        elif expected_type == ScanModel:
                            # Build a default instance then update via from_dict to
                            # allow nested conversion of enum/string fields without
                            # tripping schema validation in the constructor.
                            instance = ScanModel()
                            instance.from_dict(value)
                            value = instance
                        elif expected_type == datetime:
                            # Parse ISO datetime strings to datetime instances
                            if isinstance(value, str):
                                try:
                                    value = datetime.fromisoformat(value)
                                except Exception:
                                    print(f"Warning: could not parse datetime string for key '{key}': {value}")
                                    pass
                        elif expected_type is Feed:
                            
                            if isinstance(value, int):
                                value = Feed(value)
                            elif isinstance(value, str):
                                try:
                                    value = Feed[value]
                                except KeyError:
                                    value = Feed(int(value))
                        else:
                            raise XAPIValidationFailed(f"Invalid type for '{key}': {type(value).__name__}, expected {expected_type.__name__}")

                else:
                    # For complex or unspecified expected types, use key-based
                    # heuristics to convert common nested values (enums, dates,
                    # nested app models, etc.). This keeps from_dict robust
                    # when Schema internals are not easy to introspect.


                    # Check if the key contains the string "connected"


                    if "connected" in key:
                        # CommunicationStatus conversion
                        if isinstance(value, CommunicationStatus):
                            pass
                        elif isinstance(value, str):
                            try:
                                value = CommunicationStatus[value]
                            except KeyError:
                                value = CommunicationStatus(int(value))
                        else:
                            value = CommunicationStatus(int(value))
                    elif key == "health":
                        if isinstance(value, HealthState):
                            pass
                        elif isinstance(value, str):
                            try:
                                value = HealthState[value]
                            except KeyError:
                                value = HealthState(int(value))
                        else:
                            value = HealthState(int(value))
                    elif key == "status":
                        if isinstance(value, ScanState):
                            pass
                        elif isinstance(value, str):
                            try:
                                value = ScanState[value]
                            except KeyError:
                                value = ScanState(int(value))
                        else:
                            value = ScanState(int(value))
                    elif key in ("last_update", "created", "read_start", "read_end", "prev_read_end") and isinstance(value, str):
                        # Parse ISO datetime strings
                        try:
                            value = datetime.fromisoformat(value)
                        except Exception:
                            # leave as-is; schema validation will raise if invalid
                            pass
                    elif key == "app" and isinstance(value, dict):
                        instance = AppModel()
                        instance.from_dict(value)
                        value = instance
                    elif key == "feed" and isinstance(value, int):
                        from models.dsh import Feed
                        value = Feed(value)
                    elif key == "scan_store" and isinstance(value, dict):
                        instance = ScanStoreModel()
                        instance.from_dict(value)
                        value = instance

                    if expected_type == int:
                        value = int(value)
                    elif expected_type == float:
                        value = float(value)
                    elif expected_type == bool:
                        value = bool(value)
                    elif expected_type == list:
                        value = list(value)
                    elif expected_type == dict:
                        value = dict(value)
                    elif expected_type == CommunicationStatus:
                        # Accept enum instance, numeric value, numeric string or name
                        if isinstance(value, CommunicationStatus):
                            pass
                        elif isinstance(value, str):
                            try:
                                # try by name first
                                value = CommunicationStatus[value]
                            except KeyError:
                                # try numeric string
                                value = CommunicationStatus(int(value))
                        else:
                            value = CommunicationStatus(int(value))
                    elif expected_type == HealthState:
                        if isinstance(value, HealthState):
                            pass
                        elif isinstance(value, str):
                            try:
                                value = HealthState[value]
                            except KeyError:
                                value = HealthState(int(value))
                        else:
                            value = HealthState(int(value))
                    elif expected_type == AppModel:
                        # Build a default instance then update via from_dict to
                        # allow nested conversion of enum/string fields without
                        # tripping schema validation in the constructor.
                        instance = AppModel()
                        instance.from_dict(value)
                        value = instance
                    elif expected_type == ScanModel:
                        # Build a default instance then update via from_dict to
                        # allow nested conversion of enum/string fields without
                        # tripping schema validation in the constructor.
                        instance = ScanModel()
                        instance.from_dict(value)
                        value = instance
                    elif expected_type == datetime:
                        # Parse ISO datetime strings to datetime instances
                        if isinstance(value, str):
                            try:
                                value = datetime.fromisoformat(value)
                            except Exception:
                                print(f"Warning: could not parse datetime string for key '{key}': {value}")
                                pass
                    elif expected_type == ScanStoreModel:
                        # Build a default instance then update via from_dict to
                        # allow nested conversion of enum/string fields without
                        # tripping schema validation in the constructor.
                        instance = ScanStoreModel()
                        instance.from_dict(value)
                        value = instance
                    else:
                        # If we couldn't determine a concrete expected type
                        # from the schema (expected_type is None), don't
                        # attempt a forced conversion here; allow schema
                        # validation to raise a clear error later. Otherwise
                        # raise an explicit conversion error.
                        if expected_type is None:
                            pass
                        else:
                            raise XAPIValidationFailed(f"Invalid type for '{key}': {type(value).__name__}, expected {expected_type.__name__}")

                self._validate_transition(key, value)
                self._data[key] = value
        self._validate_schema()  # enforce schema after update

    def to_dict(self):
        # Sort keys for consistent output and convert non-serializable
        # values (e.g. datetime) into JSON-friendly representations.
        def _serialize(v):
            # enum -> name
            if isinstance(v, enum.IntEnum):
                return v.name
            # If an object exposes a to_dict() method, use it (duck-typing).
            # Avoid direct references to application model classes here to
            # prevent runtime circular imports / NameError when those classes
            # are only available for type checking.
            if hasattr(v, "to_dict") and callable(getattr(v, "to_dict")):
                try:
                    return v.to_dict()
                except Exception:
                    # Fall back to string representation if to_dict fails
                    return str(v)
            # datetime -> ISO string
            if isinstance(v, datetime):
                return v.isoformat()
            # dict -> recursively serialize
            if isinstance(v, dict):
                return {k: _serialize(val) for k, val in v.items()}
            # list/tuple -> recursively serialize elements
            if isinstance(v, list):
                return [_serialize(x) for x in v]
            if isinstance(v, tuple):
                return tuple(_serialize(x) for x in v)
            # fallback: return value as-is
            return v

        return {k: _serialize(v) for k, v in self._data.items()}