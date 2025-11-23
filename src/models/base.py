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
            raise XAPIValidationFailed(f"Base model schema error: validate failed for type {type(self).__name__}: {e}")

    def _validate_transition(self, name: str, new_value: Any):
        if name in self.allowed_transitions:
            old_value = self._data.get(name)
            allowed = self.allowed_transitions[name].get(old_value, set())
            if old_value is not None and new_value not in allowed:
                raise XInvalidTransition(
                    f"Base model attempting invalid transition in type {type(self).__name__} for name: {name}: {old_value.name} → {new_value.name}"
                )

    def __getattr__(self, name):
        # Use object.__getattribute__ to avoid infinite recursion
        try:
            data = object.__getattribute__(self, '_data')
        except AttributeError:
            raise XSoftwareFailure(f"Base model _data not initialized for type {type(self).__name__}")
        
        if name in data:
            return data[name]
        raise XSoftwareFailure(f"Base model attribute name: {name} not found for type {type(self).__name__}")

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
            return
        if name not in self.schema.schema:
            raise AttributeError(f"Invalid attribute name: {name} for type {type(self).__name__}")
        self._validate_transition(name, value)
        self._data[name] = value
        self._validate_schema()  # enforce schema after update

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """ A classmethod is a method that receives the class itself (not an instance) as its first argument 
            (typically named cls). It's called on the class rather than an instance, and can create and return new 
            instances of that class
        """
        parsed = cls._deserialise(data)
        
        # Check if parsed is a BaseModel and has the same class name
        # (to handle cases where __main__ vs module imports create different class objects)
        if not isinstance(parsed, BaseModel) or parsed.__class__.__name__ != cls.__name__:
            raise XAPIValidationFailed(f"Base model from_dict failed for type {cls.__name__}: expected {cls.__name__}, got {type(parsed).__name__}")
        
        # If this model has a last_update field, find the latest datetime in all nested models
        if 'last_update' in parsed.schema.schema:
            latest = parsed.find_latest_update()
            if latest is not None:
                # Add UTC timezone if the datetime is naive
                if latest.tzinfo is None:
                    from datetime import timezone
                    latest = latest.replace(tzinfo=timezone.utc)
                parsed._data['last_update'] = latest
        
        return parsed
    
    def find_latest_update(self):
        """Recursively find the latest last_update datetime in this model and all nested models."""
        latest = None
        
        for key, value in self._data.items():
            # Only check datetime fields that are named 'last_update'
            if key == 'last_update' and isinstance(value, datetime):
                # Normalize to naive datetime for comparison
                value_naive = value.replace(tzinfo=None) if value.tzinfo else value
                if latest is None or value_naive > latest:
                    latest = value_naive
            elif isinstance(value, BaseModel):
                nested_latest = value.find_latest_update()
                if nested_latest and (latest is None or nested_latest > latest):
                    latest = nested_latest
            elif isinstance(value, dict):
                for v in value.values():
                    if isinstance(v, BaseModel):
                        nested_latest = v.find_latest_update()
                        if nested_latest and (latest is None or nested_latest > latest):
                            latest = nested_latest
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, BaseModel):
                        nested_latest = item.find_latest_update()
                        if nested_latest and (latest is None or nested_latest > latest):
                            latest = nested_latest
        
        return latest

    def to_dict(self):
        # Sort keys for consistent output and convert non-serializable
        # values (e.g. datetime) into JSON-friendly representations.

        return {k: BaseModel._serialise(v) for k, v in self._data.items()}

    @staticmethod
    def _serialise(v):

        from astropy.coordinates import SkyCoord, AltAz, EarthLocation
        from astropy.time import Time
        from astroplan import Observer
        import astropy.units as u

        # enum -> name and enum class
        if isinstance(v, enum.IntEnum):
            return {"_type": "enum.IntEnum", "instance": type(v).__name__, "value": v.name}
        # If an object exposes a to_dict() method, use it (duck-typing).
        if hasattr(v, "to_dict") and callable(getattr(v, "to_dict")):
            try:
                return v.to_dict()
            except Exception:
                # Fall back to string representation if to_dict fails
                return str(v)
        # datetime -> ISO string
        if isinstance(v, datetime):
            return {"_type": "datetime", "value": v.isoformat()}
        # dict -> recursively serialise
        if isinstance(v, dict):
            return {k: BaseModel._serialise(val) for k, val in v.items()}
        # list/tuple -> recursively serialise elements
        if isinstance(v, list):
            return [BaseModel._serialise(x) for x in v]
        if isinstance(v, tuple):
            return tuple(BaseModel._serialise(x) for x in v)
        if isinstance(v, SkyCoord):
            # Check the frame type to determine which attributes to serialize
            if hasattr(v, 'ra') and hasattr(v, 'dec'):
                # ICRS, FK5, etc. frames with RA/Dec
                return {"_type": "SkyCoord", "ra": v.ra.deg, "dec": v.dec.deg, "frame": v.frame.name}
            elif hasattr(v, 'az') and hasattr(v, 'alt'):
                # AltAz frame
                return {
                    "_type": "SkyCoord", 
                    "az": v.az.deg, 
                    "alt": v.alt.deg, 
                    "frame": v.frame.name,
                    "obstime": BaseModel._serialise(v.obstime.datetime) if v.obstime is not None else None,
                    "location": BaseModel._serialise(v.location) if v.location is not None else None
                }
            else:
                # Fallback for other coordinate types
                return {"_type": "SkyCoord", "frame": v.frame.name, "repr": str(v)}
        if isinstance(v, AltAz):
            return {"_type": "AltAz", "alt": v.alt.deg, "az": v.az.deg, "obstime": BaseModel._serialise(v.obstime.isoformat()) if v.obstime else None, "location": BaseModel._serialise(v.location)}
        if isinstance(v, EarthLocation):
            return {"_type": "EarthLocation", "lat": v.lat.deg, "lon": v.lon.deg, "height": v.height.to_value(u.m)}
        elif isinstance(v, Time):
            return {"_type": "Time", "value": v.isot, "scale": v.scale}   
        elif isinstance(v, Observer):
            return {
                "_type": "Observer",
                "name": v.name,
                "location": BaseModel._serialise(v.location)
            } 
        # fallback: return value as-is
        return v

    @staticmethod
    def _deserialise(v):

        from astropy.coordinates import SkyCoord, AltAz, EarthLocation
        from astropy.time import Time
        from astroplan import Observer
        import astropy.units as u

        from models.app import AppModel
        from models.comms import CommunicationStatus
        from models.dig import DigitiserModel
        from models.dsh import DishModel, DishMgrModel
        from models.dsh import Feed
        from models.health import HealthState
        from models.obs import ObsState, ObsModel
        from models.oda import ObsList, ScanStore, ODAModel
        from models.oet import OETModel
        from models.proc import ProcessorModel
        from models.scan import ScanModel, ScanState
        from models.sdp import ScienceDataProcessorModel
        from models.target import TargetModel, TargetType
        from models.tm import TelescopeManagerModel
        
        if isinstance(v, dict) and "_type" in v:
            model_type = v["_type"]
            if model_type == "AltAz":
                location = BaseModel._deserialise(v["location"])
                obstime = BaseModel._deserialise(v["obstime"])
                return AltAz(alt=v["alt"]*u.deg, az=v["az"]*u.deg, obstime=obstime, location=location)
            elif model_type == "AppModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return AppModel(**deserialized_fields)
            elif model_type == "datetime":
                if isinstance(v["value"], str):
                    return datetime.fromisoformat(v["value"])  
            elif model_type == "DigitiserModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return DigitiserModel(**deserialized_fields)
            elif model_type == "DishMgrModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return DishMgrModel(**deserialized_fields)
            elif model_type == "DishModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return DishModel(**deserialized_fields)
            elif model_type == "EarthLocation":
                return EarthLocation(lat=v["lat"]*u.deg, lon=v["lon"]*u.deg, height=v["height"]*u.m)
            elif model_type == "enum.IntEnum":
                enum_class_name = v["instance"]
                enum_value_name = v["value"]

                # Map class name to actual enum class
                enum_class = {
                    "CommunicationStatus": CommunicationStatus,
                    "HealthState": HealthState,
                    "ScanState": ScanState,
                    "ObsState": ObsState,
                    "TargetType": TargetType,
                    "Feed": Feed,
                }.get(enum_class_name)
                if enum_class is not None:
                    return enum_class[enum_value_name]
                else:
                    raise ValueError(f"Unknown enum class name: {enum_class_name}")
            elif model_type == "Feed":
                if isinstance(v, str):
                    return Feed[v]
                else:
                    return Feed(int(v))
            elif model_type == "Observer":
                location = BaseModel._deserialise(v["location"])
                return Observer(name=v["name"], location=location)
            elif model_type == "ObsModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return ObsModel(**deserialized_fields)
            elif model_type == "ObsList":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return ObsList(**deserialized_fields)
            elif model_type == "OETModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return OETModel(**deserialized_fields)
            elif model_type == "ODAModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return ODAModel(**deserialized_fields)
            elif model_type == "ProcessorModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return ProcessorModel(**deserialized_fields)
            elif model_type == "ScanModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return ScanModel(**deserialized_fields)
            elif model_type == "ScanStore":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return ScanStore(**deserialized_fields)
            elif model_type == "ScienceDataProcessorModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return ScienceDataProcessorModel(**deserialized_fields)
            elif model_type == "SkyCoord":
                # Check which coordinate system is used
                if "ra" in v and "dec" in v:
                    # ICRS, FK5, etc. frames with RA/Dec
                    return SkyCoord(ra=v["ra"]*u.deg, dec=v["dec"]*u.deg, frame=v["frame"])
                elif "az" in v and "alt" in v:
                    # AltAz frame
                    obstime = BaseModel._deserialise(v["obstime"]) if v.get("obstime") else None
                    location = BaseModel._deserialise(v["location"]) if v.get("location") else None
                    return SkyCoord(az=v["az"]*u.deg, alt=v["alt"]*u.deg, frame=v["frame"], obstime=obstime, location=location)
                else:
                    # Fallback - can't reconstruct
                    raise ValueError(f"Cannot reconstruct SkyCoord from {v}")
            elif model_type == "TargetModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return TargetModel(**deserialized_fields)
            elif model_type == "TelescopeManagerModel":
                deserialized_fields = {k: BaseModel._deserialise(val) for k, val in v.items() if k != "_type"}
                return TelescopeManagerModel(**deserialized_fields)
            elif model_type == "Time":
                return Time(v["value"], scale=v["scale"])
        elif isinstance(v, (list, tuple)):
            return type(v)(BaseModel._deserialise(item) for item in v)
        elif isinstance(v, dict):
            return {k: BaseModel._deserialise(val) for k, val in v.items()}
        elif isinstance(v, enum.IntEnum):
            return type(v)(v.value)

        return v