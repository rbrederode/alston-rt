# Model Documentation

This document describes all data models in the project.

## Allocation

**Source:** `tm.py`

**Description:** A class representing a single resource allocation.

### Allocation Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "Allocation"), |
| `allocated_id` | str |  "allocated_id": And(str, lambda v: isinstance(v, str)),                     # ID of the entity to which the resource is allocated |
| `allocated_type` | str |  "allocated_type": And(str, lambda v: isinstance(v, str)),                   # Type of entity to which the resource is allocated (e.g., "observation") |
| `expires` | datetime |  "expires": Or(None, And(datetime, lambda v: isinstance(v, datetime))),      # Expiration time of the allocation, None implies no expiration |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `resource_id` | str |  "resource_id": And(str, lambda v: isinstance(v, str)),                      # ID of the resource being allocated |
| `resource_type` | str |  "resource_type": And(str, lambda v: isinstance(v, str)),                    # Type of resource (e.g., "dish", "digitiser") |
| `state` | AllocationState |  "state": And(AllocationState, lambda v: isinstance(v, AllocationState)),    # State of the allocation |

### Default Values

```python
_type: 'Allocation'
allocated_id: '<undefined>'
allocated_type: '<undefined>'
expires: None
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.632708+00:00'}
resource_id: '<undefined>'
resource_type: '<undefined>'
state: {'_type': 'enum.IntEnum', 'instance': 'AllocationState', 'value': 'REQUESTED'}
```

---

## AppModel

**Source:** `app.py`

**Description:** A class representing an App(lication) model.

### AppModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "AppModel"), |
| `app_name` | str |  "app_name": And(str, lambda v: isinstance(v, str)),                             # Name of the application e.g. "sdp", "tm", "dsh_mgr" |
| `app_running` | bool |  "app_running": And(bool, lambda v: isinstance(v, bool)),                        # Is the application currently running |
| `arguments` | dict |  "arguments": Or(None, And(dict, lambda v: isinstance(v, dict))), |
| `health` | HealthState |  "health": And(HealthState, lambda v: isinstance(v, HealthState)),               # Health state of the application (see HealthState enum) |
| `interfaces` | list |  "interfaces": And(list, lambda v: isinstance(v, list)), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `msg_timeout_ms` | int |  "msg_timeout_ms": And(int, lambda v: v >= 0), |
| `num_processors` | int |  "num_processors": And(int, lambda v: v >= 0),                                   # Number of processor instances (threads) used by the application |
| `processors` | list |  "processors": And(list, lambda v: isinstance(v, list)), |
| `queue_size` | int |  "queue_size": And(int, lambda v: v >= 0),                                       # Size of the event queue for the application |

### Default Values

```python
_type: 'AppModel'
app_name: 'app'
app_running: False
arguments: None
health: {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}
interfaces: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.643838+00:00'}
msg_timeout_ms: 10000
num_processors: 0
processors: []
queue_size: 0
```

---

## DigitiserList

**Source:** `dig.py`

**Description:** A class representing a list of digitisers.

### DigitiserList Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "DigitiserList"), |
| `dig_list` | list |  "dig_list": And(list, lambda v: isinstance(v, list)),               # List of DigitiserModel objects |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `list_id` | str |  "list_id": And(str, lambda v: isinstance(v, str)),                  # Digitiser List identifier e.g. "active" |

### Default Values

```python
_type: 'DigitiserList'
dig_list: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.644737+00:00'}
list_id: '<undefined>'
```

---

## DigitiserModel

**Source:** `dig.py`

**Description:** A class representing a digitiser application. The digitiser application is deployed at the telescope to digitise the analog RF signals.
        The digitiser is controlled by the Telescope Manager.    
        The digitiser streams digitised RF data to the SDP for processing.

### DigitiserModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "DigitiserModel"), |
| `app` | AppModel |  "app": And(AppModel, lambda v: isinstance(v, AppModel)), |
| `bandwidth` | float |  "bandwidth": And(float, lambda v: v >= 0.0), |
| `center_freq` | float |  "center_freq": And(float, lambda v: v >= 0.0), |
| `channels` | int |  "channels": And(int, lambda v: v >= 0), |
| `dig_id` | str |  "dig_id": And(str, lambda v: isinstance(v, str)), |
| `freq_correction` | int |  "freq_correction": And(int, lambda v: -1000 <= v <= 1000), |
| `gain` | float |  "gain": And(float, lambda v: 0 <= v <= 100.0), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `load` | bool |  "load": And(bool, lambda v: isinstance(v, bool)), |
| `sample_rate` | float |  "sample_rate": And(float, lambda v: v >= 0.0), |
| `scan_duration` | int |  "scan_duration": And(int, lambda v: v >= 0), |
| `scanning` | bool | dict | int | str |  "scanning": And(Or(bool, str, dict, int), lambda v: isinstance(v, bool) or isinstance(v, str) or isinstance(v, dict) or isinstance(v, int)), |
| `sdp_connected` | CommunicationStatus |  "sdp_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |
| `sdr_connected` | CommunicationStatus |  "sdr_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |
| `sdr_eeprom` | dict |  "sdr_eeprom": And(dict, lambda v: isinstance(v, dict)), |
| `tm_connected` | CommunicationStatus |  "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |

### Default Values

```python
_type: 'DigitiserModel'
app: {'_type': 'AppModel', 'app_name': 'dig', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.645587+00:00'}}
bandwidth: 0.0
center_freq: 0.0
channels: 0
dig_id: '<undefined>'
freq_correction: 0
gain: 0.0
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.645843+00:00'}
load: False
sample_rate: 0.0
scan_duration: 0
scanning: False
sdp_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
sdr_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
sdr_eeprom: {}
tm_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
```

---

## DishList

**Source:** `dsh.py`

**Description:** A class representing a list of dishes.

### DishList Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "DishList"), |
| `dish_list` | list |  "dish_list": And(list, lambda v: isinstance(v, list)),          # List of DishModel objects |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |

### Default Values

```python
_type: 'DishList'
dish_list: [{'_type': 'DishModel', 'dsh_id': 'dish001', 'short_desc': '70cm Discovery Dish', 'diameter': 0.7, 'fd_ratio': 0.37, 'latitude': 53.187052, 'longitude': -2.256079, 'height': 94.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'H3T_1420'}, 'dig_id': 'dig001', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.646862+00:00'}}, {'_type': 'DishModel', 'dsh_id': 'dish002', 'short_desc': '3m Jodrell Dish', 'diameter': 3.0, 'fd_ratio': 0.43, 'latitude': 53.2421, 'longitude': -2.3067, 'height': 80.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'NONE'}, 'dig_id': 'dig002', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.647383+00:00'}}]
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.647841+00:00'}
```

---

## DishManagerModel

**Source:** `dsh.py`

**Description:** A class representing the dish manager (application) model.

### DishManagerModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "DishManagerModel"), |
| `app` | AppModel |  "app": And(AppModel, lambda v: isinstance(v, AppModel)), |
| `dish_store` | DishList |  "dish_store": And(DishList, lambda v: isinstance(v, DishList)),                  # List of DishModel objects |
| `id` | str |  "id": And(str, lambda v: isinstance(v, str)),                                    # Dish Manager identifier e.g. "dm001" |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `tm_connected` | CommunicationStatus |  "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |

### Default Values

```python
_type: 'DishManagerModel'
app: {'_type': 'AppModel', 'app_name': 'dshmgr', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.649281+00:00'}}
dish_store: {'_type': 'DishList', 'dish_list': [{'_type': 'DishModel', 'dsh_id': 'dish001', 'short_desc': '70cm Discovery Dish', 'diameter': 0.7, 'fd_ratio': 0.37, 'latitude': 53.187052, 'longitude': -2.256079, 'height': 94.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'H3T_1420'}, 'dig_id': 'dig001', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.648253+00:00'}}, {'_type': 'DishModel', 'dsh_id': 'dish002', 'short_desc': '3m Jodrell Dish', 'diameter': 3.0, 'fd_ratio': 0.43, 'latitude': 53.2421, 'longitude': -2.3067, 'height': 80.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'NONE'}, 'dig_id': 'dig002', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.648760+00:00'}}], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.649241+00:00'}}
id: '<undefined>'
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.649527+00:00'}
tm_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
```

---

## DishModel

**Source:** `dsh.py`

**Description:** A class representing the dish model.

### DishModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "DishModel"), |
| `altaz` | AltAz |  "altaz": Or(None, And(AltAz, lambda v: isinstance(v, AltAz))),                          # Current alt-az pointing direction |
| `capability_state` | CapabilityState |  "capability_state": And(CapabilityState, lambda v: isinstance(v, CapabilityState)), |
| `diameter` | float | int |  "diameter": And(Or(int, float), lambda v: v >= 0.0),                                    # Dish diameter (meters) |
| `dig_id` | str |  "dig_id": Or(None, And(str, lambda v: isinstance(v, str))),                             # Current digitiser id assigned to the dish |
| `dsh_id` | str |  "dsh_id": And(str, lambda v: isinstance(v, str)),                                       # Dish identifer e.g. "dish001" |
| `fd_ratio` | float | int |  "fd_ratio": And(Or(int, float), lambda v: v >= 0.0),                                    # Dish focal length to diameter ratio |
| `feed` | Feed |  "feed": And(Feed, lambda v: isinstance(v, Feed)),                                       # Current feed installed on the dish |
| `height` | float | int |  "height": And(Or(int, float), lambda v: v >= 0.0),                                      # Dish height (meters) above sea level |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `latitude` | float | int |  "latitude": And(Or(int, float), lambda v: -90.0 <= v <= 90.0),                          # Dish latitude (degrees) |
| `longitude` | float | int |  "longitude": And(Or(int, float), lambda v: -180.0 <= v <= 180.0),                       # Dish longitude (degrees) |
| `mode` | DishMode |  "mode": And(DishMode, lambda v: isinstance(v, DishMode)), |
| `pointing_state` | PointingState |  "pointing_state": And(PointingState, lambda v: isinstance(v, PointingState)), |
| `short_desc` | str |  "short_desc": Or(None, And(str, lambda v: isinstance(v, str))),                         # Short description of the dish |

### Default Values

```python
_type: 'DishModel'
altaz: None
capability_state: {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'UNKNOWN'}
diameter: 0.0
dig_id: None
dsh_id: '<undefined>'
fd_ratio: 0.0
feed: {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'NONE'}
height: 0.0
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.650631+00:00'}
latitude: 0.0
longitude: 0.0
mode: {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'UNKNOWN'}
pointing_state: {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}
short_desc: None
```

---

## ODAModel

**Source:** `oda.py`

**Description:** A class representing the observation data archive.

### ODAModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "ODAModel"), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `obs_store` | ObsList |  "obs_store": And(ObsList, lambda v: isinstance(v, ObsList)), |
| `scan_store` | ScanStore |  "scan_store": And(ScanStore, lambda v: isinstance(v, ScanStore)), |

### Default Values

```python
_type: 'ODAModel'
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.651987+00:00'}
obs_store: {'_type': 'ObsList', 'obs_list': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.651936+00:00'}}
scan_store: {'_type': 'ScanStore', 'spr_files': [], 'load_files': [], 'tsys_files': [], 'gain_files': [], 'meta_files': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.651806+00:00'}}
```

---

## OETModel

**Source:** `oet.py`

**Description:** A class representing the observation execution tool model.

### OETModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "OETModel"), |
| `app` | AppModel |  "app": And(AppModel, lambda v: isinstance(v, AppModel)), |
| `id` | str |  "id": And(str, lambda v: isinstance(v, str)), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `obs_aborted` | int |  "obs_aborted": And(int, lambda v: v >= 0), |
| `obs_completed` | int |  "obs_completed": And(int, lambda v: v >= 0), |
| `obs_created` | int |  "obs_created": And(int, lambda v: v >= 0), |
| `processing_obs` | list |  "processing_obs": And(list, lambda v: all(isinstance(item, Observation) for item in v)), |
| `tm_connected` | CommunicationStatus |  "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |

---

## ObsList

**Source:** `oda.py`

**Description:** A class representing a list of observations.

### ObsList Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "ObsList"), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `obs_list` | list |  "obs_list": And(list, lambda v: isinstance(v, list)),          # List of observations |

### Default Values

```python
_type: 'ObsList'
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.653488+00:00'}
obs_list: []
```

---

## Observation

**Source:** `obs.py`

**Description:** A class representing a model of an observation

### Observation Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "Observation"), |
| `capabilities` | str |  "capabilities": And(str, lambda v: isinstance(v, str)),                 # Dish capabilities e.g. "Drift Scan over Zenith" |
| `created` | datetime |  "created": And(Or(None, datetime), lambda v: v is None or isinstance(v, datetime)),  # Creation datetime (UTC) |
| `description` | str |  "description": And(str, lambda v: isinstance(v, str)),                  # Description (no strict upper limit) |
| `diameter` | float | int |  "diameter": And(Or(int, float), lambda v: v >= 0.0),                    # Dish diameter (meters) |
| `dish_id` | str |  "dish_id": And(Or(None, str), lambda v: v is None or isinstance(v, str)),# Dish identifier e.g. "dish001" |
| `end_dt` | datetime |  "end_dt": And(datetime, lambda v: isinstance(v, datetime)),             # End datetime (UTC) of the observation |
| `estimated_observation_duration` | str |  "estimated_observation_duration": And(str, lambda v: isinstance(v, str)),   # Estimated observation duration (HH:MM:SS) |
| `estimated_slewing_time` | float | int |  "estimated_slewing_time": And(Or(int, float), lambda v: v >= 0.0),          # Estimated slewing time (seconds) |
| `f/d_ratio` | float | int |  "f/d_ratio": And(Or(int, float), lambda v: v >= 0.0),                   # Dish focal length to diameter ratio |
| `gain_calibrators` | list |  "gain_calibrators": And(list, lambda v: isinstance(v, list)),           # List of *gain.csv (gain calibration) filenames |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)),        # Last update datetime (UTC) of the observation |
| `latitude` | float | int |  "latitude": And(Or(int, float), lambda v: -90.0 <= v <= 90.0),          # Dish latitude (degrees) |
| `load_calibrators` | list |  "load_calibrators": And(list, lambda v: isinstance(v, list)),           # List of *load.csv (terminated signal chain) filenames |
| `longitude` | float | int |  "longitude": And(Or(int, float), lambda v: -180.0 <= v <= 180.0),       # Dish longitude (degrees) |
| `obs_id` | str |  "obs_id": And(Or(None, str), lambda v: v is None or isinstance(v, str)),# Unique identifier |
| `obs_state` | ObsState |  "obs_state": And(ObsState, lambda v: isinstance(v, ObsState)), |
| `scheduling_block_end` | datetime |  "scheduling_block_end": And(Or(None, datetime), lambda v: v is None or isinstance(v, datetime)),   # Scheduling block end datetime (UTC) |
| `scheduling_block_start` | datetime |  "scheduling_block_start": And(Or(None, datetime), lambda v: v is None or isinstance(v, datetime)), # Scheduling block start datetime (UTC) |
| `spr_scans` | list |  "spr_scans": And(list, lambda v: isinstance(v, list)),                  # List of *spr.csv (summed power) filenames |
| `start_dt` | datetime |  "start_dt": And(datetime, lambda v: isinstance(v, datetime)),           # Start datetime (UTC) of the observation |
| `target_configs` | list |  "target_configs": And(list, lambda v: isinstance(v, list)),             # List of target configurations (TargetConfig) |
| `target_scans` | list |  "target_scans": And(list, lambda v: isinstance(v, list)),               # List of target scan sets (TargetScanSet) |
| `targets` | list |  "targets": And(list, lambda v: isinstance(v, list)),                    # List of targets (TargetModel) |
| `tgt_index` | int |  "tgt_index": And(int, lambda v: isinstance(v, int)),                    # Index of the next target to be observed (0-based) |
| `tgt_scan` | int |  "tgt_scan": And(int, lambda v: isinstance(v, int)),                     # Index of the next scan (for the given tgt_index) to be observed (0-based) |
| `timeout_ms_config` | int |  "timeout_ms_config": And(int, lambda v: v > 0),                         # Configuration timeout in milliseconds |
| `timeout_ms_scan` | int |  "timeout_ms_scan": And(int, lambda v: v > 0),                           # Scan timeout in milliseconds |
| `title` | str |  "title": And(str, lambda v: isinstance(v, str)),                        # Short description (255 chars) |
| `total_integration_time` | float | int |  "total_integration_time": And(Or(int, float), lambda v: v >= 0.0),          # Total integration time (seconds) |
| `tsys_calibrators` | list |  "tsys_calibrators": And(list, lambda v: isinstance(v, list)),           # List of *tsys.csv (system temperature calibration) filenames |
| `user_email` | str |  "user_email": And(str, lambda v: isinstance(v, str)),                   # User email that created the observation |

### Default Values

```python
_type: 'Observation'
capabilities: ''
created: None
description: ''
diameter: 0.0
dish_id: None
end_dt: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.655805+00:00'}
estimated_observation_duration: '00:00:00'
estimated_slewing_time: 0.0
f/d_ratio: 0.0
gain_calibrators: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.655805+00:00'}
latitude: 0.0
load_calibrators: []
longitude: 0.0
obs_id: None
obs_state: {'_type': 'enum.IntEnum', 'instance': 'ObsState', 'value': 'EMPTY'}
scheduling_block_end: None
scheduling_block_start: None
spr_scans: []
start_dt: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.655803+00:00'}
target_configs: []
target_scans: []
targets: []
tgt_index: 0
tgt_scan: 0
timeout_ms_config: 120000
timeout_ms_scan: 120000
title: ''
total_integration_time: 0.0
tsys_calibrators: []
user_email: ''
```

---

## ProcessorModel

**Source:** `proc.py`

**Description:** A class representing the processor model.

### ProcessorModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "ProcessorModel"), |
| `current_event` | str |  "current_event": Or(None, And(str, lambda v: isinstance(v, str))), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `name` | str |  "name": And(str, lambda v: isinstance(v, str)), |
| `processing_time_ms` | float |  "processing_time_ms": Or(None, And(float, lambda v: v >= 0.0)), |

### Default Values

```python
_type: 'ProcessorModel'
current_event: ''
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.658302+00:00'}
name: ''
processing_time_ms: 0.0
```

---

## ResourceAllocations

**Source:** `tm.py`

**Description:** A class representing the resource allocation model.
        Contains a list of allocated resources (dishes, digitisers, etc.)

        Each allocation takes the form of a dictionary with keys:
            - resource_type: str
            - resource_id: str
            - allocated_type: str
            - allocated_id: str

### ResourceAllocations Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "ResourceAllocations"), |
| `alloc_list` | list |  "alloc_list": And(list, lambda v: isinstance(v, list)),          # List of resource allocations |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |

### Default Values

```python
_type: 'ResourceAllocations'
alloc_list: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.658552+00:00'}
```

---

## ScanModel

**Source:** `scan.py`

**Description:** A class representing the scan model.

### ScanModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "ScanModel"), |
| `center_freq` | float | int |  "center_freq": And(Or(int, float), lambda v: v >= 0.0), |
| `channels` | int |  "channels": And(int, lambda v: v >= 0), |
| `created` | datetime |  "created": And(datetime, lambda v: isinstance(v, datetime)), |
| `dig_id` | str |  "dig_id": Or(None, And(str, lambda v: isinstance(v, str))), |
| `duration` | float | int |  "duration": And(Or(int, float), lambda v: v >= 0), |
| `end_freq` | float | int |  "end_freq": Or(None, And(Or(int, float), lambda v: v >= 0.0)), |
| `files_directory` | str |  "files_directory": Or(None, And(str, lambda v: isinstance(v, str))), |
| `files_prefix` | str |  "files_prefix": Or(None, And(str, lambda v: isinstance(v, str))), |
| `freq_scan` | int |  "freq_scan": Or(None, And(int, lambda v: isinstance(v, int))), |
| `gain` | float | int |  "gain": And(Or(int, float), lambda v: 0 <= v <= 100.0), |
| `gap` | float |  "gap": Or(None, And(float, lambda v: isinstance(v, float))), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `load` | bool |  "load": And(bool, lambda v: isinstance(v, bool)), |
| `load_failures` | int |  "load_failures": And(int, lambda v: v >= 0), |
| `obs_id` | str |  "obs_id": Or(None, And(str, lambda v: isinstance(v, str))), |
| `read_end` | datetime |  "read_end": Or(None, And(datetime, lambda v: isinstance(v, datetime))), |
| `read_start` | datetime |  "read_start": Or(None, And(datetime, lambda v: isinstance(v, datetime))), |
| `sample_rate` | float | int |  "sample_rate": And(Or(int, float), lambda v: v >= 0.0), |
| `scan_iter` | int |  "scan_iter": Or(None, And(int, lambda v: isinstance(v, int))), |
| `start_freq` | float | int |  "start_freq": Or(None, And(Or(int, float), lambda v: v >= 0.0)), |
| `start_idx` | int |  "start_idx": And(int, lambda v: v >= 0), |
| `status` | ScanState |  "status": And(ScanState, lambda v: isinstance(v, ScanState)), |
| `tgt_index` | int |  "tgt_index": Or(None, And(int, lambda v: isinstance(v, int))), |

### Default Values

```python
_type: 'ScanModel'
center_freq: 0.0
channels: 0
created: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.627209+00:00'}
dig_id: None
duration: 0
end_freq: 0.0
files_directory: None
files_prefix: None
freq_scan: -1
gain: 0.0
gap: None
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.627210+00:00'}
load: False
load_failures: 0
obs_id: '2026-01-08T17:34:22.627204+00:00'
read_end: None
read_start: None
sample_rate: 0.0
scan_iter: -1
start_freq: 0.0
start_idx: 0
status: {'_type': 'enum.IntEnum', 'instance': 'ScanState', 'value': 'EMPTY'}
tgt_index: -1
```

---

## ScanStore

**Source:** `oda.py`

**Description:** A class representing the scan store.

### ScanStore Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "ScanStore"), |
| `gain_files` | list |  "gain_files": And(list, lambda v: isinstance(v, list)),         # List of *gain.csv (gain calibration) filenames |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `load_files` | list |  "load_files": And(list, lambda v: isinstance(v, list)),         # List of *load.csv (terminated signal chain) filenames |
| `meta_files` | list |  "meta_files": And(list, lambda v: isinstance(v, list)),         # List of *meta.csv (scan metadata) filenames |
| `spr_files` | list |  "spr_files": And(list, lambda v: isinstance(v, list)),          # List of *spr.csv (summed power) filenames |
| `tsys_files` | list |  "tsys_files": And(list, lambda v: isinstance(v, list)),         # List of *tsys.csv (system temperature calibration) filenames |

### Default Values

```python
_type: 'ScanStore'
gain_files: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.661533+00:00'}
load_files: []
meta_files: []
spr_files: []
tsys_files: []
```

---

## ScienceDataProcessorModel

**Source:** `sdp.py`

**Description:** A class representing the science data processor model.

### ScienceDataProcessorModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "ScienceDataProcessorModel"), |
| `app` | AppModel |  "app": And(AppModel, lambda v: isinstance(v, AppModel)), |
| `dig_store` | DigitiserList |  "dig_store": And(DigitiserList, lambda v: isinstance(v, DigitiserList)), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `processing_scans` | list |  "processing_scans": And(list, lambda v: all(isinstance(item, ScanModel) for item in v)), |
| `scans_aborted` | int |  "scans_aborted": And(int, lambda v: v >= 0), |
| `scans_completed` | int |  "scans_completed": And(int, lambda v: v >= 0), |
| `scans_created` | int |  "scans_created": And(int, lambda v: v >= 0), |
| `sdp_id` | str |  "sdp_id": And(str, lambda v: isinstance(v, str)), |
| `tm_connected` | CommunicationStatus |  "tm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |

### Default Values

```python
_type: 'ScienceDataProcessorModel'
app: {'_type': 'AppModel', 'app_name': 'sdp', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.662580+00:00'}}
dig_store: {'_type': 'DigitiserList', 'list_id': 'sdplist001', 'dig_list': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.662837+00:00'}}
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.662886+00:00'}
processing_scans: []
scans_aborted: 0
scans_completed: 0
scans_created: 0
sdp_id: '<undefined>'
tm_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
```

---

## TargetConfig

**Source:** `target.py`

**Description:** A class representing a target configuration.

### TargetConfig Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "TargetConfig"), |
| `bandwidth` | float | int |  "bandwidth": And(Or(int, float), lambda v: v >= 0.0),                   # Bandwidth (Hz) |
| `center_freq` | float | int |  "center_freq": And(Or(int, float), lambda v: v >= 0.0),                 # Center frequency (Hz) |
| `feed` | Feed |  "feed": And(Feed, lambda v: isinstance(v, Feed)),                       # Feed enum |
| `gain` | float | int |  "gain": And(Or(int, float), lambda v: v >= 0.0),                        # Gain (dBi) |
| `integration_time` | float | int |  "integration_time": And(Or(int, float), lambda v: v >= 0.0),            # Integration time (seconds) |
| `sample_rate` | float | int |  "sample_rate": And(Or(int, float), lambda v: v >= 0.0),                 # Sample rate (Hz) |
| `spectral_resolution` | int |  "spectral_resolution": And(int, lambda v: v >= 0),                      # Spectral resolution (fft size) |
| `tgt_idx` | int |  "tgt_idx": And(int, lambda v: v >= -1),                                 # Target list index (-1 = not set, 0-based) |

### Default Values

```python
_type: 'TargetConfig'
bandwidth: 0.0
center_freq: 0.0
feed: {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'NONE'}
gain: 0.0
integration_time: 0.0
sample_rate: 0.0
spectral_resolution: 0
tgt_idx: -1
```

---

## TargetModel

**Source:** `target.py`

**Description:** A class representing a target model.

### TargetModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "TargetModel"), |
| `altaz` | dict |  "altaz": Or(None, dict, lambda v: v is None or isinstance(v, (dict, SkyCoord))), # Alt-az coordinates (SkyCoord or AltAz) |
| `id` | str |  "id": Or(None, And(str, lambda v: isinstance(v, str))),                          # Target identifier e.g. "Sun", "Moon", "Mars", "Vega" |
| `pointing` | PointingType |  "pointing": And(PointingType, lambda v: isinstance(v, PointingType)),            # Target type |
| `sky_coord` | Mixed |  "sky_coord": Or(None, lambda v: v is None or isinstance(v, SkyCoord)),           # Sky coordinates (any frame) |
| `tgt_idx` | int |  "tgt_idx": And(int, lambda v: v >= -1),                                          # Target list index (-1 = not set, 0-based) |

### Default Values

```python
_type: 'TargetModel'
altaz: None
id: None
pointing: {'_type': 'enum.IntEnum', 'instance': 'PointingType', 'value': 'DRIFT_SCAN'}
sky_coord: None
tgt_idx: -1
```

---

## TargetScanSet

**Source:** `target.py`

**Description:** A class representing a set of scans for a particular target.

### TargetScanSet Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "TargetScanSet"), |
| `freq_max` | float | int |  "freq_max": And(Or(None, float, int), lambda v: v is None or v >= 0.0),      # End of frequency scanning (Hz) |
| `freq_min` | float | int |  "freq_min": And(Or(None, float, int), lambda v: v is None or v >= 0.0),      # Start of frequency scanning (Hz) |
| `freq_overlap` | float | int |  "freq_overlap": And(Or(None, float, int), lambda v: v is None or v >= 0.0),  # Overlap between frequency scans (Hz) |
| `freq_scans` | int |  "freq_scans": And(Or(None, int), lambda v: v is None or v >= 0),             # Number of frequency scans |
| `scan_duration` | float | int |  "scan_duration": And(Or(None, float, int), lambda v: v is None or v >= 0.0), # Duration of each scan (seconds) |
| `scan_iterations` | int |  "scan_iterations": And(Or(None, int), lambda v: v is None or v >= 0),        # Number of scan iterations (within a frequency scan) |
| `scans` | list |  "scans": And(list, lambda v: isinstance(v, list)),                           # List of scans to be performed for this target |
| `tgt_idx` | int |  "tgt_idx": And(int, lambda v: v >= -1),                                      # Target list index (-1 = not set, 0-based) |

### Default Values

```python
_type: 'TargetScanSet'
freq_max: None
freq_min: None
freq_overlap: None
freq_scans: 0
scan_duration: None
scan_iterations: 0
scans: []
tgt_idx: -1
```

---

## TelescopeManagerModel

**Source:** `tm.py`

**Description:** A class representing the telescope manager model.

### TelescopeManagerModel Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | str |  "_type": And(str, lambda v: v == "TelescopeManagerModel"), |
| `allocations` | ResourceAllocations |  "allocations": And(ResourceAllocations, lambda v: isinstance(v, ResourceAllocations)), |
| `app` | AppModel |  "app": And(AppModel, lambda v: isinstance(v, AppModel)), |
| `dig_connected` | CommunicationStatus |  "dig_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |
| `dm_connected` | CommunicationStatus |  "dm_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |
| `id` | str |  "id": And(str, lambda v: isinstance(v, str)), |
| `last_update` | datetime |  "last_update": And(datetime, lambda v: isinstance(v, datetime)), |
| `sdp_connected` | CommunicationStatus |  "sdp_connected": And(CommunicationStatus, lambda v: isinstance(v, CommunicationStatus)), |

### Default Values

```python
_type: 'TelescopeManagerModel'
allocations: {'_type': 'ResourceAllocations', 'alloc_list': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.665966+00:00'}}
app: {'_type': 'AppModel', 'app_name': 'tm', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T17:34:22.665720+00:00'}}
dig_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
dm_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
id: '<undefined>'
last_update: {'_type': 'datetime', 'value': '2026-01-08T17:34:22.665999+00:00'}
sdp_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
```

---

