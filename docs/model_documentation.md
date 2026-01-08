# Model Documentation

This document describes all data models in the project.

## Allocation

**Source:** `tm.py`

**Description:** A class representing a single resource allocation.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `allocated_id` | Mixed |  |
| `allocated_type` | Mixed |  |
| `expires` | Mixed |  |
| `last_update` | Mixed |  |
| `resource_id` | Mixed |  |
| `resource_type` | Mixed |  |
| `state` | Mixed |  |

### Default Values

```python
_type: 'Allocation'
allocated_id: '<undefined>'
allocated_type: '<undefined>'
expires: None
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.709610+00:00'}
resource_id: '<undefined>'
resource_type: '<undefined>'
state: {'_type': 'enum.IntEnum', 'instance': 'AllocationState', 'value': 'REQUESTED'}
```

---

## AppModel

**Source:** `app.py`

**Description:** A class representing an App(lication) model.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `app_name` | Mixed |  |
| `app_running` | Mixed |  |
| `arguments` | Mixed |  |
| `health` | Mixed |  |
| `interfaces` | Mixed |  |
| `last_update` | Mixed |  |
| `msg_timeout_ms` | Mixed |  |
| `num_processors` | Mixed |  |
| `processors` | Mixed |  |
| `queue_size` | Mixed |  |

### Default Values

```python
_type: 'AppModel'
app_name: 'app'
app_running: False
arguments: None
health: {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}
interfaces: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.719181+00:00'}
msg_timeout_ms: 10000
num_processors: 0
processors: []
queue_size: 0
```

---

## DigitiserList

**Source:** `dig.py`

**Description:** A class representing a list of digitisers.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `dig_list` | Mixed |  |
| `last_update` | Mixed |  |
| `list_id` | Mixed |  |

### Default Values

```python
_type: 'DigitiserList'
dig_list: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.719502+00:00'}
list_id: '<undefined>'
```

---

## DigitiserModel

**Source:** `dig.py`

**Description:** A class representing a digitiser application. The digitiser application is deployed at the telescope to digitise the analog RF signals.
        The digitiser is controlled by the Telescope Manager.    
        The digitiser streams digitised RF data to the SDP for processing.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `app` | Mixed |  |
| `bandwidth` | Mixed |  |
| `center_freq` | Mixed |  |
| `channels` | Mixed |  |
| `dig_id` | Mixed |  |
| `freq_correction` | Mixed |  |
| `gain` | Mixed |  |
| `last_update` | Mixed |  |
| `load` | Mixed |  |
| `sample_rate` | Mixed |  |
| `scan_duration` | Mixed |  |
| `scanning` | Mixed |  |
| `sdp_connected` | Mixed |  |
| `sdr_connected` | Mixed |  |
| `sdr_eeprom` | Mixed |  |
| `tm_connected` | Mixed |  |

### Default Values

```python
_type: 'DigitiserModel'
app: {'_type': 'AppModel', 'app_name': 'dig', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.719579+00:00'}}
bandwidth: 0.0
center_freq: 0.0
channels: 0
dig_id: '<undefined>'
freq_correction: 0
gain: 0.0
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.719823+00:00'}
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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `dish_list` | Mixed |  |
| `last_update` | Mixed |  |

### Default Values

```python
_type: 'DishList'
dish_list: [{'_type': 'DishModel', 'dsh_id': 'dish001', 'short_desc': '70cm Discovery Dish', 'diameter': 0.7, 'fd_ratio': 0.37, 'latitude': 53.187052, 'longitude': -2.256079, 'height': 94.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'H3T_1420'}, 'dig_id': 'dig001', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.720437+00:00'}}, {'_type': 'DishModel', 'dsh_id': 'dish002', 'short_desc': '3m Jodrell Dish', 'diameter': 3.0, 'fd_ratio': 0.43, 'latitude': 53.2421, 'longitude': -2.3067, 'height': 80.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'NONE'}, 'dig_id': 'dig002', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.720902+00:00'}}]
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.721348+00:00'}
```

---

## DishManagerModel

**Source:** `dsh.py`

**Description:** A class representing the dish manager (application) model.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `app` | Mixed |  |
| `dish_store` | Mixed |  |
| `id` | Mixed |  |
| `last_update` | Mixed |  |
| `tm_connected` | Mixed |  |

### Default Values

```python
_type: 'DishManagerModel'
app: {'_type': 'AppModel', 'app_name': 'dshmgr', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.722463+00:00'}}
dish_store: {'_type': 'DishList', 'dish_list': [{'_type': 'DishModel', 'dsh_id': 'dish001', 'short_desc': '70cm Discovery Dish', 'diameter': 0.7, 'fd_ratio': 0.37, 'latitude': 53.187052, 'longitude': -2.256079, 'height': 94.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'H3T_1420'}, 'dig_id': 'dig001', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.721477+00:00'}}, {'_type': 'DishModel', 'dsh_id': 'dish002', 'short_desc': '3m Jodrell Dish', 'diameter': 3.0, 'fd_ratio': 0.43, 'latitude': 53.2421, 'longitude': -2.3067, 'height': 80.0, 'feed': {'_type': 'enum.IntEnum', 'instance': 'Feed', 'value': 'NONE'}, 'dig_id': 'dig002', 'mode': {'_type': 'enum.IntEnum', 'instance': 'DishMode', 'value': 'STANDBY_FP'}, 'pointing_state': {'_type': 'enum.IntEnum', 'instance': 'PointingState', 'value': 'UNKNOWN'}, 'altaz': None, 'capability_state': {'_type': 'enum.IntEnum', 'instance': 'CapabilityState', 'value': 'OPERATE_FULL'}, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.721930+00:00'}}], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.722422+00:00'}}
id: '<undefined>'
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.722710+00:00'}
tm_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
```

---

## DishModel

**Source:** `dsh.py`

**Description:** A class representing the dish model.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `altaz` | Mixed |  |
| `capability_state` | Mixed |  |
| `diameter` | Mixed |  |
| `dig_id` | Mixed |  |
| `dsh_id` | Mixed |  |
| `fd_ratio` | Mixed |  |
| `feed` | Mixed |  |
| `height` | Mixed |  |
| `last_update` | Mixed |  |
| `latitude` | Mixed |  |
| `longitude` | Mixed |  |
| `mode` | Mixed |  |
| `pointing_state` | Mixed |  |
| `short_desc` | Mixed |  |

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
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.722955+00:00'}
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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `last_update` | Mixed |  |
| `obs_store` | Mixed |  |
| `scan_store` | Mixed |  |

### Default Values

```python
_type: 'ODAModel'
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.723608+00:00'}
obs_store: {'_type': 'ObsList', 'obs_list': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.723574+00:00'}}
scan_store: {'_type': 'ScanStore', 'spr_files': [], 'load_files': [], 'tsys_files': [], 'gain_files': [], 'meta_files': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.723457+00:00'}}
```

---

## OETModel

**Source:** `oet.py`

**Description:** A class representing the observation execution tool model.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `app` | Mixed |  |
| `id` | Mixed |  |
| `last_update` | Mixed |  |
| `obs_aborted` | Mixed |  |
| `obs_completed` | Mixed |  |
| `obs_created` | Mixed |  |
| `processing_obs` | Mixed |  |
| `tm_connected` | Mixed |  |

---

## ObsList

**Source:** `oda.py`

**Description:** A class representing a list of observations.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `last_update` | Mixed |  |
| `obs_list` | Mixed |  |

### Default Values

```python
_type: 'ObsList'
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.723990+00:00'}
obs_list: []
```

---

## Observation

**Source:** `obs.py`

**Description:** A class representing a model of an observation

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `capabilities` | Mixed |  |
| `created` | Mixed |  |
| `description` | Mixed |  |
| `diameter` | Mixed |  |
| `dish_id` | Mixed |  |
| `end_dt` | Mixed |  |
| `estimated_observation_duration` | Mixed |  |
| `estimated_slewing_time` | Mixed |  |
| `f/d_ratio` | Mixed |  |
| `gain_calibrators` | Mixed |  |
| `last_update` | Mixed |  |
| `latitude` | Mixed |  |
| `load_calibrators` | Mixed |  |
| `longitude` | Mixed |  |
| `obs_id` | Mixed |  |
| `obs_state` | Mixed |  |
| `scheduling_block_end` | Mixed |  |
| `scheduling_block_start` | Mixed |  |
| `spr_scans` | Mixed |  |
| `start_dt` | Mixed |  |
| `target_configs` | Mixed |  |
| `target_scans` | Mixed |  |
| `targets` | Mixed |  |
| `tgt_index` | Mixed |  |
| `tgt_scan` | Mixed |  |
| `timeout_ms_config` | Mixed |  |
| `timeout_ms_scan` | Mixed |  |
| `title` | Mixed |  |
| `total_integration_time` | Mixed |  |
| `tsys_calibrators` | Mixed |  |
| `user_email` | Mixed |  |

### Default Values

```python
_type: 'Observation'
capabilities: ''
created: None
description: ''
diameter: 0.0
dish_id: None
end_dt: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.724056+00:00'}
estimated_observation_duration: '00:00:00'
estimated_slewing_time: 0.0
f/d_ratio: 0.0
gain_calibrators: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.724056+00:00'}
latitude: 0.0
load_calibrators: []
longitude: 0.0
obs_id: None
obs_state: {'_type': 'enum.IntEnum', 'instance': 'ObsState', 'value': 'EMPTY'}
scheduling_block_end: None
scheduling_block_start: None
spr_scans: []
start_dt: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.724055+00:00'}
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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `current_event` | Mixed |  |
| `last_update` | Mixed |  |
| `name` | Mixed |  |
| `processing_time_ms` | Mixed |  |

### Default Values

```python
_type: 'ProcessorModel'
current_event: ''
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.725942+00:00'}
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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `alloc_list` | Mixed |  |
| `last_update` | Mixed |  |

### Default Values

```python
_type: 'ResourceAllocations'
alloc_list: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.726044+00:00'}
```

---

## ScanModel

**Source:** `scan.py`

**Description:** A class representing the scan model.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `center_freq` | Mixed |  |
| `channels` | Mixed |  |
| `created` | Mixed |  |
| `dig_id` | Mixed |  |
| `duration` | Mixed |  |
| `end_freq` | Mixed |  |
| `files_directory` | Mixed |  |
| `files_prefix` | Mixed |  |
| `freq_scan` | Mixed |  |
| `gain` | Mixed |  |
| `gap` | Mixed |  |
| `last_update` | Mixed |  |
| `load` | Mixed |  |
| `load_failures` | Mixed |  |
| `obs_id` | Mixed |  |
| `read_end` | Mixed |  |
| `read_start` | Mixed |  |
| `sample_rate` | Mixed |  |
| `scan_iter` | Mixed |  |
| `start_freq` | Mixed |  |
| `start_idx` | Mixed |  |
| `status` | Mixed |  |
| `tgt_index` | Mixed |  |

### Default Values

```python
_type: 'ScanModel'
center_freq: 0.0
channels: 0
created: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.706995+00:00'}
dig_id: None
duration: 0
end_freq: 0.0
files_directory: None
files_prefix: None
freq_scan: -1
gain: 0.0
gap: None
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.706996+00:00'}
load: False
load_failures: 0
obs_id: '2026-01-08T16:11:31.706989+00:00'
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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `gain_files` | Mixed |  |
| `last_update` | Mixed |  |
| `load_files` | Mixed |  |
| `meta_files` | Mixed |  |
| `spr_files` | Mixed |  |
| `tsys_files` | Mixed |  |

### Default Values

```python
_type: 'ScanStore'
gain_files: []
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.727254+00:00'}
load_files: []
meta_files: []
spr_files: []
tsys_files: []
```

---

## ScienceDataProcessorModel

**Source:** `sdp.py`

**Description:** A class representing the science data processor model.

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `app` | Mixed |  |
| `dig_store` | Mixed |  |
| `last_update` | Mixed |  |
| `processing_scans` | Mixed |  |
| `scans_aborted` | Mixed |  |
| `scans_completed` | Mixed |  |
| `scans_created` | Mixed |  |
| `sdp_id` | Mixed |  |
| `tm_connected` | Mixed |  |

### Default Values

```python
_type: 'ScienceDataProcessorModel'
app: {'_type': 'AppModel', 'app_name': 'sdp', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.727390+00:00'}}
dig_store: {'_type': 'DigitiserList', 'list_id': 'sdplist001', 'dig_list': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.727626+00:00'}}
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.727674+00:00'}
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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `bandwidth` | Mixed |  |
| `center_freq` | Mixed |  |
| `feed` | Mixed |  |
| `gain` | Mixed |  |
| `integration_time` | Mixed |  |
| `sample_rate` | Mixed |  |
| `spectral_resolution` | Mixed |  |
| `tgt_idx` | Mixed |  |

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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `altaz` | Mixed |  |
| `id` | Mixed |  |
| `pointing` | Mixed |  |
| `sky_coord` | Mixed |  |
| `tgt_idx` | Mixed |  |

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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `freq_max` | Mixed |  |
| `freq_min` | Mixed |  |
| `freq_overlap` | Mixed |  |
| `freq_scans` | Mixed |  |
| `scan_duration` | Mixed |  |
| `scan_iterations` | Mixed |  |
| `scans` | Mixed |  |
| `tgt_idx` | Mixed |  |

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

### Schema Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_type` | Mixed |  |
| `allocations` | Mixed |  |
| `app` | Mixed |  |
| `dig_connected` | Mixed |  |
| `dm_connected` | Mixed |  |
| `id` | Mixed |  |
| `last_update` | Mixed |  |
| `sdp_connected` | Mixed |  |

### Default Values

```python
_type: 'TelescopeManagerModel'
allocations: {'_type': 'ResourceAllocations', 'alloc_list': [], 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.728715+00:00'}}
app: {'_type': 'AppModel', 'app_name': 'tm', 'app_running': False, 'health': {'_type': 'enum.IntEnum', 'instance': 'HealthState', 'value': 'UNKNOWN'}, 'num_processors': 0, 'queue_size': 0, 'interfaces': [], 'processors': [], 'msg_timeout_ms': 10000, 'arguments': None, 'last_update': {'_type': 'datetime', 'value': '2026-01-08T16:11:31.728482+00:00'}}
dig_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
dm_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
id: '<undefined>'
last_update: {'_type': 'datetime', 'value': '2026-01-08T16:11:31.728746+00:00'}
sdp_connected: {'_type': 'enum.IntEnum', 'instance': 'CommunicationStatus', 'value': 'NOT_ESTABLISHED'}
```

---

