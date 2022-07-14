
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [unreleased] - yyyy-mm-dd

## [0.1.0-beta3.2.20] - 2022-07-14

### Changed
- Implemented generic fields filter for multiple GET endpoints

## [0.1.0-beta3.2.19] - 2022-07-12

### Fixed
- Fixed bug with filtering fields for all output endpoint

## [0.1.0-beta3.2.18] - 2022-07-12

### Added
- Fields filter function, applied to series output `ALL` GET endpoint
- Return `job_config_name` and `job_type` when returning all series output

## [0.1.0-beta3.2.17] - 2022-07-07

### Added
- Series `meta` property for meta data
- Fields filter function, applied to series templates GET endpoint

## [0.1.0-beta3.2.16] - 2022-06-30

### Added
- Series templates

### Changed
- Series can now be added with a written config or by reference to an existing template

## [0.1.0-beta3.2.15] - 2022-06-23

### Changed
- Changed resource caching by max time since last use to an LRU queue cache

### Added
- Job config template resource and endpoints

## [0.1.0-beta3.2.14] - 2022-06-21

### Added
- Endpoint for fetching all analysis results for a series (for all job types)
- Query param `future` for the `api/series/anomaly_test_series/forecasts` endpoint, to only return points in the future
- Added thingsdb support for saving hub state

## [0.1.0-beta3.2.13] - 2022-06-08

### Added
- Added functionality to add and remove job configs to a series

## [0.1.0-beta3.2.12] - 2022-05-23

### Fixed
- Adding series with an empty SeriesState instead of an dict
- Checking online status of client on loading clientlist from disk

## [0.1.0-beta3.2.11] - 2022-05-23

### Fixed
- Ignore `requires_job` when config is not found (invalid job name)

### Changed
- Using `time.time()` instead of `datetime.datetime.now()` in rest of project

## [0.1.0-beta3.2.10] - 2022-05-19

### Fixed
- Accept empty anomalies result

### Added
- Typing

### Changed
- Using `time.time()` instead of `datetime.datetime.now()` for client timestamps

## [0.1.0-beta3.2.9] - 2022-05-19

### Added
- Logging hub version on startup
- Save client data to disk
- Update worker data on reconnect

## [0.1.0-beta3.2.8] - 2022-05-18

### Added
- `time_precision` support, send to worker in job request.
- Implemented new lib for `time_precision` support and `value_type` for `EnodoModuleArgument`

## [0.1.0-beta3.2.7] - 2022-05-17

### Fixed
- Added exception handling when loading series from disk, ignoring invalid data

## [0.1.0-beta3.2.6] - 2022-05-17

### Changed
- Removed module manager, using clientmanager to fetch current known modules
- Implemented new lib version, to check if module version has been specified with the module name `<module_name>@<module_version>`
- Receive only 1 module data

### Added
- Implemented new version of lib; Check if a jobs config_name contains any spaces

## [0.1.0-beta3.2.5] - 2022-05-09

### Added
- Support for adding series with unknown modules

### Changed
- Known modules in memory only, not saved to disk
- SiriDB config renamed to `siridb_data`

## [0.1.0-beta3.2.4] - 2022-05-06

### Changed
- Removed siridb password from get settings base handler output
- Refactored manager locks
- Removed legacy code
- Moved siridb settings to config file, and `max_in_queue_before_warning` and `min_data_points` to the settings file

## [0.1.0-beta3.2.3] - 2022-05-02

### Added
- Implemented use_max_points property in job configs
  
### Fixed
- Bug with worker is_going_busy status on reconnect
- Fixed return http status on not found when fetching series details
- Fixed issue with siridb connect when changing settings

## [0.1.0-beta3.2.2] - 2022-04-29

### Added
- Seperate endpoints for fetching analysis result data
- Cleanup on series removal (siridb analytics results)
- Endpoint as proxy for querying SiriDB data db
- Friendly shutdown to clear queue
- Force shutdown support
- Online status of clients exposed to API

### Changed
- Changed series config and job config. Job config is now a list of jobs. Not categorised by job type. Forecast, anomaly detection jobs can be in the list multiple times. 
- Jobs get an unique identifier `config_name`, which can be set to now the `config_name` beforehand
- Jobs can be silenced by the silenced property in the job config. This makes sure no events will be created for the job's results.
- A job can require an other job the be run beforehand by using the `requires_job` property
- Config section `enodo` renamed to `hub` because of env prefix `ENODO_`
- Job response handling updated
- Refactored model to be renamed to module

### Fixed
- PEP8 guidelines
- Fixed socketio serialisation of config classes on emit
- SiriDB output db selection/config

## [0.1.0-beta3.0.0] - 2021-08-13

### Added
- Added realtime property to series config, to control speed of updates from listener to hub for that series

### Changed
- Arguments underscore to dash
- Using Prophet package instead of fbprophet

### Fixed

- Guard statement adding serie handler

## [0.1.0-beta2.1.1] - 2021-03-19

### Fixed

- Fixed siridb connection status update for subscribers socket.io before socket.io is setup

## [0.1.0-beta2.1] - 2021-03-17
  
Migration: series config's are changed completely. Removing series.json from the data folder is necessary. You will need to re-add the series after this.
 
### Added

- Support for series management page in the GUI
 
### Changed
  
- Series config is changed. Mainly the job config is setup per job type. This includes `model_params`
- Package size is change, removed unnecessary bytes
 
### Fixed
 
- Bug with job management
- Model fetching
 
## [0.1.0-beta2.0] - 2021-03-04
  
Migration: config file changed, all siridb related config is in a seperate settings.enodo file in the root of the data folder you give up within the conf file.
 
### Added

- FFE model for faster forcasting and anomaly detection
- Static rules (min/max thresholds)
- Environment variable support
 
### Changed
  
- Siridb connection settings moved to `settings.enodo file`, these settings can be changed during runtime
 
### Fixed
 
- Bug with listener handshake
- Cleanup