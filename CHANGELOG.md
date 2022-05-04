
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [unreleased] - yyyy-mm-dd

### Changed
- Removed siridb password from get settings base handler output
- Refactored manager locks
- Removed legacy code

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