
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [Unreleased] - yyyy-mm-dd

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