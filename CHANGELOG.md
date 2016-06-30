# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
- Node registration with ordered, raw string format
- Binary datapacket support, with token identification
- Backend storage with HDF5
- Supported datatypes: [u]int{8,16,32,64}, float (32b), double (64b), string (10 elements in hdf5)
- Server adds timestamps if no timestamp is sent or timestamp is 0
- Added data access API function to server

### Changed
- Timestamp default datatype changed to uint64 from int32
- Default storage changed to HDF5

## 0.1.0 - 2016-06-07
### Added
- Node communication and registration with JSON
- Backend storage with MySQL
- Supported datatypes: int, float, string

[Unreleased]: https://github.com/Orthogonal-Systems/Origin/compare/v0.1.0...HEAD
