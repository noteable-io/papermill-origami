# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Added origami client call to delete the active kernel session after successful notebook execution

## [0.0.11] - 2022-11-18
### Added
- Added API calls to update job instance attempt status during execution

## [0.0.10]
### Changed
- Upgrade `noteable-origami` to `0.0.14`

## [0.0.9] - 2022-11-01
## Changed
- Updated API references in origami from `types` to `defs`

## [0.0.8] - 2022-10-28
### Added
- Add support for flyte
- Allow specifying `job_instance_attempt` in engine kwargs
- Sync all papermill metadata to Noteable
- Add Noteable Dagster asset
- Sync outputs from Noteable into papermill
- Sync execution counts from cell state updates
- Add ability to parse Noteable https URLs to extract file id

### Changed
- Hide applied parameters cell by default for dagster

### Removed
- Remove dagstermill teardown cell

## [0.0.7] - 2022-10-05
### Changed
- Bump `noteable-origami` dependency to 0.0.6

## [0.0.6] - 2022-10-05
### Changed
- `execute_notebook` need not be passed in a `logger` instance explicitly, it will use the module-level logger if one is not provided.
- Skip executing empty code cells in `execute_notebook` to not get stuck waiting for the cell status update to come through
- Check `result.state.is_error_state` instead of `result.data.state.is_error_state` in `execute_notebook` due to corresponding changes in `noteable-origami`
- Ensure that `NoteableHandler` always has a `NoteableClient` instance defined when `NoteableHandler.read` is called
- Register `NoteableHandler` with scheme `noteable://` as entrypoint in `pyproject.toml`

## [0.0.5] - 2022-09-13
### Added
- Add callbacks to set kernel error outputs from Noteable into papermill
- Adds ability for papermill engine to automatically generate a NoteableClient
- Improved readme instructions

## [0.0.4] - 2022-09-09
### Fixed
- Fix python 3.8 compatibility for `removeprefix`

## [0.0.3] - 2022-09-09
### Added
- Support for dagstermill

## [0.0.2] - 2022-08-15
### Added
- Publish package to PyPI

## [0.0.1] - 2022-08-12
### Added
- Initial setup
