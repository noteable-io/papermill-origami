# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
