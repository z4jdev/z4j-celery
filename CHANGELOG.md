# Changelog

All notable changes to `z4j-celery` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.3] - 2026-04-24

### Fixed

- **Worker agents now report the correct host framework.** A Django+Celery worker process now sends `framework: django` in its hello frame; same for Flask+Celery (`flask`) and FastAPI+Celery (`fastapi`). Standalone-Celery (no web framework) still reports `bare`. Previously every Celery worker reported `framework: bare` because z4j-bare's `install_agent` had no way to override the default `BareFrameworkAdapter`. The dashboard's Framework column now shows the operator's actual stack instead of always "bare". Fix is wire-level - re-mint of agent tokens not required.

### Changed

- `_on_worker_init` now passes the resolved framework class (via the existing `_resolve_framework_adapter` precedence chain: FastAPI → Flask → Django → bare) to `install_agent(framework=...)`. Requires z4j-bare >= 1.0.5 for the new kwarg.
- Bumped minimum `z4j-core` to `>=1.0.3` and `z4j-bare` to `>=1.0.5`.

## [1.0.1] - 2026-04-21

### Changed

- Lowered minimum Python version from 3.13 to 3.11. This package now supports Python 3.11, 3.12, 3.13, and 3.14.
- Documentation polish: standardized on ASCII hyphens across README, CHANGELOG, and docstrings for consistent rendering on PyPI.


## [1.0.0] - 2026-04

### Added

<!--
TODO: describe what ships in this first public release. One bullet per
capability. Examples:
- First public release.
- <Headline feature>
- <Second feature>
- N unit tests.
-->

- First public release.

## Links

- Repository: <https://github.com/z4jdev/z4j-celery>
- Issues: <https://github.com/z4jdev/z4j-celery/issues>
- PyPI: <https://pypi.org/project/z4j-celery/>

[Unreleased]: https://github.com/z4jdev/z4j-celery/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/z4jdev/z4j-celery/releases/tag/v1.0.0
