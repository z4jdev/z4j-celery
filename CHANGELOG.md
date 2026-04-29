# Changelog

All notable changes to `z4j-celery` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.2] - 2026-04-28

### Added

- **`z4j-celery` console script** + `python -m z4j_celery` module
  form. Both work and dispatch to the same code path. Subcommands:
  - `doctor` - check upstream `celery` library + adapter import + broker URL
  - `check` - alias for doctor
  - `status` - one-line: package presence + broker URL state
  - `version` - print z4j-celery version
  Engines are libraries (no agent runtime to manage), so the CLI is
  intentionally narrower than a framework's: no `run`, no `restart`.
  The framework's doctor (z4j-django, z4j-flask, z4j-fastapi) calls
  into these same probes automatically when celery is the detected
  engine.


## [1.1.0] - 2026-04-28

### Fixed

- **Agent-HIGH-1 (round-6 audit): broker poison-message ack+drop+counter.** Pre-fix ``events/broker.py:166`` requeued a poisoned (un-decodable) message AND re-raised into Kombu, causing a tight ~2-second reconnect loop in the customer's Celery worker process - visible CPU/IO storm in the host stack. Now the agent's broker-events listener acks the poisoned message, drops it, and bumps a counter (``z4j_celery.broker_events_poisoned_total``) so operators can see the rate. The worker stays running. Operators with high-cardinality task names should monitor that counter.
- **Agent-HIGH-2 (round-6 audit): PID-guard prefork stale loop.** ``engine.py:122`` captured the asyncio loop reference in the parent process; Celery's prefork pool then forked workers that inherited the stale ref. Every task in prefork mode logged an error stacktrace; events could be silently dropped if the parent had moved on. New PID-guard refuses to use a captured loop from a different PID and re-creates the sink lazily in the child.
- **(Pre-round-6) ``submit_task`` now honors ``task_always_eager``.** Pre-fix the adapter unconditionally called ``app.send_task(name, ...)`` which bypasses the local task registry - so ``task_always_eager=True`` (used in CI / dev mode) had no effect on brain-dispatched fires, and the operator's local-test setup couldn't verify execution. Now prefers ``app.tasks[name].apply_async(...)`` when the task is locally registered (also picks up the task's decorator options: default queue, retry policy, time limits) and falls back to ``send_task`` for at-distance scheduling. Wire-identical against a real broker; only behavior change is for in-process eager mode.

### Changed

- **v1.1.0 ecosystem family bump.** Pinned ``z4j-core>=1.1.0`` and ``z4j-bare>=1.1.0`` so a Celery engine installed at 1.1.0 always resolves a known-good 1.1.0 slice of brain + agent. The driving fix lives in z4j-bare 1.1.0: the agent dispatcher now correctly routes ``schedule.fire`` to the queue engine's ``submit_task`` (this adapter), instead of rejecting every brain-side scheduler tick before the engine ever saw it. Operators on brain 1.1.0 + scheduler 1.1.0 with z4j-celery 1.0.x had every scheduled task silently fail at the agent dispatcher - this floor refuses that mixed install.

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
