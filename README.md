# z4j-celery

[![PyPI version](https://img.shields.io/pypi/v/z4j-celery.svg)](https://pypi.org/project/z4j-celery/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celery.svg)](https://pypi.org/project/z4j-celery/)
[![License](https://img.shields.io/pypi/l/z4j-celery.svg)](https://github.com/z4jdev/z4j-celery/blob/main/LICENSE)


**License:** Apache 2.0
**Status:** v1.0.0 - first public release. The flagship queue-engine adapter.

The [Celery](https://docs.celeryq.dev/) queue-engine adapter for
[z4j](https://z4j.com). Captures every task lifecycle event via Celery's
signal system, executes control actions (retry, cancel, bulk retry,
purge, pool restart) against your live Celery app, and discovers tasks
via a five-layer pipeline that handles signal registration, broker
events, and static AST scans.

## Install

```bash
pip install z4j-celery z4j-celerybeat
```

Pair with a framework adapter:

```bash
pip install z4j-django z4j-celery z4j-celerybeat      # Django
pip install z4j-flask  z4j-celery z4j-celerybeat      # Flask
pip install z4j-fastapi z4j-celery z4j-celerybeat     # FastAPI
pip install z4j-bare  z4j-celery z4j-celerybeat       # Bare worker
```

## What it ships on day 1

| Capability | Status | Notes |
|---|---|---|
| Event capture | ✅ | Dual-capture: Celery signals (`task_received`, `task_succeeded`, `task_failed`, `task_retried`, `task_revoked`) + broker events for multi-worker deployments |
| Discovery | ✅ | Five-layer pipeline: runtime `celery_app.tasks`, static `tasks.py` AST scan, signal registration, merged task registry |
| `submit_task` / `retry_task` / `cancel_task` | ✅ | Direct against the Celery app |
| `bulk_retry` | ✅ | Filter-driven; re-enqueues matching tasks |
| `purge_queue` | ✅ | With confirm-token + `Z4J_PURGE_THRESHOLD` guard |
| `requeue_dead_letter` | ✅ | From the configured DLX |
| `restart_worker` | ✅ | Broadcast `pool_restart` - **zero task loss**, child processes drain in-flight tasks before respawn |
| `pool_grow` / `pool_shrink` | ✅ | Via Celery's control API |
| `rate_limit` | ✅ | Broker-side via Celery's control channel - the only engine where this works correctly |
| `reconcile_task` | ✅ | Via `AsyncResult` against the result backend |

This is the widest feature coverage of any adapter. Celery's rich remote-
control surface lets z4j offer capabilities no other engine can match
(pool restart with zero task loss, broker-side rate limiting).

## Reliability

- No exception from the adapter propagates to Celery signal handlers.
- Events buffer to an in-memory ring; a background thread flushes to the
  brain. Signal handlers never do network I/O.
- When the brain is unreachable, events buffer locally (via `z4j-bare`'s
  SQLite buffer). Your Celery workers never slow down or block.

## Scheduler pairing

Use [`z4j-celerybeat`](https://github.com/z4jdev/z4j-celerybeat) for
Celery Beat / `django-celery-beat` schedules on the Schedules page.

## Documentation

- [Celery engine guide](https://z4j.dev/engines/celery/)
- [Architecture](https://z4j.dev/concepts/architecture/)
- [Adapter protocol](https://z4j.dev/concepts/adapter-axes/)

## License

Apache 2.0 - see [LICENSE](LICENSE). Your application is never
AGPL-tainted by importing `z4j_celery`.

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Issues: <https://github.com/z4jdev/z4j-celery/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
