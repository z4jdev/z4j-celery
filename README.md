# z4j-celery

[![PyPI version](https://img.shields.io/pypi/v/z4j-celery.svg)](https://pypi.org/project/z4j-celery/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celery.svg)](https://pypi.org/project/z4j-celery/)
[![License](https://img.shields.io/pypi/l/z4j-celery.svg)](https://github.com/z4jdev/z4j-celery/blob/main/LICENSE)

The Celery engine adapter for [z4j](https://z4j.com).

Streams supported task lifecycle events from your Celery workers to the z4j
brain and accepts operator control actions from the dashboard. Pair
with z4j-celerybeat to manage periodic / cron schedules.

## Compatibility

- Celery 5.2.2+ (no upper cap)
- Python 3.11+

Full per-adapter matrix at <https://docs.z4j.com/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| Task lifecycle events | received, started, succeeded, failed, retried, revoked |
| Task discovery | runtime registry merge + static `tasks.py` scan |
| Submit / retry / cancel | direct against the Celery app |
| Bulk retry | retries brain-selected explicit task IDs individually; does not sweep the broker |
| Purge queue | with confirm-token guard |
| Requeue dead-letter | unsupported: broker-specific atomic consume/ack and routing are required |
| Restart worker | fire-and-forget `pool_restart`; requires Celery's `worker_pool_restarts` setting and operator verification |
| Pool grow / shrink | via Celery's control API |
| Rate limit | fire-and-forget Celery remote-control broadcast to the selected worker(s) |
| Reconcile task | via the result backend |

The widest feature coverage of any z4j engine adapter, Celery's remote-control
surface provides controls other engines cannot expose. Worker-control
broadcasts do not acknowledge execution: verify the worker state afterward.
`pool_restart` is intended for prefork deployments; Celery's gevent and
eventlet pools do not support it reliably.

## Install

```bash
pip install z4j-celery z4j-celerybeat
```

Pair with a framework adapter:

```bash
pip install z4j-django  z4j-celery z4j-celerybeat   # Django
pip install z4j-flask   z4j-celery z4j-celerybeat   # Flask
pip install z4j-fastapi z4j-celery z4j-celerybeat   # FastAPI
pip install z4j-bare    z4j-celery z4j-celerybeat   # framework-free worker
```

## Pairs with

- [`z4j-celerybeat`](https://github.com/z4jdev/z4j-celerybeat), schedule adapter for Celery Beat / django-celery-beat

## Retry prerequisites

Retry uses original arguments from Celery's result backend when available. Enable `result_extended=True` (`CELERY_RESULT_EXTENDED=True` with Django's `CELERY` namespace) to store them, and configure a result backend. Those original values are stored outside z4j's redaction layer; apply the backend's access and retention controls. If arguments are unavailable, provide both `args` and `kwargs` explicitly. The adapter refuses to replay a task using incomplete or redacted arguments.

## Reliability

- Lifecycle-capture failures are isolated from Celery workers and signal
  handlers; capture hooks make no brain network request inline.
- The in-process event queue and SQLite outbound buffer are bounded. A full
  queue evicts its oldest queued event to admit the new one, up to three
  times. Queue overflow drops new events only if that cannot make room.
  Buffer pressure evicts oldest rows. Every loss is logged.

## Documentation

Full docs at [docs.z4j.com/engines/celery/](https://docs.z4j.com/engines/celery/).

## License

Apache-2.0, see [LICENSE](https://github.com/z4jdev/z4j-celery/blob/main/LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://docs.z4j.com
- PyPI: https://pypi.org/project/z4j-celery/
- Issues: https://github.com/z4jdev/z4j-celery/issues
- Changelog: [CHANGELOG.md](https://github.com/z4jdev/z4j-celery/blob/main/CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](https://github.com/z4jdev/z4j-celery/blob/main/SECURITY.md))
