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

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| Task lifecycle events | submitted, started, succeeded, failed, retried, revoked |
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

## Reliability

- Lifecycle-capture failures are isolated from Celery workers and signal
  handlers; capture hooks make no brain network request inline.
- The in-process event queue and SQLite outbound buffer are bounded. Queue
  overflow drops new events and buffer pressure evicts oldest rows; both losses
  are logged.

## Documentation

Full docs at [z4j.dev/engines/celery/](https://z4j.dev/engines/celery/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-celery/
- Issues: https://github.com/z4jdev/z4j-celery/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
