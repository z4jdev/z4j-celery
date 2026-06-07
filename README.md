# z4j-celery

[![PyPI version](https://img.shields.io/pypi/v/z4j-celery.svg?v=1.6.6)](https://pypi.org/project/z4j-celery/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celery.svg?v=1.6.6)](https://pypi.org/project/z4j-celery/)
[![License](https://img.shields.io/pypi/l/z4j-celery.svg?v=1.6.6)](https://github.com/z4jdev/z4j-celery/blob/main/LICENSE)

The Celery engine adapter for [z4j](https://z4j.com).

Streams every task lifecycle event from your Celery workers to the z4j
brain and accepts operator control actions from the dashboard. Pair
with z4j-celerybeat to manage periodic / cron schedules.

## What it ships

| Capability | Notes |
|---|---|
| Task lifecycle events | submitted, started, succeeded, failed, retried, revoked |
| Task discovery | runtime registry merge + static `tasks.py` scan |
| Submit / retry / cancel | direct against the Celery app |
| Bulk retry | filter-driven; re-enqueues matching tasks |
| Purge queue | with confirm-token guard |
| Requeue dead-letter | from the configured DLX |
| Restart worker | broadcast pool restart, zero task loss |
| Pool grow / shrink | via Celery's control API |
| Rate limit | broker-side via Celery's control channel |
| Reconcile task | via the result backend |

The widest feature coverage of any z4j engine adapter, Celery's rich
remote-control surface lets z4j ship capabilities other engines can't
match (pool restart with zero task loss, broker-side rate limiting).

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

- No exception from the adapter ever propagates back into your Celery
  workers or signal handlers.
- Events buffer locally when z4j is unreachable; your workers
  never slow down or block on network I/O.

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
