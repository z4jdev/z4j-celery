# z4j-celery

[![PyPI version](https://img.shields.io/pypi/v/z4j-celery.svg)](https://pypi.org/project/z4j-celery/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celery.svg)](https://pypi.org/project/z4j-celery/)
[![License](https://img.shields.io/pypi/l/z4j-celery.svg)](https://github.com/z4jdev/z4j-celery/blob/main/LICENSE)

The Celery engine adapter for [z4j](https://z4j.com).

Streams Celery task lifecycle events to the z4j brain and
accepts control actions (retry, cancel, bulk retry, purge,
restart) from the dashboard. Pair with z4j-celerybeat to
surface periodic schedules.

## Install

```bash
pip install z4j-celery z4j-celerybeat
```

## Pairs with

- [`z4j-celerybeat`](https://github.com/z4jdev/z4j-celerybeat) — schedule adapter for Celery Beat / django-celery-beat

## Documentation

Full docs at [z4j.dev/engines/celery/](https://z4j.dev/engines/celery/).

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-celery/
- Issues: https://github.com/z4jdev/z4j-celery/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
