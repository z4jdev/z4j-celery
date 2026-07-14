# Changelog

## 1.7.0 (2026-07-07)

* Fixed dropped-event and unknown-event-kind diagnostics that passed structlog keyword arguments to a stdlib logger.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.6.7 (2026-06-07)

R8 audit security batch carry-in. Fixes a multi-tenant cache bleed
in `CeleryEngineAdapter` (R8-L2): `_last_worker_stats_at` and
`_cached_worker_stats` were class-level mutable defaults so two
adapter instances in the same Python process saw each other's
cached `get_worker_details()` payload. In single-tenant prod that
was harmless (one adapter per agent process); in multi-tenant test
rigs and the 1.7 soak harness it would let project A's worker conf
land in project B's heartbeat. Both attributes now initialize in
`__init__` per instance. No API change; no broker / queue behavior
change; in-place pip upgrade.

Version lifted from 1.6.6 to 1.6.7 to ride with the umbrella R8
ship so the agent-side version line stays coherent across the
adapters touched in this wave (z4j-rq, z4j-bare, z4j-django, and
now z4j-celery all at 1.6.7).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: Celery engine adapter. Pool restart with zero task loss, broker-side rate limiting.
