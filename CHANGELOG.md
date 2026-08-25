# Changelog

## 1.9.0 (2026-08-25)

* Purge action and engine corrections carried with the fleet release.
* Raise the uncapped Celery floor to 5.2.2, the first release that closes
  CVE-2021-23727, so published metadata cannot resolve the vulnerable
  5.2.0/5.2.1 line on a fresh install.
* **Breaking safety correction:** The unsafe `requeue_dead_letter` capability
  has been removed. The 1.8 implementation published a normal retry without
  consuming or acknowledging the original broker dead-letter entry and could
  route the copy to a different queue, so it could duplicate work. Direct calls
  now fail closed without publishing. Use broker-specific tooling that
  atomically consumes or acknowledges the original entry and preserves its
  routing.

## 1.8.0 (2026-07-23)

* The adapter now attests its safe by-reference retry contract to the exact agent session and requires the coordinated 1.8.0 bare/core runtime.
* Retry now fails closed when arguments are unresolvable (no operator override and `result_extended` stored nothing) instead of re-running the task with an empty payload, and it honors the brain-supplied task name.
* Destructive actions (cancel / purge / retry) offload their broker I/O on a dedicated bounded pool, with a consecutive-timeout circuit breaker on bulk retry, so a broker incident can no longer freeze the agent; a timed-out mutation is reported indeterminate, not falsely successful.
* The in-worker agent starts on `worker_ready` (was `worker_init`, which could wedge a prefork pool after a restart) and the periodic health refresh no longer overruns its 5s cap.
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

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
