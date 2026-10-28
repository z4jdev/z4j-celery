# Changelog

## 1.13.2 (2026-10-29)

- A long traceback keeps its last 4096 characters (2000 in a reconcile
  result), where the innermost frames and the exception line are, instead of
  its first; a marker at the front shows where it was cut.

## 1.13.1 (2026-10-28)

- Fix a prefork worker that stopped taking tasks for good once its pool
  recycled children (`worker_max_tasks_per_child`,
  `worker_max_memory_per_child`, a crash or a time-limit kill). Each
  replacement child closes Celery's shared broker pool before it reports
  ready. On Redis, a pooled channel that the agent's health refresh
  (`control.inspect`) had left waiting on a BRPOP made that close read from
  the socket the child shares with the main process. After one such reply
  wait, every replacement blocked there and logged "Timed out waiting for UP
  message" until the worker was restarted. Reply waits now run on a
  connection the agent opens and closes itself, with a 10 second Redis read
  timeout unless `broker_transport_options` sets `socket_timeout`, so a
  half-open connection can no longer park the health-refresh thread forever.
  The worker log notes at startup when the pool recycles children.

## 1.13.0 (2026-10-19)

- Preserve useful failure details and safe retry task names across out-of-order events and reject unresolved task identities.

## 1.12.0 (2026-10-03)

* Implement `list_dead_letters` as a fail-closed refusal: Celery has no
  first-class dead-letter store (user-defined DLQ routing is broker-specific),
  so the `list_dead_letters` capability stays absent, the agent dispatcher
  never reaches the method, and a direct call raises `AdapterError` without
  touching the broker. This is the same posture as `requeue_dead_letter`.

## 1.11.0 (2026-09-10)

* Keep task submission off the agent event loop on the dedicated broker pool.
  A publish that exceeds the bounded wait is explicitly indeterminate because
  the broker operation can still complete; it is never automatically retried.

* Align runtime version metadata and sibling dependency floors with the coordinated 1.11.0 release.

## 1.10.0 (2026-08-28)

* Carried with the coordinated fleet release. No behaviour changed.

## 1.9.1 (2026-08-27)

* Carried with the coordinated fleet release. No adapter behaviour changed.

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
