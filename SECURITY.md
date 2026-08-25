# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability in `z4j-celery`,
**do not open a public GitHub issue**. Email `security@z4j.com` instead.

We acknowledge reports within **48 hours**, provide a preliminary assessment
within **5 business days**, and target fixes within **30 days** (**7 days** for
confirmed critical issues). Reporting timelines, safe harbor, supported-version
policy, and published advisories are maintained in the
[canonical z4j project security policy](https://github.com/z4jdev/z4j/blob/main/SECURITY.md).

## Security-critical surface

This adapter runs inside Celery workers with their broker and result-backend
access. An authenticated brain can invoke submit, retry, cancel, bulk retry,
purge, pool, restart, and rate-limit controls. Task-id/name and
argument validation, cross-project selection, confirm tokens, broker event
mapping, and fire-and-forget control semantics are package-specific security
surfaces; transport, redaction, and authorization policy remain owned by
`z4j-core` and the brain.
