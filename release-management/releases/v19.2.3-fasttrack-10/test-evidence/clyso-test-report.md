# CLYSO test report — cobaltcore-storage v19.2.3-fasttrack-10

Source: release announcement email from Marcel Lauhoff (CLYSO),
2026-06-04. Transcribed verbatim into in-tree test evidence at promote
time so the audit trail at `release/cobaltcore-storage-v19.2.3-fasttrack-10`
contains what was tested.

Ceph version (build SHA): `19.2.3-42-g0ebaff4ba79` (upstream v19.2.3 +
42 commits + abbrev. `0ebaff4ba79`).

## Production / dashboard observations

- Tests carried out on CLYSO's S3 production service.
- Rollout was smooth.
- Dashboards show no performance regressions.
- No errors have been detected in CLYSO's systems.

## Unit tests

- Passed, with two exceptions: `run-tox-alerts` tests encountered Python
  errors. **Assessment: do not affect this release.**

## Teuthology

- Running as successfully as the upstream tests.

## Rook

- Semi-automated Minikube Rook installation connected to Keystone and
  Barbican: tests passed.
- Small-scale SSE-KMS benchmarks using Warp: passed.

## Out of scope (covered by SAP)

- Large-scale Ceph cluster tests
- Scalability tests
- Benchmarks
- Additional regression tests of the new features

CLYSO has requested that SAP share scalability test results upstream.

## Operational guidance (from announcement)

Please be kindly reminded to ensure you have a backup before roll-out.
While the software is designed for high reliability and performance,
please note that CLYSO does not provide a guarantee against data loss.
Data integrity and regular backups remain the sole responsibility of
the customer. 
