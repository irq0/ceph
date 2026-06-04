---
release: cobaltcore-storage-v19.2.3-fasttrack-10
release_tag: release/cobaltcore-storage-v19.2.3-fasttrack-10
source_commit: 0ebaff4ba79671acb4ff232df669de4280c61b1b   # the RC commit R; embedded in binaries
rc_tag: (none — released directly from branch)

manifest_sha256: 35f45c53c3e5b96dbb36e6f6c23c8e390cf6e4a59e4c2f066501d73bc60052fe

artifact_digests:
  - kind: container-image
    ref: harbor.clyso.com/custom-ceph/ceph/ceph
    digest: sha256:e316b9513c3c1a084e7660fc1e0a03e5689eacc808562a5e9d406f528a8e1020

release_engineer:
  github_handle: irq0

test_evidence:
  files:
    - release-management/releases/v19.2.3-fasttrack-10/test-evidence/clyso-test-report.md
  summary: |
    Tested by CLYSO per agreement. Production S3 rollout was smooth with no performance regressions or errors observed in dashboards. Unit tests passed (two run-tox-alerts tests had unrelated Python errors). Teuthology results match upstream. Rook integration passed against a semi-automated Minikube install with Keystone and Barbican; small-scale SSE-KMS Warp benchmarks passed. Out of scope (covered by SAP): large-scale cluster tests, scalability benchmarks, additional regression tests of new features. See test-evidence/clyso-test-report.md for the full report.
---
