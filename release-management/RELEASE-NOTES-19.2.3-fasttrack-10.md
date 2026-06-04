# cobaltcore-storage 19.2.3-fasttrack-10

**Upstream base:** 19.2.3 (v19.2.3)
**Downstream tag:** release/cobaltcore-storage-v19.2.3-fasttrack-10
**Branch:** release/cobaltcore-storage-v19.2.3-fasttrack-10

## Built artifacts

- **container-image**: `harbor.clyso.com/custom-ceph/ceph/ceph:cobaltcore-storage-v19.2.3-fasttrack-10` (`sha256:e316b9513c3c1a084e7660fc1e0a03e5689eacc808562a5e9d406f528a8e1020`)
## Verification

Tested by CLYSO per agreement. Production S3 rollout was smooth with no performance regressions or errors observed in dashboards. Unit tests passed (two run-tox-alerts tests had unrelated Python errors). Teuthology results match upstream. Rook integration passed against a semi-automated Minikube install with Keystone and Barbican; small-scale SSE-KMS Warp benchmarks passed. Out of scope (covered by SAP): large-scale cluster tests, scalability benchmarks, additional regression tests of new features. See test-evidence/clyso-test-report.md for the full report.

See `release-management/releases/v19.2.3-fasttrack-10/RELEASE.md` for the authoritative build and
test record (source commit, artifact digests, RE identity, full test
summary) and `release-management/releases/v19.2.3-fasttrack-10/test-evidence/` for raw
test artifacts.

## New features

### SSE-KMS Secrets Cache

Adds a secure in-memory cache for SSE-KMS encryption keys retrieved from Barbican
(or other KMS backends), eliminating redundant round-trips on repeated
access to the same encrypted objects. Uses a new SIEVE-based eviction
cache (WebCache) and stores decrypted secrets in the Linux kernel keyring.
Please refer to the Ceph documentation on how to configure this feature.

#### Related tickets

- https://github.com/cobaltcore-dev/cloud-storage/issues/125

#### Behavior change

Repeated SSE-KMS access no longer hits the KMS backend on every
request; decrypted secrets are cached in-memory and in the Linux
kernel keyring. KMS-cache is **ON** out of the box.

#### Risks

- TTLs mean that a deleted or rotated Barbican key won't be immediately
  reflected.
- New async primitives (`async::call_once`) and the `WebCache` are
  significant new library code shipping for the first time.
- Cache size tuning and system wide keyring quota adjustment needed
  for production workloads (refer to the docs).

#### Upgrade notes

Feature flag: `rgw_crypt_s3_kms_cache_enabled` — set `false` to disable
caching entirely and revert to per-request KMS lookups.

**Upstream PRs:** https://github.com/ceph/ceph/pull/61256

### Write Lock - lua: postAuth hook, bucket tags, request blocking

Lua script extensions for RGW to allow "Write Lock" feature

- New `postAuth` Lua hook point that runs after authentication /
  authorization but before the operation executes.
- Lua scripts can now read bucket tags.
- Lua scripts can abort request processing.

#### Related tickets

- https://github.com/cobaltcore-dev/cloud-storage/issues/310

#### Behavior change

A new `postAuth` Lua hook point is available. Scripts can read bucket
tags and abort request processing. Lua is **inactive** out of the box
(no scripts are uploaded by default).

#### Risks

The `postAuth` hook runs on every request. A slow or buggy Lua script
will add latency to all S3 operations.

#### Upgrade notes

No dedicated feature flag; controlled by whether a Lua script is uploaded
via `radosgw-admin`.

**Upstream PRs:** https://github.com/ceph/ceph/pull/66065, https://github.com/ceph/ceph/pull/67219, https://github.com/ceph/ceph/pull/66567, https://github.com/ceph/ceph/pull/67548

### RGW Keystone Scope Logging + Role Injection into IAM Policy

- Adds Keystone authentication scope (project, domain, roles) to the RGW
  ops log.
- Allows Keystone role names to be injected as `keystone:role` condition
  keys in S3 bucket policies, enabling role-based access control via
  standard IAM policy conditions.

#### Related tickets

- https://github.com/cobaltcore-dev/cloud-storage/issues/335
- https://github.com/cobaltcore-dev/cloud-storage/issues/380

#### Behavior change

Keystone scope (project, domain, roles) now appears in RGW ops logs.
Keystone role names can be referenced from S3 bucket policies as
`keystone:role`. Both features are **OFF** out of the box.

#### Risks

- Role injection (`rgw_keystone_inject_roles`) changes IAM policy
  evaluation semantics. Existing bucket policies with `keystone:role`
  conditions that previously had no effect will **start matching**. Audit
  policies before enabling.
- Scope logging with `rgw_keystone_scope_include_user=true` writes
  human-readable usernames to ops logs, which may have GDPR / privacy
  implications.
- Future upstream versions will change `StringMatch` operator semantics
  (see <https://github.com/ceph/ceph/pull/65606/commits>). This backport
  already accounts for the new behavior.

#### Upgrade notes

- `rgw_keystone_scope_enabled` — enable Keystone scope in the ops log.
- `rgw_keystone_inject_roles` — inject Keystone roles as IAM policy
  condition keys.

Existing bucket policies referencing `keystone:role` should be audited
before enabling role injection.

**Upstream PRs:** https://github.com/ceph/ceph/pull/66111, https://github.com/ceph/ceph/pull/67752

## Integration and build patches

### backport: fix clang-14 issues in WebCache

Build adaptation: adds `typename` qualifiers to dependent template
return types in the new WebCache code so it compiles under clang-14.

#### Behavior change

None.

#### Risks

Pure C++ template-correctness fix. Touches no runtime code path. The
test file change is the addition of a `[[maybe_unused]]` attribute on
a discarded value, which is also a compiler-warning fix.

#### Upgrade notes

None.


### backport: remove ambiguous CephContext fwd decl in rgw_keystone_scope.h

Build adaptation: removes a redundant `class CephContext;` forward
declaration from `src/rgw/rgw_keystone_scope.h` that becomes ambiguous
against the existing declaration on the 19.2.3 base.

#### Behavior change

None.

#### Risks

Removes 2 lines from a header. No runtime behavior. Compiles cleanly
in CI.

#### Upgrade notes

None.


## Active patches

Cumulative; one row per backport carried in this release.

| Component | Title | Category | Risk |
|---|---|---|---|
| build | backport: fix clang-14 issues in WebCache | integration | low (6) |
| rgw | SSE-KMS Secrets Cache | feature | high (10) |
| rgw | Write Lock - lua: postAuth hook, bucket tags, request blocking | feature | medium (9) |
| rgw | RGW Keystone Scope Logging + Role Injection into IAM Policy | feature | medium (8) |
| rgw | backport: remove ambiguous CephContext fwd decl in rgw_keystone_scope.h | integration | low (6) |
