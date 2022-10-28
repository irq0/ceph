# Changelog

All notable changes to this project will be documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- Fixed segfault when SFSAtomicWriter::complete is called whith mtime output
  variable set to nullptr

### Changed

- In order to make stat_bucket call available, SFSBucket::update_container_stat now returns 0.

### Added

- Added a mechanism to check for incompatibility issues without changing the
  original metadata database. When any incompatibility is found it is also shown
  in the logs.

## [0.7.0] - 2022-10-20

### Fixed

- Fixed queries to users by access key when user has multiple keys.
- Fixed a circular lock dependency, which could lead to a deadlock when aborting
  multiparts for an object while finishing a different object.
- Fixed a few SQLite users operations that did not acquire the required locks
  when accessing the database.

## [0.6.0] - 2022-09-29

### Added

- Undelete objects.
- Ability to list buckets via admin REST API
- Delete buckets.
- Support for bucket ACL.
- Multipart uploads
- Objects are stored in metadata using bucket id instead of bucket name.
- CI: run rgw/sfs unit test suite on every pull request submission.

### Fixed

- Show delete markers when listing object versions.

### Notes

- Multipart uploads are currently tracked solely in memory. Should the gateway
  be stopped, on-going multipart uploads will be lost.
- Listing multipart uploads does not account for prefix or delimiters.
- Metadata stored in sqlite is no longer compatible with previous versions.

## [0.5.0] - 2022-09-15

### Added

- Added columns in the sqlite buckets table:
  - zone_group
  - quota

### Fixed

- Fixed the admin API request: get-bucket-info where the client was receiving
  an empty response.

## [0.4.0] - 2022-09-01

### Added

- rgw/sfs: object versioning.

### Notes

- rgw/sfs Versioning.

#### What works

- Enable / disable bucket versioning.
- When versioning is enabled and a new object is pushed it creates a new version,
  keeping the previous one.
- Objects versions list
- Download specific version (older versions than the last one)
- Object delete (delete mark is added in a new version)

#### What's missing / does not work

- Remove delete marks (undelete objects)
- Store checksum in sqlite metadata for a version

### Fixed

- rgw/sfs: fix an issue where the creation time of a bucket is displayed
  as the current machine time.
- rgw/sfs: fix the json response for creation bucket rest call for system
  users.

## [0.3.0] - 2022-08-05

### Added

- rgw/sfs: new on-disk format, based on filesystem hash tree for data
  and sqlite for metadata.
- rgw/sfs: maintain one single sqlite database connection.
- rgw/sfs: protect sqlite access with 'std::shared_lock'; allows multiple
  parallel reads, but only one write at a time.
- rgw/sfs: allow copying objects; the current implementation breaks S3
  semantics by returning EEXIST if the destination object exists.

### Known Issues

- object copy fails if the destination object exists; this will be addressed at
  a later stage.

### Changed

- rgw/sfs: no longer create directory hierarchy when initing the store; instead,
  ensure the sfs path exists by creating its directory if missing.

### Removed

- rgw/sfs: remove unused data and metadata functions, artifacts from our
  previous file-based implementation.

## [0.2.0] - 2022-07-28

- Nothing changed on this version.


## [0.1.0] - 2022-07-14

### Added

- sfs: support for object GET/PUT/LIST/DELETE
- sfs: support for bucket listing, create
- common: obtain env variable contents
- ci: build and test radosgw with sfs
- sfs: introduce sqlite orm library
- sfs: keep users in sqlite
- sfs: basic user management via REST api
