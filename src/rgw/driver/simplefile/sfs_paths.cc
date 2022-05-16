// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "rgw_sal_simplefile.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

/**
 * Returns path to meta directory.
 */
std::filesystem::path SimpleFileStore::meta_path() const {
  return data_path / "meta";
}

// buckets_path returns the directory where every subdirectory contains a bucket
std::filesystem::path SimpleFileStore::buckets_path() const {
  return data_path / "buckets";
}

//
std::filesystem::path SimpleFileStore::users_path() const {
  return data_path / "users";
}

// bucket_path returns the directory where bucket metadata is stored
std::filesystem::path SimpleFileStore::bucket_path(
    const rgw_bucket &bucket) const {
  return buckets_path() / bucket.name;
}

// bucket_path returns the path to a bucket metadata file named metadata_fn
std::filesystem::path SimpleFileStore::bucket_metadata_path(
    const rgw_bucket &bucket, const std::string &metadata_fn) const {
  return bucket_path(bucket) / metadata_fn;
}

// objects_path returns the directory where all objects of a bucket are stored
std::filesystem::path SimpleFileStore::objects_path(const rgw_bucket &bucket) const {
  return bucket_path(bucket) / "objects";
}

static std::string hash_rgw_obj_key(const rgw_obj_key &obj) {
  const std::string_view in{obj.name};
  const auto hash = calc_hash_sha256(in);
  return hash.to_str();
}
 
// object_path returns the directory where metadata of an object is stored
std::filesystem::path SimpleFileStore::object_path(
    const rgw_bucket &bucket, const rgw_obj_key &obj) const {
  return objects_path(bucket) / hash_rgw_obj_key(obj);
}

// object_data_path returns the path of the object data file
std::filesystem::path SimpleFileStore::object_data_path(
    const rgw_bucket &bucket, const rgw_obj_key &obj) const {
  return object_path(bucket, obj) / "data";
}

// object_metadata_path returns the path to an object metadata file named
// metadata_fn
std::filesystem::path SimpleFileStore::object_metadata_path(
    const rgw_bucket &bucket, const rgw_obj_key &obj,
    const std::string &metadata_fn) const {
  return object_path(bucket, obj) / metadata_fn;
}

// bucket_path returns the path containing bucket metadata and objects
std::filesystem::path SimpleFileBucket::bucket_path() const {
  return path;
}

// bucket_metadata_path returns the path to the metadata file metadata_fn
std::filesystem::path SimpleFileBucket::bucket_metadata_path(
    const std::string &metadata_fn) const {
  return path / metadata_fn;
}
// objects_path returns the path to the buckets objects. Each
// subdirectory points to an object
std::filesystem::path SimpleFileBucket::objects_path() const {
  return path / "objects";
}

} // ns rgw::sal
