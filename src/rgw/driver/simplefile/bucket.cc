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
#include <fstream>
#include "rgw_sal_simplefile.h"
#include "multipart.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {


SimpleFileBucket::SimpleFileBucket(
  const std::filesystem::path& _path,
  SimpleFileStore *_store
) : store(_store), path(_path), acls() {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
}

void SimpleFileBucket::init(
  const DoutPrefixProvider *dpp,
  const rgw_bucket &b
) {
  ldpp_dout(dpp, 10) << __func__ << ": init bucket: "
                     << get_name() << "[" << path << "]" << dendl;
  auto meta_path = bucket_metadata_path();
  auto obj_path = objects_path();
  ceph_assert(!std::filesystem::exists(meta_path));
  ceph_assert(!std::filesystem::exists(obj_path));
  std::filesystem::create_directory(obj_path);

  info.bucket = b;
  info.creation_time = ceph::real_clock::now();
  info.placement_rule.name = "default";
  info.placement_rule.storage_class = "STANDARD";

  write_meta(dpp);
  write_object_map(dpp);
}

void SimpleFileBucket::write_meta(const DoutPrefixProvider *dpp) {

  auto meta_path = bucket_metadata_path();

  lsfs_dout(dpp, 10) << "write metadata to " << meta_path << dendl;

  SimpleFileBucket::Meta meta;
  meta.info = info;
  // meta.multipart = multipart;

  ofstream ofs;
  ofs.open(meta_path);

  JSONFormatter f(true);
  f.open_object_section("meta");
  encode_json("meta", meta, &f);
  f.close_section();
  f.flush(ofs);
  ofs.close();
}

void SimpleFileBucket::write_object_map(const DoutPrefixProvider *dpp) {
  version_t new_version = object_map_version + 1;
  bufferlist bl;
  ceph::encode(new_version, bl);
  ceph::encode(objects_map, bl);

  auto obj_map_path = path / "_objects.map";
  auto new_map_path = path / ("_objects.map.v" + std::to_string(new_version));
  // ensure we did not go back in time.
  ceph_assert(!std::filesystem::exists(new_map_path));

  lsfs_dout(dpp, 10) << "version " << new_version << " with "
                     << objects_map.size() << " objects to " << new_map_path
                     << dendl;
  bl.write_file(new_map_path.c_str());
  std::filesystem::remove(obj_map_path);
  std::filesystem::create_symlink(new_map_path, obj_map_path);
}

void SimpleFileBucket::load_object_map(const DoutPrefixProvider *dpp) {
  lsfs_dout(dpp, 10) << "load objects map for bucket " << get_name() << dendl;
  auto obj_map_path = path / "_objects.map";
  bufferlist bl;
  std::string err;
  bl.read_file(obj_map_path.c_str(), &err);
  if (!err.empty()) {
    lsfs_dout(dpp, 0) << "unable to load object map for bucket " << get_name()
                      << ": " << err << dendl;
    ceph_abort("unable to load object map for bucket");
  }

  auto it = bl.cbegin();
  ceph::decode(object_map_version, it);
  ceph::decode(objects_map, it);

  lsfs_dout(dpp, 10) << "loaded object map version " << object_map_version
                     << " with " << objects_map.size() << " objects" << dendl;
}

std::unique_ptr<Object> SimpleFileBucket::get_object(const rgw_obj_key &key) {
  ldout(store->ceph_context(), 10) << "bucket::" << __func__
                                   << ": key" << key << dendl;
  return make_unique<SimpleFileObject>(this->store, key, this);
}

/**
 * List objects in this bucket.
 */
int SimpleFileBucket::list(const DoutPrefixProvider *dpp, ListParams &, int,
                           ListResults &results, optional_yield y) {
  lsfs_dout(dpp, 10) << "iterate bucket " << get_name() << dendl;
  lsfs_dout(dpp, 10) << "num objects: " << objects_map.size() << dendl;

  for (const auto &entry: objects_map) {
    lsfs_dout(dpp, 10) << "obj: " << entry.first << ", h: " << entry.second << dendl;
  }

  for (const auto &[name, hash]: objects_map) {
    lsfs_dout(dpp, 10) << "object: " << name << ", hash: " << hash << dendl;

    rgw_obj_key objkey(name);
    auto data_path = store->object_path(get_key(), objkey);
    ceph_assert(std::filesystem::exists(data_path));
    ceph_assert(std::filesystem::is_regular_file(data_path));

    auto obj = get_object(objkey);
    rgw_bucket_dir_entry dirent;
    dirent.key = cls_rgw_obj_key(name);
    dirent.meta.accounted_size = obj->get_obj_size();
    dirent.meta.mtime = obj->get_mtime();
    results.objs.push_back(dirent);
  }

  lsfs_dout(dpp, 10) << "found " << results.objs.size() << " objects" << dendl;
  return 0;
}

int SimpleFileBucket::remove_bucket(const DoutPrefixProvider *dpp,
                                    bool delete_children,
                                    bool forward_to_master, req_info *req_info,
                                    optional_yield y) {
  /** Remove this bucket from the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::remove_bucket_bypass_gc(int concurrent_max,
                                              bool keep_index_consistent,
                                              optional_yield y,
                                              const DoutPrefixProvider *dpp) {
  /** Remove this bucket, bypassing garbage collection.  May be removed */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::load_bucket(const DoutPrefixProvider* dpp,
                                  optional_yield y, bool get_stats) {
  std::filesystem::path meta_file_path = bucket_metadata_path();
  ceph_assert(std::filesystem::exists(meta_file_path));
  JSONParser bucket_meta_parser;
  if (!bucket_meta_parser.parse(meta_file_path.c_str())) {
    lsfs_dout(dpp, 10) << "Failed to parse bucket metadata from "
                       << meta_file_path << ". Returing EINVAL" << dendl;
    return -EINVAL;
  }

  auto it = bucket_meta_parser.find("meta");
  ceph_assert(!it.end());

  SimpleFileBucket::Meta meta;
  JSONDecoder::decode_json("meta", meta, &bucket_meta_parser); 
  lsfs_dout(dpp, 10) "bucket name: " << meta.info.bucket.get_key() << dendl;

  info = meta.info;
  // multipart = meta.multipart;

  load_object_map(dpp);

  auto f = new JSONFormatter(true);
  lsfs_dout(dpp, 10) << ": info: ";
  info.dump(f);
  f->flush(*_dout);
  *_dout << dendl;
  return 0;
}

int SimpleFileBucket::chown(const DoutPrefixProvider *dpp, User &new_user,
                            optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
bool SimpleFileBucket::is_owner(User *user) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}
int SimpleFileBucket::check_empty(const DoutPrefixProvider *dpp,
                                  optional_yield y) {
  /** Check in the backing store if this bucket is empty */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                            Attrs &new_attrs,
                                            optional_yield y) {
  /** Set the attributes in attrs, leaving any other existing attrs set, and
   * write them to the backing store; a merge operation */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<MultipartUpload> SimpleFileBucket::get_multipart_upload(
    const std::string &oid,
    std::optional<std::string> upload_id,
    ACLOwner owner,
    ceph::real_time mtime
) {
  ldout(store->ceph_context(), 10) << "bucket::" << __func__ << ": oid: " << oid
                                  << ", upload id: " << upload_id << dendl;
  auto p = new SimpleFileMultipartUpload(
    store->ctx(), store, this, oid, upload_id, std::move(owner), mtime
  );
  /** Create a multipart upload in this bucket */
  return std::unique_ptr<SimpleFileMultipartUpload>(p);
  // return std::make_unique<SimpleFileMultipartUpload>(
  //   store, this, oid, upload_id, std::move(owner), mtime
  // );
}

int SimpleFileBucket::list_multiparts(
    const DoutPrefixProvider *dpp, const std::string &prefix,
    std::string &marker, const std::string &delim, const int &max_uploads,
    std::vector<std::unique_ptr<MultipartUpload>> &uploads,
    std::map<std::string, bool> *common_prefixes, bool *is_truncated) {
  /** List multipart uploads currently in this bucket */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::abort_multiparts(const DoutPrefixProvider *dpp,
                                       CephContext *cct) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::try_refresh_info(const DoutPrefixProvider *dpp,
                                       ceph::real_time *pmtime) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::read_usage(
    const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
    std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::trim_usage(const DoutPrefixProvider *dpp,
                                 uint64_t start_epoch, uint64_t end_epoch) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::rebuild_index(const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::check_quota(
  const DoutPrefixProvider *dpp,
  RGWQuota &quota,
  uint64_t obj_size,
  optional_yield y,
  bool check_size_only
) {
  ldpp_dout(dpp, 10) << __func__ << ": user(max size: "
                     << quota.user_quota.max_size
                     << ", max objs: " << quota.user_quota.max_objects
                     << "), bucket(max size: " << quota.bucket_quota.max_size
                     << ", max objs: " << quota.bucket_quota.max_objects
                     << "), obj size: " << obj_size << dendl;
  ldpp_dout(dpp, 10) << __func__ << ": not implemented, return okay." << dendl;
  return 0;
}

int SimpleFileBucket::read_stats(
    const DoutPrefixProvider *dpp,
    const bucket_index_layout_generation &idx_layout,
    int shard_id,
    std::string *bucket_ver,
    std::string *master_ver,
    std::map<RGWObjCategory, RGWStorageStats> &stats,
    std::string *max_marker,
    bool *syncstopped
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::read_stats_async(
    const DoutPrefixProvider *dpp,
    const bucket_index_layout_generation &idx_layout,
    int shard_id,
    RGWGetBucketStats_CB *ctx
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileBucket::sync_user_stats(const DoutPrefixProvider *dpp,
                                      optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::update_container_stats(const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::check_bucket_shards(const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileBucket::put_info(const DoutPrefixProvider *dpp, bool exclusive,
                               ceph::real_time mtime) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

bool SimpleFileBucket::maybe_add_object(
  const DoutPrefixProvider *dpp,
  SimpleFileObject *obj
) {
  auto objname = obj->get_name();
  lsfs_dout(dpp, 10) << "bucket: " << get_name()
                     << ", object: " << objname << dendl;

  if (objects_map.find(objname) != objects_map.end()) {
    lsfs_dout(dpp, 10) << "object " << objname << " already exists." << dendl;
    return true;
  }

  objects_map[objname] = store->hash_rgw_obj_key(obj->get_key());
  write_object_map(dpp);
  return true;
}

// bucket_path returns the path containing bucket metadata and objects
std::filesystem::path SimpleFileBucket::bucket_path() const { return path; }
// bucket_metadata_path returns the path to the metadata file metadata_fn
std::filesystem::path SimpleFileBucket::bucket_metadata_path() const {
  return path / "_meta.json";
}
// objects_path returns the path to the buckets objects. Each
// subdirectory points to an object
std::filesystem::path SimpleFileBucket::objects_path() const {
  return path / "objects";
}

} // ns rgw::sal
