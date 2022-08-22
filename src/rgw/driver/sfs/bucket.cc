// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "bucket.h"

#include <fstream>

#include "driver/sfs/multipart.h"
#include "driver/sfs/object.h"
#include "driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SFSBucket::SFSBucket(
    SFStore* _store, sfs::BucketRef _bucket, const RGWBucketInfo& info
)
    : store(_store), bucket(_bucket), acls() {
  this->info = info;
}

void SFSBucket::write_meta(const DoutPrefixProvider* dpp) {
  // TODO
}

std::unique_ptr<Object> SFSBucket::_get_object(sfs::ObjectRef obj) {
  rgw_obj_key key(obj->name);
  return make_unique<SFSObject>(this->store, key, this, bucket);
}

std::unique_ptr<Object> SFSBucket::get_object(const rgw_obj_key& key) {
  ldout(store->ceph_context(), 10)
      << "bucket::" << __func__ << ": key" << key << dendl;
  std::lock_guard l(bucket->obj_map_lock);
  auto it = bucket->objects.find(key.name);
  if (it == bucket->objects.end()) {
    ldout(store->ceph_context(), 10)
        << "unable to find key " << key << " in bucket " << bucket->get_name()
        << dendl;
    // possibly a copy, return a placeholder
    return make_unique<SFSObject>(this->store, key, this, bucket);
  }
  return _get_object(it->second);
}

/**
 * List objects in this bucket.
 */
int SFSBucket::list(
    const DoutPrefixProvider* dpp, ListParams& params, int max,
    ListResults& results, optional_yield y
) {
  lsfs_dout(dpp, 10) << "iterate bucket " << get_name() << dendl;

  if (params.list_versions) {
    return list_versions(dpp, params, max, results, y);
  }

  std::lock_guard l(bucket->obj_map_lock);
  for (const auto& [name, objref] : bucket->objects) {
    lsfs_dout(dpp, 10) << "object: " << name << dendl;

    auto obj = _get_object(objref);
    rgw_bucket_dir_entry dirent;
    dirent.key = cls_rgw_obj_key(name, objref->instance);
    dirent.meta.accounted_size = obj->get_obj_size();
    dirent.meta.mtime = obj->get_mtime();
    dirent.meta.etag = objref->meta.etag;
    dirent.meta.owner_display_name = bucket->get_owner().display_name;
    dirent.meta.owner = bucket->get_owner().user_id.id;
    results.objs.push_back(dirent);
  }

  lsfs_dout(dpp, 10) << "found " << results.objs.size() << " objects" << dendl;
  return 0;
}

int SFSBucket::list_versions(
    const DoutPrefixProvider* dpp, ListParams& params, int,
    ListResults& results, optional_yield y
) {
  std::lock_guard l(bucket->obj_map_lock);
  for (const auto& [name, objref] : bucket->objects) {
    lsfs_dout(dpp, 10) << "object: " << name << dendl;
    // get all available versions from db
    sfs::sqlite::SQLiteVersionedObjects db_versioned_objects(store->db_conn);
    auto last_version =
        db_versioned_objects.get_last_versioned_object(objref->path.get_uuid());
    auto object_versions =
        db_versioned_objects.get_versioned_objects(objref->path.get_uuid());
    for (const auto& object_version : object_versions) {
      rgw_bucket_dir_entry dirent;
      dirent.key = cls_rgw_obj_key(objref->name, object_version.version_id);
      dirent.meta.accounted_size = object_version.size;
      dirent.meta.mtime = object_version.creation_time;
      dirent.meta.etag = objref->meta.etag;
      dirent.flags = rgw_bucket_dir_entry::FLAG_VER;
      if (last_version.has_value() && last_version->id == object_version.id) {
        dirent.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
      }
      dirent.meta.owner_display_name = bucket->get_owner().display_name;
      dirent.meta.owner = bucket->get_owner().user_id.id;
      results.objs.push_back(dirent);
    }
  }
  lsfs_dout(dpp, 10) << "found " << results.objs.size() << " objects" << dendl;
  return 0;
}

int SFSBucket::remove_bucket(
    const DoutPrefixProvider* dpp, bool delete_children, bool forward_to_master,
    req_info* req_info, optional_yield y
) {
  /** Remove this bucket from the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::remove_bucket_bypass_gc(
    int concurrent_max, bool keep_index_consistent, optional_yield y,
    const DoutPrefixProvider* dpp
) {
  /** Remove this bucket, bypassing garbage collection.  May be removed */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::load_bucket(
    const DoutPrefixProvider* dpp, optional_yield y, bool get_stats
) {
  // TODO
  return 0;
}

int SFSBucket::chown(
    const DoutPrefixProvider* dpp, User& new_user, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
bool SFSBucket::is_owner(User* user) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}
int SFSBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y) {
  /** Check in the backing store if this bucket is empty */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::merge_and_store_attrs(
    const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y
) {
  /** Set the attributes in attrs, leaving any other existing attrs set, and
   * write them to the backing store; a merge operation */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<MultipartUpload> SFSBucket::get_multipart_upload(
    const std::string& oid, std::optional<std::string> upload_id,
    ACLOwner owner, ceph::real_time mtime
) {
  ldout(store->ceph_context(), 10) << "bucket::" << __func__ << ": oid: " << oid
                                   << ", upload id: " << upload_id << dendl;
  auto p = new SFSMultipartUpload(
      store->ctx(), store, this, oid, upload_id, std::move(owner), mtime
  );
  /** Create a multipart upload in this bucket */
  return std::unique_ptr<SFSMultipartUpload>(p);
  // return std::make_unique<SFSMultipartUpload>(
  //   store, this, oid, upload_id, std::move(owner), mtime
  // );
}

int SFSBucket::list_multiparts(
    const DoutPrefixProvider* dpp, const std::string& prefix,
    std::string& marker, const std::string& delim, const int& max_uploads,
    std::vector<std::unique_ptr<MultipartUpload>>& uploads,
    std::map<std::string, bool>* common_prefixes, bool* is_truncated
) {
  /** List multipart uploads currently in this bucket */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::abort_multiparts(
    const DoutPrefixProvider* dpp, CephContext* cct
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::try_refresh_info(
    const DoutPrefixProvider* dpp, ceph::real_time* pmtime
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::read_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
    std::map<rgw_user_bucket, rgw_usage_log_entry>& usage
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSBucket::trim_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::rebuild_index(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::check_quota(
    const DoutPrefixProvider* dpp, RGWQuota& quota, uint64_t obj_size,
    optional_yield y, bool check_size_only
) {
  ldpp_dout(dpp, 10) << __func__
                     << ": user(max size: " << quota.user_quota.max_size
                     << ", max objs: " << quota.user_quota.max_objects
                     << "), bucket(max size: " << quota.bucket_quota.max_size
                     << ", max objs: " << quota.bucket_quota.max_objects
                     << "), obj size: " << obj_size << dendl;
  ldpp_dout(dpp, 10) << __func__ << ": not implemented, return okay." << dendl;
  return 0;
}

int SFSBucket::read_stats(
    const DoutPrefixProvider* dpp,
    const bucket_index_layout_generation& idx_layout, int shard_id,
    std::string* bucket_ver, std::string* master_ver,
    std::map<RGWObjCategory, RGWStorageStats>& stats, std::string* max_marker,
    bool* syncstopped
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSBucket::read_stats_async(
    const DoutPrefixProvider* dpp,
    const bucket_index_layout_generation& idx_layout, int shard_id,
    RGWGetBucketStats_CB* ctx
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::sync_user_stats(
    const DoutPrefixProvider* dpp, optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSBucket::update_container_stats(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSBucket::check_bucket_shards(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSBucket::put_info(
    const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime
) {
  sfs::sqlite::DBOPBucketInfo store_info;
  store_info.binfo = info;

  auto meta_buckets = sfs::get_meta_buckets(store->db_conn);
  meta_buckets->store_bucket(store_info);
  store->_refresh_buckets_safe();
  return 0;
}

}  // namespace rgw::sal
