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

#include <common/strtol.h>
#include <driver/sfs/multipart_types.h>
#include <driver/sfs/sqlite/buckets/multipart_definitions.h>
#include <driver/sfs/sqlite/dbconn.h>
#include <driver/sfs/sqlite/sqlite_buckets.h>
#include <driver/sfs/sqlite/sqlite_multipart.h>
#include <fmt/core.h>

#include <cerrno>
#include <fstream>
#include <limits>
#include <string>

#include "common/Formatter.h"
#include "driver/sfs/multipart.h"
#include "driver/sfs/object.h"
#include "driver/sfs/object_state.h"
#include "driver/sfs/sfs_log.h"
#include "driver/sfs/sqlite/objects/object_definitions.h"
#include "driver/sfs/sqlite/sqlite_list.h"
#include "driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "driver/sfs/types.h"
#include "rgw_common.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw_sfs

using namespace std;
using namespace sqlite_orm;

namespace rgw::sal {

SFSBucket::SFSBucket(SFStore* _store, sfs::BucketRef _bucket)
    : StoreBucket(_bucket->get_info()), store(_store), bucket(_bucket) {
  set_attrs(bucket->get_attrs());
  mtime = _bucket->get_mtime();

  auto it = attrs.find(RGW_ATTR_ACL);
  if (it != attrs.end()) {
    auto lval = it->second.cbegin();
    acls.decode(lval);
  }
}

void SFSBucket::write_meta(const DoutPrefixProvider* /*dpp*/) {
  // TODO
}

void SFSBucket::Meta::dump(ceph::Formatter* f) const {
  f->open_object_section("info");
  info.dump(f);
  f->close_section();
}

void SFSBucket::Meta::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("info", info, obj);
}

std::unique_ptr<Object> SFSBucket::get_object(const rgw_obj_key& key) {
  ldout(store->ceph_context(), SFS_LOG_DEBUG)
      << "bucket::" << __func__ << ": key : " << key << dendl;
  try {
    auto objref = bucket->get(key);
    // bucket->get retrieves all the information from the db
    // (incling the version_id for the last version)
    // But in cases like delete operations we don't want to update the
    // instance. That could convert a "delete marker" operation into a "delete
    // specific version" operation.
    // Return the object with the same key as it was requested.
    objref->instance = key.instance;
    return make_unique<SFSObject>(this->store, key, this, bucket);
  } catch (const sfs::UnknownObjectException& _) {
    ldout(store->ceph_context(), SFS_LOG_VERBOSE)
        << "unable to find key " << key << " in bucket " << bucket->get_name()
        << dendl;
    // possibly a copy, return a placeholder
    return make_unique<SFSObject>(this->store, key, this, bucket);
  }
}

int SFSBucket::verify_list_params(
    const DoutPrefixProvider* dpp, const ListParams& params, int max
) const {
  if (max < 0) {
    return -EINVAL;
  }
  if (params.allow_unordered) {
    // allow unordered is a ceph extension intended to improve performance
    // of list() by not sorting through results from all over the cluster
    lsfs_dout(
        dpp, SFS_LOG_VERBOSE
    ) << "unsupported allow unordered list requested. returning ordered result."
      << get_name() << dendl;

    // unordered only supports a limited set of filters. check this here
    // to not surprise clients
    if (!params.delim.empty()) {
      return -ENOTSUP;
    }
  }
  if (!params.end_marker.empty()) {
    lsfs_verb(dpp) << "unsupported end marker (SWIFT) requested " << get_name()
                   << dendl;
    return -ENOTSUP;
  }
  if (!params.ns.empty() && params.ns != RGW_OBJ_NS_MULTIPART) {
    return -ENOTSUP;
  }
  if (params.ns == RGW_OBJ_NS_MULTIPART) {
    if (params.list_versions) {
      return -EINVAL;
    }
  }
  if (params.ns.empty()) {
    if (params.access_list_filter) {
      return -ENOTSUP;
    }
  }
  if (params.force_check_filter) {
    // RADOS extension. forced filter by func()
    return -ENOTSUP;
  }
  if (params.shard_id != RGW_NO_SHARD) {
    // we don't support sharding
    return -ENOTSUP;
  }
  return 0;
}

/**
 * List objects in this bucket.
 */
int SFSBucket::list(
    const DoutPrefixProvider* dpp, ListParams& params, int max,
    ListResults& results, optional_yield /* y */
) {
  lsfs_debug(dpp) << fmt::format(
                         "listing bucket {} {} {}: max:{} params:", get_name(),
                         params.ns == RGW_OBJ_NS_MULTIPART ? "multipart" : "",
                         params.list_versions ? "versions" : "objects", max
                     )
                  << params << dendl;

  const int list_params_ok = verify_list_params(dpp, params, max);
  if (list_params_ok < 0) {
    return list_params_ok;
  }
  if (max == 0) {
    results.is_truncated = false;
    return 0;
  }

  // LC: Like list_multiparts, but returns (1) identifier + (2) mtime to
  // (1) find the entry via get_multipart_upload, (2) LC expire
  if (params.ns == RGW_OBJ_NS_MULTIPART) {
    // Ignore params.access_list_filter. A filter for multipart "meta"
    // objects that SFS doesn't have.
    sfs::sqlite::SQLiteMultipart multipart(store->db_conn);
    std::vector<sfs::sqlite::DBMultipart> multiparts =
        multipart.list_multiparts_by_bucket_id(
            get_bucket_id(), params.prefix, params.marker.name, "", max,
            &results.is_truncated, false
        );
    for (const auto& mp : multiparts) {
      rgw_bucket_dir_entry e;
      e.key.name = std::to_string(mp.id);
      e.meta.mtime = mp.mtime;
      results.objs.emplace_back(e);
    }
    lsfs_debug(dpp) << fmt::format(
                           "success (prefix:{}, start_after:{}, "
                           "max:{}). #objs_returned:{} "
                           "next:{} have_more:{}",
                           params.prefix, params.marker.name, max, params.delim,
                           results.objs.size(), results.next_marker,
                           results.is_truncated
                       )
                    << dendl;
    return 0;
  }

  sfs::sqlite::SQLiteList list(store->db_conn);
  std::string start_with(params.marker.name);
  if (!params.delim.empty()) {
    // Having a marker and delimiter means that the user wants to skip
    // a whole common prefix. Since we compare names ASCII greater
    // than style, append characters to make this the "greatest"
    // prefixed name
    auto delim_pos = start_with.find(params.delim);
    if (delim_pos != params.delim.npos) {
      start_with.append(
          sfs::S3_MAX_OBJECT_NAME_BYTES - delim_pos,
          std::numeric_limits<char>::max()
      );
    }
  }

  // Version listing on unversioned buckets is equivalent to object listing
  const bool want_list_versions =
      versioning_enabled() ? params.list_versions : false;
  const bool listing_succeeded = [&]() {
    if (want_list_versions) {
      return list.versions(
          get_bucket_id(), params.prefix, start_with, max, results.objs,
          &results.is_truncated
      );
    } else {
      return list.objects(
          get_bucket_id(), params.prefix, start_with, max, results.objs,
          &results.is_truncated
      );
    }
  }();
  if (!listing_succeeded) {
    lsfs_info(dpp) << fmt::format(
                          "list (prefix:{}, start_after:{}, "
                          "max:{}) failed.",
                          params.prefix, start_with, max, results.objs.size(),
                          results.next_marker, results.is_truncated
                      )
                   << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  if (!params.delim.empty()) {
    std::vector<rgw_bucket_dir_entry> new_results;
    list.roll_up_common_prefixes(
        params.prefix, params.delim, results.objs, results.common_prefixes,
        new_results
    );

    // Is there actually more after the rolled up common prefix? We
    // can't tell from the original object query. Ask again for
    // anything after the prefix.
    if (!results.common_prefixes.empty()) {
      std::vector<rgw_bucket_dir_entry> objects_after;
      std::string query = std::prev(results.common_prefixes.end())->first;
      query.append(
          sfs::S3_MAX_OBJECT_NAME_BYTES - query.size(),
          std::numeric_limits<char>::max()
      );
      list.objects(get_bucket_id(), params.prefix, query, 1, objects_after);
      results.is_truncated = objects_after.size() > 0;
    }
    lsfs_debug(dpp) << fmt::format(
                           "common prefix rollup #objs:{} -> #objs:{}, "
                           "#prefix:{}, more:{}",
                           results.objs.size(), new_results.size(),
                           results.common_prefixes.size(), results.is_truncated
                       )
                    << dendl;

    results.objs = new_results;
  }
  if (results.is_truncated) {
    if (results.common_prefixes.empty()) {
      const auto& last = results.objs.back();
      results.next_marker = rgw_obj_key(last.key.name, last.key.instance);
    } else {
      const std::string& last = std::prev(results.common_prefixes.end())->first;
      results.next_marker = rgw_obj_key(last);
    }
  }

  // Since we don't support per-object ownership (a bucket ACL
  // feature), apply the bucket owner to every object.
  // See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html
  // TODO(irq0) make conditional when SAL gains support for that
  sfs::sqlite::SQLiteBuckets buckets(store->db_conn);
  const auto maybe_owner = buckets.get_owner(get_bucket_id());
  if (maybe_owner.has_value()) {
    for (auto& obj : results.objs) {
      obj.meta.owner = maybe_owner->first;
      obj.meta.owner_display_name = maybe_owner->second;
    }
  }

  lsfs_debug(dpp)
      << fmt::format(
             "success (prefix:{}, start_after:{}, "
             "max:{} delim:{}). #objs_returned:{} "
             "?owner:{} ?versionlist:{} #common_pref:{} next:{} have_more:{}",
             params.prefix, start_with, max, params.delim, results.objs.size(),
             maybe_owner.has_value(), want_list_versions,
             results.common_prefixes.size(), results.next_marker,
             results.is_truncated
         )
      << dendl;
  return 0;
}

int SFSBucket::remove_bucket(
    const DoutPrefixProvider* dpp, bool delete_children,
    bool /*forward_to_master*/, req_info* /*req_info*/, optional_yield y
) {
  if (!delete_children) {
    if (check_empty(dpp, y)) {
      return -ENOENT;
    }
  }

  auto res = sfs::SFSMultipartUploadV2::abort_multiparts(dpp, store, this);
  if (res < 0) {
    lsfs_err(dpp) << fmt::format(
                         "unable to abort multiparts on bucket {}: {}",
                         get_name(), res
                     )
                  << dendl;
    if (res == -ERR_NO_SUCH_BUCKET) {
      return -ENOENT;
    }
    return -1;
  }

  // at this point bucket should be empty and we're good to go
  sfs::sqlite::SQLiteBuckets db_buckets(store->db_conn);
  auto db_bucket = db_buckets.get_bucket(get_bucket_id());
  if (!db_bucket.has_value()) {
    lsfs_verb(dpp) << __func__ << ": Bucket metadata was not found." << dendl;
    return -ENOENT;
  }
  db_bucket->deleted = true;
  db_bucket->mtime = ceph::real_time::clock::now();
  db_buckets.store_bucket(*db_bucket);
  store->_delete_bucket(get_name());
  return 0;
}

int SFSBucket::remove_bucket_bypass_gc(
    int /*concurrent_max*/, bool /*keep_index_consistent*/,
    optional_yield /*y*/, const DoutPrefixProvider* dpp
) {
  /** Remove this bucket, bypassing garbage collection.  May be removed */
  lsfs_warn(dpp) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::load_bucket(
    const DoutPrefixProvider* /*dpp*/, optional_yield /*y*/, bool /*get_stats*/
) {
  // TODO
  return 0;
}

int SFSBucket::set_acl(
    const DoutPrefixProvider* /*dpp*/, RGWAccessControlPolicy& acl,
    optional_yield /*y*/
) {
  acls = acl;

  bufferlist aclp_bl;
  acls.encode(aclp_bl);
  attrs[RGW_ATTR_ACL] = aclp_bl;

  sfs::get_meta_buckets(get_store().db_conn)
      ->store_bucket(get_db_op_bucket_info());

  store->_refresh_buckets_safe();
  return 0;
}

int SFSBucket::chown(
    const DoutPrefixProvider* dpp, User& /*new_user*/, optional_yield /*y*/
) {
  lsfs_warn(dpp) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

bool SFSBucket::is_owner(User* /*user*/) {
  ldout(store->ceph_context(), SFS_LOG_WARN) << __func__ << ": TODO" << dendl;
  return true;
}

int SFSBucket::
    check_empty(const DoutPrefixProvider* dpp, optional_yield /*y*/) {
  /** Check in the backing store if this bucket is empty */
  // check if there are still objects owned by the bucket
  sfs::sqlite::SQLiteBuckets db_buckets(store->db_conn);
  if (!db_buckets.bucket_empty(get_bucket_id())) {
    lsfs_debug(dpp) << __func__ << ": Bucket Not Empty." << dendl;
    return -ENOTEMPTY;
  }
  return 0;
}

int SFSBucket::merge_and_store_attrs(
    const DoutPrefixProvider* /*dpp*/, Attrs& new_attrs, optional_yield /*y*/
) {
  for (auto& it : new_attrs) {
    attrs[it.first] = it.second;

    if (it.first == RGW_ATTR_ACL) {
      auto lval = it.second.cbegin();
      acls.decode(lval);
    }
  }
  for (auto& it : attrs) {
    auto it_find = new_attrs.find(it.first);
    if (it_find == new_attrs.end()) {
      // this is an old attr that is not defined in the new_attrs
      // delete it
      attrs.erase(it.first);
    }
  }

  sfs::get_meta_buckets(get_store().db_conn)
      ->store_bucket(get_db_op_bucket_info());

  store->_refresh_buckets_safe();
  return 0;
}

// try_resolve_mp_from_oid tries to parse an integer id from oid to
// find an MP upload, returning object_name and upload_id
static bool try_resolve_mp_from_oid(
    sfs::sqlite::DBConnRef dbconn, const std::string& oid,
    std::string& out_object_name, std::string& out_upload_id
) {
  sfs::sqlite::SQLiteMultipart mpdb(dbconn);
  std::string err;
  const int id = strict_strtol(oid, 10, &err);
  if ((id == 0) && !err.empty()) {
    return false;
  }
  std::optional<sfs::sqlite::DBMultipart> maybe_mp = mpdb.get_multipart(id);
  if (!maybe_mp.has_value()) {
    return false;
  }
  out_object_name = maybe_mp->object_name;
  out_upload_id = maybe_mp->upload_id;
  return true;
}

std::unique_ptr<MultipartUpload> SFSBucket::get_multipart_upload(
    const std::string& with_oid, std::optional<std::string> with_upload_id,
    ACLOwner with_owner, ceph::real_time with_mtime
) {
  ldout(store->ceph_context(), SFS_LOG_DEBUG)
      << "bucket::" << __func__ << ": oid: " << with_oid
      << ", upload id: " << with_upload_id << dendl;

  std::string next_oid(with_oid);
  std::string next_upload_id;
  // LC calls this to mp.abort() the resulting MultipartUpload
  // `with_oid` from bucket->list(ns=multipart) and `with_upload_id`
  // == nullopt. To _uniquily_ identify MPs we interpret `oid` as an
  // ID into the MP table and resolve that to a upload id here.
  if (!with_upload_id.has_value() &&
      try_resolve_mp_from_oid(
          store->db_conn, with_oid, next_oid, next_upload_id
      )) {
    ldout(store->ceph_context(), SFS_LOG_DEBUG)
        << fmt::format(
               "called without upload_id. resolved oid {} to MP oid:{} "
               "upload:{}",
               with_oid, next_oid, next_upload_id
           )
        << dendl;
  } else {
    next_upload_id = with_upload_id.value_or("");
  }
  if (next_upload_id.empty()) {
    next_upload_id = bucket->gen_multipart_upload_id();
  }
  ceph_assert(!next_upload_id.empty());
  ceph_assert(!next_oid.empty());
  return std::make_unique<sfs::SFSMultipartUploadV2>(
      store, this, bucket, next_upload_id, next_oid, with_owner, with_mtime
  );
}

/**
 * @brief Obtain a list of on-going multipart uploads on this bucket.
 *
 * @param dpp
 * @param prefix
 * @param marker First key (non-inclusive) to be returned. This is not the same
 * key as the one the user provides; instead, it's the meta-key for the upload
 * with the key the user provided.
 * @param delim
 * @param max_uploads Maximum number of entries in the list. Defaults to 1000.
 * @param uploads Vector to be populated with the results.
 * @param common_prefixes
 * @param is_truncated Whether the returned list is complete.
 * @return int
 */
int SFSBucket::list_multiparts(
    const DoutPrefixProvider* dpp, const std::string& prefix,
    std::string& marker, const std::string& delim, const int& max_uploads,
    std::vector<std::unique_ptr<MultipartUpload>>& uploads,
    std::map<std::string, bool>* common_prefixes, bool* is_truncated
) {
  lsfs_debug(dpp) << fmt::format(
                         "prefix: {}, marker: {}, delim: {}, max_uploads: {}",
                         prefix, marker, delim, max_uploads
                     )
                  << dendl;

  return sfs::SFSMultipartUploadV2::list_multiparts(
      dpp, store, this, bucket, prefix, marker, delim, max_uploads, uploads,
      common_prefixes, is_truncated
  );
}

int SFSBucket::abort_multiparts(
    const DoutPrefixProvider* dpp, CephContext* /*cct*/
) {
  lsfs_debug(dpp
  ) << fmt::format("aborting multipart uploads on bucket {}", get_name())
    << dendl;
  return sfs::SFSMultipartUploadV2::abort_multiparts(dpp, store, this);
}

int SFSBucket::try_refresh_info(
    const DoutPrefixProvider* dpp, ceph::real_time* /*pmtime*/
) {
  lsfs_warn(dpp) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::read_usage(
    const DoutPrefixProvider* dpp, uint64_t /*start_epoch*/,
    uint64_t /*end_epoch*/, uint32_t /*max_entries*/, bool* /*is_truncated*/,
    RGWUsageIter& /*usage_iter*/,
    std::map<rgw_user_bucket, rgw_usage_log_entry>& /*usage*/
) {
  lsfs_warn(dpp) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSBucket::trim_usage(
    const DoutPrefixProvider* dpp, uint64_t /*start_epoch*/,
    uint64_t /*end_epoch*/
) {
  lsfs_warn(dpp) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::rebuild_index(const DoutPrefixProvider* dpp) {
  lsfs_warn(dpp) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SFSBucket::check_quota(
    const DoutPrefixProvider* dpp, RGWQuota& quota, uint64_t obj_size,
    optional_yield /*y*/, bool /*check_size_only*/
) {
  lsfs_debug(dpp) << __func__
                  << ": user(max size: " << quota.user_quota.max_size
                  << ", max objs: " << quota.user_quota.max_objects
                  << "), bucket(max size: " << quota.bucket_quota.max_size
                  << ", max objs: " << quota.bucket_quota.max_objects
                  << "), obj size: " << obj_size << dendl;
  lsfs_warn(dpp) << __func__ << ": not implemented, return okay." << dendl;
  return 0;
}

int SFSBucket::read_stats(
    const DoutPrefixProvider* /*dpp*/,
    const bucket_index_layout_generation& /*idx_layout*/, int /*shard_id*/,
    std::string* /*bucket_ver*/, std::string* /*master_ver*/,
    std::map<RGWObjCategory, RGWStorageStats>& /*stats*/,
    std::string* /*max_marker*/, bool* /*syncstopped*/
) {
  return 0;
}
int SFSBucket::read_stats_async(
    const DoutPrefixProvider* /*dpp*/,
    const bucket_index_layout_generation& /*idx_layout*/, int /*shard_id*/,
    RGWGetBucketStats_CB* /*ctx*/
) {
  return 0;
}

int SFSBucket::sync_user_stats(
    const DoutPrefixProvider* /*dpp*/, optional_yield /*y*/
) {
  return 0;
}

int SFSBucket::update_container_stats(const DoutPrefixProvider* dpp) {
  lsfs_debug(dpp) << fmt::format(
                         "update bucket {} (id {}) stats", get_name(),
                         get_bucket_id()
                     )
                  << dendl;
  sfs::sqlite::SQLiteBuckets bucketdb(store->db_conn);
  auto stats = bucketdb.get_stats(get_bucket_id());

  if (!stats.has_value()) {
    lsfs_verb(dpp) << fmt::format(
                          "unable to obtain stats for bucket {} (id {}) -- "
                          "no such bucket!",
                          get_name(), get_bucket_id()
                      )
                   << dendl;
    return -ERR_NO_SUCH_BUCKET;
  }

  lsfs_debug(dpp) << fmt::format(
                         "bucket {} stats: size: {}, obj_cnt: {}", get_name(),
                         stats->size, stats->obj_count
                     )
                  << dendl;
  ent.size = ent.size_rounded = stats->size;
  ent.count = stats->obj_count;
  return 0;
}

int SFSBucket::check_bucket_shards(const DoutPrefixProvider* dpp) {
  lsfs_warn(dpp) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SFSBucket::put_info(
    const DoutPrefixProvider* /*dpp*/, bool /*exclusive*/,
    ceph::real_time /*set_mtime*/
) {
  if (get_info().flags & BUCKET_VERSIONS_SUSPENDED) {
    return -ERR_NOT_IMPLEMENTED;
  }

  sfs::get_meta_buckets(get_store().db_conn)
      ->store_bucket(get_db_op_bucket_info());

  store->_refresh_buckets_safe();
  return 0;
}

}  // namespace rgw::sal
