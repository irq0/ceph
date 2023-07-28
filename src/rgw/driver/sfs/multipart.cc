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
#include "multipart.h"

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include <fstream>

#include "rgw/driver/sfs/fmt.h"
#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/driver/sfs/sqlite/buckets/multipart_definitions.h"
#include "rgw_sal_sfs.h"
#include "sqlite/buckets/multipart_conversions.h"
#include "writer.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal::sfs {

SFSMultipartUploadV2::SFSMultipartUploadV2(
    rgw::sal::SFStore* _store, SFSBucket* _bucket, sfs::BucketRef _bucketref,
    const std::string& _upload_id, const std::string& _oid, ACLOwner _owner,
    ceph::real_time _mtime
)
    : StoreMultipartUpload(_bucket),
      store(_store),
      bucketref(_bucketref),
      upload_id(_upload_id),
      oid(_oid),
      owner(_owner),
      mtime(_mtime),
      meta_str("_meta" + _oid + "." + _upload_id) {
  // load required data from db, if available.
  sfs::sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto mp = mpdb.get_multipart(upload_id);
  if (mp.has_value()) {
    placement = mp->placement;
  }
}

std::unique_ptr<rgw::sal::Object> SFSMultipartUploadV2::get_meta_obj() {
  rgw_obj_key key(meta_str, string(), RGW_OBJ_NS_MULTIPART);
  auto mmo =
      std::make_unique<SFSMultipartMetaObject>(store, key, bucket, bucketref);

  sfs::sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto mp = mpdb.get_multipart(upload_id);
  ceph_assert(mp.has_value());
  mmo->set_attrs(mp->attrs);
  // TODO(jecluis): this needs to be fixed once we get rid of the objref
  mmo->set_object_ref(
      std::shared_ptr<sfs::Object>(sfs::Object::create_from_obj_key(key))
  );
  return mmo;
}

int SFSMultipartUploadV2::init(
    const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& acl_owner,
    rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs
) {
  lsfs_dout(dpp, 10) << "upload_id: " << upload_id << ", oid: " << get_key()
                     << ", meta: " << meta_str
                     << ", owner: " << acl_owner.get_display_name()
                     << ", attrs: " << attrs << dendl;

  sfs::sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto mp = mpdb.get_multipart(upload_id);
  if (mp.has_value()) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "BUG: upload already exists, upload_id: {}, oid: {}", upload_id,
               get_key()
           )
        << dendl;
    // upload exists and has been initiated, return error.
    return -ERR_INTERNAL_ERROR;
  }

  // create multipart
  uuid_d uuid;
  uuid.generate_random();
  auto now = ceph::real_time::clock::now();

  sfs::sqlite::DBOPMultipart mpop{
      .bucket_id = bucket->get_bucket_id(),
      .upload_id = upload_id,
      .state = sfs::MultipartState::INIT,
      .state_change_time = now,
      .object_name = oid,
      .object_uuid = uuid,
      .meta_str = meta_str,
      .owner_id = acl_owner,
      .mtime = now,
      .attrs = attrs,
      .placement = dest_placement,
  };

  try {
    auto id = mpdb.insert(mpop);
    ceph_assert(id > 0);
  } catch (std::system_error& e) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "BUG: upload already exists, raced! upload_id: {}, oid: {}",
               upload_id, get_key()
           )
        << dendl;
    return -ERR_INTERNAL_ERROR;
  }
  lsfs_dout(dpp, 10)
      << fmt::format(
             "created multipart upload_id: {}, oid: {}, owner: {}", upload_id,
             get_key(), acl_owner.get_display_name()
         )
      << dendl;
  lsfs_dout(dpp, 10) << "attrs: " << attrs << dendl;

  // set placement in case we get an info request
  placement = dest_placement;
  return 0;
}

int SFSMultipartUploadV2::list_parts(
    const DoutPrefixProvider* dpp, CephContext* cct, int num_parts, int marker,
    int* next_marker, bool* truncated, bool assume_unsorted
) {
  lsfs_dout(dpp, 10) << "num_parts: " << num_parts << ", marker: " << marker
                     << dendl;

  ceph_assert(marker >= 0);
  ceph_assert(num_parts >= 0);

  sfs::sqlite::SQLiteMultipart mpdb(store->db_conn);

  auto entries =
      mpdb.list_parts(upload_id, num_parts, marker, next_marker, truncated);

  for (const auto& entry : entries) {
    if (!entry.is_finished()) {
      // part is not finished, ignore.
      continue;
    }
    parts[entry.part_num] = std::make_unique<SFSMultipartPartV2>(entry);
  }
  return 0;
}

int SFSMultipartUploadV2::abort(
    const DoutPrefixProvider* dpp, CephContext* cct
) {
  lsfs_dout(dpp, 10) << "upload_id: " << upload_id << dendl;

  sfs::sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto res = mpdb.abort(upload_id);

  lsfs_dout(dpp, 10) << "upload_id: " << upload_id << ", aborted: " << res
                     << dendl;

  return (res ? 0 : -ERR_NO_SUCH_UPLOAD);
}

int SFSMultipartUploadV2::complete(
    const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
    std::map<int, std::string>& part_etags,
    std::list<rgw_obj_index_key>& remove_objs, uint64_t& accounted_size,
    bool& compressed, RGWCompressionInfo& cs_info, off_t& ofs, std::string& tag,
    ACLOwner& acl_owner, uint64_t olh_epoch, rgw::sal::Object* target_obj
) {
  lsfs_dout(dpp, 10) << fmt::format(
                            "upload_id: {}, accounted_size: {}, tag: {}, "
                            "owner: {}, olh_epoch: {}"
                            ", target_obj: {}",
                            upload_id, accounted_size, tag,
                            acl_owner.get_display_name(), olh_epoch,
                            target_obj->get_key()
                        )
                     << dendl;
  lsfs_dout(dpp, 10) << "part_etags: " << part_etags << dendl;

  sfs::sqlite::SQLiteMultipart mpdb(store->db_conn);
  bool duplicate = false;
  auto res = mpdb.mark_complete(upload_id, &duplicate);
  if (!res) {
    lsfs_dout(dpp, 10) << fmt::format(
                              "unable to find on-going multipart upload id {}",
                              upload_id
                          )
                       << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  } else if (duplicate) {
    lsfs_dout(dpp, 10)
        << fmt::format(
               "multipart id {} already completed, returning success!",
               upload_id
           )
        << dendl;
    return 0;
  }

  auto parts = mpdb.get_parts(upload_id);
  if (parts.size() != part_etags.size()) {
    return -ERR_INVALID_PART;
  }

  auto mp = mpdb.get_multipart(upload_id);
  ceph_assert(mp.has_value());
  ceph_assert(mp->upload_id == upload_id);
  ceph_assert(mp->state == sfs::MultipartState::COMPLETE);

  // validate parts & build final etag

  // we can only have at most 10k parts
  if (part_etags.size() > 10000) {
    return -ERR_INVALID_PART;
  }

  std::map<int, sqlite::DBMultipartPart> to_complete;
  std::map<int, sqlite::DBMultipartPart> parts_map;
  for (const auto& p : parts) {
    parts_map[p.part_num] = p;
  }

  ETagBuilder hash;
  uint64_t expected_size = 0;
  // by nature, the `part_etags` map is an ordered container; all parts are
  // already provided sorted.
  for (auto it = part_etags.cbegin(); it != part_etags.cend(); ++it) {
    auto& k = it->first;
    auto& v = it->second;

    auto p = parts_map.find(k);
    if (p == parts_map.end()) {
      lsfs_dout(dpp, 1) << fmt::format(
                               "client-specified part {} does not exist!", k
                           )
                        << dendl;
      return -ERR_INVALID_PART;
    }
    auto part = p->second;
    if (!part.is_finished()) {
      lsfs_dout(
          dpp, 1
      ) << fmt::format("client-specified part {} is not finished yet!", k)
        << dendl;
      return -ERR_INVALID_PART;

    } else if (!part.etag.has_value()) {
      lsfs_dout(
          dpp, -1
      ) << fmt::format("BUG: Part {} is finished and should have an etag!", k)
        << dendl;
      return -ERR_INTERNAL_ERROR;
    }

    ceph_assert(part.etag.has_value());
    auto part_etag = part.etag.value();
    auto etag = rgw_string_unquote(v);
    if (part_etag != etag) {
      lsfs_dout(dpp, 1)
          << fmt::format(
                 "client-specified part {} etag mismatch; expected {}, got {}",
                 k, part_etag, etag
             )
          << dendl;
      return -ERR_INVALID_PART;
    }

    if ((part.size < 5 * 1024 * 1024) &&
        (std::distance(it, part_etags.cend()) > 1)) {
      lsfs_dout(dpp, 1) << fmt::format(
                               "part {} is too small and not the last part!", k
                           )
                        << dendl;
      return -ERR_TOO_SMALL;
    }

    to_complete[k] = p->second;
  }

  if (store->filesystem_stats_avail_bytes.load() < expected_size) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "filesystem stat reservation check hit. "
                              "avail_bytes: {}, avail_pct: "
                              "{}, total_bytes: {}, expected size: {}",
                              store->filesystem_stats_avail_bytes,
                              store->filesystem_stats_avail_percent,
                              store->filesystem_stats_total_bytes, expected_size
                          )
                       << dendl;
    return -ERR_QUOTA_EXCEEDED;
  }

  // calculate final etag
  std::string etag = fmt::format("{}-{}", hash.final(), part_etags.size());

  lsfs_dout(dpp, 10) << fmt::format(
                            "upload_id: {}, final etag: {}", upload_id, etag
                        )
                     << dendl;

  // start aggregating final object
  res = mpdb.mark_aggregating(upload_id);
  ceph_assert(res == true);

  std::filesystem::path objpath =
      store->get_data_path() / UUIDPath(mp->object_uuid).to_path();

  int objfd = ::open(objpath.c_str(), O_WRONLY | O_BINARY | O_CREAT, 0600);
  if (objfd < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "unable to open object file {} to write: {}",
                              objpath, cpp_strerror(errno)
                          )
                       << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  size_t accounted_bytes = 0;

  for (const auto& [part_num, part] : to_complete) {
    MultipartPartPath partpath(mp->object_uuid, part_num);
    std::filesystem::path path = store->get_data_path() / partpath.to_path();

    ceph_assert(std::filesystem::exists(path));
    auto partsize = std::filesystem::file_size(path);
    if (partsize != part.size) {
      lsfs_dout(dpp, 1) << fmt::format(
                               "part size mismatch, expected {}, found: {}",
                               part.size, partsize
                           )
                        << dendl;
      return -ERR_INVALID_PART;
    }

    int partfd = ::open(path.c_str(), O_RDONLY | O_BINARY);
    if (partfd < 0) {
      lsfs_dout(dpp, -1) << fmt::format(
                                "unable to open part file {} for reading: {}",
                                path, cpp_strerror(errno)
                            )
                         << dendl;
      return -ERR_INTERNAL_ERROR;
    }
    int ret = ::copy_file_range(partfd, NULL, objfd, NULL, partsize, 0);
    if (ret < 0) {
      // this is an unexpected error, we don't know how to recover from it.
      lsfs_dout(dpp, -1)
          << fmt::format(
                 "unable to copy part {} (fd {}) to object file {} (fd {}): {}",
                 part.part_num, partfd, objpath, objfd, cpp_strerror(errno)
             )
          << dendl;
      ceph_abort("Unexpected error aggregating multipart upload");
    }
    accounted_bytes += partsize;
    ret = ::fsync(objfd);
    if (ret < 0) {
      lsfs_dout(dpp, -1) << fmt::format(
                                "failed fsync fd: {}, on obj file: {}: {}",
                                objfd, objpath, cpp_strerror(ret)
                            )
                         << dendl;
      ceph_abort("Unexpected error fsync'ing obj path");
    }
    ret = ::close(partfd);
    if (ret < 0) {
      lsfs_dout(dpp, -1) << fmt::format(
                                "failed closing fd: {}, on part file: {}: {}",
                                partfd, path, cpp_strerror(ret)
                            )
                         << dendl;
      ceph_abort("Unexpected error on closing part path");
    }
  }

  int ret = ::close(objfd);
  if (ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed closing fd: {}, on obj file: {}: {}",
                              objfd, objpath, cpp_strerror(ret)
                          )
                       << dendl;
  }
  auto final_obj_size = std::filesystem::file_size(objpath);
  if (accounted_bytes != final_obj_size) {
    // this is an unexpected error, probably a bug - die.
    lsfs_dout(dpp, -1) << fmt::format(
                              "BUG: expected {} bytes, found {} bytes",
                              accounted_bytes, final_obj_size
                          )
                       << dendl;
    ceph_abort("BUG: on final object for multipart upload!");
  }

  lsfs_dout(dpp, 10)
      << fmt::format(
             "finished building final object file at {}, size: {}, etag: {}",
             objpath, final_obj_size, etag
         )
      << dendl;

  // NOTE(jecluis): this is the annoying bit: we have the final object having
  // been built at the path described by `mp->obj_uuid`, but we need to have it
  // as a new version of the object of `mp->obj_name`. We will need to create a
  // new object, or a new version, and move the file to its location as if we
  // were writing directly to it.

  ObjectRef objref;
  try {
    objref = bucketref->create_version(target_obj->get_key());
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "error while fetching obj ref from bucket: {}, oid: {}: {}",
               bucketref->get_bucket_id(), mp->object_name, e.what()
           )
        << dendl;
    return -ERR_INTERNAL_ERROR;
  }
  auto destpath = store->get_data_path() / objref->get_storage_path();
  lsfs_dout(
      dpp, 10
  ) << fmt::format("moving final object from {} to {}", objpath, destpath)
    << dendl;

  std::error_code ec;
  std::filesystem::create_directories(destpath.parent_path(), ec);
  if (ec) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "failed to create directories for destination object {}: {}",
               destpath, ec.message()
           )
        << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  ec.clear();
  ret = ::rename(objpath.c_str(), destpath.c_str());
  if (ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to rename object file from {} to {}: {}",
                              objpath, destpath, cpp_strerror(errno)
                          )
                       << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  objref->update_attrs(mp->attrs);
  objref->update_meta(
      {.size = accounted_bytes,
       .etag = etag,
       .mtime = ceph::real_time::clock::now()}
  );
  try {
    objref->metadata_finish(store, bucketref->get_info().versioning_enabled());
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to update db object {}: {}", objref->name,
                              e.what()
                          )
                       << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  // mark multipart upload done
  res = mpdb.mark_done(upload_id);
  ceph_assert(res);

  return 0;
}

int SFSMultipartUploadV2::get_info(
    const DoutPrefixProvider* dpp, optional_yield y, rgw_placement_rule** rule,
    rgw::sal::Attrs* attrs
) {
  lsfs_dout(dpp, 10) << fmt::format(
                            "upload_id: {}, obj: {}", upload_id, get_key()
                        )
                     << dendl;

  sfs::sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto mp = mpdb.get_multipart(upload_id);
  if (!mp.has_value()) {
    lsfs_dout(dpp, 10) << fmt::format(
                              "unable to find upload_id: {} in db", upload_id
                          )
                       << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  }

  if (mp->state != MultipartState::INIT &&
      mp->state != MultipartState::INPROGRESS) {
    lsfs_dout(dpp, 10) << fmt::format(
                              "upload id {} not in available state", upload_id
                          )
                       << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  }

  if (rule) {
    if (placement.empty()) {
      *rule = nullptr;
    } else {
      *rule = &placement;
    }
  }

  if (attrs) {
    *attrs = mp->attrs;
  }

  return 0;
}

std::unique_ptr<Writer> SFSMultipartUploadV2::get_writer(
    const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Object* head_obj,
    const rgw_user& writer_owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
    const std::string& part_num_str
) {
  ceph_assert(part_num <= 10000);
  uint32_t pnum = static_cast<uint32_t>(part_num);

  lsfs_dout(dpp, 10)
      << fmt::format(
             "head_obj: {}, owner: {}, upload_id: {}, part_num: {}",
             head_obj->get_key().name, writer_owner.id, upload_id, pnum
         )
      << dendl;

  return std::make_unique<SFSMultipartWriterV2>(dpp, y, upload_id, store, pnum);
}

int SFSMultipartUploadV2::list_multiparts(
    const DoutPrefixProvider* dpp, rgw::sal::SFStore* store,
    rgw::sal::SFSBucket* bucket, BucketRef bucketref, const std::string& prefix,
    std::string& marker, const std::string& delim, const int& max_uploads,
    std::vector<std::unique_ptr<MultipartUpload>>& uploads,
    std::map<std::string, bool>* common_prefixes, bool* is_truncated
) {
  auto cls = SFSMultipartUploadV2::get_cls_name();
  auto bucket_name = bucket->get_name();
  lsfs_dout_for(dpp, 10, cls)
      << fmt::format(
             "bucket: {}, prefix: {}, marker: {}, delim: {}, max_uploads: {}",
             bucket_name, prefix, marker, delim, max_uploads
         )
      << dendl;

  sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto entries = mpdb.list_multiparts(
      bucket_name, prefix, marker, delim, max_uploads, is_truncated
  );
  if (!entries.has_value()) {
    lsfs_dout_for(dpp, -1, cls) << fmt::format(
                                       "unable to find multipart uploads for "
                                       "bucket {} -- bucket not found!",
                                       bucket_name
                                   )
                                << dendl;
    return -ERR_NO_SUCH_BUCKET;
  }

  ceph_assert(uploads.size() == 0);  // make sure rgw is not being naughty :)
  for (const auto& entry : entries.value()) {
    uploads.push_back(std::make_unique<SFSMultipartUploadV2>(
        store, bucket, bucketref, entry.upload_id, entry.object_name,
        entry.owner_id, entry.mtime
    ));
    lsfs_dout_for(dpp, 10, cls)
        << fmt::format(
               "found multipart upload id: {}, bucket: {}, obj: {}",
               entry.upload_id, bucket->get_key().name, entry.object_name
           )
        << dendl;
  }
  lsfs_dout_for(dpp, 10, cls)
      << fmt::format("found {} multipart uploads", uploads.size()) << dendl;
  return 0;
}

int SFSMultipartUploadV2::abort_multiparts(
    const DoutPrefixProvider* dpp, rgw::sal::SFStore* store,
    rgw::sal::SFSBucket* bucket
) {
  auto cls = SFSMultipartUploadV2::get_cls_name();
  auto bucket_name = bucket->get_name();
  lsfs_dout_for(dpp, 10, cls)
      << fmt::format("bucket: {}", bucket_name) << dendl;

  sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto num_aborted = mpdb.abort_multiparts(bucket_name);
  if (num_aborted < 0) {
    lsfs_dout_for(dpp, -1, cls) << fmt::format(
                                       "error aborting multipart uploads on "
                                       "bucket {} -- bucket not found!",
                                       bucket_name
                                   )
                                << dendl;
    return -ERR_NO_SUCH_BUCKET;
  }
  lsfs_dout_for(dpp, 10, cls)
      << fmt::format(
             "aborted {} multipart uploads on bucket {}", num_aborted,
             bucket_name
         )
      << dendl;
  return 0;
}

}  // namespace rgw::sal::sfs
