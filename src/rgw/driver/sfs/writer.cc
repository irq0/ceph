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
#include "driver/sfs/writer.h"

#include <fmt/ostream.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <memory>
#include <ranges>
#include <system_error>

#include "driver/sfs/bucket.h"
#include "driver/sfs/writer.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {
SFSAtomicWriter::SFSAtomicWriter(
    const DoutPrefixProvider* _dpp, optional_yield _y,
    rgw::sal::Object* _head_obj, SFStore* _store, sfs::BucketRef _bucketref,
    const rgw_user& _owner, const rgw_placement_rule* _ptail_placement_rule,
    uint64_t _olh_epoch, const std::string& _unique_tag
)
    : StoreWriter(_dpp, _y),
      store(_store),
      obj(_store, _head_obj->get_key(), _head_obj->get_bucket(), _bucketref,
          false),
      bucketref(_bucketref),
      owner(_owner),
      placement_rule(_ptail_placement_rule),
      olh_epoch(_olh_epoch),
      unique_tag(_unique_tag),
      bytes_written(0),
      io_failed(false),
      fd(-1) {
  lsfs_dout(dpp, 10) << fmt::format(
                            "head_obj: {}, bucket: {}", _head_obj->get_key(),
                            _head_obj->get_bucket()->get_name()
                        )
                     << dendl;
}

SFSAtomicWriter::~SFSAtomicWriter() {
  if (fd >= 0) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "BUG: fd:{} still open. closing. (io_failed:{}, object_path:{})",
               fd, io_failed, object_path.string()
           )
        << dendl;
    close();
  }
}

int SFSAtomicWriter::open() noexcept {
  std::error_code ec;
  std::filesystem::create_directories(object_path.parent_path(), ec);
  if (ec) {
    lsfs_dout(dpp, -1) << "failed to mkdir object path " << object_path << ": "
                       << ec << dendl;
    switch (ec.value()) {
      case ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }

  int ret;
  ret = ::open(
      object_path.c_str(), O_CREAT | O_TRUNC | O_CLOEXEC | O_WRONLY, 0644
  );
  if (ret < 0) {
    lsfs_dout(dpp, -1) << "error opening file " << object_path << ": "
                       << cpp_strerror(errno) << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  fd = ret;
  return 0;
}

int SFSAtomicWriter::close() noexcept {
  ceph_assert(fd >= 0);
  int result = 0;
  int ret;

  ret = ::fsync(fd);
  if (ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to fsync fd:{}: {}. continuing.", fd,
                              cpp_strerror(ret)
                          )
                       << dendl;
  }

  ret = ::close(fd);
  fd = -1;
  if (ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed closing fd:{}: {}. continuing.", fd,
                              cpp_strerror(ret)
                          )
                       << dendl;
    switch (ret) {
      case -EDQUOT:
      case -ENOSPC:
        result = -ERR_QUOTA_EXCEEDED;
        break;
      default:
        result = -ERR_INTERNAL_ERROR;
    }
    io_failed = true;
  }
  return result;
}

void SFSAtomicWriter::cleanup() noexcept {
  lsfs_dout(dpp, -1) << fmt::format(
                            "cleaning up failed upload to file {}. "
                            "returning error.",
                            object_path.string()
                        )
                     << dendl;

  std::error_code ec;
  std::filesystem::remove(object_path, ec);
  if (ec) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed deleting file {}: {} {}. ignoring.",
                              object_path.string(), ec.message(), ec.value()
                          )
                       << dendl;
  }

  const auto dir_fd = ::open(object_path.parent_path().c_str(), O_RDONLY);
  int ret = ::fsync(dir_fd);
  if (ret < 0) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "failed fsyncing dir {} fd:{} for obj file {}: {}. ignoring.",
               object_path.parent_path().string(), dir_fd, object_path.string(),
               cpp_strerror(ret)
           )
        << dendl;
  }

  try {
    sfs::ObjectDeleter deleter(
        object_path, store->db_conn, objref->path.get_uuid()
    );
    deleter.delete_version(objref->version_id);
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "failed to remove failed upload version from database {}: {}",
               store->db_conn->get_storage().filename(), e.what()
           )
        << dendl;
  }
}

int SFSAtomicWriter::prepare(optional_yield y) {
  if (store->filesystem_stats_avail_bytes.load() <
      store->min_space_left_for_data_write_ops_bytes) {
    lsfs_dout(dpp, 10) << fmt::format(
                              "filesystem stat reservation check hit. "
                              "avail_bytes:{} avail_pct:{} total_bytes:{}. "
                              "returning quota error.",
                              store->filesystem_stats_avail_bytes,
                              store->filesystem_stats_avail_percent,
                              store->filesystem_stats_total_bytes
                          )
                       << dendl;
    return -ERR_QUOTA_EXCEEDED;
  }

  try {
    objref = bucketref->get_or_create(obj.get_key());
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "exception while fetching obj ref from bucket {} db:{}: {}. "
               "failing operation.",
               bucketref->get_bucket_id(),
               store->db_conn->get_storage().filename(), e.what()
           )
        << dendl;
    switch (e.code().value()) {
      case -EDQUOT:
      case -ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }
  object_path = store->get_data_path() / objref->get_storage_path();

  lsfs_dout(dpp, 10) << "creating file at " << object_path << dendl;

  return open();
}

int SFSAtomicWriter::process(bufferlist&& data, uint64_t offset) {
  lsfs_dout(dpp, 10)
      << fmt::format(
             "data len: {}, offset: {}, io_failed: {}, fd: {}, fn: {}",
             data.length(), offset, io_failed, fd, object_path.string()
         )
      << dendl;
  if (io_failed) {
    return -ERR_INTERNAL_ERROR;
  }

  if (data.length() == 0) {
    lsfs_dout(dpp, 10) << "final piece, wrote " << bytes_written << " bytes"
                       << dendl;
    return 0;
  }

  ceph_assert(fd >= 0);
  int write_ret = data.write_fd(fd, offset);
  if (write_ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to write size:{} offset:{} to fd:{}: {}. "
                              "marking writer failed. "
                              "failing future io. "
                              "will delete partial data on completion. "
                              "returning internal error.",
                              data.length(), offset, fd, cpp_strerror(write_ret)
                          )
                       << dendl;
    io_failed = true;
    close();
    cleanup();
    switch (write_ret) {
      case -EDQUOT:
      case -ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }
  bytes_written += data.length();
  return 0;
}

int SFSAtomicWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y
) {
  lsfs_dout(dpp, 10)
      << fmt::format(
             "accounted_size: {}, etag: {}, set_mtime: {}, attrs: {}, "
             "delete_at: {}, if_match: {}, if_nomatch: {}",
             accounted_size, etag, to_iso_8601(set_mtime),
             fmt::join(std::views::keys(attrs), ", "), to_iso_8601(delete_at),
             if_match ? if_match : "NA", if_nomatch ? if_nomatch : "NA"
         )
      << dendl;

  const auto now = ceph::real_clock::now();
  if (bytes_written != accounted_size) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "data written != accounted size. {} vs. {}. failing operation. "
               "returning internal error.",
               bytes_written, accounted_size
           )
        << dendl;
    close();
    cleanup();
    return -ERR_INTERNAL_ERROR;
  }

  int result = close();
  if (io_failed) {
    cleanup();
    return result;
  }

  // for object-locking enabled buckets, set the bucket's object-locking
  // profile when not defined on the object itself
  if (bucketref->get_info().obj_lock_enabled() &&
      bucketref->get_info().obj_lock.has_rule()) {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter == attrs.end()) {
      real_time lock_until_date =
          bucketref->get_info().obj_lock.get_lock_until_date(now);
      string mode = bucketref->get_info().obj_lock.get_mode();
      RGWObjectRetention obj_retention(mode, lock_until_date);
      bufferlist bl;
      obj_retention.encode(bl);
      attrs[RGW_ATTR_OBJECT_RETENTION] = bl;
    }
  }

  objref->update_attrs(attrs);
  objref->update_meta(
      {.size = accounted_size,
       .etag = etag,
       .mtime = now,
       .set_mtime = set_mtime,
       .delete_at = delete_at}
  );

  if (mtime != nullptr) {
    *mtime = now;
  }
  try {
    objref->metadata_finish(store);
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to update db object {}: {}. "
                              "failing operation. ",
                              objref->name, e.what()
                          )
                       << dendl;
    io_failed = true;
    cleanup();
    switch (e.code().value()) {
      case -EDQUOT:
      case -ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }
  return 0;
}

int SFSMultipartWriter::prepare(optional_yield y) {
  lsfs_dout(dpp, 10) << "upload_id: " << partref->upload_id
                     << ", part: " << partnum
                     << ", obj: " << partref->objref->name
                     << ", path: " << partref->objref->path.to_path() << dendl;

  if (store->filesystem_stats_avail_bytes.load() <
      store->min_space_left_for_data_write_ops_bytes) {
    lsfs_dout(dpp, 10) << fmt::format(
                              "filesystem stat reservation check hit. "
                              "avail_bytes:{} avail_pct:{} total_bytes:{}. "
                              "returning quota error.",
                              store->filesystem_stats_avail_bytes,
                              store->filesystem_stats_avail_percent,
                              store->filesystem_stats_total_bytes
                          )
                       << dendl;
    return -ERR_QUOTA_EXCEEDED;
  }

  ceph_assert(
      partref->state == sfs::MultipartObject::State::NONE ||
      partref->state == sfs::MultipartObject::State::PREPARED ||
      partref->state == sfs::MultipartObject::State::INPROGRESS
  );

  // a part does not have a version, so just obtain its raw path, ignoring the
  // typical path including the object version.
  std::filesystem::path objpath =
      store->get_data_path() / partref->objref->path.to_path();
  std::filesystem::create_directories(objpath.parent_path());

  // truncate file
  std::ofstream ofs(objpath, std::ofstream::trunc);
  ofs.seekp(0);
  ofs.flush();
  ofs.close();

  partref->state = sfs::MultipartObject::State::PREPARED;
  return 0;
}

int SFSMultipartWriter::process(bufferlist&& data, uint64_t offset) {
  auto len = data.length();
  lsfs_dout(dpp, 10) << "upload_id: " << partref->upload_id
                     << ", part: " << partnum << ", data(len: " << len
                     << ", offset: " << offset
                     << "), offset: " << internal_offset << dendl;

  ceph_assert(
      partref->state == sfs::MultipartObject::State::PREPARED ||
      partref->state == sfs::MultipartObject::State::INPROGRESS
  );

  if (partref->state == sfs::MultipartObject::State::PREPARED) {
    part_offset = offset;
  }
  partref->state = sfs::MultipartObject::State::INPROGRESS;

  std::filesystem::path objpath =
      store->get_data_path() / partref->objref->path.to_path();
  ceph_assert(std::filesystem::exists(objpath));

  auto mode = std::ofstream::binary | std::ofstream::out | std::ofstream::app;
  std::ofstream ofs(objpath, mode);
  ofs.seekp(internal_offset);
  data.write_stream(ofs);
  ofs.flush();
  ofs.close();

  internal_offset += len;
  part_len += len;

  return 0;
}

int SFSMultipartWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y
) {
  lsfs_dout(dpp, 10) << "upload_id: " << partref->upload_id
                     << ", part: " << partnum
                     << ", accounted_size: " << accounted_size
                     << ", etag: " << etag << ", mtime: " << to_iso_8601(*mtime)
                     << ", part offset: " << part_offset
                     << ", part len: " << part_len << dendl;

  partref->finish_write(part_offset, part_len, etag);
  return 0;
}

}  // namespace rgw::sal
