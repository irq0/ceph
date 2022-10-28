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

#include <memory>

#include "driver/sfs/bucket.h"
#include "driver/sfs/writer.h"
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
      bytes_written(0) {
  lsfs_dout(dpp, 10) << "head_obj: " << _head_obj->get_key()
                     << ", bucket: " << _head_obj->get_bucket()->get_name()
                     << dendl;
}

int SFSAtomicWriter::prepare(optional_yield y) {
  objref = bucketref->get_or_create(obj.get_key());

  std::filesystem::path object_path =
      store->get_data_path() / objref->get_storage_path();
  std::filesystem::create_directories(object_path.parent_path());

  lsfs_dout(dpp, 10) << "truncate file at " << object_path << dendl;
  std::ofstream ofs(object_path, std::ofstream::trunc);
  ofs.seekp(0);
  ofs.flush();
  ofs.close();
  return 0;
}

int SFSAtomicWriter::process(bufferlist&& data, uint64_t offset) {
  lsfs_dout(dpp, 10) << "data len: " << data.length() << ", offset: " << offset
                     << dendl;

  objref->metadata_change_version_state(store, ObjectState::WRITING);

  std::filesystem::path object_path =
      store->get_data_path() / objref->get_storage_path();
  ceph_assert(std::filesystem::exists(object_path));

  lsfs_dout(dpp, 10) << "write to object at " << object_path << dendl;

  auto mode = std::ofstream::binary | std::ofstream::out | std::ofstream::app;
  std::ofstream ofs(object_path, mode);
  ofs.seekp(offset);
  data.write_stream(ofs);
  ofs.flush();
  ofs.close();
  bytes_written += data.length();

  if (data.length() == 0) {
    lsfs_dout(dpp, 10) << "final piece, wrote " << bytes_written << " bytes"
                       << dendl;
  }

  return 0;
}

int SFSAtomicWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y
) {
  lsfs_dout(dpp, 10) << "accounted_size: " << accounted_size
                     << ", etag: " << etag
                     << ", set_mtime: " << to_iso_8601(set_mtime)
                     << ", attrs: " << attrs
                     << ", delete_at: " << to_iso_8601(delete_at)
                     << ", if_match: " << if_match
                     << ", if_nomatch: " << if_nomatch << dendl;

  ceph_assert(bytes_written == accounted_size);

  sfs::Object::Meta& meta = objref->meta;
  meta.size = accounted_size;
  meta.etag = etag;
  meta.mtime = ceph::real_clock::now();
  meta.set_mtime = set_mtime;
  meta.delete_at = delete_at;
  meta.attrs = attrs;
  bucketref->finish(dpp, obj.get_name());

  if (mtime != nullptr) {
    *mtime = meta.mtime;
  }
  objref->metadata_finish(store);
  return 0;
}

int SFSMultipartWriter::prepare(optional_yield y) {
  lsfs_dout(dpp, 10) << "upload_id: " << partref->upload_id
                     << ", part: " << partnum
                     << ", obj: " << partref->objref->name
                     << ", path: " << partref->objref->path.to_path() << dendl;

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
