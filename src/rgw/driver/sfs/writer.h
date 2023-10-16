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
#ifndef RGW_STORE_SFS_WRITER_H
#define RGW_STORE_SFS_WRITER_H

#include <memory>

#include "driver/sfs/bucket.h"
#include "driver/sfs/object.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"

namespace rgw::sal {

class SFStore;

class SFSAtomicWriter : public StoreWriter {
 protected:
  rgw::sal::SFStore* store;
  SFSObject obj;
  sfs::BucketRef bucketref;
  std::unique_ptr<sfs::Object> objref;
  const rgw_user& owner;
  const rgw_placement_rule* placement_rule;
  uint64_t olh_epoch;
  const std::string& unique_tag;
  uint64_t bytes_written;
  uint versioned_object_id;

 private:
  std::filesystem::path object_path;
  bool io_failed;
  int fd;

  int open() noexcept;
  int close() noexcept;
  void cleanup() noexcept;

 public:
  SFSAtomicWriter(
      const DoutPrefixProvider* _dpp, optional_yield _y,
      rgw::sal::Object* _head_obj, SFStore* _store, sfs::BucketRef bucketref,
      const rgw_user& _owner, const rgw_placement_rule* _ptail_placement_rule,
      uint64_t _olh_epoch, const std::string& _unique_tag
  );
  ~SFSAtomicWriter();

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(
      size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
      ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
      ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
      const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
      optional_yield y
  ) override;

  const std::string get_cls_name() const { return "atomic_writer"; }
};

namespace sfs {

class SFSMultipartWriterV2 : public StoreWriter {
  const rgw::sal::SFStore* store;
  const std::string upload_id;
  uint32_t part_num;
  uint64_t bytes_written;
  int fd;

 public:
  SFSMultipartWriterV2(
      const DoutPrefixProvider* _dpp, optional_yield _y,
      const std::string& _upload_id, const rgw::sal::SFStore* _store,
      uint32_t _part_num
  )
      : StoreWriter(_dpp, _y),
        store(_store),
        upload_id(_upload_id),
        part_num(_part_num),
        bytes_written(0),
        fd(-1) {}
  virtual ~SFSMultipartWriterV2();

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(
      size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
      ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
      ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
      const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
      optional_yield y
  ) override;

  const std::string get_cls_name() const { return "multipart_writer_v2"; }

 private:
  int close() noexcept;
};

}  // namespace sfs

}  // namespace rgw::sal

#endif  // RGW_STORE_SFS_WRITER_H
