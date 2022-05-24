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
#include <memory>
#include "rgw_sal.h"
#include "rgw_sal_simplefile.h"
#include "driver/simplefile/bucket.h"
#include "driver/simplefile/writer.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SimpleFileAtomicWriter::SimpleFileAtomicWriter(
  const DoutPrefixProvider *_dpp,
  optional_yield _y,
  rgw::sal::Object* _head_obj,
  SimpleFileStore *_store,
  const rgw_user& _owner,
  const rgw_placement_rule *_ptail_placement_rule,
  uint64_t _olh_epoch,
  const std::string &_unique_tag
) : StoreWriter(_dpp, _y), store(_store),
    obj(_store, _head_obj->get_key(), _head_obj->get_bucket()),
    owner(_owner),
    placement_rule(_ptail_placement_rule), olh_epoch(_olh_epoch),
    unique_tag(_unique_tag), bytes_written(0) {

  lsfs_dout(dpp, 10) << "head_obj: " << _head_obj->get_key()
                     << ", bucket: " << _head_obj->get_bucket()->get_name()
                     << dendl;
}

int SimpleFileAtomicWriter::prepare(optional_yield y) {
  lsfs_dout(dpp, 10) << ": unimplemented, return success." << dendl;
  // TODO: create meta file for this new object
  return 0;
}

int SimpleFileAtomicWriter::process(bufferlist &&data, uint64_t offset) {
  lsfs_dout(dpp, 10) << "data len: " << data.length()
                     << ", offset: " << offset << dendl;

  SimpleFileBucket *b = static_cast<SimpleFileBucket*>(obj.get_bucket());
  std::filesystem::path object_path = b->objects_path() / obj.get_name();

  lsfs_dout(dpp, 10) << "write to object at " << object_path << dendl;

  auto mode = std::ofstream::binary|std::ofstream::out|std::ofstream::app;
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

int SimpleFileAtomicWriter::complete(
  size_t accounted_size,
  const std::string &etag,
  ceph::real_time *mtime,
  ceph::real_time set_mtime,
  std::map<std::string, bufferlist> &attrs,
  ceph::real_time delete_at,
  const char *if_match,
  const char *if_nomatch,
  const std::string *user_data,
  rgw_zone_set *zones_trace,
  bool *canceled,
  optional_yield y
) {
  lsfs_dout(dpp, 10) << "accounted_size: " << accounted_size
                     << ", etag: " << etag
                     << ", mtime: " << to_iso_8601(*mtime)
                     << ", set_mtime: " << to_iso_8601(set_mtime)
                     << ", attrs: " << attrs
                     << ", delete_at: " << to_iso_8601(delete_at)
                     << ", if_match: " << if_match
                     << ", if_nomatch: " << if_nomatch
                     << dendl;
  lsfs_dout(dpp, 10) << "unimplemented, return success." << dendl;
  return 0;
}

SimpleFileMultipartWriter::SimpleFileMultipartWriter(
  const DoutPrefixProvider *_dpp,
  optional_yield y,
  MultipartUpload *upload,
  rgw::sal::Object* _head_obj,
  const SimpleFileStore *store,
  const rgw_user &_owner,
  const rgw_placement_rule *_ptail_placement_rule,
  uint64_t _part_num,
  const std::string &_part_num_str
) : StoreWriter(_dpp, y) { }

int SimpleFileMultipartWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": unimplemented, return success" << dendl;
  return 0;
}

int SimpleFileMultipartWriter::process(bufferlist &&data, uint64_t offset) {
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": unimplemented, return success" << dendl;
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": data len: " << data.length() << ", offset: " << offset << dendl;
  return 0;
}

int SimpleFileMultipartWriter::complete(
  size_t accounted_size,
  const std::string &etag,
  ceph::real_time *mtime,
  ceph::real_time set_mtime,
  std::map<std::string, bufferlist> &attrs,
  ceph::real_time delete_at,
  const char *if_match,
  const char *if_nomatch,
  const std::string *user_data,
  rgw_zone_set *zones_trace,
  bool *canceled,
  optional_yield y
) {
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": unimplemented, return success" << dendl;
  return 0;
}

} // ns rgw::sal
