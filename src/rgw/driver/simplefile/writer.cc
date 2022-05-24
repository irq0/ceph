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
) :
  StoreWriter(_dpp, _y) { }

int SimpleFileAtomicWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": unimplemented, return success." << dendl;
  return 0;
}

int SimpleFileAtomicWriter::process(bufferlist &&data, uint64_t offset) {
  ldpp_dout(dpp, 10) << "atomic_writer::" << __func__ << ": data len: "
                     << data.length() << ", offset: " << offset << dendl;
  ldpp_dout(dpp, 10) << "atomic_writer::" << __func__
                     << ": unimplemented, return success." << dendl;
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
  ldpp_dout(dpp, 10) << __func__ << ": unimplemented, return success." << dendl;
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
