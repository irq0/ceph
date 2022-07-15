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
#pragma once

namespace rgw::sal::sfs::sqlite  {

template <typename BLOB_HOLDER, typename DEST>
void decode_blob(const BLOB_HOLDER & blob_holder, DEST & dest) {
  bufferlist buffer;
  buffer.append(reinterpret_cast<const char *>(blob_holder.data()), blob_holder.size());
  ceph::decode(dest, buffer);
}

template <typename ORIGIN, typename BLOB_HOLDER>
void encode_blob(const ORIGIN & origin, BLOB_HOLDER & dest) {
  bufferlist buffer;
  ceph::encode(origin, buffer);
  dest.reserve(buffer.length());
  std::copy(buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest));
}

template <typename BLOB_HOLDER>
void encode_blob(const RGWAccessControlPolicy & origin, BLOB_HOLDER & dest) {
  bufferlist buffer;
  encode(origin, buffer);
  dest.reserve(buffer.length());
  std::copy(buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest));
}

template <typename BLOB_HOLDER>
void encode_blob(const RGWQuotaInfo & origin, BLOB_HOLDER & dest) {
  bufferlist buffer;
  encode(origin, buffer);
  dest.reserve(buffer.length());
  std::copy(buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest));
}

template <typename BLOB_HOLDER>
void encode_blob(const RGWUserCaps & origin, BLOB_HOLDER & dest) {
  bufferlist buffer;
  encode(origin, buffer);
  dest.reserve(buffer.length());
  std::copy(buffer.c_str(), buffer.c_str() + buffer.length(), std::back_inserter(dest));
}

template <typename SOURCE, typename DEST>
typename std::enable_if<!std::is_same<SOURCE, std::vector<char>>::value,void>::type
assign_value(const SOURCE & source, DEST & dest) {
  dest = source;
}

template <typename SOURCE, typename DEST>
typename std::enable_if<std::is_same<SOURCE, std::vector<char>>::value,void>::type
assign_value(const SOURCE & source, DEST & dest) {
  decode_blob(source, dest);
}

template <typename OPTIONAL, typename DEST>
void assign_optional_value (const OPTIONAL & optional_value, DEST & dest) {
  // if value is not set, do nothing
  if (!optional_value) return;
  assign_value(*optional_value, dest);
}

template <typename SOURCE, typename DEST>
void assign_db_value(const SOURCE & source, DEST & dest) {
  dest = source;
}

template <typename DEST>
void assign_db_value(const std::string & source, DEST & dest) {
  if (source.empty()) return;
  dest = source;
}

template <typename SOURCE>
void assign_db_value(const SOURCE & source, std::optional<std::vector<char>> & dest) {
  std::vector<char> blob_vector;
  encode_blob(source, blob_vector);
  dest = blob_vector;
}

}  // namespace rgw::sal::sfs::sqlite
