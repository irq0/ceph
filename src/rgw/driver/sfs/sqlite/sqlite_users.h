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

#include "dbconn.h"

namespace rgw::sal::sfs::sqlite  {

class SQLiteUsers {
  DBConnRef conn;

 public:
  explicit SQLiteUsers(DBConnRef _conn);
  virtual ~SQLiteUsers() = default;

  SQLiteUsers(const SQLiteUsers&) = delete;
  SQLiteUsers& operator=(const SQLiteUsers&) = delete;

  std::optional<DBOPUserInfo> get_user_by_email(const std::string & email) const;
  std::optional<DBOPUserInfo> get_user_by_access_key(const std::string & key) const;
  std::optional<DBOPUserInfo> get_user(const std::string & userid) const;
  std::vector<std::string> get_user_ids() const;

  void store_user(const DBOPUserInfo & user) const;
  void remove_user(const std::string & userid) const;

 private:
  template<class... Args>
  std::vector<DBOPUserInfo> get_users_by(Args... args) const;
};

}  // namespace rgw::sal::sfs::sqlite
