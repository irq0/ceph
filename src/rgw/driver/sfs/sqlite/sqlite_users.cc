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
#include "sqlite_users.h"
#include "users/users_conversions.h"

#include <filesystem>
#include <iostream>

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteUsers::SQLiteUsers(CephContext *cct)
  : SQLiteSchema(cct) {
}

std::optional<DBOPUserInfo> SQLiteUsers::get_user(const std::string & userid) const {
  auto storage = get_storage();
  auto user = storage.get_pointer<DBUser>(userid);
  std::optional<DBOPUserInfo> ret_value;
  if (user) {
    ret_value = get_rgw_user(*user);
  }
  return ret_value;
}

std::optional<DBOPUserInfo> SQLiteUsers::get_user_by_email(const std::string & email) const {
  auto users = get_users_by(where(c(&DBUser::user_email) = email));
  std::optional<DBOPUserInfo> ret_value;
  if (users.size()) {
    ret_value = users[0];
  }
  return ret_value;
}

std::optional<DBOPUserInfo> SQLiteUsers::get_user_by_access_key(const std::string & key) const {
  auto users = get_users_by(where(c(&DBUser::access_keys_id) = key));
  std::optional<DBOPUserInfo> ret_value;
  if (users.size()) {
    ret_value = users[0];
  }
  return ret_value;
}

std::vector<std::string> SQLiteUsers::get_user_ids() const {
  auto storage = get_storage();
  return storage.select(&DBUser::user_id);
}

void SQLiteUsers::store_user(const DBOPUserInfo & user) const {
  auto storage = get_storage();
  auto db_user = get_db_user(user);
  storage.replace(db_user);
}

void SQLiteUsers::remove_user(const std::string & userid) const {
  auto storage = get_storage();
  storage.remove<DBUser>(userid);
}

template<class... Args>
std::vector<DBOPUserInfo> SQLiteUsers::get_users_by(Args... args) const {
  std::vector<DBOPUserInfo> users_return;
  auto storage = get_storage();
  auto users = storage.get_all<DBUser>(args...);
  for (auto & user: users) {
    users_return.push_back(get_rgw_user(user));
  }
  return users_return;
}

}  // namespace rgw::sal::sfs::sqlite
