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

SQLiteUsers::SQLiteUsers(DBConnRef _conn) : conn(_conn) { }

std::optional<DBOPUserInfo> SQLiteUsers::get_user(const std::string & userid) const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
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
  auto user_id = get_user_id_by_access_key(key);
  std::optional<DBOPUserInfo> ret_value;
  if (user_id.has_value()) {
    auto storage = conn->get_storage();
    auto user = storage.get_pointer<DBUser>(user_id);
    if (user) {
      ret_value = get_rgw_user(*user);
    }
  }
  return ret_value;
}

std::vector<std::string> SQLiteUsers::get_user_ids() const {
  std::shared_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  return storage.select(&DBUser::user_id);
}

void SQLiteUsers::store_user(const DBOPUserInfo & user) const {
  std::unique_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  auto db_user = get_db_user(user);
  storage.replace(db_user);
  store_access_keys(user);
}

void SQLiteUsers::remove_user(const std::string & userid) const {
  std::unique_lock l(conn->rwlock);
  auto storage = conn->get_storage();
  remove_access_keys(userid);
  storage.remove<DBUser>(userid);
}

template<class... Args>
std::vector<DBOPUserInfo> SQLiteUsers::get_users_by(Args... args) const {
  std::shared_lock l(conn->rwlock);
  std::vector<DBOPUserInfo> users_return;
  auto storage = conn->get_storage();
  auto users = storage.get_all<DBUser>(args...);
  for (auto & user: users) {
    users_return.push_back(get_rgw_user(user));
  }
  return users_return;
}

void SQLiteUsers::store_access_keys(const DBOPUserInfo & user) const {
  auto storage = conn->get_storage();
  // remove existing keys for the user (in case any of them had changed)
  remove_access_keys(user.uinfo.user_id.id);
  for (auto const& key: user.uinfo.access_keys) {
    DBAccessKey db_key;
    db_key.access_key = key.first;
    db_key.user_id = user.uinfo.user_id.id;
    storage.insert(db_key);
  }
}

void SQLiteUsers::remove_access_keys(const std::string & userid) const {
  auto storage = conn->get_storage();
  storage.remove_all<DBAccessKey>(where(c(&DBAccessKey::user_id) = userid));
}

std::optional<std::string> SQLiteUsers::get_user_id_by_access_key(
                                              const std::string & key) const {
  auto storage = conn->get_storage();
  auto keys = storage.get_all<DBAccessKey>(where(c(&DBAccessKey::access_key) = key));
  std::optional<std::string> ret_value;
  if (keys.size() > 0) {
    // in case we have 2 keys that are equal in different users we return
    // the first one.
    ret_value = keys[0].user_id;
  }
  return ret_value;
}

}  // namespace rgw::sal::sfs::sqlite
