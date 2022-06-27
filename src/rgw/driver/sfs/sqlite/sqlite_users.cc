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
#include "users_conversions.h"

#include <filesystem>
#include <iostream>

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteUsers::SQLiteUsers(CephContext *cct)
  : ceph_context_(cct) {
    sync();
}

std::optional<DBOPUserInfo> SQLiteUsers::getUser(const std::string & userid) const {
  auto storage = getStorage();
  auto user = storage.get_pointer<DBUser>(userid);
  std::optional<DBOPUserInfo> ret_value;
  if (user) {
    ret_value = getRGWUser(*user);
  }
  return ret_value;
}

std::optional<DBOPUserInfo> SQLiteUsers::getUserByEmail(const std::string & email) const {
  auto users = getUsersBy(where(c(&DBUser::UserEmail) = email));
  std::optional<DBOPUserInfo> ret_value;
  if (users.size()) {
    ret_value = users[0];
  }
  return ret_value;
}

std::optional<DBOPUserInfo> SQLiteUsers::getUserByAccessKey(const std::string & key) const {
  auto users = getUsersBy(where(c(&DBUser::AccessKeysID) = key));
  std::optional<DBOPUserInfo> ret_value;
  if (users.size()) {
    ret_value = users[0];
  }
  return ret_value;
}

std::vector<std::string> SQLiteUsers::getUserIDs() const {
  auto storage = getStorage();
  auto selectStatement = storage.prepare(select(&DBUser::UserID));
  return storage.execute(selectStatement);
}

void SQLiteUsers::storeUser(const DBOPUserInfo & user) const {
  auto storage = getStorage();
  auto db_user = getDBUser(user);
  storage.replace(db_user);
}

void SQLiteUsers::removeUser(const std::string & userid) const {
  auto storage = getStorage();
  storage.remove<DBUser>(userid);
}

template<class... Args>
std::vector<DBOPUserInfo> SQLiteUsers::getUsersBy(Args... args) const {
  std::vector<DBOPUserInfo> users_return;
  auto storage = getStorage();
  auto users = storage.get_all<DBUser>(args...);
  for (auto & user: users) {
    users_return.push_back(getRGWUser(user));
  }
  return users_return;
}

std::string SQLiteUsers::getDBPath() const {
  auto rgw_sfs_path = ceph_context_->_conf.get_val<std::string>("rgw_sfs_data_path");
  auto db_path = std::filesystem::path(rgw_sfs_path) / std::string(USERS_DB_NAME);
  return db_path.string();
}

void SQLiteUsers::sync() const {
  // sync schema does the 'sqlite_orm' magic
  // In case something was changed in the storage class declared with make_storage it will
  // apply those changes to the database.
  // We can also call passing true, which will preserve previous tables definitions in copies
  // of the tables, so we don't lose information.
  getStorage().sync_schema();
}

}  // namespace rgw::sal::sfs::sqlite
