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

#include "users_definitions.h"
#include "sqlite_orm.h"

namespace rgw::sal::sfs::sqlite  {

constexpr std::string_view USERS_TABLE = "users";
constexpr std::string_view USERS_DB_NAME = "users.db";

class SQLiteUsers {
 public:
  explicit SQLiteUsers(CephContext *cct);
  ~SQLiteUsers() = default;

  SQLiteUsers( const SQLiteUsers& ) = delete; 
  SQLiteUsers& operator=( const SQLiteUsers& ) = delete; 

  inline auto getStorage() const {
    // Creates the sqlite_orm storage.
    // make_storage defines the tables and its columns and the mapping between the object that represents a row in a table.
    // it's basically a C++ declaration of the database.
    // In this case the object represented in the database is DBUser.
    // All types are deducted by sqlite_orm, which makes all the queries strongly typed.
    return sqlite_orm::make_storage(getDBPath(),
                    sqlite_orm::make_table(std::string(USERS_TABLE),
                          sqlite_orm::make_column("UserID", &DBUser::UserID, sqlite_orm::primary_key()),
                          sqlite_orm::make_column("Tenant", &DBUser::Tenant),
                          sqlite_orm::make_column("NS", &DBUser::NS),
                          sqlite_orm::make_column("DisplayName", &DBUser::DisplayName),
                          sqlite_orm::make_column("UserEmail", &DBUser::UserEmail),
                          sqlite_orm::make_column("AccessKeysID", &DBUser::AccessKeysID),
                          sqlite_orm::make_column("AccessKeysSecret", &DBUser::AccessKeysSecret),
                          sqlite_orm::make_column("AccessKeys", &DBUser::AccessKeys),
                          sqlite_orm::make_column("SwiftKeys", &DBUser::SwiftKeys),
                          sqlite_orm::make_column("SubUsers", &DBUser::SubUsers),
                          sqlite_orm::make_column("Suspended", &DBUser::Suspended),
                          sqlite_orm::make_column("MaxBuckets", &DBUser::MaxBuckets),
                          sqlite_orm::make_column("OpMask", &DBUser::OpMask),
                          sqlite_orm::make_column("UserCaps", &DBUser::UserCaps),
                          sqlite_orm::make_column("Admin", &DBUser::Admin),
                          sqlite_orm::make_column("System", &DBUser::System),
                          sqlite_orm::make_column("PlacementName", &DBUser::PlacementName),
                          sqlite_orm::make_column("PlacementStorageClass", &DBUser::PlacementStorageClass),
                          sqlite_orm::make_column("PlacementTags", &DBUser::PlacementTags),
                          sqlite_orm::make_column("BuckeQuota", &DBUser::BuckeQuota),
                          sqlite_orm::make_column("TempURLKeys", &DBUser::TempURLKeys),
                          sqlite_orm::make_column("UserQuota", &DBUser::UserQuota),
                          sqlite_orm::make_column("TYPE", &DBUser::TYPE),
                          sqlite_orm::make_column("MfaIDs", &DBUser::MfaIDs),
                          sqlite_orm::make_column("AssumedRoleARN", &DBUser::AssumedRoleARN),
                          sqlite_orm::make_column("UserAttrs", &DBUser::UserAttrs),
                          sqlite_orm::make_column("UserVersion", &DBUser::UserVersion),
                          sqlite_orm::make_column("UserVersionTag", &DBUser::UserVersionTag)
          ));
  }

  std::optional<DBOPUserInfo> getUserByEmail(const std::string & email) const;
  std::optional<DBOPUserInfo> getUserByAccessKey(const std::string & key) const;
  std::optional<DBOPUserInfo> getUser(const std::string & userid) const;
  std::vector<std::string> getUserIDs() const;

  void storeUser(const DBOPUserInfo & user) const;
  void removeUser(const std::string & userid) const;

 private:
  template<class... Args>
  std::vector<DBOPUserInfo> getUsersBy(Args... args) const;

  std::string getDBPath() const;

  void sync() const;

  CephContext *ceph_context_ = nullptr;
};

}  // namespace rgw::sal::sfs::sqlite
