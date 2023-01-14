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
#include "rgw/driver/simplefile/sqlite/sqlite_users.h"
#include "rgw_sal_simplefile.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

std::unique_ptr<User> SimpleFileStore::get_user(const rgw_user& u) {
  return std::make_unique<SimpleFileUser>(u, this);
}
int SimpleFileStore::get_user_by_access_key(
    const DoutPrefixProvider* dpp, const std::string& key, optional_yield y,
    std::unique_ptr<User>* user
) {
  int err = 0;
  rgw::sal::simplefile::sqlite::SQLiteUsers sqlite_users(dpp->get_cct());
  auto db_user = sqlite_users.getUserByAccessKey(key);
  if (db_user) {
    db_user->uinfo.user_id.id = "";  // TODO Remove this when ACL is implemented
    user->reset(new SimpleFileUser(db_user->uinfo, this));
  } else {
    ldpp_dout(dpp, 10) << __func__ << ": User not found" << dendl;
    err = -ENOENT;
  }
  return err;
}

int SimpleFileStore::get_user_by_email(
    const DoutPrefixProvider* dpp, const std::string& email, optional_yield y,
    std::unique_ptr<User>* user
) {
  int err = 0;
  rgw::sal::simplefile::sqlite::SQLiteUsers sqlite_users(dpp->get_cct());
  auto db_user = sqlite_users.getUserByEmail(email);
  if (db_user) {
    db_user->uinfo.user_id.id = "";  // TODO Remove this when ACL is implemented
    user->reset(new SimpleFileUser(db_user->uinfo, this));
  } else {
    ldpp_dout(dpp, 10) << __func__ << ": User not found" << dendl;
    err = -ENOENT;
  }
  return err;
}

int SimpleFileStore::get_user_by_swift(
    const DoutPrefixProvider* dpp, const std::string& user_str,
    optional_yield y, std::unique_ptr<User>* user
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

}  // namespace rgw::sal
