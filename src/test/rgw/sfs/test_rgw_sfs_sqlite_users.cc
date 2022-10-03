// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/driver/sfs/sqlite/users/users_conversions.h"

#include "rgw/rgw_sal_sfs.h"
#include "rgw_sfs_utils.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

class TestSFSSQLiteUsers : public ::testing::Test {
protected:
  void SetUp() override {
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
  }

  void TearDown() override {
    fs::current_path(fs::temp_directory_path());
    fs::remove_all(TEST_DIR);
  }

  std::string getTestDir() const {
    auto test_dir = fs::temp_directory_path() / TEST_DIR;
    return test_dir.string();
  }

  fs::path getDBFullPath(const std::string & base_dir) const {
    auto db_full_name = "s3gw.db";
    auto db_full_path = fs::path(base_dir) /  db_full_name;
    return db_full_path;
  }

  fs::path getDBFullPath() const {
    return getDBFullPath(getTestDir());
  }
};

void compareUsers(const DBOPUserInfo & origin, const DBOPUserInfo & dest) {
  compareUsersRGWInfo(origin.uinfo, dest.uinfo);
  compareUserAttrs(origin.user_attrs, dest.user_attrs);
  compareUserVersion(origin.user_version, dest.user_version);
}

DBOPUserInfo createTestUser(const std::string & suffix) {
  DBOPUserInfo user;
  user.uinfo.user_id.id = "test" + suffix;
  user.uinfo.user_id.tenant = "Tenant" + suffix;
  user.uinfo.user_id.ns = "NS" + suffix;
  user.uinfo.display_name = "DisplayName1" + suffix;
  user.uinfo.user_email = "user" + suffix + "@test.com";
  user.uinfo.access_keys["key1_" + suffix] = RGWAccessKey("key1_" + suffix, "secret1");
  user.uinfo.access_keys["key2_" + suffix] = RGWAccessKey("key2_" + suffix, "secret2");
  user.uinfo.swift_keys["swift_key_1"] = RGWAccessKey("swift_key1_" + suffix, "swift_secret1");
  RGWSubUser subuser;
  subuser.name = "subuser1";
  subuser.perm_mask = 1999;
  user.uinfo.subusers["subuser"] = subuser;
  user.uinfo.suspended = 0;
  user.uinfo.max_buckets = 3121;
  user.uinfo.op_mask = 2020;
  user.uinfo.caps.add_from_string("users=*;user=*");
  user.uinfo.system = 1;
  user.uinfo.default_placement.name = "default_placement_name";
  user.uinfo.default_placement.storage_class = "default_placement_class";
  user.uinfo.placement_tags.push_back("tag1");
  user.uinfo.placement_tags.push_back("tag2");
  user.uinfo.quota.bucket_quota.max_size = 1999;
  user.uinfo.quota.bucket_quota.max_objects = 3121;
  user.uinfo.quota.bucket_quota.enabled = true;
  user.uinfo.quota.bucket_quota.check_on_raw = false;
  user.uinfo.temp_url_keys[1] = "url_key_1";
  user.uinfo.temp_url_keys[2] = "url_key_2";
  user.uinfo.quota.user_quota.max_size = 2333;
  user.uinfo.quota.user_quota.max_objects = 22;
  user.uinfo.quota.user_quota.enabled = false;
  user.uinfo.quota.user_quota.check_on_raw = true;
  user.uinfo.type = 44;
  user.uinfo.mfa_ids.insert("id1");
  user.uinfo.assumed_role_arn = "assumed_role_arn";
  bufferlist buffer;
  std::string blob = "blob_test";
  buffer.append(reinterpret_cast<const char *>(blob.c_str()), blob.length());
  user.user_attrs["attr1"] = buffer;
  user.user_version.ver = 1;
  user.user_version.tag = "user_version_tag";
  return user;
}

TEST_F(TestSFSSQLiteUsers, CreateAndGet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user = createTestUser("1");
  db_users->store_user(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, CreateAndGetByEmail) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user = createTestUser("1");
  db_users->store_user(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user_by_email("user1@test.com");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, CreateAndGetByAccessKey) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user = createTestUser("1");
  db_users->store_user(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user_by_access_key("key1_1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, ListUserIDs) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);

  db_users->store_user(createTestUser("1"));
  db_users->store_user(createTestUser("2"));
  db_users->store_user(createTestUser("3"));
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto user_ids = db_users->get_user_ids();
  EXPECT_EQ(user_ids.size(), 3);
  EXPECT_EQ(user_ids[0], "test1");
  EXPECT_EQ(user_ids[1], "test2");
  EXPECT_EQ(user_ids[2], "test3");
}

TEST_F(TestSFSSQLiteUsers, RemoveUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);

  db_users->store_user(createTestUser("1"));
  db_users->store_user(createTestUser("2"));
  db_users->store_user(createTestUser("3"));

  db_users->remove_user("test2");
  auto user_ids = db_users->get_user_ids();
  EXPECT_EQ(user_ids.size(), 2);
  EXPECT_EQ(user_ids[0], "test1");
  EXPECT_EQ(user_ids[1], "test3");

  auto ret_user = db_users->get_user("test2");
  ASSERT_FALSE(ret_user.has_value());
}

TEST_F(TestSFSSQLiteUsers, RemoveUserThatDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);

  db_users->store_user(createTestUser("1"));
  db_users->store_user(createTestUser("2"));
  db_users->store_user(createTestUser("3"));

  db_users->remove_user("testX");
  auto user_ids = db_users->get_user_ids();
  EXPECT_EQ(user_ids.size(), 3);
  EXPECT_EQ(user_ids[0], "test1");
  EXPECT_EQ(user_ids[1], "test2");
  EXPECT_EQ(user_ids[2], "test3");
}

TEST_F(TestSFSSQLiteUsers, CreateAndUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user = createTestUser("1");
  db_users->store_user(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);

  user.uinfo.user_email = "email_changed@test.com";
  db_users->store_user(user);
  ret_user = db_users->get_user("test1");
  ASSERT_TRUE(ret_user.has_value());
  ASSERT_EQ(ret_user->uinfo.user_email, "email_changed@test.com");
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, GetExisting) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user = createTestUser("1");
  db_users->store_user(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);

  // create a new instance, user should exist
  auto db_users_2 = std::make_shared<SQLiteUsers>(conn);
  ret_user = db_users_2->get_user("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, AddMoreThanOneUserWithSameEmail) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user1 = createTestUser("1");
  auto user2 = createTestUser("2");
  auto user3 = createTestUser("3");
  user2.uinfo.user_email = "user1@test.com";
  user3.uinfo.user_email = "user1@test.com";
  db_users->store_user(user1);
  db_users->store_user(user2);
  db_users->store_user(user3);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user_by_email("user1@test.com");
  ASSERT_TRUE(ret_user.has_value());
  // it will only return the first user with that email
  compareUsers(user1, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, AddMoreThanOneUserWithSameAccessKey) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user1 = createTestUser("1");
  auto user2 = createTestUser("2");
  auto user3 = createTestUser("3");
  std::map<std::string, RGWAccessKey> access_keys;
  access_keys["key1"] = RGWAccessKey("key1", "secret1");
  access_keys["key2"] = RGWAccessKey("key2", "secret2");
  user1.uinfo.access_keys = access_keys;
  user2.uinfo.access_keys = access_keys;
  user2.uinfo.access_keys = access_keys;
  db_users->store_user(user1);
  db_users->store_user(user2);
  db_users->store_user(user3);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user_by_access_key("key1");
  ASSERT_TRUE(ret_user.has_value());
  // it will only return the first user with that access key
  compareUsers(user1, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, UseStorage) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  SQLiteUsers db_users(conn);
  auto storage = conn->get_storage();

  DBUser db_user;
  db_user.user_id = "test_storage";

  // we have to use replace because the primary key of rgw_user is a string
  storage.replace(db_user);

  auto user = storage.get_pointer<DBUser>("test_storage");

  ASSERT_NE(user, nullptr);
  ASSERT_EQ(user->user_id, "test_storage");

  // convert the DBUser to RGWUser (blobs are decoded here)
  auto rgw_user = get_rgw_user(*user);
  ASSERT_EQ(rgw_user.uinfo.user_id.id, user->user_id);

  // creates a RGWUser for testing (id = test1, email = test1@test.com, etc..)
  auto rgw_user_2 = createTestUser("1");

  // convert to DBUser (blobs are encoded here)
  auto db_user_2 = get_db_user(rgw_user_2);

  // we have to use replace because the primary key of rgw_user is a string
  storage.replace(db_user_2);

  // now use the SqliteUsers method, so user is already converted
  auto ret_user = db_users.get_user("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(rgw_user_2, *ret_user);
}

// User management tests ------------------------------------------------------

TEST_F(TestSFSSQLiteUsers, StoreListUsers) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = std::make_shared<rgw::sal::SFStore>(ceph_context.get(), getTestDir());

  const NoDoutPrefix no_dpp(ceph_context.get(), 1);
  std::list<std::string> userIds;
  bool truncated;
  void * meta_handle;
  store->meta_list_keys_init(&no_dpp, std::string("user"), std::string(), &meta_handle);
  store->meta_list_keys_next(&no_dpp, meta_handle, std::numeric_limits<int>::max(), userIds, &truncated);
  ASSERT_FALSE(truncated);
  ASSERT_EQ(userIds.size(), 0);

  auto user1 = createTestUser("1");
  auto user2 = createTestUser("2");
  auto user3 = createTestUser("3");

  auto db_users = std::make_shared<SQLiteUsers>(store->db_conn);
  db_users->store_user(user1);
  db_users->store_user(user2);
  db_users->store_user(user3);

  store->meta_list_keys_next(&no_dpp, meta_handle, std::numeric_limits<int>::max(),userIds, &truncated);
  ASSERT_FALSE(truncated);
  ASSERT_EQ(userIds.size(), 3);
  std::vector<std::string> users_vector{ std::make_move_iterator(userIds.begin()),
                                  std::make_move_iterator(userIds.end()) };
  ASSERT_EQ(users_vector[0], "test1");
  ASSERT_EQ(users_vector[1], "test2");
  ASSERT_EQ(users_vector[2], "test3");
}

TEST_F(TestSFSSQLiteUsers, StoreAddUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = std::make_shared<rgw::sal::SFStore>(ceph_context.get(), getTestDir());

  auto user1 = createTestUser("1");
  auto sfs_user = std::make_shared<rgw::sal::SFSUser>(user1.uinfo, store.get());
  sfs_user->set_attrs(user1.user_attrs);

  const NoDoutPrefix no_dpp(ceph_context.get(), 1);
  EXPECT_EQ(sfs_user->store_user(&no_dpp, null_yield, true), 0);

  std::list<std::string> userIds;
  bool truncated;
  void * meta_handle;
  store->meta_list_keys_init(&no_dpp, std::string("user"), std::string(), &meta_handle);
  store->meta_list_keys_next(&no_dpp, meta_handle, std::numeric_limits<int>::max(), userIds, &truncated);
  ASSERT_FALSE(truncated);
  ASSERT_EQ(userIds.size(), 1);

  // read the user straight from the db
  auto db_users = std::make_shared<SQLiteUsers>(store->db_conn);
  auto ret_user = db_users->get_user("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user1, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, StoreLoadUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = std::make_shared<rgw::sal::SFStore>(ceph_context.get(), getTestDir());

  auto user1 = createTestUser("1");
  auto sfs_user = std::make_shared<rgw::sal::SFSUser>(user1.uinfo, store.get());
  sfs_user->set_attrs(user1.user_attrs);

  const NoDoutPrefix no_dpp(ceph_context.get(), 1);
  EXPECT_EQ(sfs_user->store_user(&no_dpp, null_yield, true), 0);

  auto stored_user = store->get_user({"", "test1", ""});
  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);
  // check that all fields are as expected
  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 1
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 1);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");
}

TEST_F(TestSFSSQLiteUsers, StoreUpdateUserCheckOldInfo) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = std::make_shared<rgw::sal::SFStore>(ceph_context.get(), getTestDir());

  auto user1 = createTestUser("1");
  auto stored_user = store->get_user(user1.uinfo.user_id);
  stored_user->set_attrs(user1.user_attrs);
  stored_user->get_info() = user1.uinfo;

  const NoDoutPrefix no_dpp(ceph_context.get(), 1);
  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true), 0);

  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);
  // check that all fields are as expected
  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 1
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 1);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");

  // store again (change email)
  auto prev_info = user1.uinfo;

  stored_user->get_info().user_email = "this_was_updated@test.com";
  user1.uinfo.user_email = "this_was_updated@test.com";  // set the same value so we can compare later

  // store and retrieve the old info
  RGWUserInfo old_info;
  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true, &old_info), 0);
  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);

  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 2 now
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 2);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");

  // check old info
  compareUsersRGWInfo(prev_info, old_info);
  ASSERT_NE(old_info.user_email, "this_was_updated@test.com");
}

TEST_F(TestSFSSQLiteUsers, StoreUpdateUserCheckVersioning) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = std::make_shared<rgw::sal::SFStore>(ceph_context.get(), getTestDir());

  auto user1 = createTestUser("1");
  auto stored_user = store->get_user(user1.uinfo.user_id);
  stored_user->set_attrs(user1.user_attrs);
  stored_user->get_info() = user1.uinfo;

  const NoDoutPrefix no_dpp(ceph_context.get(), 1);
  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true), 0);

  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);
  // check that all fields are as expected
  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 1
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 1);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");

  // store again (no changes)
  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true), 0);
  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);
  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 2 now
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 2);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");

  // store again (change email)
  stored_user->get_info().user_email = "this_was_updated@test.com";
  user1.uinfo.user_email = "this_was_updated@test.com";  // set the same value so we can compare later

  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true), 0);
  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);

  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 3 now
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 3);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");
}

TEST_F(TestSFSSQLiteUsers, StoreUpdateUserErrorVersioning) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = std::make_shared<rgw::sal::SFStore>(ceph_context.get(), getTestDir());

  auto user1 = createTestUser("1");
  auto stored_user = store->get_user(user1.uinfo.user_id);
  stored_user->set_attrs(user1.user_attrs);
  stored_user->get_info() = user1.uinfo;

  const NoDoutPrefix no_dpp(ceph_context.get(), 1);
  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true), 0);

  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);
  // check that all fields are as expected
  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 1
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 1);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");

  // store again (no changes)
  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true), 0);
  EXPECT_EQ(stored_user->load_user(&no_dpp, null_yield), 0);
  compareUsersRGWInfo(stored_user->get_info(), user1.uinfo);
  compareUserAttrs(stored_user->get_attrs(), user1.user_attrs);
  // version stored should be 2 now
  EXPECT_EQ(stored_user->get_version_tracker().read_version.ver, 2);
  EXPECT_EQ(stored_user->get_version_tracker().read_version.tag, "user_version_tag");

  // store again (this time we manipulate the version so when storing next time it doesn't match)
  stored_user->get_info().user_email = "this_was_updated@test.com";
  user1.uinfo.user_email = "this_was_updated@test.com";  // set the same value so we can compare later

  // change here the version obtained with load_user
  stored_user->get_version_tracker().read_version.ver = 1;
  // store should be cancelled
  EXPECT_EQ(stored_user->store_user(&no_dpp, null_yield, true), -ECANCELED);
}

TEST_F(TestSFSSQLiteUsers, StoreRemoveUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = std::make_shared<rgw::sal::SFStore>(ceph_context.get(), getTestDir());

  const NoDoutPrefix no_dpp(ceph_context.get(), 1);
  std::list<std::string> userIds;
  bool truncated;
  void * meta_handle;
  store->meta_list_keys_init(&no_dpp, std::string("user"), std::string(), &meta_handle);
  store->meta_list_keys_next(&no_dpp, meta_handle, std::numeric_limits<int>::max(), userIds, &truncated);
  ASSERT_FALSE(truncated);
  ASSERT_EQ(userIds.size(), 0);

  auto user1 = createTestUser("1");
  auto user2 = createTestUser("2");
  auto user3 = createTestUser("3");
  auto db_users = std::make_shared<SQLiteUsers>(store->db_conn);
  db_users->store_user(user1);
  db_users->store_user(user2);
  db_users->store_user(user3);

  store->meta_list_keys_next(&no_dpp, meta_handle, std::numeric_limits<int>::max(), userIds, &truncated);
  ASSERT_FALSE(truncated);
  ASSERT_EQ(userIds.size(), 3);
  std::vector<std::string> users_vector{ std::make_move_iterator(userIds.begin()),
                                  std::make_move_iterator(userIds.end()) };
  ASSERT_EQ(users_vector[0], "test1");
  ASSERT_EQ(users_vector[1], "test2");
  ASSERT_EQ(users_vector[2], "test3");

  // now remove user test2
  auto stored_user = store->get_user(user2.uinfo.user_id);
  EXPECT_EQ(stored_user->remove_user(&no_dpp, null_yield), 0);

  // ensure the user was removed
  userIds.clear();
  store->meta_list_keys_next(&no_dpp, meta_handle, std::numeric_limits<int>::max(), userIds, &truncated);
  ASSERT_FALSE(truncated);
  ASSERT_EQ(userIds.size(), 2);
  std::vector<std::string> users_after_remove_vector{ std::make_move_iterator(userIds.begin()),
                                  std::make_move_iterator(userIds.end()) };
  ASSERT_EQ(users_after_remove_vector[0], "test1");
  ASSERT_EQ(users_after_remove_vector[1], "test3");
}

TEST_F(TestSFSSQLiteUsers, GetByAccessKeyMultipleKeys) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_users = std::make_shared<SQLiteUsers>(conn);
  auto user = createTestUser("1");
  // add extra key
  user.uinfo.access_keys["key3"] = RGWAccessKey("key3", "secret3");

  db_users->store_user(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->get_user_by_access_key("key1_1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);

  ret_user = db_users->get_user_by_access_key("key2_1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);

  ret_user = db_users->get_user_by_access_key("key3");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}
