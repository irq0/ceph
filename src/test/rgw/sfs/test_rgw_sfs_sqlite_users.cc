// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/driver/sfs/sqlite/users_conversions.h"

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
    auto db_full_name = "users.db";
    auto db_full_path = fs::path(base_dir) /  db_full_name;
    return db_full_path;
  }

  fs::path getDBFullPath() const {
    return getDBFullPath(getTestDir());
  }
};

template <typename T>
bool compare(const T & origin, const T & dest) {
  return origin == dest;
}

bool compare(const RGWAccessKey & origin, const RGWAccessKey & dest) {
  if (origin.id != dest.id) return false;
  if (origin.key != dest.key) return false;
  if (origin.subuser != dest.subuser) return false;
  return true;
}

bool compare(const RGWSubUser & origin, const RGWSubUser & dest) {
  if (origin.name != dest.name) return false;
  if (origin.perm_mask != dest.perm_mask) return false;
  return true;
}

template <typename T>
bool compareMaps(const T & origin, const T & dest) {
  if (origin.size() != dest.size()) return false;
  for (auto const& [key, val] : origin)
  {
    if (dest.find(key) == dest.end()) return false;
    auto & dest_val = dest.at(key);
    if (!compare(val, dest_val)) return false;
  }
  return true;
}

std::string getCapsString(const RGWUserCaps & caps) {
  auto formatter = std::make_unique<ceph::JSONFormatter>(new ceph::JSONFormatter(true));
  encode_json("caps", caps, formatter.get());
  std::ostringstream oss;
  formatter->flush(oss);
  return oss.str();
}

void compareUsers(const DBOPUserInfo & origin, const DBOPUserInfo & dest) {
  ASSERT_EQ(origin.uinfo.user_id.id, dest.uinfo.user_id.id);
  ASSERT_EQ(origin.uinfo.user_id.tenant, dest.uinfo.user_id.tenant);
  ASSERT_EQ(origin.uinfo.user_id.ns, dest.uinfo.user_id.ns);
  ASSERT_EQ(origin.uinfo.display_name, dest.uinfo.display_name);
  ASSERT_EQ(origin.uinfo.user_email, dest.uinfo.user_email);
  ASSERT_TRUE(compareMaps(origin.uinfo.access_keys, dest.uinfo.access_keys));
  ASSERT_TRUE(compareMaps(origin.uinfo.swift_keys, dest.uinfo.swift_keys));
  ASSERT_TRUE(compareMaps(origin.uinfo.subusers, dest.uinfo.subusers));
  ASSERT_EQ(origin.uinfo.suspended, dest.uinfo.suspended);
  ASSERT_EQ(origin.uinfo.max_buckets, dest.uinfo.max_buckets);
  ASSERT_EQ(origin.uinfo.op_mask, dest.uinfo.op_mask);
  ASSERT_EQ(getCapsString(origin.uinfo.caps), getCapsString(dest.uinfo.caps));
  ASSERT_EQ(origin.uinfo.system, dest.uinfo.system);
  ASSERT_EQ(origin.uinfo.default_placement.name, dest.uinfo.default_placement.name);
  ASSERT_EQ(origin.uinfo.default_placement.storage_class, dest.uinfo.default_placement.storage_class);
  ASSERT_EQ(origin.uinfo.placement_tags, dest.uinfo.placement_tags);
  ASSERT_EQ(
    origin.uinfo.quota.bucket_quota.max_size,
    dest.uinfo.quota.bucket_quota.max_size
  );
  ASSERT_EQ(
    origin.uinfo.quota.bucket_quota.max_objects,
    dest.uinfo.quota.bucket_quota.max_objects
  );
  ASSERT_EQ(
    origin.uinfo.quota.bucket_quota.enabled,
    dest.uinfo.quota.bucket_quota.enabled
  );
  ASSERT_EQ(
    origin.uinfo.quota.bucket_quota.check_on_raw,
    dest.uinfo.quota.bucket_quota.check_on_raw
  );
  ASSERT_TRUE(compareMaps(origin.uinfo.temp_url_keys, dest.uinfo.temp_url_keys));
  ASSERT_EQ(
    origin.uinfo.quota.user_quota.max_size,
    dest.uinfo.quota.user_quota.max_size
  );
  ASSERT_EQ(
    origin.uinfo.quota.user_quota.max_objects,
    dest.uinfo.quota.user_quota.max_objects
  );
  ASSERT_EQ(
    origin.uinfo.quota.user_quota.enabled,
    dest.uinfo.quota.user_quota.enabled
  );
  ASSERT_EQ(
    origin.uinfo.quota.user_quota.check_on_raw,
    dest.uinfo.quota.user_quota.check_on_raw
  );
  ASSERT_EQ(origin.uinfo.type, dest.uinfo.type);
  ASSERT_EQ(origin.uinfo.mfa_ids, dest.uinfo.mfa_ids);
  ASSERT_EQ(origin.uinfo.assumed_role_arn, dest.uinfo.assumed_role_arn);
  ASSERT_TRUE(compareMaps(origin.user_attrs, dest.user_attrs));
  ASSERT_EQ(origin.user_version.ver, dest.user_version.ver);
  ASSERT_EQ(origin.user_version.tag, dest.user_version.tag);
}

DBOPUserInfo createTestUser(const std::string & suffix) {
  DBOPUserInfo user;
  user.uinfo.user_id.id = "test" + suffix;
  user.uinfo.user_id.tenant = "Tenant" + suffix;
  user.uinfo.user_id.ns = "NS" + suffix;
  user.uinfo.display_name = "DisplayName1" + suffix;
  user.uinfo.user_email = "user" + suffix + "@test.com";
  user.uinfo.access_keys["key1"] = RGWAccessKey("key1_" + suffix, "secret1");
  user.uinfo.access_keys["key2"] = RGWAccessKey("key2_" + suffix, "secret2");
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
  user.user_version.ver = 1999;
  user.user_version.tag = "version_tag";
  return user;
}

TEST_F(TestSFSSQLiteUsers, CreateAndGet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());
  auto user = createTestUser("1");
  db_users->storeUser(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->getUser("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, CreateAndGetByEmail) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());
  auto user = createTestUser("1");
  db_users->storeUser(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->getUserByEmail("user1@test.com");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, CreateAndGetByAccessKey) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());
  auto user = createTestUser("1");
  db_users->storeUser(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->getUserByAccessKey("key1_1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, ListUserIDs) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());

  db_users->storeUser(createTestUser("1"));
  db_users->storeUser(createTestUser("2"));
  db_users->storeUser(createTestUser("3"));
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto user_ids = db_users->getUserIDs();
  EXPECT_EQ(user_ids.size(), 3);
  EXPECT_EQ(user_ids[0], "test1");
  EXPECT_EQ(user_ids[1], "test2");
  EXPECT_EQ(user_ids[2], "test3");
}

TEST_F(TestSFSSQLiteUsers, RemoveUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());

  db_users->storeUser(createTestUser("1"));
  db_users->storeUser(createTestUser("2"));
  db_users->storeUser(createTestUser("3"));

  db_users->removeUser("test2");
  auto user_ids = db_users->getUserIDs();
  EXPECT_EQ(user_ids.size(), 2);
  EXPECT_EQ(user_ids[0], "test1");
  EXPECT_EQ(user_ids[1], "test3");

  auto ret_user = db_users->getUser("test2");
  ASSERT_FALSE(ret_user.has_value());
}

TEST_F(TestSFSSQLiteUsers, RemoveUserThatDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());

  db_users->storeUser(createTestUser("1"));
  db_users->storeUser(createTestUser("2"));
  db_users->storeUser(createTestUser("3"));

  db_users->removeUser("testX");
  auto user_ids = db_users->getUserIDs();
  EXPECT_EQ(user_ids.size(), 3);
  EXPECT_EQ(user_ids[0], "test1");
  EXPECT_EQ(user_ids[1], "test2");
  EXPECT_EQ(user_ids[2], "test3");
}

TEST_F(TestSFSSQLiteUsers, CreateAndUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());
  auto user = createTestUser("1");
  db_users->storeUser(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->getUser("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);

  user.uinfo.user_email = "email_changed@test.com";
  db_users->storeUser(user);
  ret_user = db_users->getUser("test1");
  ASSERT_TRUE(ret_user.has_value());
  ASSERT_EQ(ret_user->uinfo.user_email, "email_changed@test.com");
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, GetExisting) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());
  auto user = createTestUser("1");
  db_users->storeUser(user);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->getUser("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);

  // create a new instance, user should exist
  auto db_users_2 = std::make_shared<SQLiteUsers>(ceph_context.get());
  ret_user = db_users_2->getUser("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(user, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, AddMoreThanOneUserWithSameEmail) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());
  auto user1 = createTestUser("1");
  auto user2 = createTestUser("2");
  auto user3 = createTestUser("3");
  user2.uinfo.user_email = "user1@test.com";
  user3.uinfo.user_email = "user1@test.com";
  db_users->storeUser(user1);
  db_users->storeUser(user2);
  db_users->storeUser(user3);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->getUserByEmail("user1@test.com");
  ASSERT_TRUE(ret_user.has_value());
  // it will only return the first user with that email
  compareUsers(user1, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, AddMoreThanOneUserWithSameAccessKey) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_users = std::make_shared<SQLiteUsers>(ceph_context.get());
  auto user1 = createTestUser("1");
  auto user2 = createTestUser("2");
  auto user3 = createTestUser("3");
  std::map<std::string, RGWAccessKey> access_keys;
  access_keys["key1"] = RGWAccessKey("key1", "secret1");
  access_keys["key2"] = RGWAccessKey("key2", "secret2");
  user1.uinfo.access_keys = access_keys;
  user2.uinfo.access_keys = access_keys;
  user2.uinfo.access_keys = access_keys;
  db_users->storeUser(user1);
  db_users->storeUser(user2);
  db_users->storeUser(user3);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_user = db_users->getUserByAccessKey("key1");
  ASSERT_TRUE(ret_user.has_value());
  // it will only return the first user with that access key
  compareUsers(user1, *ret_user);
}

TEST_F(TestSFSSQLiteUsers, UseStorage) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  SQLiteUsers db_users(ceph_context.get());
  auto storage = db_users.getStorage();

  DBUser db_user;
  db_user.UserID = "test_storage";

  // we have to use replace because the primary key of rgw_user is a string
  storage.replace(db_user);

  auto user = storage.get_pointer<DBUser>("test_storage");

  ASSERT_NE(user, nullptr);
  ASSERT_EQ(user->UserID, "test_storage");

  // convert the DBUser to RGWUser (blobs are decoded here)
  auto rgw_user = getRGWUser(*user);
  ASSERT_EQ(rgw_user.uinfo.user_id.id, user->UserID);

  // creates a RGWUser for testing (id = test1, email = test1@test.com, etc..)
  auto rgw_user_2 = createTestUser("1");

  // convert to DBUser (blobs are encoded here)
  auto db_user_2 = getDBUser(rgw_user_2);

  // we have to use replace because the primary key of rgw_user is a string
  storage.replace(db_user_2);

  // now use the SqliteUsers method, so user is already converted
  auto ret_user = db_users.getUser("test1");
  ASSERT_TRUE(ret_user.has_value());
  compareUsers(rgw_user_2, *ret_user);
}
