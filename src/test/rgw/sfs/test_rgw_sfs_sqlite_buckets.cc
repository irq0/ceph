// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_conversions.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"

#include "rgw/rgw_sal_sfs.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

class TestSFSSQLiteBuckets : public ::testing::Test {
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

  void createUser(const std::string & username, DBConnRef conn) {
    SQLiteUsers users(conn);
    DBOPUserInfo user;
    user.uinfo.user_id.id = username;
    users.store_user(user);
  }
};

void compareBucketRGWInfo(const RGWBucketInfo & origin, const RGWBucketInfo & dest) {
  ASSERT_EQ(origin.bucket.name, dest.bucket.name);
  ASSERT_EQ(origin.bucket.tenant, dest.bucket.tenant);
  ASSERT_EQ(origin.bucket.marker, dest.bucket.marker);
  ASSERT_EQ(origin.bucket.bucket_id, dest.bucket.bucket_id);
  ASSERT_EQ(origin.owner.id, dest.owner.id);
  ASSERT_EQ(origin.creation_time, dest.creation_time);
  ASSERT_EQ(origin.placement_rule.name, dest.placement_rule.name);
  ASSERT_EQ(origin.placement_rule.storage_class, dest.placement_rule.storage_class);
  ASSERT_EQ(origin.owner.id, dest.owner.id);
  ASSERT_EQ(origin.flags, dest.flags);
  ASSERT_EQ(origin.zonegroup, dest.zonegroup);
  ASSERT_EQ(origin.quota.max_size, dest.quota.max_size);
  ASSERT_EQ(origin.quota.max_objects, dest.quota.max_objects);
  ASSERT_EQ(origin.quota.enabled, dest.quota.enabled);
  ASSERT_EQ(origin.quota.check_on_raw, dest.quota.check_on_raw);
}

void compareBuckets(const DBOPBucketInfo & origin, const DBOPBucketInfo & dest) {
  compareBucketRGWInfo(origin.binfo, dest.binfo);
}

DBOPBucketInfo createTestBucket(const std::string & suffix) {
  DBOPBucketInfo bucket;
  bucket.binfo.bucket.name = "test" + suffix;
  bucket.binfo.bucket.tenant = "Tenant" + suffix;
  bucket.binfo.bucket.marker = "Marker" + suffix;
  bucket.binfo.bucket.bucket_id = "BucketID" + suffix;
  bucket.binfo.creation_time = ceph::real_clock::from_time_t(1657703755);
  bucket.binfo.placement_rule.name = "default";
  bucket.binfo.placement_rule.storage_class = "STANDARD";
  bucket.binfo.owner.id = "usertest";
  bucket.binfo.flags = static_cast<uint32_t>(rand());
  bucket.binfo.zonegroup = "zonegroup" + suffix;
  bucket.binfo.quota.max_size = 1048576;
  bucket.binfo.quota.max_objects = 512;
  bucket.binfo.quota.enabled = true;
  bucket.binfo.quota.check_on_raw = true;
  return bucket;
}

TEST_F(TestSFSSQLiteBuckets, CreateAndGet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  auto bucket = createTestBucket("1");
  db_buckets->store_bucket(bucket);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_bucket = db_buckets->get_bucket("test1");
  ASSERT_TRUE(ret_bucket.has_value());
  compareBuckets(bucket, *ret_bucket);
}

TEST_F(TestSFSSQLiteBuckets, ListBucketsIDs) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);

  db_buckets->store_bucket(createTestBucket("1"));
  db_buckets->store_bucket(createTestBucket("2"));
  db_buckets->store_bucket(createTestBucket("3"));
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto buckets_ids = db_buckets->get_bucket_ids();
  EXPECT_EQ(buckets_ids.size(), 3);
  EXPECT_EQ(buckets_ids[0], "test1");
  EXPECT_EQ(buckets_ids[1], "test2");
  EXPECT_EQ(buckets_ids[2], "test3");
}

TEST_F(TestSFSSQLiteBuckets, ListBuckets) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);

  auto bucket_1 = createTestBucket("1");
  db_buckets->store_bucket(bucket_1);

  auto bucket_2 = createTestBucket("2");
  db_buckets->store_bucket(bucket_2);

  auto bucket_3 = createTestBucket("3");
  db_buckets->store_bucket(bucket_3);

  auto buckets = db_buckets->get_buckets();
  EXPECT_EQ(buckets.size(), 3);
  compareBuckets(bucket_1, buckets[0]);
  compareBuckets(bucket_2, buckets[1]);
  compareBuckets(bucket_3, buckets[2]);
}

TEST_F(TestSFSSQLiteBuckets, ListBucketsByOwner) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);
  createUser("user1", conn);
  createUser("user2", conn);
  createUser("user3", conn);

  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);

  auto bucket_1 = createTestBucket("1");
  bucket_1.binfo.owner.id = "user1";
  db_buckets->store_bucket(bucket_1);

  auto bucket_2 = createTestBucket("2");
  bucket_2.binfo.owner.id = "user2";
  db_buckets->store_bucket(bucket_2);

  auto bucket_3 = createTestBucket("3");
  bucket_3.binfo.owner.id = "user3";
  db_buckets->store_bucket(bucket_3);

  auto buckets = db_buckets->get_buckets("user1");
  EXPECT_EQ(buckets.size(), 1);
  compareBuckets(bucket_1, buckets[0]);

  buckets = db_buckets->get_buckets("user2");
  EXPECT_EQ(buckets.size(), 1);
  compareBuckets(bucket_2, buckets[0]);

  buckets = db_buckets->get_buckets("user3");
  EXPECT_EQ(buckets.size(), 1);
  compareBuckets(bucket_3, buckets[0]);

  buckets = db_buckets->get_buckets("this_user_does_not_exist");
  EXPECT_EQ(buckets.size(), 0);
}

TEST_F(TestSFSSQLiteBuckets, ListBucketsIDsPerUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  // Create the rest of users
  createUser("user1", conn);
  createUser("user2", conn);
  createUser("user3", conn);


  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);

  auto test_bucket_1 = createTestBucket("1");
  test_bucket_1.binfo.owner.id = "user1";
  db_buckets->store_bucket(test_bucket_1);

  auto test_bucket_2 = createTestBucket("2");
  test_bucket_2.binfo.owner.id = "user2";
  db_buckets->store_bucket(test_bucket_2);

  auto test_bucket_3 = createTestBucket("3");
  test_bucket_3.binfo.owner.id = "user3";
  db_buckets->store_bucket(test_bucket_3);

  auto buckets_ids = db_buckets->get_bucket_ids("user1");
  ASSERT_EQ(buckets_ids.size(), 1);
  EXPECT_EQ(buckets_ids[0], "test1");

  buckets_ids = db_buckets->get_bucket_ids("user2");
  ASSERT_EQ(buckets_ids.size(), 1);
  EXPECT_EQ(buckets_ids[0], "test2");

  buckets_ids = db_buckets->get_bucket_ids("user3");
  ASSERT_EQ(buckets_ids.size(), 1);
  EXPECT_EQ(buckets_ids[0], "test3");
}

TEST_F(TestSFSSQLiteBuckets, remove_bucket) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);

  db_buckets->store_bucket(createTestBucket("1"));
  db_buckets->store_bucket(createTestBucket("2"));
  db_buckets->store_bucket(createTestBucket("3"));

  db_buckets->remove_bucket("test2");
  auto bucket_ids = db_buckets->get_bucket_ids();
  EXPECT_EQ(bucket_ids.size(), 2);
  EXPECT_EQ(bucket_ids[0], "test1");
  EXPECT_EQ(bucket_ids[1], "test3");

  auto ret_bucket = db_buckets->get_bucket("test2");
  ASSERT_FALSE(ret_bucket.has_value());
}

TEST_F(TestSFSSQLiteBuckets, RemoveUserThatDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);

  db_buckets->store_bucket(createTestBucket("1"));
  db_buckets->store_bucket(createTestBucket("2"));
  db_buckets->store_bucket(createTestBucket("3"));

  db_buckets->remove_bucket("testX");
  auto buckets_ids = db_buckets->get_bucket_ids();
  EXPECT_EQ(buckets_ids.size(), 3);
  EXPECT_EQ(buckets_ids[0], "test1");
  EXPECT_EQ(buckets_ids[1], "test2");
  EXPECT_EQ(buckets_ids[2], "test3");
}

TEST_F(TestSFSSQLiteBuckets, CreateAndUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);
  auto bucket = createTestBucket("1");
  db_buckets->store_bucket(bucket);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_bucket = db_buckets->get_bucket("test1");
  ASSERT_TRUE(ret_bucket.has_value());
  compareBuckets(bucket, *ret_bucket);

  bucket.binfo.bucket.marker = "MakerChanged";
  db_buckets->store_bucket(bucket);
  ret_bucket = db_buckets->get_bucket("test1");
  ASSERT_TRUE(ret_bucket.has_value());
  ASSERT_EQ(ret_bucket->binfo.bucket.marker, "MakerChanged");
  compareBuckets(bucket, *ret_bucket);
}

TEST_F(TestSFSSQLiteBuckets, GetExisting) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);
  auto bucket = createTestBucket("1");
  db_buckets->store_bucket(bucket);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_bucket = db_buckets->get_bucket("test1");
  ASSERT_TRUE(ret_bucket.has_value());
  compareBuckets(bucket, *ret_bucket);

  // create a new instance, bucket should exist
  auto db_buckets_2 = std::make_shared<SQLiteBuckets>(conn);
  ret_bucket = db_buckets_2->get_bucket("test1");
  ASSERT_TRUE(ret_bucket.has_value());
  compareBuckets(bucket, *ret_bucket);
}

TEST_F(TestSFSSQLiteBuckets, UseStorage) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  SQLiteBuckets db_buckets(conn);
  auto storage = conn->get_storage();

  DBBucket db_bucket;
  db_bucket.bucket_name = "test_storage";
  db_bucket.owner_id = "usertest";

  // we have to use replace because the primary key of rgw_bucket is a string
  storage.replace(db_bucket);

  auto bucket = storage.get_pointer<DBBucket>("test_storage");

  ASSERT_NE(bucket, nullptr);
  ASSERT_EQ(bucket->bucket_name, "test_storage");

  // convert the DBBucket to RGWBucket (blobs are decoded here)
  auto rgw_bucket = get_rgw_bucket(*bucket);
  ASSERT_EQ(rgw_bucket.binfo.bucket.name, bucket->bucket_name);

  // creates a RGWBucket for testing (id = test1, etc..)
  auto rgw_bucket_2 = createTestBucket("1");

  // convert to DBBucket (blobs are encoded here)
  auto db_bucket_2 = get_db_bucket(rgw_bucket_2);

  // we have to use replace because the primary key of rgw_bucket is a string
  storage.replace(db_bucket_2);

  // now use the SqliteBuckets method, so user is already converted
  auto ret_bucket = db_buckets.get_bucket("test1");
  ASSERT_TRUE(ret_bucket.has_value());
  compareBuckets(rgw_bucket_2, *ret_bucket);
}

TEST_F(TestSFSSQLiteBuckets, CreateBucketForNonExistingUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  SQLiteBuckets db_buckets(conn);
  auto storage = conn->get_storage();

  DBBucket db_bucket;
  db_bucket.bucket_name = "test_storage";
  db_bucket.owner_id = "this_user_does_not_exist";

  EXPECT_THROW({
    try {
        storage.replace(db_bucket);;
    } catch( const std::system_error & e ) {
        EXPECT_STREQ( "FOREIGN KEY constraint failed: constraint failed", e.what() );
        throw;
    }
  }, std::system_error );
}


TEST_F(TestSFSSQLiteBuckets, CreateBucketOwnerNotSet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);

  SQLiteBuckets db_buckets(conn);
  auto storage = conn->get_storage();

  DBBucket db_bucket;
  db_bucket.bucket_name = "test_storage";

  EXPECT_THROW({
    try {
        storage.replace(db_bucket);;
    } catch( const std::system_error & e ) {
        EXPECT_STREQ( "FOREIGN KEY constraint failed: constraint failed", e.what() );
        throw;
    }
  }, std::system_error );
}
