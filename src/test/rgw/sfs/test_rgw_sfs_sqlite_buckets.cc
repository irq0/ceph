// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <random>

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_conversions.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

/*
  These structs are in-memory mockable versions of actual structs/classes
  that have a private rep.
  Real types normally populate their rep via encode/decode methods.
  For the sake of convenience, we define binary equivalent types with
  public editable members.
*/
namespace mockable {
struct DefaultRetention {
  std::string mode;
  int days;
  int years;
};

struct ObjectLockRule {
  mockable::DefaultRetention defaultRetention;
};

struct RGWObjectLock {
  bool enabled;
  bool rule_exist;
  mockable::ObjectLockRule rule;
};

mockable::RGWObjectLock& actual2mock(::RGWObjectLock& actual) {
  return (mockable::RGWObjectLock&)actual;
}
}  // namespace mockable

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

  fs::path getDBFullPath(const std::string& base_dir) const {
    auto db_full_name = "s3gw.db";
    auto db_full_path = fs::path(base_dir) / db_full_name;
    return db_full_path;
  }

  fs::path getDBFullPath() const { return getDBFullPath(getTestDir()); }

  void createUser(const std::string& username, DBConnRef conn) {
    SQLiteUsers users(conn);
    DBOPUserInfo user;
    user.uinfo.user_id.id = username;
    users.store_user(user);
  }
};

void compareBucketRGWInfo(
    const RGWBucketInfo& origin, const RGWBucketInfo& dest
) {
  ASSERT_EQ(origin.bucket.name, dest.bucket.name);
  ASSERT_EQ(origin.bucket.tenant, dest.bucket.tenant);
  ASSERT_EQ(origin.bucket.marker, dest.bucket.marker);
  ASSERT_EQ(origin.bucket.bucket_id, dest.bucket.bucket_id);
  ASSERT_EQ(origin.owner.id, dest.owner.id);
  ASSERT_EQ(origin.creation_time, dest.creation_time);
  ASSERT_EQ(origin.placement_rule.name, dest.placement_rule.name);
  ASSERT_EQ(
      origin.placement_rule.storage_class, dest.placement_rule.storage_class
  );
  ASSERT_EQ(origin.owner.id, dest.owner.id);
  ASSERT_EQ(origin.flags, dest.flags);
  ASSERT_EQ(origin.zonegroup, dest.zonegroup);
  ASSERT_EQ(origin.quota.max_size, dest.quota.max_size);
  ASSERT_EQ(origin.quota.max_objects, dest.quota.max_objects);
  ASSERT_EQ(origin.quota.enabled, dest.quota.enabled);
  ASSERT_EQ(origin.quota.check_on_raw, dest.quota.check_on_raw);
  ASSERT_EQ(origin.obj_lock.get_days(), dest.obj_lock.get_days());
  ASSERT_EQ(origin.obj_lock.get_years(), dest.obj_lock.get_years());
  ASSERT_EQ(origin.obj_lock.get_mode(), dest.obj_lock.get_mode());
  ASSERT_EQ(origin.obj_lock.has_rule(), dest.obj_lock.has_rule());
  ASSERT_EQ(
      origin.obj_lock.retention_period_valid(),
      dest.obj_lock.retention_period_valid()
  );
}

void compareBucketAttrs(
    const std::optional<rgw::sal::Attrs>& origin,
    const std::optional<rgw::sal::Attrs>& dest
) {
  ASSERT_EQ(origin.has_value(), true);
  ASSERT_EQ(origin.has_value(), dest.has_value());
  auto orig_acl_bl_it = origin->find(RGW_ATTR_ACL);
  EXPECT_TRUE(orig_acl_bl_it != origin->end());
  auto dest_acl_bl_it = dest->find(RGW_ATTR_ACL);
  EXPECT_TRUE(dest_acl_bl_it != dest->end());

  RGWAccessControlPolicy orig_aclp;
  auto orig_ci_lval = orig_acl_bl_it->second.cbegin();
  orig_aclp.decode(orig_ci_lval);
  RGWAccessControlPolicy dest_aclp;
  auto dest_ci_lval = dest_acl_bl_it->second.cbegin();
  dest_aclp.decode(dest_ci_lval);
  ASSERT_EQ(orig_aclp, dest_aclp);
}

void compareBuckets(const DBOPBucketInfo& origin, const DBOPBucketInfo& dest) {
  compareBucketRGWInfo(origin.binfo, dest.binfo);
  compareBucketAttrs(origin.battrs, dest.battrs);
  ASSERT_EQ(origin.deleted, dest.deleted);
}

bool randomBool() {
  std::random_device generator;
  std::uniform_int_distribution<int> distribution(0, 1);
  return static_cast<bool>(distribution(generator));
}

DBOPBucketInfo createTestBucket(const std::string& suffix) {
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

  //set attrs with default ACL
  {
    RGWAccessControlPolicy aclp;
    rgw_user aclu("usertest");
    aclp.get_acl().create_default(aclu, "usertest");
    aclp.get_owner().set_name("usertest");
    aclp.get_owner().set_id(aclu);
    bufferlist acl_bl;
    aclp.encode(acl_bl);
    rgw::sal::Attrs attrs;
    attrs[RGW_ATTR_ACL] = acl_bl;
    bucket.battrs = attrs;
  }

  bucket.deleted = randomBool();

  //object locking
  mockable::RGWObjectLock& ol = mockable::actual2mock(bucket.binfo.obj_lock);
  ol.enabled = true;
  ol.rule.defaultRetention.years = 12;
  ol.rule.defaultRetention.days = 31;
  ol.rule.defaultRetention.mode = "GOVERNANCE";
  ol.rule_exist = true;

  return bucket;
}

void createDBBucketBasic(
    const std::string& user, const std::string& name,
    const std::string& bucket_id, const std::shared_ptr<DBConn>& conn
) {
  auto storage = conn->get_storage();
  DBBucket db_bucket;
  db_bucket.bucket_name = name;
  db_bucket.bucket_id = bucket_id;
  db_bucket.owner_id = user;
  db_bucket.deleted = false;
  storage.replace(db_bucket);
}

void deleteDBBucketBasic(
    const std::string& bucket_id, const std::shared_ptr<DBConn>& conn
) {
  auto storage = conn->get_storage();
  auto bucket = storage.get_pointer<DBBucket>(bucket_id);
  ASSERT_TRUE(bucket != nullptr);
  bucket->deleted = true;
  storage.replace(*bucket);
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

  auto ret_bucket = db_buckets->get_bucket("BucketID1");
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

  db_buckets->remove_bucket("BucketID2");
  auto bucket_ids = db_buckets->get_bucket_ids();
  EXPECT_EQ(bucket_ids.size(), 2);
  EXPECT_EQ(bucket_ids[0], "test1");
  EXPECT_EQ(bucket_ids[1], "test3");

  auto ret_bucket = db_buckets->get_bucket("BucketID2");
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

  auto ret_bucket = db_buckets->get_bucket("BucketID1");
  ASSERT_TRUE(ret_bucket.has_value());
  compareBuckets(bucket, *ret_bucket);

  bucket.binfo.bucket.marker = "MakerChanged";
  db_buckets->store_bucket(bucket);
  ret_bucket = db_buckets->get_bucket("BucketID1");
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

  auto ret_bucket = db_buckets->get_bucket("BucketID1");
  ASSERT_TRUE(ret_bucket.has_value());
  compareBuckets(bucket, *ret_bucket);

  // create a new instance, bucket should exist
  auto db_buckets_2 = std::make_shared<SQLiteBuckets>(conn);
  ret_bucket = db_buckets_2->get_bucket("BucketID1");
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
  db_bucket.bucket_id = "test_storage_id";

  // we have to use replace because the primary key of rgw_bucket is a string
  storage.replace(db_bucket);

  auto bucket = storage.get_pointer<DBBucket>("test_storage_id");

  ASSERT_NE(bucket, nullptr);
  ASSERT_EQ(bucket->bucket_name, "test_storage");
  ASSERT_EQ(bucket->bucket_id, "test_storage_id");

  // convert the DBBucket to RGWBucket (blobs are decoded here)
  auto rgw_bucket = get_rgw_bucket(*bucket);
  ASSERT_EQ(rgw_bucket.binfo.bucket.name, bucket->bucket_name);
  ASSERT_EQ(rgw_bucket.binfo.bucket.bucket_id, bucket->bucket_id);

  // creates a RGWBucket for testing (id = test1, etc..)
  auto rgw_bucket_2 = createTestBucket("1");

  // convert to DBBucket (blobs are encoded here)
  auto db_bucket_2 = get_db_bucket(rgw_bucket_2);

  // we have to use replace because the primary key of rgw_bucket is a string
  storage.replace(db_bucket_2);

  // now use the SqliteBuckets method, so user is already converted
  auto ret_bucket = db_buckets.get_bucket("BucketID1");
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

  EXPECT_THROW(
      {
        try {
          storage.replace(db_bucket);
          ;
        } catch (const std::system_error& e) {
          EXPECT_STREQ(
              "FOREIGN KEY constraint failed: constraint failed", e.what()
          );
          throw;
        }
      },
      std::system_error
  );
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

  EXPECT_THROW(
      {
        try {
          storage.replace(db_bucket);
        } catch (const std::system_error& e) {
          EXPECT_STREQ(
              "FOREIGN KEY constraint failed: constraint failed", e.what()
          );
          throw;
        }
      },
      std::system_error
  );
}

TEST_F(TestSFSSQLiteBuckets, GetDeletedBucketsIds) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);
  // create a few buckets
  createDBBucketBasic("usertest", "bucket1", "bucket1_id", conn);
  createDBBucketBasic("usertest", "bucket2", "bucket2_id", conn);
  createDBBucketBasic("usertest", "bucket3", "bucket3_id", conn);
  createDBBucketBasic("usertest", "bucket4", "bucket4_id", conn);
  createDBBucketBasic("usertest", "bucket5", "bucket5_id", conn);

  SQLiteBuckets db_buckets(conn);
  // no buckets are deleted yet
  auto deleted_bucket_ids = db_buckets.get_deleted_buckets_ids();
  EXPECT_EQ(deleted_bucket_ids.size(), 0);

  // delete 2 buckets
  deleteDBBucketBasic("bucket3_id", conn);
  deleteDBBucketBasic("bucket5_id", conn);

  // we should get 2 buckets now
  deleted_bucket_ids = db_buckets.get_deleted_buckets_ids();
  ASSERT_EQ(deleted_bucket_ids.size(), 2);
  EXPECT_EQ(deleted_bucket_ids[0], "bucket3_id");
  EXPECT_EQ(deleted_bucket_ids[1], "bucket5_id");

  // delete one more bucket
  deleteDBBucketBasic("bucket1_id", conn);

  // we should get 3 buckets now
  deleted_bucket_ids = db_buckets.get_deleted_buckets_ids();
  ASSERT_EQ(deleted_bucket_ids.size(), 3);
  EXPECT_EQ(deleted_bucket_ids[0], "bucket3_id");
  EXPECT_EQ(deleted_bucket_ids[1], "bucket5_id");
  EXPECT_EQ(deleted_bucket_ids[2], "bucket1_id");
}

TEST_F(TestSFSSQLiteBuckets, TestBucketEmpty) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  // Create the user, we need it because OwnerID is a foreign key of User::UserID
  createUser("usertest", conn);
  // create a bucket
  createDBBucketBasic("usertest", "bucket1", "bucket1_id", conn);

  // after bucket is created it is empty
  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);
  EXPECT_TRUE(db_buckets->bucket_empty("bucket1_id"));

  // create an object and version (version is OPEN)
  auto db_versions = std::make_shared<SQLiteVersionedObjects>(conn);
  auto version1 = db_versions->create_new_versioned_object_transact(
      "bucket1_id", "object_1", "version1"
  );
  ASSERT_TRUE(version1.has_value());

  // with 1 version (OPEN) bucket is considered empty
  EXPECT_TRUE(db_buckets->bucket_empty("bucket1_id"));

  // commit version1
  version1->object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  db_versions->store_versioned_object(*version1);
  // bucket is not empty now
  EXPECT_FALSE(db_buckets->bucket_empty("bucket1_id"));

  // add a delete marker
  bool delete_marker_added = false;
  db_versions->add_delete_marker_transact(
      version1->object_id, "delete_maker_1", delete_marker_added
  );
  ASSERT_TRUE(delete_marker_added);

  // bucket is still not empty
  EXPECT_FALSE(db_buckets->bucket_empty("bucket1_id"));

  // now delete version1
  version1->object_state = rgw::sal::sfs::ObjectState::DELETED;
  db_versions->store_versioned_object(*version1);

  // now bucket should be empty (all versions are deleted)
  EXPECT_TRUE(db_buckets->bucket_empty("bucket1_id"));
}
