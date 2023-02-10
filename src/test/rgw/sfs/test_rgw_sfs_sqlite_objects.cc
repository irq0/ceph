// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/driver/sfs/sqlite/objects/object_conversions.h"

#include "rgw/rgw_sal_sfs.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

class TestSFSSQLiteObjects : public ::testing::Test {
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

  void createBucket(const std::string & username, const std::string & bucketname, DBConnRef conn) {
    createUser(username, conn);
    SQLiteBuckets buckets(conn);
    DBOPBucketInfo bucket;
    bucket.binfo.bucket.bucket_id = bucketname;
    bucket.binfo.bucket.name = bucketname;
    bucket.binfo.owner.id = username;
    buckets.store_bucket(bucket);
  }
};

void compareObjects(const DBOPObjectInfo & origin, const DBOPObjectInfo & dest) {
  ASSERT_EQ(origin.uuid, dest.uuid);
  ASSERT_EQ(origin.bucket_id, dest.bucket_id);
  ASSERT_EQ(origin.name, dest.name);
  ASSERT_EQ(origin.size, dest.size);
  ASSERT_EQ(origin.etag, dest.etag);
  ASSERT_EQ(origin.mtime, dest.mtime);
  ASSERT_EQ(origin.set_mtime, dest.set_mtime);
  ASSERT_EQ(origin.delete_at, dest.delete_at);
}

DBOPObjectInfo createTestObject(const std::string & suffix, CephContext *context, const std::string & username="usertest") {
  DBOPObjectInfo object;
  object.uuid.generate_random();
  object.bucket_id = "test_bucket";
  object.name = "test" + suffix;
  object.size = rand();
  object.etag = "test_etag_" + suffix;
  object.mtime = ceph::real_clock::now();
  object.set_mtime = ceph::real_clock::now();
  object.delete_at = ceph::real_clock::now();
  return object;
}

bool uuidInVector(const uuid_d & uuid, const std::vector<uuid_d> & uuids)
{
  for (auto const & list_uuid : uuids) {
    if (list_uuid == uuid) return true;
  }
  return false;
}

TEST_F(TestSFSSQLiteObjects, CreateAndGet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto object = createTestObject("1", ceph_context.get());

  db_objects->store_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);
}


TEST_F(TestSFSSQLiteObjects, ListObjectsIDs) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  auto obj1 = createTestObject("1", ceph_context.get());
  db_objects->store_object(obj1);
  auto obj2 = createTestObject("2", ceph_context.get());
  db_objects->store_object(obj2);
  auto obj3 = createTestObject("3", ceph_context.get());
  db_objects->store_object(obj3);

  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto object_ids = db_objects->get_object_ids();
  EXPECT_EQ(object_ids.size(), 3);

  EXPECT_TRUE(uuidInVector(obj1.uuid, object_ids));
  EXPECT_TRUE(uuidInVector(obj2.uuid, object_ids));
  EXPECT_TRUE(uuidInVector(obj3.uuid, object_ids));
}

TEST_F(TestSFSSQLiteObjects, ListBucketsIDsPerBucket) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  createBucket("usertest", "test_bucket_1", conn);
  createBucket("usertest", "test_bucket_2", conn);
  createBucket("usertest", "test_bucket_3", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  auto test_object_1 = createTestObject("1", ceph_context.get());
  test_object_1.bucket_id = "test_bucket_1";
  db_objects->store_object(test_object_1);

  auto test_object_2 = createTestObject("2", ceph_context.get());
  test_object_2.bucket_id = "test_bucket_2";
  db_objects->store_object(test_object_2);

  auto test_object_3 = createTestObject("3", ceph_context.get());
  test_object_3.bucket_id = "test_bucket_3";
  db_objects->store_object(test_object_3);

  auto objects_ids = db_objects->get_object_ids("test_bucket_1");
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_1.uuid);

  objects_ids = db_objects->get_object_ids("test_bucket_2");
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_2.uuid);

  objects_ids = db_objects->get_object_ids("test_bucket_3");
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_3.uuid);
}


TEST_F(TestSFSSQLiteObjects, remove_object) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  auto obj1 = createTestObject("1", ceph_context.get());
  db_objects->store_object(obj1);
  auto obj2 = createTestObject("2", ceph_context.get());
  db_objects->store_object(obj2);
  auto obj3 = createTestObject("3", ceph_context.get());
  db_objects->store_object(obj3);

  db_objects->remove_object(obj2.uuid);
  auto object_ids = db_objects->get_object_ids();
  EXPECT_EQ(object_ids.size(), 2);

  EXPECT_TRUE(uuidInVector(obj1.uuid, object_ids));
  EXPECT_FALSE(uuidInVector(obj2.uuid, object_ids));
  EXPECT_TRUE(uuidInVector(obj3.uuid, object_ids));

  auto ret_object = db_objects->get_object(obj2.uuid);
  ASSERT_FALSE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteObjects, remove_objectThatDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  auto obj1 = createTestObject("1", ceph_context.get());
  db_objects->store_object(obj1);
  auto obj2 = createTestObject("2", ceph_context.get());
  db_objects->store_object(obj2);
  auto obj3 = createTestObject("3", ceph_context.get());
  db_objects->store_object(obj3);

  uuid_d non_existing_uuid;
  db_objects->remove_object(non_existing_uuid);
  auto object_ids = db_objects->get_object_ids();
  EXPECT_EQ(object_ids.size(), 3);

  EXPECT_TRUE(uuidInVector(obj1.uuid, object_ids));
  EXPECT_TRUE(uuidInVector(obj2.uuid, object_ids));
  EXPECT_TRUE(uuidInVector(obj3.uuid, object_ids));
}

TEST_F(TestSFSSQLiteObjects, CreateAndUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);
  auto object = createTestObject("1", ceph_context.get());
  db_objects->store_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);

  object.name = "ObjectRenamed";
  db_objects->store_object(object);
  ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  ASSERT_EQ(ret_object->name, "ObjectRenamed");
  compareObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteObjects, GetExisting) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);
  auto object = createTestObject("1", ceph_context.get());
  db_objects->store_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);

  // create a new instance, bucket should exist
  auto db_objects_2 = std::make_shared<SQLiteObjects>(conn);
  ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteObjects, CreateObjectForNonExistingBucket) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  SQLiteObjects db_objects(conn);
  auto storage = conn->get_storage();

  DBObject db_object;

  db_object.object_id = "254ddc1a-06a6-11ed-b939-0242ac120002";
  db_object.name = "test";

  EXPECT_THROW({
    try {
        storage.replace(db_object);;
    } catch( const std::system_error & e ) {
        EXPECT_STREQ( "FOREIGN KEY constraint failed: constraint failed", e.what() );
        throw;
    }
  }, std::system_error );
}

TEST_F(TestSFSSQLiteObjects, GetObjectByBucketAndObjectName) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create a few buckets
  createBucket("usertest", "test_bucket", conn);
  createBucket("usertest", "test_bucket_2", conn);
  createBucket("usertest", "test_bucket_3", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  // create objects with names "test1", "test2" and "test3"... in bucket "test_bucket"
  auto object_1 = createTestObject("1", ceph_context.get()); // name is "test1", bucket is "test_bucket"
  db_objects->store_object(object_1);
  auto object_2 = createTestObject("2", ceph_context.get()); // name is "test2", bucket is "test_bucket"
  db_objects->store_object(object_2);
  auto object_3 = createTestObject("3", ceph_context.get()); // name is "test3", bucket is "test_bucket"
  db_objects->store_object(object_3);

  // create object "test1" in bucket "test_bucket_2"
  auto object_1_2 = createTestObject("1", ceph_context.get()); // name is "test3", bucket is "test_bucket"
  object_1_2.bucket_id = "test_bucket_2";
  db_objects->store_object(object_1_2);

  // run a few queries
  auto ret_object = db_objects->get_object("test_bucket", "this_object_does_not_exist");
  ASSERT_FALSE(ret_object.has_value());

  ret_object = db_objects->get_object("test_bucket", "test1");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_1, *ret_object);

  ret_object = db_objects->get_object("test_bucket", "test2");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_2, *ret_object);

  ret_object = db_objects->get_object("test_bucket", "test3");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_3, *ret_object);

  ret_object = db_objects->get_object("test_bucket_2", "test1");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_1_2, *ret_object);

  ret_object = db_objects->get_object("this_bucket_does_not_exist", "test1");
  ASSERT_FALSE(ret_object.has_value());
}
