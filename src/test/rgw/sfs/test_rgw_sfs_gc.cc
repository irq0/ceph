// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_conversions.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"

#include "rgw/driver/sfs/uuid_path.h"
#include "rgw/driver/sfs/sfs_gc.h"

#include "rgw/rgw_sal_sfs.h"

#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <thread>

using namespace rgw::sal::sfs::sqlite;
using namespace std::this_thread;
using namespace std::chrono_literals;
using std::chrono::system_clock;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";
const static std::string TEST_USERNAME = "test_user";

class TestSFSGC : public ::testing::Test {
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

  std::size_t getStoreDataFileCount() {
    using std::filesystem::recursive_directory_iterator;
    return std::count_if(
        recursive_directory_iterator(getTestDir()),
        recursive_directory_iterator{}, [](const std::filesystem::path& path) {
          return (std::filesystem::is_regular_file(path) &&
                  !path.filename().string().starts_with("s3gw.db"));
        });
  }

  std::size_t databaseFileExists() {
    return std::filesystem::exists(getDBFullPath());
  }

  void createTestUser(DBConnRef conn) {
    SQLiteUsers users(conn);
    DBOPUserInfo user;
    user.uinfo.user_id.id = TEST_USERNAME;
    users.store_user(user);
  }

  void storeRandomObjectVersion(const std::shared_ptr<rgw::sal::sfs::Object> & object) {
    std::filesystem::path object_path =
        getTestDir() / object->get_storage_path();
    std::filesystem::create_directories(object_path.parent_path());
    auto mode = \
        std::ofstream::binary | \
        std::ofstream::out | \
        std::ofstream::app;
    std::ofstream ofs(object_path, mode);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(1, 4096);
    auto file_size = dist(gen);
    while(file_size) {
        ofs << dist(gen);
        --file_size;
    }
    ofs.flush();
    ofs.close();
  }

  void createTestBucket(const std::string & bucket_id,
                        DBConnRef conn) {
    SQLiteBuckets db_buckets(conn);
    DBOPBucketInfo bucket;
    bucket.binfo.bucket.name = bucket_id + "_name";
    bucket.binfo.bucket.bucket_id = bucket_id;
    bucket.binfo.owner.id = TEST_USERNAME;
    bucket.deleted = false;
    db_buckets.store_bucket(bucket);
  }

  bool bucketExists(const std::string & bucket_id,
                        DBConnRef conn) {
    SQLiteBuckets db_buckets(conn);
    auto bucket = db_buckets.get_bucket(bucket_id);
    return bucket.has_value();
  }

  std::shared_ptr<rgw::sal::sfs::Object> createTestObject(
                                                const std::string & bucket_id,
                                                const std::string & name,
                                                DBConnRef conn) {
    auto object = std::make_shared<rgw::sal::sfs::Object>(name);
    SQLiteObjects db_objects(conn);
    DBOPObjectInfo db_object;
    db_object.uuid = object->path.get_uuid();
    db_object.name = name;
    db_object.bucket_id = bucket_id;
    db_objects.store_object(db_object);
    return object;
  }

  void createTestObjectVersion(std::shared_ptr<rgw::sal::sfs::Object> & object,
                               uint version,
                               DBConnRef conn) {
    object->version_id = version;
    storeRandomObjectVersion(object);
    SQLiteVersionedObjects db_versioned_objects(conn);
    DBOPVersionedObjectInfo db_version;
    db_version.id = version;
    db_version.object_id = object->path.get_uuid();
    db_version.object_state = rgw::sal::ObjectState::COMMITTED;
    db_versioned_objects.insert_versioned_object(db_version);
  }

  void deleteTestObject(std::shared_ptr<rgw::sal::sfs::Object> & object,
                        DBConnRef conn) {
    // delete mark the object
    SQLiteVersionedObjects db_versioned_objects(conn);
    auto last_version = db_versioned_objects.get_last_versioned_object(
                                                       object->path.get_uuid());
    ASSERT_TRUE(last_version.has_value());
    last_version->object_state = rgw::sal::ObjectState::DELETED;
    db_versioned_objects.insert_versioned_object(*last_version);
  }

  void deleteTestBucket(const std::string & bucket_id, DBConnRef conn) {
    SQLiteBuckets db_buckets(conn);
    auto bucket = db_buckets.get_bucket(bucket_id);
    ASSERT_TRUE(bucket.has_value());

    SQLiteObjects db_objects(conn);
    auto objects = db_objects.get_objects(bucket_id);
    for (auto & object: objects) {
        auto objptr = std::make_shared<rgw::sal::sfs::Object>(object.name,
                                                              object.uuid,
                                                              false);
        deleteTestObject(objptr, conn);
    }
    bucket->deleted = true;
    db_buckets.store_bucket(*bucket);
  }

  size_t getNumberObjectsForBucket(const std::string & bucket_id, DBConnRef conn) {
      SQLiteObjects db_objs(conn);
      auto objects = db_objs.get_objects(bucket_id);
      return objects.size();
  }
};

TEST_F(TestSFSGC, TestDeletedBuckets) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_conf.set_val("rgw_gc_processor_period", "1");
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  auto gc = store->gc;
  gc->suspend();  // start suspended so we have control over processing

  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 2 buckets
  createTestBucket("test_bucket_1", store->db_conn);
  createTestBucket("test_bucket_2", store->db_conn);

  // create a few objects in bucket_1 with a few versions
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_2", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  // we should have 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // nothing should be removed
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());
  SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versions = db_versioned_objs.get_versioned_object_ids();
  EXPECT_EQ(versions.size(), 5);

  // delete bucket 2
  deleteTestBucket("test_bucket_2", store->db_conn);
  // nothing should be removed permanently yet
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());
  versions = db_versioned_objs.get_versioned_object_ids();
  // we should have 1 more version (delete marker for 1 object)
  EXPECT_EQ(versions.size(), 6);

  gc->process();

  // only objects for bucket 1 should be available
  EXPECT_EQ(getStoreDataFileCount(), 3);
  EXPECT_TRUE(databaseFileExists());
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_2", store->db_conn));
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_1", store->db_conn));
  EXPECT_TRUE(bucketExists("test_bucket_1", store->db_conn));

  // delete bucket 1 now
  deleteTestBucket("test_bucket_1", store->db_conn);
  gc->process();

  // only the db file should be present
  EXPECT_EQ(getStoreDataFileCount(), 0);
  EXPECT_TRUE(databaseFileExists());
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_2", store->db_conn));
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_1", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_1", store->db_conn));
}

TEST_F(TestSFSGC, TestDeletedBucketsMaxObjects) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_conf.set_val("rgw_gc_processor_period", "1");
  // gc will only remove 1 objects per iteration
  ceph_context->_conf.set_val("rgw_gc_max_objs", "1");
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  auto gc = store->gc;
  gc->suspend();

  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 2 buckets
  createTestBucket("test_bucket_1", store->db_conn);
  createTestBucket("test_bucket_2", store->db_conn);

  // create a few objects in bucket_1 with a few versions
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_2", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  // we should have 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // delete bucket 2
  deleteTestBucket("test_bucket_2", store->db_conn);

  gc->process();

  // only 1 file should be removed
  EXPECT_EQ(getStoreDataFileCount(), 4);
  EXPECT_TRUE(databaseFileExists());
  // the object is still reachable in the db
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_1", store->db_conn));

  gc->process();

  // one more version removed
  EXPECT_EQ(getStoreDataFileCount(), 3);
  EXPECT_TRUE(databaseFileExists());
  // the object is still reachable in the db (versions removed)
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_1", store->db_conn));

  gc->process();
  // one more version removed
  EXPECT_EQ(getStoreDataFileCount(), 3);
  EXPECT_TRUE(databaseFileExists());
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_1", store->db_conn));

  gc->process();
  // one more version removed
  EXPECT_EQ(getStoreDataFileCount(), 3);
  EXPECT_TRUE(databaseFileExists());
  // the object is finally removed
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_1", store->db_conn));
}
