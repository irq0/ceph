// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

const static std::string TEST_USERNAME = "test_username";
const static std::string TEST_BUCKET = "test_bucket";
const static std::string TEST_BUCKET_2 = "test_bucket_2";
const static std::string TEST_OBJECT_ID =
    "80943a6d-9f72-4001-bac0-a9a036be8c49";
const static std::string TEST_OBJECT_ID_1 =
    "9f06d9d3-307f-4c98-865b-cd3b087acc4f";
const static std::string TEST_OBJECT_ID_2 =
    "af06d9d3-307f-4c98-865b-cd3b087acc4f";
const static std::string TEST_OBJECT_ID_3 =
    "bf06d9d3-307f-4c98-865b-cd3b087acc4f";
const static std::string TEST_OBJECT_ID_4 =
    "cf06d9d3-307f-4c98-865b-cd3b087acc4f";

class TestSFSSQLiteVersionedObjects : public ::testing::Test {
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

  void createBucket(
      const std::string& username, const std::string& bucketname, DBConnRef conn
  ) {
    createUser(username, conn);
    SQLiteBuckets buckets(conn);
    DBOPBucketInfo bucket;
    bucket.binfo.bucket.name = bucketname;
    bucket.binfo.bucket.bucket_id = bucketname;
    bucket.binfo.owner.id = username;
    buckets.store_bucket(bucket);
  }

  void createObject(
      const std::string& username, const std::string& bucketname,
      const std::string object_id, CephContext* context, DBConnRef conn
  ) {
    createBucket(username, bucketname, conn);
    SQLiteObjects objects(conn);

    DBObject object;
    object.uuid.parse(object_id.c_str());
    object.bucket_id = bucketname;
    object.name = "test_name";
    objects.store_object(object);
  }
};

DBVersionedObject createTestVersionedObject(
    uint id, const std::string& object_id, const std::string& suffix
) {
  DBVersionedObject test_versioned_object;
  test_versioned_object.id = id;
  uuid_d uuid;
  uuid.parse(object_id.c_str());
  test_versioned_object.object_id = uuid;
  test_versioned_object.checksum = "test_checksum_" + suffix;
  // test_versioned_object.size = rand();
  test_versioned_object.size = 1999;
  test_versioned_object.create_time = ceph::real_clock::now();
  test_versioned_object.delete_time = ceph::real_clock::now();
  test_versioned_object.commit_time = ceph::real_clock::now();
  test_versioned_object.mtime = ceph::real_clock::now();
  test_versioned_object.object_state = rgw::sal::sfs::ObjectState::OPEN;
  test_versioned_object.version_id = "test_version_id_" + suffix;
  test_versioned_object.etag = "test_etag_" + suffix;
  test_versioned_object.version_type =
      rgw::sal::sfs::VersionType::DELETE_MARKER;

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
    test_versioned_object.attrs = attrs;
  }

  return test_versioned_object;
}

void compareVersionedObjectsAttrs(
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

void compareVersionedObjects(
    const DBVersionedObject& origin, const DBVersionedObject& dest
) {
  ASSERT_EQ(origin.id, dest.id);
  ASSERT_EQ(origin.object_id, dest.object_id);
  ASSERT_EQ(origin.checksum, dest.checksum);
  ASSERT_EQ(origin.size, dest.size);
  ASSERT_EQ(origin.create_time, dest.create_time);
  ASSERT_EQ(origin.delete_time, dest.delete_time);
  ASSERT_EQ(origin.commit_time, dest.commit_time);
  ASSERT_EQ(origin.object_state, dest.object_state);
  ASSERT_EQ(origin.version_id, dest.version_id);
  ASSERT_EQ(origin.etag, dest.etag);
  compareVersionedObjectsAttrs(origin.attrs, dest.attrs);
  ASSERT_EQ(origin.version_type, dest.version_type);
}

TEST_F(TestSFSSQLiteVersionedObjects, CreateAndGet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto versioned_object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");

  db_versioned_objects->insert_versioned_object(versioned_object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_ver_object =
      db_versioned_objects->get_versioned_object(versioned_object.id);
  ASSERT_TRUE(ret_ver_object.has_value());
  compareVersionedObjects(versioned_object, *ret_ver_object);

  // get by version id
  ret_ver_object =
      db_versioned_objects->get_versioned_object("test_version_id_1");
  ASSERT_TRUE(ret_ver_object.has_value());
  compareVersionedObjects(versioned_object, *ret_ver_object);
}

TEST_F(TestSFSSQLiteVersionedObjects, ListObjectsIDs) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  auto obj1 = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->insert_versioned_object(obj1);
  auto obj2 = createTestVersionedObject(2, TEST_OBJECT_ID, "2");
  db_versioned_objects->insert_versioned_object(obj2);
  auto obj3 = createTestVersionedObject(3, TEST_OBJECT_ID, "3");
  db_versioned_objects->insert_versioned_object(obj3);

  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto object_ids = db_versioned_objects->get_versioned_object_ids();
  EXPECT_EQ(object_ids.size(), 3);

  EXPECT_EQ(1, object_ids[0]);
  EXPECT_EQ(2, object_ids[1]);
  EXPECT_EQ(3, object_ids[2]);

  uuid_d uuid;
  uuid.parse(TEST_OBJECT_ID.c_str());
  auto objects = db_versioned_objects->get_versioned_objects(uuid);
  ASSERT_EQ(objects.size(), 3);
  compareVersionedObjects(objects[0], obj3);
  compareVersionedObjects(objects[1], obj2);
  compareVersionedObjects(objects[2], obj1);
}

TEST_F(TestSFSSQLiteVersionedObjects, ListBucketsIDsPerObject) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_1, ceph_context.get(), conn
  );
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_2, ceph_context.get(), conn
  );
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_3, ceph_context.get(), conn
  );

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  auto test_object_1 = createTestVersionedObject(1, TEST_OBJECT_ID_1, "1");
  db_versioned_objects->insert_versioned_object(test_object_1);

  auto test_object_2 = createTestVersionedObject(2, TEST_OBJECT_ID_2, "2");
  db_versioned_objects->insert_versioned_object(test_object_2);

  auto test_object_3 = createTestVersionedObject(3, TEST_OBJECT_ID_3, "3");
  db_versioned_objects->insert_versioned_object(test_object_3);

  uuid_d uuid1;
  uuid1.parse(TEST_OBJECT_ID_1.c_str());
  auto objects_ids = db_versioned_objects->get_versioned_object_ids(uuid1);
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_1.id);

  uuid_d uuid2;
  uuid2.parse(TEST_OBJECT_ID_2.c_str());
  objects_ids = db_versioned_objects->get_versioned_object_ids(uuid2);
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_2.id);

  uuid_d uuid3;
  uuid3.parse(TEST_OBJECT_ID_3.c_str());
  objects_ids = db_versioned_objects->get_versioned_object_ids(uuid3);
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_3.id);
}

TEST_F(TestSFSSQLiteVersionedObjects, RemoveObject) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  auto obj1 = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->insert_versioned_object(obj1);
  auto obj2 = createTestVersionedObject(2, TEST_OBJECT_ID, "2");
  db_versioned_objects->insert_versioned_object(obj2);
  auto obj3 = createTestVersionedObject(3, TEST_OBJECT_ID, "3");
  db_versioned_objects->insert_versioned_object(obj3);

  db_versioned_objects->remove_versioned_object(obj2.id);
  auto object_ids = db_versioned_objects->get_versioned_object_ids();
  EXPECT_EQ(object_ids.size(), 2);

  EXPECT_EQ(1, object_ids[0]);
  EXPECT_EQ(3, object_ids[1]);

  auto ret_object = db_versioned_objects->get_versioned_object(obj2.id);
  ASSERT_FALSE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, RemoveObjectThatDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  auto obj1 = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->insert_versioned_object(obj1);
  auto obj2 = createTestVersionedObject(2, TEST_OBJECT_ID, "2");
  db_versioned_objects->insert_versioned_object(obj2);
  auto obj3 = createTestVersionedObject(3, TEST_OBJECT_ID, "3");
  db_versioned_objects->insert_versioned_object(obj3);

  db_versioned_objects->remove_versioned_object(10);
  auto object_ids = db_versioned_objects->get_versioned_object_ids();
  EXPECT_EQ(object_ids.size(), 3);

  EXPECT_EQ(1, object_ids[0]);
  EXPECT_EQ(2, object_ids[1]);
  EXPECT_EQ(2, object_ids[1]);

  auto ret_object = db_versioned_objects->get_versioned_object(obj2.id);
  ASSERT_TRUE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, CreateAndUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto versioned_object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");

  db_versioned_objects->insert_versioned_object(versioned_object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_ver_object =
      db_versioned_objects->get_versioned_object(versioned_object.id);
  ASSERT_TRUE(ret_ver_object.has_value());
  compareVersionedObjects(versioned_object, *ret_ver_object);

  // update the size, add new version
  auto original_size = versioned_object.size;
  auto new_versioned = versioned_object;
  new_versioned.size = 1999;
  new_versioned.id = 2;
  new_versioned.version_id = "2";
  db_versioned_objects->insert_versioned_object(new_versioned);

  // get the first version
  ret_ver_object =
      db_versioned_objects->get_versioned_object(versioned_object.id);
  ASSERT_TRUE(ret_ver_object.has_value());
  ASSERT_EQ(original_size, ret_ver_object->size);
  versioned_object.size = original_size;
  compareVersionedObjects(versioned_object, *ret_ver_object);

  // check that there are 2 versions now
  uuid_d uuid_object;
  uuid_object.parse(TEST_OBJECT_ID.c_str());
  auto ids = db_versioned_objects->get_versioned_object_ids(uuid_object);
  ASSERT_EQ(2, ids.size());
  EXPECT_EQ(1, ids[0]);
  EXPECT_EQ(2, ids[1]);

  // get the new version
  ret_ver_object = db_versioned_objects->get_versioned_object(2);
  ASSERT_TRUE(ret_ver_object.has_value());
  ASSERT_EQ(1999, ret_ver_object->size);
  compareVersionedObjects(new_versioned, *ret_ver_object);
}

TEST_F(TestSFSSQLiteVersionedObjects, GetExisting) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->insert_versioned_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_versioned_objects->get_versioned_object(object.id);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);

  // create a new instance, bucket should exist
  auto db_objects_2 = std::make_shared<SQLiteVersionedObjects>(conn);
  ret_object = db_objects_2->get_versioned_object(object.id);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteVersionedObjects, CreateObjectForNonExistingBucket) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  SQLiteVersionedObjects db_objects(conn);
  auto storage = conn->get_storage();

  DBVersionedObject db_object;

  db_object.id = 1;
  uuid_d uuid_val;
  uuid_val.parse("254ddc1a-06a6-11ed-b939-0242ac120002");
  db_object.object_id = uuid_val;
  db_object.checksum = "test";
  db_object.size = rand();

  EXPECT_THROW(
      {
        try {
          storage.replace(db_object);
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

TEST_F(TestSFSSQLiteVersionedObjects, StoreCreatesNewVersions) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->insert_versioned_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_versioned_objects->get_versioned_object(object.id);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);

  // just update the size
  auto original_size = object.size;
  object.size = 1;
  object.version_id = "test_version_id_2";
  db_versioned_objects->insert_versioned_object(object);

  // change nothing, but it should also create a new version
  object.version_id = "test_version_id_3";
  db_versioned_objects->insert_versioned_object(object);
  auto ids = db_versioned_objects->get_versioned_object_ids();
  ASSERT_EQ(3, ids.size());
  EXPECT_EQ(1, ids[0]);
  EXPECT_EQ(2, ids[1]);
  EXPECT_EQ(3, ids[2]);

  ret_object = db_versioned_objects->get_versioned_object(1);
  ASSERT_TRUE(ret_object.has_value());
  object.size = original_size;
  object.id = 1;
  object.version_id = "test_version_id_1";
  compareVersionedObjects(object, *ret_object);

  ret_object = db_versioned_objects->get_versioned_object(2);
  ASSERT_TRUE(ret_object.has_value());
  object.size = 1;
  object.id = 2;
  object.version_id = "test_version_id_2";
  compareVersionedObjects(object, *ret_object);

  ret_object = db_versioned_objects->get_versioned_object(3);
  ASSERT_TRUE(ret_object.has_value());
  object.size = 1;
  object.id = 3;
  object.version_id = "test_version_id_3";
  compareVersionedObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteVersionedObjects, GetLastVersion) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->insert_versioned_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  uuid_d uuid;
  uuid.parse(TEST_OBJECT_ID.c_str());
  auto ret_object = db_versioned_objects->get_last_versioned_object(uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);

  // just update the size, and add a new version
  object.size = 1999;
  object.version_id = "test_version_id_2";
  object.commit_time = ceph::real_clock::now();
  db_versioned_objects->insert_versioned_object(object);

  // now it should return the last one
  ret_object = db_versioned_objects->get_last_versioned_object(uuid);
  ASSERT_TRUE(ret_object.has_value());
  object.id = 2;
  compareVersionedObjects(object, *ret_object);

  uuid_d uuid_that_does_not_exist;
  uuid_that_does_not_exist.parse(TEST_OBJECT_ID_2.c_str());

  ret_object =
      db_versioned_objects->get_last_versioned_object(uuid_that_does_not_exist);
  ASSERT_FALSE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, GetLastVersionRepeatedCommitTime) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  // keep it for later
  auto commit_time = object.commit_time;
  db_versioned_objects->insert_versioned_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  uuid_d uuid;
  uuid.parse(TEST_OBJECT_ID.c_str());
  auto ret_object = db_versioned_objects->get_last_versioned_object(uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);

  // just update the size, and add a new version
  object.size = 1999;
  object.version_id = "test_version_id_2";
  // set the same commit time
  object.commit_time = commit_time;
  db_versioned_objects->insert_versioned_object(object);

  // now we have 2 entries with the same commit time.
  // it should return the one with the highest id
  ret_object = db_versioned_objects->get_last_versioned_object(uuid);
  ASSERT_TRUE(ret_object.has_value());
  object.id = 2;
  compareVersionedObjects(object, *ret_object);

  // just update the size, and add a new version
  object.size = 3121;
  object.version_id = "test_version_id_3";
  // set the same commit time
  object.commit_time = commit_time;
  db_versioned_objects->insert_versioned_object(object);

  // now we have 3 entries with the same commit time.
  // it should return the one with the highest id (3)
  ret_object = db_versioned_objects->get_last_versioned_object(uuid);
  ASSERT_TRUE(ret_object.has_value());
  object.id = 3;
  compareVersionedObjects(object, *ret_object);

  uuid_d uuid_that_does_not_exist;
  uuid_that_does_not_exist.parse(TEST_OBJECT_ID_2.c_str());

  ret_object =
      db_versioned_objects->get_last_versioned_object(uuid_that_does_not_exist);
  ASSERT_FALSE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, TestInsertIncreaseID) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_2";
  EXPECT_EQ(2, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_3";
  EXPECT_EQ(3, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_4";
  EXPECT_EQ(4, db_versioned_objects->insert_versioned_object(object));

  auto ret_ver_object = db_versioned_objects->get_versioned_object(1);
  ASSERT_TRUE(ret_ver_object.has_value());
  object.version_id = "test_version_id_1";
  compareVersionedObjects(object, *ret_ver_object);

  ret_ver_object = db_versioned_objects->get_versioned_object(2);
  ASSERT_TRUE(ret_ver_object.has_value());
  object.id = 2;
  object.version_id = "test_version_id_2";
  compareVersionedObjects(object, *ret_ver_object);

  ret_ver_object = db_versioned_objects->get_versioned_object(3);
  ASSERT_TRUE(ret_ver_object.has_value());
  object.id = 3;
  object.version_id = "test_version_id_3";
  compareVersionedObjects(object, *ret_ver_object);

  ret_ver_object = db_versioned_objects->get_versioned_object(4);
  ASSERT_TRUE(ret_ver_object.has_value());
  object.id = 4;
  object.version_id = "test_version_id_4";
  compareVersionedObjects(object, *ret_ver_object);

  ret_ver_object = db_versioned_objects->get_versioned_object(5);
  ASSERT_FALSE(ret_ver_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, TestUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(object));

  auto ret_ver_object = db_versioned_objects->get_versioned_object(1);
  ASSERT_TRUE(ret_ver_object.has_value());
  compareVersionedObjects(object, *ret_ver_object);

  object.object_state = rgw::sal::sfs::ObjectState::OPEN;
  db_versioned_objects->store_versioned_object(object);

  ret_ver_object = db_versioned_objects->get_versioned_object(1);
  ASSERT_TRUE(ret_ver_object.has_value());
  compareVersionedObjects(object, *ret_ver_object);

  ret_ver_object = db_versioned_objects->get_versioned_object(2);
  ASSERT_FALSE(ret_ver_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, StoreUnsupportedTimestamp) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  SQLiteVersionedObjects db_versions(conn);
  auto storage = conn->get_storage();

  DBVersionedObject db_version;

  db_version.id = 1;
  uuid_d uuid_val;
  uuid_val.parse(TEST_OBJECT_ID.c_str());
  db_version.object_id = uuid_val;
  db_version.checksum = "test";
  db_version.size = rand();

  // our max supported value is int64::max
  uint64_t nanos_int64_plus_one =
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
  db_version.create_time =
      ceph::real_time(std::chrono::nanoseconds(nanos_int64_plus_one));

  EXPECT_THROW(
      {
        try {
          storage.replace(db_version);
          ;
        } catch (const std::system_error& e) {
          EXPECT_STREQ(
              "Error converting ceph::real_time to int64. Nanoseconds "
              "value: "
              "9223372036854775808 is out of range: Numerical result out "
              "of "
              "range",
              e.what()
          );
          throw;
        }
      },
      std::system_error
  );
}

TEST_F(TestSFSSQLiteVersionedObjects, TestFilterDeleted) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  // create 5 versions
  // versions 2 and 3 are deleted
  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_2";
  object.object_state = rgw::sal::sfs::ObjectState::DELETED;
  EXPECT_EQ(2, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_3";
  EXPECT_EQ(3, db_versioned_objects->insert_versioned_object(object));
  object.object_state = rgw::sal::sfs::ObjectState::OPEN;
  object.version_id = "test_version_id_4";
  EXPECT_EQ(4, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_5";
  EXPECT_EQ(5, db_versioned_objects->insert_versioned_object(object));

  // get_versioned_object(uint id)
  // try to get version 1 (not deleted)
  auto not_deleted = db_versioned_objects->get_versioned_object(1);
  ASSERT_TRUE(not_deleted.has_value());
  ASSERT_NE(rgw::sal::sfs::ObjectState::DELETED, not_deleted->object_state);

  // now version 2 (deleted)
  auto deleted = db_versioned_objects->get_versioned_object(2);
  ASSERT_FALSE(deleted.has_value());
  // now version 2, not filtering deleted
  deleted = db_versioned_objects->get_versioned_object(2, false);
  ASSERT_TRUE(deleted.has_value());
  ASSERT_EQ(rgw::sal::sfs::ObjectState::DELETED, deleted->object_state);

  // get_versioned_object(const std::string & version_id)
  // try to get version 1 (not deleted)
  not_deleted = db_versioned_objects->get_versioned_object("test_version_id_1");
  ASSERT_TRUE(not_deleted.has_value());
  ASSERT_NE(rgw::sal::sfs::ObjectState::DELETED, not_deleted->object_state);

  // now version 2 (deleted)
  deleted = db_versioned_objects->get_versioned_object("test_version_id_2");
  ASSERT_FALSE(deleted.has_value());
  // now version 2, not filtering deleted
  deleted =
      db_versioned_objects->get_versioned_object("test_version_id_2", false);
  ASSERT_TRUE(deleted.has_value());
  ASSERT_EQ(rgw::sal::sfs::ObjectState::DELETED, deleted->object_state);

  // get_versioned_object_ids
  auto ids = db_versioned_objects->get_versioned_object_ids();
  ASSERT_EQ(3, ids.size());  // 2 and 3 will not be returned
  for (const auto& id : ids) {
    ASSERT_NE(2, id);
    ASSERT_NE(3, id);
  }

  ids = db_versioned_objects->get_versioned_object_ids(false);
  ASSERT_EQ(5, ids.size());  // 2 and 3 will be returned

  // get_versioned_object_ids(const uuid_d & object_id)
  uuid_d object_id;
  object_id.parse(TEST_OBJECT_ID.c_str());
  ids = db_versioned_objects->get_versioned_object_ids(object_id);
  ASSERT_EQ(3, ids.size());  // 2 and 3 will not be returned
  for (const auto& id : ids) {
    ASSERT_NE(2, id);
    ASSERT_NE(3, id);
  }

  ids = db_versioned_objects->get_versioned_object_ids(object_id, false);
  ASSERT_EQ(5, ids.size());  // 2 and 3 will be returned

  // get_versioned_objects(const uuid_d & object_id)
  auto versions = db_versioned_objects->get_versioned_objects(object_id);
  ASSERT_EQ(3, versions.size());  // 2 and 3 will not be returned
  for (const auto& version : versions) {
    ASSERT_NE(2, version.id);
    ASSERT_NE(3, version.id);
    ASSERT_NE(rgw::sal::sfs::ObjectState::DELETED, version.object_state);
  }
  versions = db_versioned_objects->get_versioned_objects(object_id, false);
  ASSERT_EQ(5, versions.size());  // 2 and 3 will be returned

  // get_last_versioned_object
  // this time last version (5) is not deleted
  // get it first, then flag as deleted and check
  auto last_version =
      db_versioned_objects->get_last_versioned_object(object_id);
  ASSERT_TRUE(last_version.has_value());
  ASSERT_EQ(5, last_version->id);

  // now flag the last version as DELETED
  last_version->object_state = rgw::sal::sfs::ObjectState::DELETED;
  db_versioned_objects->store_versioned_object(*last_version);

  // we update, so no new version should be created
  versions = db_versioned_objects->get_versioned_objects(object_id, false);
  ASSERT_EQ(5, versions.size());  // will still return 5 versions

  versions = db_versioned_objects->get_versioned_objects(object_id);
  ASSERT_EQ(2, versions.size());  // now only 2 are not deleted

  // now last version should be 4
  last_version = db_versioned_objects->get_last_versioned_object(object_id);
  ASSERT_TRUE(last_version.has_value());
  ASSERT_EQ(4, last_version->id);

  // if we don't filter deleted it's still 5
  last_version =
      db_versioned_objects->get_last_versioned_object(object_id, false);
  ASSERT_TRUE(last_version.has_value());
  ASSERT_EQ(5, last_version->id);
}

TEST_F(TestSFSSQLiteVersionedObjects, TestDeleteLastAndGetPrevious) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  // create 3 versions (last one is a delete marker)
  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  object.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_2";
  EXPECT_EQ(2, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_3";
  object.version_type = rgw::sal::sfs::VersionType::DELETE_MARKER;
  EXPECT_EQ(3, db_versioned_objects->insert_versioned_object(object));

  auto last_version_now =
      db_versioned_objects->delete_version_and_get_previous_transact(3);
  ASSERT_TRUE(last_version_now.has_value());
  ASSERT_EQ(2, last_version_now->id);
  ASSERT_EQ("test_version_id_2", last_version_now->version_id);

  uuid_d object_id;
  object_id.parse(TEST_OBJECT_ID.c_str());
  last_version_now = db_versioned_objects->get_last_versioned_object(object_id);
  ASSERT_TRUE(last_version_now.has_value());
  ASSERT_EQ(2, last_version_now->id);
  ASSERT_EQ("test_version_id_2", last_version_now->version_id);
}

TEST_F(TestSFSSQLiteVersionedObjects, TestGetByBucketAndObjectName) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  // SCENARIO 1. ONLY 1 object with committed versions
  // Create the object, we need it because of foreign key constrains
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  // insert 3 committed versions
  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  object.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_2";
  object.commit_time = ceph::real_clock::now();
  EXPECT_EQ(2, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_3";
  object.commit_time = ceph::real_clock::now();
  EXPECT_EQ(3, db_versioned_objects->insert_versioned_object(object));

  // try to get version (TEST_BUCKET, "test_name", "test_version_id_2")
  // corresponding to the second version
  auto version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", "test_version_id_2"
  );
  ASSERT_TRUE(version.has_value());
  EXPECT_EQ("test_version_id_2", version->version_id);
  EXPECT_EQ(2, version->id);

  // don't pass any version. Should return the last one
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", ""
  );
  ASSERT_TRUE(version.has_value());
  EXPECT_EQ("test_version_id_3", version->version_id);
  EXPECT_EQ(3, version->id);

  // pass a non existing version_id
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", "this_version_does_not_exist"
  );
  ASSERT_FALSE(version.has_value());

  // SCENARIO 2. There is one object with all versions deleted (waiting to be
  // removed by the garbage collector) and the alive object, both with the same
  // object name but different uuid
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_2, ceph_context.get(), conn
  );
  object = createTestVersionedObject(4, TEST_OBJECT_ID_2, "4");
  object.object_state = rgw::sal::sfs::ObjectState::DELETED;
  EXPECT_EQ(4, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_5";
  object.commit_time = ceph::real_clock::now();
  EXPECT_EQ(5, db_versioned_objects->insert_versioned_object(object));

  // even though commit times for this versions are later in time than for the
  // first object it should still return versions from the first object

  // don't pass any version. Should return the last one
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", ""
  );
  ASSERT_TRUE(version.has_value());
  EXPECT_EQ("test_version_id_3", version->version_id);
  EXPECT_EQ(3, version->id);

  // try to get a deleted version (TEST_BUCKET, "test_name", "test_version_id_5")
  // corresponding to the second version
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", "test_version_id_5"
  );
  // should not return that object
  // (it is deleted waiting for the garbage collector)
  ASSERT_FALSE(version.has_value());

  // still return valid version
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", "test_version_id_3"
  );
  ASSERT_TRUE(version.has_value());
  EXPECT_EQ("test_version_id_3", version->version_id);
  EXPECT_EQ(3, version->id);

  // SCENARIO 3. 2 Objects with the same name in different buckets.
  // in this case the object in bucket TEST_BUCKET_2 is in committed state
  createObject(
      TEST_USERNAME, TEST_BUCKET_2, TEST_OBJECT_ID_3, ceph_context.get(), conn
  );
  object = createTestVersionedObject(6, TEST_OBJECT_ID_3, "6");
  object.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  EXPECT_EQ(6, db_versioned_objects->insert_versioned_object(object));

  // still return valid version for 1st object
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", "test_version_id_3"
  );
  ASSERT_TRUE(version.has_value());
  EXPECT_EQ("test_version_id_3", version->version_id);
  EXPECT_EQ(3, version->id);

  // and also valid for the object in the second bucket
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET_2, "test_name", "test_version_id_6"
  );
  ASSERT_TRUE(version.has_value());
  EXPECT_EQ("test_version_id_6", version->version_id);
  EXPECT_EQ(6, version->id);

  // but version 6 is not on first bucket
  version = db_versioned_objects->get_committed_versioned_object(
      TEST_BUCKET, "test_name", "test_version_id_6"
  );
  ASSERT_FALSE(version.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, TestUpdateAndDeleteRest) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  // insert 3 open versions
  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  object.object_state = rgw::sal::sfs::ObjectState::OPEN;
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_2";
  object.commit_time = ceph::real_clock::now();
  EXPECT_EQ(2, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_3";
  object.commit_time = ceph::real_clock::now();
  EXPECT_EQ(3, db_versioned_objects->insert_versioned_object(object));

  // create a different object
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_2, ceph_context.get(), conn
  );
  // with also 3 open versions
  object = createTestVersionedObject(4, TEST_OBJECT_ID_2, "4");
  object.object_state = rgw::sal::sfs::ObjectState::OPEN;
  EXPECT_EQ(4, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_5";
  object.commit_time = ceph::real_clock::now();
  EXPECT_EQ(5, db_versioned_objects->insert_versioned_object(object));
  object.version_id = "test_version_id_6";
  object.commit_time = ceph::real_clock::now();
  EXPECT_EQ(6, db_versioned_objects->insert_versioned_object(object));

  // update object 2 to COMMITTED and DELETE the rest in a transaction
  auto object_2 = db_versioned_objects->get_versioned_object(2, false);
  ASSERT_TRUE(object_2.has_value());
  object_2->object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  db_versioned_objects->store_versioned_object_delete_rest_transact(*object_2);

  // all the rest should be updated (but only for that object)
  auto object_ret = db_versioned_objects->get_versioned_object(1, false);
  ASSERT_TRUE(object_ret.has_value());
  EXPECT_EQ(rgw::sal::sfs::ObjectState::DELETED, object_ret->object_state);
  object_ret = db_versioned_objects->get_versioned_object(3, false);
  ASSERT_TRUE(object_ret.has_value());
  EXPECT_EQ(rgw::sal::sfs::ObjectState::DELETED, object_ret->object_state);

  // the other object versions should be still open
  uuid_d uuid_second_object;
  uuid_second_object.parse(TEST_OBJECT_ID_2.c_str());
  // get the objects but not filtering deleted ones (we get all)
  auto versions =
      db_versioned_objects->get_versioned_objects(uuid_second_object, false);
  EXPECT_EQ(3, versions.size());
  for (const auto& ver : versions) {
    EXPECT_EQ(rgw::sal::sfs::ObjectState::OPEN, ver.object_state);
  }
}

TEST_F(TestSFSSQLiteVersionedObjects, TestUpdateDeleteVersionDeletesObject) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  // insert 3 committed versions
  auto version = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  version.object_state = rgw::sal::sfs::ObjectState::DELETED;
  version.version_type = rgw::sal::sfs::VersionType::REGULAR;
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_2";
  version.commit_time = ceph::real_clock::now();
  EXPECT_EQ(2, db_versioned_objects->insert_versioned_object(version));
  version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  version.version_id = "test_version_id_3";
  version.commit_time = ceph::real_clock::now();
  EXPECT_EQ(3, db_versioned_objects->insert_versioned_object(version));

  // insert 3 committed versions
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_2, ceph_context.get(), conn
  );
  version = createTestVersionedObject(4, TEST_OBJECT_ID_2, "4");
  version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  version.version_type = rgw::sal::sfs::VersionType::REGULAR;
  EXPECT_EQ(4, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_5";
  EXPECT_EQ(5, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_6";
  EXPECT_EQ(6, db_versioned_objects->insert_versioned_object(version));

  // insert 3 committed versions for another object
  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_3, ceph_context.get(), conn
  );
  version = createTestVersionedObject(7, TEST_OBJECT_ID_3, "7");
  version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  version.version_type = rgw::sal::sfs::VersionType::REGULAR;
  EXPECT_EQ(7, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_8";
  EXPECT_EQ(8, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_9";
  EXPECT_EQ(9, db_versioned_objects->insert_versioned_object(version));

  // insert 3 committed versions for another object in another bucket
  createObject(
      TEST_USERNAME, TEST_BUCKET_2, TEST_OBJECT_ID_4, ceph_context.get(), conn
  );
  version = createTestVersionedObject(10, TEST_OBJECT_ID_4, "10");
  version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  version.version_type = rgw::sal::sfs::VersionType::REGULAR;
  EXPECT_EQ(10, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_11";
  EXPECT_EQ(11, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_12";
  EXPECT_EQ(12, db_versioned_objects->insert_versioned_object(version));

  // we have 3 objects with 3 versions in TEST_BUCKET
  // one of the objects has 2 version deleted. The rest have all versions alive.

  // we also have object with 3 version in TEST_BUCKET_2
  auto object_list =
      db_versioned_objects->list_last_versioned_objects(TEST_BUCKET);
  ASSERT_EQ(3, object_list.size());
  // first item
  uuid_d uuid_object;
  uuid_object.parse(TEST_OBJECT_ID.c_str());
  EXPECT_EQ(uuid_object, rgw::sal::sfs::sqlite::get_uuid(object_list[0]));
  // versions 1 and 2 for TEST_OBJECT_ID are deleted
  EXPECT_EQ(
      "test_version_id_3", rgw::sal::sfs::sqlite::get_version_id(object_list[0])
  );
  EXPECT_EQ(3, rgw::sal::sfs::sqlite::get_id(object_list[0]));

  // second item
  uuid_object.parse(TEST_OBJECT_ID_2.c_str());
  EXPECT_EQ(uuid_object, rgw::sal::sfs::sqlite::get_uuid(object_list[1]));
  EXPECT_EQ(
      "test_version_id_6", rgw::sal::sfs::sqlite::get_version_id(object_list[1])
  );
  EXPECT_EQ(6, rgw::sal::sfs::sqlite::get_id(object_list[1]));

  // third item
  uuid_object.parse(TEST_OBJECT_ID_3.c_str());
  EXPECT_EQ(uuid_object, rgw::sal::sfs::sqlite::get_uuid(object_list[2]));
  EXPECT_EQ(
      "test_version_id_9", rgw::sal::sfs::sqlite::get_version_id(object_list[2])
  );
  EXPECT_EQ(9, rgw::sal::sfs::sqlite::get_id(object_list[2]));

  // now delete the 3rd version of TEST_OBJECT_ID
  auto version_to_delete = db_versioned_objects->get_versioned_object(3);
  version_to_delete->object_state = rgw::sal::sfs::ObjectState::DELETED;
  db_versioned_objects->store_versioned_object(*version_to_delete);

  // list again
  object_list = db_versioned_objects->list_last_versioned_objects(TEST_BUCKET);
  // the object with all version deleted should not be listed
  ASSERT_EQ(2, object_list.size());

  // second item
  uuid_object.parse(TEST_OBJECT_ID_2.c_str());
  EXPECT_EQ(uuid_object, rgw::sal::sfs::sqlite::get_uuid(object_list[0]));
  EXPECT_EQ(
      "test_version_id_6", rgw::sal::sfs::sqlite::get_version_id(object_list[0])
  );
  EXPECT_EQ(6, rgw::sal::sfs::sqlite::get_id(object_list[0]));

  // third item
  uuid_object.parse(TEST_OBJECT_ID_3.c_str());
  EXPECT_EQ(uuid_object, rgw::sal::sfs::sqlite::get_uuid(object_list[1]));
  EXPECT_EQ(
      "test_version_id_9", rgw::sal::sfs::sqlite::get_version_id(object_list[1])
  );
  EXPECT_EQ(9, rgw::sal::sfs::sqlite::get_id(object_list[1]));
}

TEST_F(TestSFSSQLiteVersionedObjects, TestAddDeleteMarker) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(conn);

  createObject(
      TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get(), conn
  );

  // insert 3 committed versions
  auto version = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  version.version_type = rgw::sal::sfs::VersionType::REGULAR;
  EXPECT_EQ(1, db_versioned_objects->insert_versioned_object(version));
  version.version_id = "test_version_id_2";
  version.commit_time = ceph::real_clock::now();
  EXPECT_EQ(2, db_versioned_objects->insert_versioned_object(version));
  version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  version.version_id = "test_version_id_3";
  version.commit_time = ceph::real_clock::now();
  EXPECT_EQ(3, db_versioned_objects->insert_versioned_object(version));

  // add a delete marker
  auto delete_marker_id = "delete_marker_id";
  uuid_d uuid;
  uuid.parse(TEST_OBJECT_ID.c_str());
  bool added;
  auto id = db_versioned_objects->add_delete_marker_transact(
      uuid, delete_marker_id, added
  );
  EXPECT_TRUE(added);
  EXPECT_EQ(4, id);
  auto delete_marker = db_versioned_objects->get_versioned_object(4);
  ASSERT_TRUE(delete_marker.has_value());
  EXPECT_EQ(
      rgw::sal::sfs::VersionType::DELETE_MARKER, delete_marker->version_type
  );
  EXPECT_EQ(rgw::sal::sfs::ObjectState::COMMITTED, delete_marker->object_state);
  EXPECT_EQ(version.etag, delete_marker->etag);
  EXPECT_EQ("delete_marker_id", delete_marker->version_id);

  // add another delete marker (should not add it because the marker already
  // exists)
  id = db_versioned_objects->add_delete_marker_transact(
      uuid, delete_marker_id, added
  );
  EXPECT_FALSE(added);
  EXPECT_EQ(0, id);
  auto last_version = db_versioned_objects->get_versioned_object(5);
  ASSERT_FALSE(last_version.has_value());

  // delete the delete marker
  db_versioned_objects->remove_versioned_object(4);

  // now lets say version 2 and 3 are expired and deleted by LC
  // (for whatever reason)
  auto read_version = db_versioned_objects->get_versioned_object(2);
  ASSERT_TRUE(read_version.has_value());
  read_version->object_state = rgw::sal::sfs::ObjectState::DELETED;
  db_versioned_objects->store_versioned_object(*read_version);

  read_version = db_versioned_objects->get_versioned_object(3);
  ASSERT_TRUE(read_version.has_value());
  read_version->object_state = rgw::sal::sfs::ObjectState::DELETED;
  db_versioned_objects->store_versioned_object(*read_version);

  // try to create the delete marker (we still have 1 alive version)
  id = db_versioned_objects->add_delete_marker_transact(
      uuid, delete_marker_id, added
  );
  EXPECT_TRUE(added);
  EXPECT_EQ(5, id);
  delete_marker = db_versioned_objects->get_versioned_object(5);
  ASSERT_TRUE(delete_marker.has_value());
  EXPECT_EQ(
      rgw::sal::sfs::VersionType::DELETE_MARKER, delete_marker->version_type
  );
  EXPECT_EQ(rgw::sal::sfs::ObjectState::COMMITTED, delete_marker->object_state);
  EXPECT_EQ(version.etag, delete_marker->etag);
  EXPECT_EQ("delete_marker_id", delete_marker->version_id);

  // delete the marker
  db_versioned_objects->remove_versioned_object(5);

  // mark the alive version as deleted
  read_version = db_versioned_objects->get_versioned_object(1);
  ASSERT_TRUE(read_version.has_value());
  read_version->object_state = rgw::sal::sfs::ObjectState::DELETED;
  db_versioned_objects->store_versioned_object(*read_version);

  // add another delete marker (should not add it because all the versions of
  // the object are deleted)
  id = db_versioned_objects->add_delete_marker_transact(
      uuid, delete_marker_id, added
  );
  EXPECT_FALSE(added);
  EXPECT_EQ(0, id);
}
