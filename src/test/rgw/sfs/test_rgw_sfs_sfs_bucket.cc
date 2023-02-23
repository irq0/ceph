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

/*
  HINT
  s3gw.db will create here: /tmp/rgw_sfs_tests
*/

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

class TestSFSBucket : public ::testing::Test {
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
    user.uinfo.display_name = username + "_display_name";
    users.store_user(user);
  }

  std::shared_ptr<rgw::sal::sfs::Object> createTestObject(
                                                const std::string & bucket_id,
                                                const std::string & name,
                                                DBConnRef conn) {
    auto object = std::shared_ptr<rgw::sal::sfs::Object>(
	rgw::sal::sfs::Object::create_for_testing(name));
    SQLiteObjects db_objects(conn);
    DBOPObjectInfo db_object;
    db_object.uuid = object->path.get_uuid();
    db_object.name = name;
    db_object.bucket_id = bucket_id;
    db_objects.store_object(db_object);
    return object;
  }

  void createTestBucket(const std::string & bucket_id,
                        const std::string & user_id,
                        DBConnRef conn) {
    SQLiteBuckets db_buckets(conn);
    DBOPBucketInfo bucket;
    bucket.binfo.bucket.name = bucket_id + "_name";
    bucket.binfo.bucket.bucket_id = bucket_id;
    bucket.binfo.owner.id = user_id;
    bucket.deleted = false;
    db_buckets.store_bucket(bucket);
  }

  void createTestObjectVersion(std::shared_ptr<rgw::sal::sfs::Object> & object,
                               uint version,
                               DBConnRef conn) {
    object->version_id = version;
    SQLiteVersionedObjects db_versioned_objects(conn);
    DBOPVersionedObjectInfo db_version;
    db_version.id = version;
    db_version.object_id = object->path.get_uuid();
    db_version.object_state = rgw::sal::ObjectState::COMMITTED;
    db_versioned_objects.insert_versioned_object(db_version);
  }
};

RGWAccessControlPolicy get_aclp_default(){
  RGWAccessControlPolicy aclp;
  rgw_user aclu("usr_id");
  aclp.get_acl().create_default(aclu, "usr_id");
  aclp.get_owner().set_name("usr_id");
  aclp.get_owner().set_id(aclu);
  bufferlist acl_bl;
  aclp.encode(acl_bl);
  return aclp;
}

RGWAccessControlPolicy get_aclp_1(){
  RGWAccessControlPolicy aclp;
  rgw_user aclu("usr_id");
  RGWAccessControlList &acl = aclp.get_acl();
  ACLGrant aclg;
  rgw_user gusr("usr_id_2");
  aclg.set_canon(gusr, "usr_id_2", (RGW_PERM_READ_OBJS | RGW_PERM_WRITE_OBJS));
  acl.add_grant(&aclg);
  aclp.get_owner().set_name("usr_id");
  aclp.get_owner().set_id(aclu);
  bufferlist acl_bl;
  aclp.encode(acl_bl);
  return aclp;
}

RGWBucketInfo get_binfo(){
  RGWBucketInfo arg_info;
  return arg_info;
}

void compareListEntry(const rgw_bucket_dir_entry & entry,
                      std::shared_ptr<rgw::sal::sfs::Object> object,
                      const std::string & username) {
  EXPECT_EQ(entry.key.name, object->name);
  EXPECT_EQ(entry.meta.etag, object->get_meta().etag);
  EXPECT_EQ(entry.meta.mtime, object->get_meta().mtime);
  EXPECT_EQ(entry.meta.owner_display_name, username + "_display_name");
  EXPECT_EQ(entry.meta.owner, username);
}

TEST_F(TestSFSBucket, UserCreateBucketCheckGotFromCreate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  
  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = store->get_user(arg_user);

  rgw_bucket arg_bucket("t_id", "b_name", "");
  rgw_placement_rule arg_pl_rule("default", "STANDARD");
  std::string arg_swift_ver_location;
  RGWQuotaInfo arg_quota_info;
  RGWAccessControlPolicy arg_aclp = get_aclp_default();
  rgw::sal::Attrs arg_attrs; 
  {
    bufferlist acl_bl;
    arg_aclp.encode(acl_bl);
    arg_attrs[RGW_ATTR_ACL] = acl_bl;
  }
  
  RGWBucketInfo arg_info = get_binfo();
  obj_version arg_objv;
  bool existed = false;
  req_info arg_req_info(ceph_context.get(), &env);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_create;

  EXPECT_EQ(user->create_bucket(&ndp,                       //dpp
                                arg_bucket,                 //b
                                "zg1",                      //zonegroup_id
                                arg_pl_rule,                //placement_rule
                                arg_swift_ver_location,     //swift_ver_location
                                &arg_quota_info,            //pquota_info
                                arg_aclp,                   //policy
                                arg_attrs,                  //attrs
                                arg_info,                   //info
                                arg_objv,                   //ep_objv
                                false,                      //exclusive
                                false,                      //obj_lock_enabled
                                &existed,                   //existed
                                arg_req_info,               //req_info
                                &bucket_from_create,        //bucket
                                null_yield                  //optional_yield
                                ), 
            0);

  EXPECT_EQ(existed, false);
  EXPECT_EQ(bucket_from_create->get_tenant(), "t_id");
  EXPECT_EQ(bucket_from_create->get_name(), "b_name");
  EXPECT_EQ(bucket_from_create->get_placement_rule().name, "default");
  EXPECT_EQ(bucket_from_create->get_placement_rule().storage_class, "STANDARD");
  EXPECT_NE(bucket_from_create->get_attrs().find(RGW_ATTR_ACL), bucket_from_create->get_attrs().end());
  
  EXPECT_FALSE(arg_info.flags & BUCKET_VERSIONED);
  EXPECT_FALSE(arg_info.flags & BUCKET_OBJ_LOCK_ENABLED);

  auto acl_bl_it = bucket_from_create->get_attrs().find(RGW_ATTR_ACL);
  {
    RGWAccessControlPolicy aclp;
    auto ci_lval = acl_bl_it->second.cbegin();
    aclp.decode(ci_lval);
    EXPECT_EQ(aclp, arg_aclp);
  }

  EXPECT_EQ(bucket_from_create->get_acl(), arg_aclp);
  
  //@warning this triggers segfault
  //EXPECT_EQ(bucket_from_create->get_owner()->get_id().id, "usr_id");

}

TEST_F(TestSFSBucket, UserCreateBucketCheckGotFromStore) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  
  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = store->get_user(arg_user);

  rgw_bucket arg_bucket("t_id", "b_name", "");
  rgw_placement_rule arg_pl_rule("default", "STANDARD");
  std::string arg_swift_ver_location;
  RGWQuotaInfo arg_quota_info;
  RGWAccessControlPolicy arg_aclp = get_aclp_default();
  rgw::sal::Attrs arg_attrs; 
  {
    bufferlist acl_bl;
    arg_aclp.encode(acl_bl);
    arg_attrs[RGW_ATTR_ACL] = acl_bl;
  }
  
  RGWBucketInfo arg_info = get_binfo();
  obj_version arg_objv;
  bool existed = false;
  req_info arg_req_info(ceph_context.get(), &env);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_create;

  EXPECT_EQ(user->create_bucket(&ndp,                       //dpp
                                arg_bucket,                 //b
                                "zg1",                      //zonegroup_id
                                arg_pl_rule,                //placement_rule
                                arg_swift_ver_location,     //swift_ver_location
                                &arg_quota_info,            //pquota_info
                                arg_aclp,                   //policy
                                arg_attrs,                  //attrs
                                arg_info,                   //info
                                arg_objv,                   //ep_objv
                                false,                      //exclusive
                                false,                      //obj_lock_enabled
                                &existed,                   //existed
                                arg_req_info,               //req_info
                                &bucket_from_create,        //bucket
                                null_yield                  //optional_yield
                                ), 
            0);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;

  EXPECT_EQ(store->get_bucket(&ndp,
                              user.get(), 
                              arg_info.bucket,
                              &bucket_from_store,
                              null_yield), 
            0);

  EXPECT_EQ(*bucket_from_store, *bucket_from_create);
  EXPECT_NE(bucket_from_store->get_attrs().find(RGW_ATTR_ACL), bucket_from_store->get_attrs().end());
  
  auto acl_bl_it = bucket_from_store->get_attrs().find(RGW_ATTR_ACL);
  {
    RGWAccessControlPolicy aclp;
    auto ci_lval = acl_bl_it->second.cbegin();
    aclp.decode(ci_lval);
    EXPECT_EQ(aclp, arg_aclp);
  }

  EXPECT_EQ(bucket_from_store->get_acl(), bucket_from_create->get_acl());
}

TEST_F(TestSFSBucket, BucketSetAcl) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  
  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = store->get_user(arg_user);

  rgw_bucket arg_bucket("t_id", "b_name", "");
  rgw_placement_rule arg_pl_rule("default", "STANDARD");
  std::string arg_swift_ver_location;
  RGWQuotaInfo arg_quota_info;
  RGWAccessControlPolicy arg_aclp = get_aclp_default();
  rgw::sal::Attrs arg_attrs; 
  {
    bufferlist acl_bl;
    arg_aclp.encode(acl_bl);
    arg_attrs[RGW_ATTR_ACL] = acl_bl;
  }
  
  RGWBucketInfo arg_info = get_binfo();
  obj_version arg_objv;
  bool existed = false;
  req_info arg_req_info(ceph_context.get(), &env);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_create;

  EXPECT_EQ(user->create_bucket(&ndp,                       //dpp
                                arg_bucket,                 //b
                                "zg1",                      //zonegroup_id
                                arg_pl_rule,                //placement_rule
                                arg_swift_ver_location,     //swift_ver_location
                                &arg_quota_info,            //pquota_info
                                arg_aclp,                   //policy
                                arg_attrs,                  //attrs
                                arg_info,                   //info
                                arg_objv,                   //ep_objv
                                false,                      //exclusive
                                false,                      //obj_lock_enabled
                                &existed,                   //existed
                                arg_req_info,               //req_info
                                &bucket_from_create,        //bucket
                                null_yield                  //optional_yield
                                ), 
            0);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;

  EXPECT_EQ(store->get_bucket(&ndp,
                              user.get(), 
                              arg_info.bucket,
                              &bucket_from_store,
                              null_yield), 
            0);

  RGWAccessControlPolicy arg_aclp_1 = get_aclp_1();

  EXPECT_EQ(bucket_from_store->set_acl(&ndp,
                                       arg_aclp_1,
                                       null_yield),
            0);

  EXPECT_NE(bucket_from_store->get_acl(), bucket_from_create->get_acl());
  EXPECT_EQ(bucket_from_store->get_acl(), get_aclp_1());

  std::unique_ptr<rgw::sal::Bucket> bucket_from_store_1;

  EXPECT_EQ(store->get_bucket(&ndp,
                            user.get(), 
                            arg_info.bucket,
                            &bucket_from_store_1,
                            null_yield), 
          0);

  EXPECT_EQ(bucket_from_store->get_acl(), bucket_from_store_1->get_acl());
}

TEST_F(TestSFSBucket, BucketMergeAndStoreAttrs) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  
  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = store->get_user(arg_user);

  rgw_bucket arg_bucket("t_id", "b_name", "");
  rgw_placement_rule arg_pl_rule("default", "STANDARD");
  std::string arg_swift_ver_location;
  RGWQuotaInfo arg_quota_info;
  RGWAccessControlPolicy arg_aclp = get_aclp_default();
  rgw::sal::Attrs arg_attrs; 
  {
    bufferlist acl_bl;
    arg_aclp.encode(acl_bl);
    arg_attrs[RGW_ATTR_ACL] = acl_bl;
  }
  
  RGWBucketInfo arg_info = get_binfo();
  obj_version arg_objv;
  bool existed = false;
  req_info arg_req_info(ceph_context.get(), &env);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_create;

  EXPECT_EQ(user->create_bucket(&ndp,                       //dpp
                                arg_bucket,                 //b
                                "zg1",                      //zonegroup_id
                                arg_pl_rule,                //placement_rule
                                arg_swift_ver_location,     //swift_ver_location
                                &arg_quota_info,            //pquota_info
                                arg_aclp,                   //policy
                                arg_attrs,                  //attrs
                                arg_info,                   //info
                                arg_objv,                   //ep_objv
                                false,                      //exclusive
                                false,                      //obj_lock_enabled
                                &existed,                   //existed
                                arg_req_info,               //req_info
                                &bucket_from_create,        //bucket
                                null_yield                  //optional_yield
                                ), 
            0);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;

  EXPECT_EQ(store->get_bucket(&ndp,
                              user.get(), 
                              arg_info.bucket,
                              &bucket_from_store,
                              null_yield), 
            0);

  rgw::sal::Attrs new_attrs;
  RGWAccessControlPolicy arg_aclp_1 = get_aclp_1();
  {
    bufferlist acl_bl;
    arg_aclp_1.encode(acl_bl);
    new_attrs[RGW_ATTR_ACL] = acl_bl;
  }

  EXPECT_EQ(bucket_from_store->merge_and_store_attrs(&ndp,
                                                     new_attrs,
                                                     null_yield),
            0);

  EXPECT_EQ(bucket_from_store->get_attrs(), new_attrs);
  EXPECT_NE(bucket_from_store->get_acl(), bucket_from_create->get_acl());
  EXPECT_EQ(bucket_from_store->get_acl(), get_aclp_1());

  std::unique_ptr<rgw::sal::Bucket> bucket_from_store_1;

  EXPECT_EQ(store->get_bucket(&ndp,
                            user.get(), 
                            arg_info.bucket,
                            &bucket_from_store_1,
                            null_yield), 
          0);

  EXPECT_EQ(bucket_from_store_1->get_attrs(), new_attrs);
  EXPECT_EQ(bucket_from_store->get_acl(), bucket_from_store_1->get_acl());
}

TEST_F(TestSFSBucket, DeleteBucket) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = store->get_user(arg_user);

  rgw_bucket arg_bucket("t_id", "b_name", "");
  rgw_placement_rule arg_pl_rule("default", "STANDARD");
  std::string arg_swift_ver_location;
  RGWQuotaInfo arg_quota_info;
  RGWAccessControlPolicy arg_aclp = get_aclp_default();
  rgw::sal::Attrs arg_attrs;
  {
    bufferlist acl_bl;
    arg_aclp.encode(acl_bl);
    arg_attrs[RGW_ATTR_ACL] = acl_bl;
  }

  RGWBucketInfo arg_info = get_binfo();
  obj_version arg_objv;
  bool existed = false;
  req_info arg_req_info(ceph_context.get(), &env);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_create;

  EXPECT_EQ(user->create_bucket(&ndp,                       //dpp
                                arg_bucket,                 //b
                                "zg1",                      //zonegroup_id
                                arg_pl_rule,                //placement_rule
                                arg_swift_ver_location,     //swift_ver_location
                                &arg_quota_info,            //pquota_info
                                arg_aclp,                   //policy
                                arg_attrs,                  //attrs
                                arg_info,                   //info
                                arg_objv,                   //ep_objv
                                false,                      //exclusive
                                false,                      //obj_lock_enabled
                                &existed,                   //existed
                                arg_req_info,               //req_info
                                &bucket_from_create,        //bucket
                                null_yield                  //optional_yield
                                ),
            0);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;

  EXPECT_EQ(store->get_bucket(&ndp,
                              user.get(),
                              arg_info.bucket,
                              &bucket_from_store,
                              null_yield),
            0);

  EXPECT_EQ(*bucket_from_store, *bucket_from_create);

  // perform a basic check in the metadata state
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_buckets = std::make_shared<SQLiteBuckets>(conn);
  ASSERT_NE(bucket_from_create->get_bucket_id(), "");
  auto b_metadata = db_buckets->get_bucket(bucket_from_create->get_bucket_id());
  ASSERT_TRUE(b_metadata.has_value());
  EXPECT_FALSE(b_metadata->deleted);

  EXPECT_EQ(bucket_from_store->remove_bucket(&ndp,
                                             true,
                                             false,
                                             nullptr,
                                             null_yield),
            0);

  // after removing bucket should not be available anymore
  EXPECT_EQ(store->get_bucket(&ndp,
                              user.get(),
                              arg_info.bucket,
                              &bucket_from_store,
                              null_yield),
            -ENOENT);

  // Also verify in metadata that the bucket has the deleted marker
  b_metadata = db_buckets->get_bucket(bucket_from_create->get_bucket_id());
  ASSERT_TRUE(b_metadata.has_value());
  EXPECT_TRUE(b_metadata->deleted);

  // now create the bucket again (should be ok, but bucket_id should differ)
  auto prev_bucket_id = bucket_from_create->get_bucket_id();
  EXPECT_EQ(user->create_bucket(&ndp,                     //dpp
                              arg_bucket,                 //b
                              "zg1",                      //zonegroup_id
                              arg_pl_rule,                //placement_rule
                              arg_swift_ver_location,     //swift_ver_location
                              &arg_quota_info,            //pquota_info
                              arg_aclp,                   //policy
                              arg_attrs,                  //attrs
                              arg_info,                   //info
                              arg_objv,                   //ep_objv
                              false,                      //exclusive
                              false,                      //obj_lock_enabled
                              &existed,                   //existed
                              arg_req_info,               //req_info
                              &bucket_from_create,        //bucket
                              null_yield                  //optional_yield
                              ),
          0);

  EXPECT_EQ(store->get_bucket(&ndp,
                              user.get(),
                              arg_info.bucket,
                              &bucket_from_store,
                              null_yield),
            0);

  EXPECT_EQ(*bucket_from_store, *bucket_from_create);
  EXPECT_NE(prev_bucket_id, bucket_from_create->get_bucket_id());
  b_metadata = db_buckets->get_bucket(bucket_from_create->get_bucket_id());
  ASSERT_TRUE(b_metadata.has_value());
  EXPECT_FALSE(b_metadata->deleted);

  // if we query in metadata for buckets with the same name it should
  // return 2 entries.
  auto bucket_name = bucket_from_create->get_name();
  auto metadata_same_name = db_buckets->get_bucket_by_name(bucket_name);
  ASSERT_EQ(metadata_same_name.size(), 2);
  ASSERT_TRUE(metadata_same_name[0].deleted);
  ASSERT_FALSE(metadata_same_name[1].deleted);

}

TEST_F(TestSFSBucket, TestListObjectsAndVersions) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());

  // create the test user
  createUser("test_user", store->db_conn);

  // create test bucket
  createTestBucket("test_bucket", "test_user", store->db_conn);

  // create a few objects in test_bucket with a few versions
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket", "folder/obj1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket", "folder/obj2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  auto object3 = createTestObject("test_bucket", "folder/obj3", store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);

  auto object4 = createTestObject("test_bucket", "obj4", store->db_conn);
  createTestObjectVersion(object4, version_id++, store->db_conn);
  createTestObjectVersion(object4, version_id++, store->db_conn);

  // loads buckets from metadata
  store->_refresh_buckets();

  rgw_user arg_user("", "test_user", "");
  auto user = store->get_user(arg_user);
  ASSERT_NE(user, nullptr);

  RGWBucketInfo arg_info = get_binfo();
  arg_info.bucket.name = "test_bucket_name";
  arg_info.bucket.bucket_id = "test_bucket";
  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;
  EXPECT_EQ(store->get_bucket(&ndp,
                            user.get(),
                            arg_info.bucket,
                            &bucket_from_store,
                            null_yield),
          0);

  ASSERT_NE(bucket_from_store, nullptr);

  rgw::sal::Bucket::ListParams params;

  // list with empty prefix
  params.prefix = "";
  rgw::sal::Bucket::ListResults results;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results,
                                    null_yield),
          0);

  std::map<std::string, std::shared_ptr<rgw::sal::sfs::Object>> expected_objects;
  expected_objects[object1->name] = object1;
  expected_objects[object2->name] = object2;
  expected_objects[object3->name] = object3;
  expected_objects[object4->name] = object4;
  auto nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size(), results.objs.size());
  for (auto & ret_obj : results.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size(), nb_found_objects);

  // list with 'folder/' prefix
  params.prefix = "folder/";
  rgw::sal::Bucket::ListResults results_folder_prefix;
  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_folder_prefix,
                                    null_yield),
          0);

  expected_objects.clear();
  expected_objects[object1->name] = object1;
  expected_objects[object2->name] = object2;
  expected_objects[object3->name] = object3;
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size(), results_folder_prefix.objs.size());
  for (auto & ret_obj : results_folder_prefix.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size(), nb_found_objects);

  // list with 'ob' prefix
  params.prefix = "ob";
  rgw::sal::Bucket::ListResults results_ob_prefix;
  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_ob_prefix,
                                    null_yield),
          0);

  expected_objects.clear();
  expected_objects[object4->name] = object4;
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size(), results_ob_prefix.objs.size());
  for (auto & ret_obj : results_ob_prefix.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size(), nb_found_objects);


  // list all versions
  // list with empty prefix
  params.prefix = "";
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results_versions;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_versions,
                                    null_yield),
          0);
  // we'll find 4 objects with 2 versions each (8 entries)
  EXPECT_EQ(8, results_versions.objs.size());
  expected_objects.clear();
  expected_objects[object1->name] = object1;
  expected_objects[object2->name] = object2;
  expected_objects[object3->name] = object3;
  expected_objects[object4->name] = object4;
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size(), results.objs.size());
  for (auto & ret_obj : results_versions.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size() * 2, nb_found_objects);

  // list versions with 'folder/' prefix
  params.prefix = "folder/";
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results_versions_folder_prefix;
  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_versions_folder_prefix,
                                    null_yield),
          0);

  expected_objects.clear();
  expected_objects[object1->name] = object1;
  expected_objects[object2->name] = object2;
  expected_objects[object3->name] = object3;
  nb_found_objects = 0;
  // 3 objects match the prefix with 2 versions each
  EXPECT_EQ(6, results_versions_folder_prefix.objs.size());
  for (auto & ret_obj : results_versions_folder_prefix.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size() * 2, nb_found_objects);


  // list versions with 'ob' prefix
  params.prefix = "ob";
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results_versions_ob_prefix;
  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_versions_ob_prefix,
                                    null_yield),
          0);

  expected_objects.clear();
  expected_objects[object4->name] = object4;
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size() * 2, results_versions_ob_prefix.objs.size());
  for (auto & ret_obj : results_versions_ob_prefix.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size() * 2, nb_found_objects);
}

TEST_F(TestSFSBucket, TestListObjectsDelimiter) {

  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());

  // create the test user
  createUser("test_user", store->db_conn);

  // create test bucket
  createTestBucket("test_bucket", "test_user", store->db_conn);

  // create the following objects:
  // directory/
  // directory/directory/
  // directory/directory/file
  // directory/file
  // file
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket", "directory/", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket", "directory/directory/", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  auto object3 = createTestObject("test_bucket", "directory/directory/file", store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);

  auto object4 = createTestObject("test_bucket", "directory/file", store->db_conn);
  createTestObjectVersion(object4, version_id++, store->db_conn);
  createTestObjectVersion(object4, version_id++, store->db_conn);

  auto object5 = createTestObject("test_bucket", "file", store->db_conn);
  createTestObjectVersion(object5, version_id++, store->db_conn);
  createTestObjectVersion(object5, version_id++, store->db_conn);

  // loads buckets from metadata
  store->_refresh_buckets();

  rgw_user arg_user("", "test_user", "");
  auto user = store->get_user(arg_user);
  ASSERT_NE(user, nullptr);

  RGWBucketInfo arg_info = get_binfo();
  arg_info.bucket.name = "test_bucket_name";
  arg_info.bucket.bucket_id = "test_bucket";
  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;
  EXPECT_EQ(store->get_bucket(&ndp,
                            user.get(),
                            arg_info.bucket,
                            &bucket_from_store,
                            null_yield),
          0);

  ASSERT_NE(bucket_from_store, nullptr);

  rgw::sal::Bucket::ListParams params;

  // list with empty prefix and empty delimiter
  params.prefix = "";
  params.delim = "";
  rgw::sal::Bucket::ListResults results;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results,
                                    null_yield),
          0);

  // we expect to get all the objects
  std::map<std::string, std::shared_ptr<rgw::sal::sfs::Object>> expected_objects;
  expected_objects[object1->name] = object1;
  expected_objects[object2->name] = object2;
  expected_objects[object3->name] = object3;
  expected_objects[object4->name] = object4;
  expected_objects[object5->name] = object5;
  auto nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size(), results.objs.size());
  for (auto & ret_obj : results.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size(), nb_found_objects);
  // check there are no common_prefixes
  EXPECT_EQ(results.common_prefixes.size(), 0);

  // use delimiter 'i'
  params.prefix = "";
  params.delim = "i";
  rgw::sal::Bucket::ListResults results_delimiter_i;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_i,
                                    null_yield),
          0);

  // we expect zero objects
  EXPECT_EQ(results_delimiter_i.objs.size(), 0);

  // check common_prefixes (should be fi and di)
  EXPECT_EQ(results_delimiter_i.common_prefixes.size(), 2);
  EXPECT_NE(results_delimiter_i.common_prefixes.find("fi"),
            results_delimiter_i.common_prefixes.end());
  EXPECT_NE(results_delimiter_i.common_prefixes.find("di"),
            results_delimiter_i.common_prefixes.end());

  // use delimiter '/'
  params.prefix = "";
  params.delim = "/";
  rgw::sal::Bucket::ListResults results_delimiter_slash;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_slash,
                                    null_yield),
          0);

  // we expect only the "file" oject (the rest are aggregated in common prefix)
  expected_objects.clear();
  expected_objects[object5->name] = object5;
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size(), results_delimiter_slash.objs.size());
  for (auto & ret_obj : results_delimiter_slash.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size(), nb_found_objects);

  // check common_prefixes
  EXPECT_EQ(results_delimiter_slash.common_prefixes.size(), 1);
  EXPECT_NE(results_delimiter_slash.common_prefixes.find("directory/"),
            results_delimiter_slash.common_prefixes.end());

  // use delimiter '/directory'
  params.prefix = "";
  params.delim = "/directory";
  rgw::sal::Bucket::ListResults results_delimiter_directory;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_directory,
                                    null_yield),
          0);

  // we expect
  // directory/
  // directory/file
  // file
  expected_objects.clear();
  expected_objects[object1->name] = object1; //  directory/
  expected_objects[object4->name] = object4; //  directory/file
  expected_objects[object5->name] = object5; //  file
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size(), results_delimiter_directory.objs.size());
  for (auto & ret_obj : results_delimiter_directory.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size(), nb_found_objects);

  // check common_prefixes
  EXPECT_EQ(results_delimiter_directory.common_prefixes.size(), 1);
  EXPECT_NE(results_delimiter_directory.common_prefixes.find("directory/directory"),
            results_delimiter_directory.common_prefixes.end());


  // use delimiter 'i' and prefix 'd'
  params.prefix = "d";
  params.delim = "i";
  rgw::sal::Bucket::ListResults results_delimiter_i_prefix_d;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_i_prefix_d,
                                    null_yield),
          0);

  // we expect zero objects
  EXPECT_EQ(results_delimiter_i_prefix_d.objs.size(), 0);

  // check common_prefixes (should be fi and di)
  EXPECT_EQ(results_delimiter_i_prefix_d.common_prefixes.size(), 1);
  EXPECT_NE(results_delimiter_i_prefix_d.common_prefixes.find("di"),
            results_delimiter_i_prefix_d.common_prefixes.end());
}

TEST_F(TestSFSBucket, TestListObjectVersionsDelimiter) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());

  // create the test user
  createUser("test_user", store->db_conn);

  // create test bucket
  createTestBucket("test_bucket", "test_user", store->db_conn);

  // create the following objects:
  // directory/
  // directory/directory/
  // directory/directory/file
  // directory/file
  // file
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket", "directory/", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket", "directory/directory/", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  auto object3 = createTestObject("test_bucket", "directory/directory/file", store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);

  auto object4 = createTestObject("test_bucket", "directory/file", store->db_conn);
  createTestObjectVersion(object4, version_id++, store->db_conn);
  createTestObjectVersion(object4, version_id++, store->db_conn);

  auto object5 = createTestObject("test_bucket", "file", store->db_conn);
  createTestObjectVersion(object5, version_id++, store->db_conn);
  createTestObjectVersion(object5, version_id++, store->db_conn);

  // loads buckets from metadata
  store->_refresh_buckets();

  rgw_user arg_user("", "test_user", "");
  auto user = store->get_user(arg_user);
  ASSERT_NE(user, nullptr);

  RGWBucketInfo arg_info = get_binfo();
  arg_info.bucket.name = "test_bucket_name";
  arg_info.bucket.bucket_id = "test_bucket";
  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;
  EXPECT_EQ(store->get_bucket(&ndp,
                            user.get(),
                            arg_info.bucket,
                            &bucket_from_store,
                            null_yield),
          0);

  ASSERT_NE(bucket_from_store, nullptr);

  rgw::sal::Bucket::ListParams params;

  // list with empty prefix and empty delimiter
  params.prefix = "";
  params.delim = "";
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results,
                                    null_yield),
          0);

  // we expect to get all the objects
  std::map<std::string, std::shared_ptr<rgw::sal::sfs::Object>> expected_objects;
  expected_objects[object1->name] = object1;
  expected_objects[object2->name] = object2;
  expected_objects[object3->name] = object3;
  expected_objects[object4->name] = object4;
  expected_objects[object5->name] = object5;
  auto nb_found_objects = 0;
  // we have 2 versions per object
  EXPECT_EQ(expected_objects.size() * 2, results.objs.size());
  for (auto & ret_obj : results.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size() * 2, nb_found_objects);
  // check there are no common_prefixes
  EXPECT_EQ(results.common_prefixes.size(), 0);

  // use delimiter 'i'
  params.prefix = "";
  params.delim = "i";
  rgw::sal::Bucket::ListResults results_delimiter_i;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_i,
                                    null_yield),
          0);

  // we expect zero objects
  EXPECT_EQ(results_delimiter_i.objs.size(), 0);

  // check common_prefixes (should be fi and di)
  EXPECT_EQ(results_delimiter_i.common_prefixes.size(), 2);
  EXPECT_NE(results_delimiter_i.common_prefixes.find("fi"),
            results_delimiter_i.common_prefixes.end());
  EXPECT_NE(results_delimiter_i.common_prefixes.find("di"),
            results_delimiter_i.common_prefixes.end());

  // use delimiter '/'
  params.prefix = "";
  params.delim = "/";
  rgw::sal::Bucket::ListResults results_delimiter_slash;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_slash,
                                    null_yield),
          0);

  // we expect only the "file" oject (the rest are aggregated in common prefix)
  expected_objects.clear();
  expected_objects[object5->name] = object5;
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size() * 2, results_delimiter_slash.objs.size());
  for (auto & ret_obj : results_delimiter_slash.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size() * 2, nb_found_objects);

  // check common_prefixes
  EXPECT_EQ(results_delimiter_slash.common_prefixes.size(), 1);
  EXPECT_NE(results_delimiter_slash.common_prefixes.find("directory/"),
            results_delimiter_slash.common_prefixes.end());

  // use delimiter '/directory'
  params.prefix = "";
  params.delim = "/directory";
  rgw::sal::Bucket::ListResults results_delimiter_directory;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_directory,
                                    null_yield),
          0);

  // we expect
  // directory/
  // directory/file
  // file
  expected_objects.clear();
  expected_objects[object1->name] = object1; //  directory/
  expected_objects[object4->name] = object4; //  directory/file
  expected_objects[object5->name] = object5; //  file
  nb_found_objects = 0;
  EXPECT_EQ(expected_objects.size() * 2, results_delimiter_directory.objs.size());
  for (auto & ret_obj : results_delimiter_directory.objs) {
    auto it = expected_objects.find(ret_obj.key.name);
    if (it != expected_objects.end()) {
      compareListEntry(ret_obj, it->second, "test_user");
      nb_found_objects++;
    }
  }
  // ensure all objects expected were found
  EXPECT_EQ(expected_objects.size() * 2, nb_found_objects);

  // check common_prefixes
  EXPECT_EQ(results_delimiter_directory.common_prefixes.size(), 1);
  EXPECT_NE(results_delimiter_directory.common_prefixes.find("directory/directory"),
            results_delimiter_directory.common_prefixes.end());


  // use delimiter 'i' and prefix 'd'
  params.prefix = "d";
  params.delim = "i";
  rgw::sal::Bucket::ListResults results_delimiter_i_prefix_d;

  EXPECT_EQ(bucket_from_store->list(&ndp,
                                    params,
                                    0,
                                    results_delimiter_i_prefix_d,
                                    null_yield),
          0);

  // we expect zero objects
  EXPECT_EQ(results_delimiter_i_prefix_d.objs.size(), 0);

  // check common_prefixes (should be fi and di)
  EXPECT_EQ(results_delimiter_i_prefix_d.common_prefixes.size(), 1);
  EXPECT_NE(results_delimiter_i_prefix_d.common_prefixes.find("di"),
            results_delimiter_i_prefix_d.common_prefixes.end());
}

TEST_F(TestSFSBucket, UserCreateBucketObjectLockEnabled) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());

  createUser("ol_user_id", store->db_conn);

  rgw_user arg_user("", "ol_user_id", "");
  auto user = store->get_user(arg_user);

  rgw_bucket arg_bucket("t_id", "ol_name", "");
  rgw_placement_rule arg_pl_rule("default", "STANDARD");
  std::string arg_swift_ver_location;
  RGWQuotaInfo arg_quota_info;
  RGWAccessControlPolicy arg_aclp = get_aclp_default();
  rgw::sal::Attrs arg_attrs;
  {
    bufferlist acl_bl;
    arg_aclp.encode(acl_bl);
    arg_attrs[RGW_ATTR_ACL] = acl_bl;
  }

  RGWBucketInfo arg_info = get_binfo();
  obj_version arg_objv;
  bool existed = false;
  req_info arg_req_info(ceph_context.get(), &env);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_create;

  EXPECT_EQ(user->create_bucket(&ndp,                       //dpp
                                arg_bucket,                 //b
                                "zg1",                      //zonegroup_id
                                arg_pl_rule,                //placement_rule
                                arg_swift_ver_location,     //swift_ver_location
                                &arg_quota_info,            //pquota_info
                                arg_aclp,                   //policy
                                arg_attrs,                  //attrs
                                arg_info,                   //info
                                arg_objv,                   //ep_objv
                                false,                      //exclusive
                                true,                       //obj_lock_enabled
                                &existed,                   //existed
                                arg_req_info,               //req_info
                                &bucket_from_create,        //bucket
                                null_yield                  //optional_yield
                                ),
            0);

  EXPECT_TRUE(arg_info.flags & BUCKET_VERSIONED);
  EXPECT_TRUE(arg_info.flags & BUCKET_OBJ_LOCK_ENABLED);

  std::unique_ptr<rgw::sal::Bucket> bucket_from_store;

  EXPECT_EQ(store->get_bucket(&ndp,
                              user.get(),
                              arg_info.bucket,
                              &bucket_from_store,
                              null_yield),
            0);

  EXPECT_EQ(bucket_from_create->get_info().flags, arg_info.flags);
  EXPECT_EQ(bucket_from_create->get_info().flags, bucket_from_store->get_info().flags);
}
