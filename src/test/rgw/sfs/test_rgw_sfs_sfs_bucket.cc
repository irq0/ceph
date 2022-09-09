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
    users.store_user(user);
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
