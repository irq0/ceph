// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_conversions.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"

#include "rgw/rgw_sal_sfs.h"

#include "rgw_sfs_utils.h"

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


class TestSFSUser : public ::testing::Test {
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

  void createBucket(const std::string & name,
                    const std::string & user_id,
                    rgw::sal::SFStore * store,
                    const std::shared_ptr<CephContext> & ceph_context) {
    rgw_bucket arg_bucket("t_" + name, name, "id_" + name);
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
    RGWEnv env;
    req_info arg_req_info(ceph_context.get(), &env);

    std::unique_ptr<rgw::sal::Bucket> bucket_from_create;
    NoDoutPrefix ndp(ceph_context.get(), 1);

    rgw_user arg_user("", user_id, "");
    auto user = store->get_user(arg_user);
    ASSERT_TRUE(user != nullptr);
    EXPECT_EQ(user->create_bucket(&ndp,                   //dpp
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

  }

  bool bucketExists(const std::string & bucket_name,
                    rgw::sal::BucketList & bucket_list) {
    auto it = bucket_list.get_buckets().find(bucket_name);
    return it != bucket_list.get_buckets().end();
  }
};

void compareUsers(const std::unique_ptr<rgw::sal::SFSUser> & rgw_user,
                  const DBOPUserInfo & db_user) {
  compareUsersRGWInfo(rgw_user->get_info(), db_user.uinfo);
  compareUserAttrs(rgw_user->get_attrs(), db_user.user_attrs);
  compareUserVersion(rgw_user->get_version_tracker().read_version,
                     db_user.user_version);
}

TEST_F(TestSFSUser, ListBuckets) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  
  NoDoutPrefix ndp(ceph_context.get(), 1);
  RGWEnv env;
  env.init(ceph_context.get());
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = store->get_user(arg_user);


  // should have 0 buckets now
  rgw::sal::BucketList bucket_list;
  EXPECT_EQ(user->list_buckets(&ndp,
                               "", // marker is ignored atm
                               "", // end_maker is ignored atm
                               0,  // max is ignored atm
                               false, // need_stats is ignored atm
                               bucket_list,
                               null_yield),
            0);
  EXPECT_EQ(bucket_list.count(), 0);

  // create buckets
  createBucket("bucket_test_1", "usr_id", store, ceph_context);
  createBucket("bucket_test_2", "usr_id", store, ceph_context);
  createBucket("bucket_test_3", "usr_id", store, ceph_context);

  // should have 3 buckets now
  EXPECT_EQ(user->list_buckets(&ndp,
                               "", // marker is ignored atm
                               "", // end_maker is ignored atm
                               0,  // max is ignored atm
                               false, // need_stats is ignored atm
                               bucket_list,
                               null_yield),
            0);
  EXPECT_EQ(bucket_list.count(), 3);

  EXPECT_TRUE(bucketExists("bucket_test_1", bucket_list));
  EXPECT_TRUE(bucketExists("bucket_test_2", bucket_list));
  EXPECT_TRUE(bucketExists("bucket_test_3", bucket_list));

  // create a new user
  createUser("usr_id2", store->db_conn);

  rgw_user arg_user2("", "usr_id2", "");
  auto user2 = store->get_user(arg_user2);
  ASSERT_NE(user2, nullptr);

  // user2 has no buckets yet
  bucket_list.clear();
  EXPECT_EQ(user2->list_buckets(&ndp,
                               "", // marker is ignored atm
                               "", // end_maker is ignored atm
                               0,  // max is ignored atm
                               false, // need_stats is ignored atm
                               bucket_list,
                               null_yield),
            0);
  EXPECT_EQ(bucket_list.count(), 0);

  // create buckets for user2
  createBucket("bucket_test_2_1", "usr_id2", store, ceph_context);
  createBucket("bucket_test_2_2", "usr_id2", store, ceph_context);

  // should have 2 buckets now
  EXPECT_EQ(user2->list_buckets(&ndp,
                               "", // marker is ignored atm
                               "", // end_maker is ignored atm
                               0,  // max is ignored atm
                               false, // need_stats is ignored atm
                               bucket_list,
                               null_yield),
            0);
  EXPECT_EQ(bucket_list.count(), 2);
  EXPECT_TRUE(bucketExists("bucket_test_2_1", bucket_list));
  EXPECT_TRUE(bucketExists("bucket_test_2_2", bucket_list));


  // first user has the same list
  bucket_list.clear();
  EXPECT_EQ(user->list_buckets(&ndp,
                               "", // marker is ignored atm
                               "", // end_maker is ignored atm
                               0,  // max is ignored atm
                               false, // need_stats is ignored atm
                               bucket_list,
                               null_yield),
            0);
  EXPECT_EQ(bucket_list.count(), 3);

  EXPECT_TRUE(bucketExists("bucket_test_1", bucket_list));
  EXPECT_TRUE(bucketExists("bucket_test_2", bucket_list));
  EXPECT_TRUE(bucketExists("bucket_test_3", bucket_list));
}

TEST_F(TestSFSUser, LoadUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  createUser("usr_id", store->db_conn);

  // get info from sqlite
  rgw::sal::sfs::sqlite::SQLiteUsers sqlite_users(store->db_conn);
  auto db_user = sqlite_users.get_user("usr_id");
  ASSERT_TRUE(db_user.has_value());

  rgw_user arg_user("", "usr_id", "");
  auto user = std::make_unique<rgw::sal::SFSUser>(arg_user, store);

  // it's empty now
  ASSERT_EQ(user->get_info().access_keys.size(), 0);

  // load data
  EXPECT_EQ(user->load_user(&ndp, null_yield), 0);

  compareUsers(user, *db_user);
}

TEST_F(TestSFSUser, LoadUserDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id_does_not_exist", "");
  auto user = std::make_unique<rgw::sal::SFSUser>(arg_user, store);
  EXPECT_EQ(user->load_user(&ndp, null_yield), -ENOENT);
}

TEST_F(TestSFSUser, StoreUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  createUser("usr_id", store->db_conn);

  // get info from sqlite
  rgw::sal::sfs::sqlite::SQLiteUsers sqlite_users(store->db_conn);
  auto db_user = sqlite_users.get_user("usr_id");
  ASSERT_TRUE(db_user.has_value());

  rgw_user arg_user("", "usr_id", "");
  auto user = std::make_unique<rgw::sal::SFSUser>(arg_user, store);

  // load data of user
  EXPECT_EQ(user->load_user(&ndp, null_yield), 0);

  RGWUserInfo old_info_before_changes = user->get_info();
  std::map<std::string, RGWAccessKey> access_keys;
  access_keys["key1"] = RGWAccessKey("key1", "secret1");
  access_keys["key2"] = RGWAccessKey("key2", "secret2");

  user->get_info().access_keys = access_keys;

  RGWUserInfo old_info;
  EXPECT_EQ(user->store_user(&ndp, null_yield, false, &old_info), 0);

  // reload info
  EXPECT_EQ(user->load_user(&ndp, null_yield), 0);

  // version should be increased by 1
  EXPECT_EQ(user->get_version_tracker().read_version.ver,
            db_user->user_version.ver + 1);

  ASSERT_TRUE(compareMaps(user->get_info().access_keys, access_keys));

  // old info obtained in store should match what sqlite had before
  compareUsersRGWInfo(old_info, old_info_before_changes);
}

TEST_F(TestSFSUser, StoreUserVersionMismatch) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = std::make_unique<rgw::sal::SFSUser>(arg_user, store);
  EXPECT_EQ(user->load_user(&ndp, null_yield), 0);

  // change the version so it does not match when storing
  user->get_version_tracker().read_version.ver = 1999;

  RGWUserInfo old_info;
  EXPECT_EQ(user->store_user(&ndp, null_yield, false, &old_info), -ECANCELED);
}

TEST_F(TestSFSUser, RemoveUser) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  auto store = new rgw::sal::SFStore(ceph_context.get(), getTestDir());

  NoDoutPrefix ndp(ceph_context.get(), 1);
  createUser("usr_id", store->db_conn);

  rgw_user arg_user("", "usr_id", "");
  auto user = std::make_unique<rgw::sal::SFSUser>(arg_user, store);
  EXPECT_EQ(user->load_user(&ndp, null_yield), 0);
  EXPECT_EQ(user->remove_user(&ndp, null_yield), 0);
  // try to remove again
  EXPECT_EQ(user->remove_user(&ndp, null_yield), -ECANCELED);

  // user is removed from metadata
  rgw::sal::sfs::sqlite::SQLiteUsers sqlite_users(store->db_conn);
  auto db_user = sqlite_users.get_user("usr_id");
  ASSERT_FALSE(db_user.has_value());
}
