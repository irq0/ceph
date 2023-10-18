// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/core.h>
#include <gtest/gtest.h>

#include "cls/version/cls_version_types.h"
#include "common/ceph_context.h"
#include "rgw_acl.h"
#include "rgw_common.h"
#include "rgw_perf_counters.h"
#include "rgw_placement_types.h"
#include "rgw_process.h"
#include "rgw_process_env.h"
#include "rgw_request.h"
#include "rgw_sal.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;

class TestSFSRGWIntegration : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;
  const std::filesystem::path database_directory;
  const DoutPrefix dp;
  const DoutPrefixProvider* dpp;
  RGWEnv env;
  DriverManager::Config conf;
  rgw::sal::Driver* driver;

  TestSFSRGWIntegration()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        database_directory(create_database_directory()),
        dp(cct.get(), dout_subsys, "sfs_rgw_test: "),
        dpp(&dp),
        env() {
    env.init(cct.get());
    cct->_conf.set_val("rgw_backend_store", "sfs");
    cct->_conf.set_val("rgw_sfs_sqlite_profile", "1");
    cct->_conf.set_val("rgw_sfs_data_path", database_directory.string());
    cct->_log->start();

    rgw_perf_start(cct.get());
    g_ceph_context = cct.get();

    conf = DriverManager::get_config(false, cct.get());
    driver = DriverManager::get_storage(
        dpp, cct.get(), conf, false, false, false, false, false, false, false
    );
  }

  ~TestSFSRGWIntegration() override {
    rgw_perf_stop(cct.get());
    cct->_log->flush();
    cct->_log->stop();
    DriverManager::close_storage(driver);
  }

  void SetUp() override {
    ASSERT_TRUE(std::filesystem::exists(database_directory))
        << database_directory;
  }

  void TearDown() override { std::filesystem::remove_all(database_directory); }

  std::filesystem::path create_database_directory() const {
    const std::string rand = gen_rand_alphanumeric(cct.get(), 23);
    const auto result{std::filesystem::temp_directory_path() / rand};
    std::filesystem::create_directory(result);
    return result;
  }
};

TEST_F(TestSFSRGWIntegration, smoke_create_bucket_and_object_write_read_data) {
  optional_yield y = null_yield;

  // user
  rgw_user testid_user("", "testid", "");
  std::unique_ptr<rgw::sal::User> user = driver->get_user(testid_user);
  ASSERT_EQ(0, user->load_user(dpp, y));

  // create bucket
  bool exists;
  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::map<std::string, bufferlist> attrs;
  rgw_placement_rule placement_rule;
  std::string unused_swift_ver;
  RGWBucketInfo bucket_info;
  obj_version objv;
  req_info reqi(cct.get(), &env);
  ASSERT_EQ(0, user->create_bucket(
      dpp, rgw_bucket("", "testbucketname", "testid"), "default",
      placement_rule, unused_swift_ver, nullptr, RGWAccessControlPolicy{},
      attrs, bucket_info, objv, true, false, &exists, reqi, &bucket, y
                                   ));

  // write
  auto obj = bucket->get_object(rgw_obj_key("testkey"));
  auto writer = driver->get_atomic_writer(
      dpp, y, obj.get(), testid_user, &placement_rule, -1, "foo"
  );
  ASSERT_EQ(writer->prepare(y), 0);
  bufferlist bl;
  bl.append("test");
  ASSERT_EQ(writer->process(std::move(bl), 0), 0);
  ceph::real_time mtime;
  ceph::real_time now = ceph::real_clock::now();
  attrs.clear();
  std::string user_data;
  ASSERT_EQ(
      writer->complete(
          4, "foo", &mtime, now, attrs, ceph::real_clock::zero(), nullptr,
          nullptr, &user_data, nullptr, nullptr, y
      ),
      0
  );

  // read back
  auto readop = obj->get_read_op();
  EXPECT_EQ(obj->get_obj_size(), 4);
  ASSERT_EQ(readop->prepare(y, dpp), 0);
  bufferlist read_bl;
  EXPECT_EQ(readop->read(0, 4, read_bl, y, dpp), 0);
  EXPECT_EQ(read_bl.to_str(), "test");

  // delete obj
  ASSERT_EQ(obj->delete_object(dpp, y), 0);

  // delete bucket
  ASSERT_EQ(bucket->remove_bucket(dpp, true, false, nullptr, y), 0);
}
