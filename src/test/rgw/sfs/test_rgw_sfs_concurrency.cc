// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/core.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-param-util.h>

#include <array>
#include <atomic>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <random>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>

#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "driver/sfs/object_state.h"
#include "driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "driver/sfs/sqlite/versioned_object/versioned_object_definitions.h"
#include "driver/sfs/version_type.h"
#include "include/random.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_conversions.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/driver/sfs/types.h"
#include "rgw/rgw_sal_sfs.h"
#include "rgw_common.h"
#include "rgw_perf_counters.h"

using namespace rgw::sal::sfs;

namespace fs = std::filesystem;

struct SFSConcurrencyFixture {
  CephContext* cct;
  sqlite::Storage storage;
  rgw::sal::SFStore* store;
  Bucket* predef_bucket;
  Object* predef_object;
  sqlite::DBVersionedObject* predef_db_object;
};

class TestSFSConcurrency
    : public ::testing::TestWithParam<std::pair<
          std::string, std::function<void(const SFSConcurrencyFixture&)>>> {
 protected:
  const std::unique_ptr<CephContext> cct;
  const fs::path database_directory;

  std::unique_ptr<rgw::sal::SFStore> store;
  sqlite::DBConnRef dbconn;
  BucketRef bucket;
  ObjectRef predef_object;
  sqlite::DBVersionedObject predef_db_object;

  TestSFSConcurrency()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        database_directory(create_database_directory()) {
    cct->_conf.set_val("rgw_sfs_data_path", database_directory);
    cct->_conf.set_val("rgw_sfs_sqlite_profile", "1");
    cct->_log->start();
    rgw_perf_start(cct.get());
  }
  void SetUp() override {
    ASSERT_TRUE(fs::exists(database_directory)) << database_directory;
    dbconn = std::make_shared<sqlite::DBConn>(cct.get());
    store.reset(new rgw::sal::SFStore(cct.get(), database_directory));

    sqlite::SQLiteUsers users(dbconn);
    sqlite::DBOPUserInfo user;
    user.uinfo.user_id.id = "testuser";
    user.uinfo.display_name = "display_name";
    users.store_user(user);

    sqlite::SQLiteBuckets db_buckets(dbconn);
    sqlite::DBOPBucketInfo db_binfo;
    db_binfo.binfo.bucket = rgw_bucket("", "testbucket", "1234");
    db_binfo.binfo.owner = rgw_user("testuser");
    db_binfo.binfo.creation_time = ceph::real_clock::now();
    db_binfo.binfo.placement_rule = rgw_placement_rule();
    db_binfo.binfo.zonegroup = "zone";
    db_binfo.deleted = false;
    db_buckets.store_bucket(db_binfo);
    RGWUserInfo bucket_owner;

    bucket = std::make_shared<Bucket>(
        cct.get(), store.get(), db_binfo.binfo, bucket_owner, db_binfo.battrs
    );

    predef_object = bucket->create_version(rgw_obj_key("predef_object"));
    predef_object->metadata_finish(store.get(), false);
    sqlite::SQLiteVersionedObjects svos(dbconn);
    predef_db_object =
        svos.get_versioned_object(predef_object->version_id).value();
  }

  void TearDown() override { fs::remove_all(database_directory); }

  fs::path create_database_directory() const {
    const std::string rand = gen_rand_alphanumeric(cct.get(), 23);
    const auto result{fs::temp_directory_path() / rand};
    fs::create_directory(result);
    return result;
  }

  void log_retry_perfcounters() {
    lderr(cct.get()
    ) << "total: "
      << perfcounter->get(l_rgw_sfs_sqlite_retry_total)
      << " failed:" << perfcounter->get(l_rgw_sfs_sqlite_retry_failed_count)
      << " retried: " << perfcounter->get(l_rgw_sfs_sqlite_retry_retried_count)
      << dendl;
  }

  sqlite::Storage storage() { return dbconn->get_storage(); }
};

TEST_P(TestSFSConcurrency, parallel_executions_must_not_throw) {
  const static size_t parallelism = std::thread::hardware_concurrency() * 10;
  std::vector<std::thread> threads;

  for (size_t i = 0; i < parallelism; i++) {
    std::thread t([&] {
      auto fn = GetParam().second;
      EXPECT_NO_THROW(
          fn({.cct = cct.get(),
              .storage = storage(),
              .store = store.get(),
              .predef_bucket = bucket.get(),
              .predef_object = predef_object.get(),
              .predef_db_object = &predef_db_object})
      );
    });
    threads.push_back(std::move(t));
  }
  for (size_t i = 0; i < parallelism; i++) {
    threads[i].join();
  }
  log_retry_perfcounters();
}

INSTANTIATE_TEST_SUITE_P(
    Common, TestSFSConcurrency,
    testing::Values(
        std::make_pair(
            "create_new_object",
            [](const SFSConcurrencyFixture& fixture) {
              std::string name = gen_rand_alphanumeric(fixture.cct, 23);
              fixture.predef_bucket->create_version(rgw_obj_key(name, name));
            }
        ),
        std::make_pair(
            "create_new_version__unversioned",
            [](const SFSConcurrencyFixture& fixture) {
              std::string version = gen_rand_alphanumeric(fixture.cct, 23);
              ObjectRef obj;
              while (!obj) {
                // create version is ok to return null if it did not
                // succeed. To test metadata_finish we need to retry..
                obj = fixture.predef_bucket->create_version(
                    rgw_obj_key("object", version)
                );
              }
              obj->metadata_finish(fixture.store, false);
            }

        ),
        std::make_pair(
            "create_new_version__versioned",
            [](const SFSConcurrencyFixture& fixture) {
              std::string version = gen_rand_alphanumeric(fixture.cct, 23);
              ObjectRef obj;
              while (!obj) {
                // create version is ok to return null if it did not
                // succeed. To test metadata_finish we need to retry..
                obj = fixture.predef_bucket->create_version(
                    rgw_obj_key("object", version)
                );
              }
              obj->metadata_finish(fixture.store, true);
            }
        ),
        std::make_pair(
            "unversioned_create_simplified",
            [](const SFSConcurrencyFixture& fixture) {
              std::string version = gen_rand_alphanumeric(fixture.cct, 23);
              sqlite::SQLiteVersionedObjects uut(fixture.store->db_conn);
              sqlite::DBVersionedObject vo;
	      while (true) {
		auto maybe_vo = uut.create_new_versioned_object_transact(
                         fixture.predef_bucket->get_bucket_id(),
                         fixture.predef_object->name, version);
		if (maybe_vo.has_value()) {
		  vo = maybe_vo.value();
		  break;
		}
	      }
              vo.size = util::generate_random_number();
              vo.object_state = ObjectState::COMMITTED;
              uut.store_versioned_object_delete_committed_transact_if_state(
                  vo, {ObjectState::OPEN}
              );
            }
        )
    ),
    [](const testing::TestParamInfo<TestSFSConcurrency::ParamType>& info) {
      return info.param.first;
    }
);
