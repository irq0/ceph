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
#include "common/ceph_time.h"
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

PerfCounters* perfcounter_exec_time_hist;
PerfCounters* perfcounter_exec_time_sum;
PerfHistogramCommon::axis_config_d perfcounter_exec_time_config{
    "Latency (µs)",
    PerfHistogramCommon::SCALE_LOG2,  // Latency in logarithmic scale
    10,                               // Start
    90,                               // Quantization unit
    18,                               // buckets
};

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
    //    cct->_conf.set_val("rgw_sfs_sqlite_profile", "1");
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

    PerfCountersBuilder exec_hist(cct.get(), "exec_time", 999, 1001);
    PerfCountersBuilder exec_sum(cct.get(), "exec_time", 999, 1001);
    exec_hist.add_u64_counter_histogram(
        1000, "exec_time", perfcounter_exec_time_config,
        perfcounter_op_hist_y_axis_config, "Histogram of execution time in µs"
    );
    exec_sum.add_time(1000, "total_exec_time");
    perfcounter_exec_time_hist = exec_hist.create_perf_counters();
    cct->get_perfcounters_collection()->add(perfcounter_exec_time_hist);
    perfcounter_exec_time_sum = exec_sum.create_perf_counters();
    cct->get_perfcounters_collection()->add(perfcounter_exec_time_sum);
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
  void log_exec_time_perfcounters() {
    const auto sum = perfcounter_exec_time_sum->tget(1000);
    cct->get_perfcounters_collection()->with_counters(
        [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
          for (const auto& kv : by_path) {
            auto& data = *(kv.second.data);
            auto& perf_counters = *(kv.second.perf_counters);
            if (&perf_counters == perfcounter_exec_time_hist) {
              const PerfHistogramCommon::axis_config_d ac =
                  perfcounter_exec_time_config;
              ceph_assert(ac.m_scale_type == PerfHistogramCommon::SCALE_LOG2);
              uint64_t prev_upper = ac.m_min;
              uint64_t count = 0;
              for (int64_t bucket_no = 1; bucket_no < ac.m_buckets - 1;
                   bucket_no++) {
                uint64_t upper = std::max(
                    0L,
                    ac.m_min + (int64_t(1) << (bucket_no - 1)) * ac.m_quant_size
                );
                uint64_t value = data.histogram->read_bucket(bucket_no, 0);
                lderr(cct.get())
                    << fmt::format("<{}µs\t\t{}", upper, value) << dendl;
                count += value;
              }
              lderr(cct.get())
                  << fmt::format(
                         "<∞\t\t{}", prev_upper,
                         data.histogram->read_bucket(ac.m_buckets - 1, 0)
                     )
                  << dendl;
              lderr(cct.get())
                  << fmt::format(
                         "count: {}, time total: {}ms, avg time: {:2f}ms avg "
                         "rate: {:2f}/s",
                         count, sum.to_msec(),
                         (static_cast<double>(sum.to_msec())) /
                             (static_cast<double>(count)),
                         (static_cast<double>(count) /
                          (static_cast<double>(sum.to_msec()) / 1000))
                     )
                  << dendl;
            }
          }
        }
    );
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

TEST_P(TestSFSConcurrency, performance_single_thread) {
  for (size_t i = 0; i < 5000; i++) {
    auto fn = GetParam().second;
    ceph::mono_time start = mono_clock::now();
    fn({.cct = cct.get(),
        .storage = storage(),
        .store = store.get(),
        .predef_bucket = bucket.get(),
        .predef_object = predef_object.get(),
        .predef_db_object = &predef_db_object});
    ceph::mono_time finish = mono_clock::now();
    perfcounter_exec_time_hist->hinc(
        1000,
        std::chrono::duration_cast<std::chrono::microseconds>(finish - start)
            .count(),
        1
    );
    perfcounter_exec_time_sum->tinc(1000, (finish - start));
  }
  log_exec_time_perfcounters();
  log_retry_perfcounters();
}

TEST_P(TestSFSConcurrency, performance_multi_thread) {
  const static size_t parallelism = std::thread::hardware_concurrency();
  const static size_t ops_per_thread = 5000 / parallelism;
  std::vector<std::thread> threads;

  for (size_t i = 0; i < parallelism; i++) {
    std::thread t([&] {
      for (size_t i = 0; i < ops_per_thread; i++) {
        auto fn = GetParam().second;
        ceph::mono_time start = mono_clock::now();
        fn({.cct = cct.get(),
            .storage = storage(),
            .store = store.get(),
            .predef_bucket = bucket.get(),
            .predef_object = predef_object.get(),
            .predef_db_object = &predef_db_object});
        ceph::mono_time finish = mono_clock::now();
        perfcounter_exec_time_hist->hinc(
            1000,
            std::chrono::duration_cast<std::chrono::microseconds>(
                finish - start
            )
                .count(),
            1
        );
        perfcounter_exec_time_sum->tinc(1000, (finish - start));
      }
    });
    threads.push_back(std::move(t));
  }
  for (size_t i = 0; i < parallelism; i++) {
    threads[i].join();
  }
  log_exec_time_perfcounters();
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
                    fixture.predef_object->name, version
                );
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
