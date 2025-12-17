#include <common/async/yield_context.h>
#include <common/ceph_argparse.h>
#include <common/common_init.h>
#include <common/perf_counters.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <global/global_init.h>
#include <gtest/gtest.h>
#include <rgw_crypt.h>

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <cerrno>
#include <chrono>

#include "common/web_cache.h"
#include "rgw_common.h"
#include "rgw_kms.h"
#include "rgw_kms_cache.h"
#include "rgw_perf_counters.h"

class TestKMSCacheReaperLifecycle : public ::testing::Test {
  CephContext* cct = g_ceph_context;
  const NoDoutPrefix no_dpp{cct, ceph_subsys_rgw};

 public:
  std::unique_ptr<rgw::kms::KMSCache> uut =
      std::make_unique<rgw::kms::KMSCache>(cct);
};

static void rethrow(const std::exception_ptr& eptr) {
  if (eptr) {
    std::rethrow_exception(eptr);
  }
}

TEST_F(TestKMSCacheReaperLifecycle, Threaded) {
  uut->initialize_ttl_reaper(std::nullopt);
  EXPECT_TRUE(uut->reaper_initialized());
  EXPECT_TRUE(std::holds_alternative<std::jthread>(uut->reaper_state));

  uut->stop_ttl_reaper();
  EXPECT_FALSE(uut->reaper_initialized());
  EXPECT_FALSE(std::holds_alternative<std::jthread>(uut->reaper_state));
}

TEST_F(TestKMSCacheReaperLifecycle, NoInit) {
  EXPECT_FALSE(uut->reaper_initialized());
  EXPECT_TRUE(std::holds_alternative<std::monostate>(uut->reaper_state));
  uut->stop_ttl_reaper();
  EXPECT_FALSE(uut->reaper_initialized());
  EXPECT_TRUE(std::holds_alternative<std::monostate>(uut->reaper_state));
}

TEST_F(TestKMSCacheReaperLifecycle, Async) {
  boost::asio::io_context io;
  uut->initialize_ttl_reaper(io.get_executor());
  io.poll();
  EXPECT_TRUE(uut->reaper_initialized());
  EXPECT_TRUE(
      std::holds_alternative<boost::asio::cancellation_signal>(
          uut->reaper_state));
  uut->stop_ttl_reaper();
  io.run();
  EXPECT_EQ(io.run(), 0);
}

class TestKMSCache : public ::testing::Test {
 protected:
  CephContext* cct = g_ceph_context;
  const NoDoutPrefix no_dpp{cct, ceph_subsys_rgw};

 public:
  std::unique_ptr<rgw::kms::KMSCache> uut =
      std::make_unique<rgw::kms::KMSCache>(cct);

  void TearDown() {
    perfcounter->reset();
  }
};

TEST_F(TestKMSCache, TransientFetchError) {
  std::string tmp;
  ASSERT_EQ(uut->do_cache(&no_dpp, "testing", "foo", [](std::string&) {
    return -EINVAL;
  }, tmp, null_yield), -EINVAL);
  ASSERT_EQ(perfcounter->get(l_rgw_kms_error_transient), 1);
  ASSERT_EQ(perfcounter->get(l_rgw_kms_error_permanent), 0);
  ASSERT_EQ(perfcounter->get(l_rgw_kms_error_secret_store), 0);
  ASSERT_TRUE(cct->_conf->rgw_crypt_s3_kms_cache_enabled);
}

TEST_F(TestKMSCache, PermanentFetchError) {
  std::string tmp;
  ASSERT_EQ(uut->do_cache(&no_dpp, "testing", "foo", [](std::string&) {
    return -ENOENT;
  }, tmp, null_yield), -ENOENT);
  ASSERT_EQ(perfcounter->get(l_rgw_kms_error_transient), 0);
  ASSERT_EQ(perfcounter->get(l_rgw_kms_error_permanent), 1);
  ASSERT_EQ(perfcounter->get(l_rgw_kms_error_secret_store), 0);
  ASSERT_TRUE(cct->_conf->rgw_crypt_s3_kms_cache_enabled);
}

TEST_F(TestKMSCache, Cache) {
  const std::string test_key = "37u481923789123u72189ou3jsdf978of";
  std::string_view test_val = "dlksafj3029jfmsjf8322ty7nghb67435";
  std::string cache_return;
  ASSERT_EQ(uut->do_cache(&no_dpp, "testing", test_key, [&](std::string& val) {
    val = test_val;
    return 0;
  }, cache_return, null_yield), 0);
  EXPECT_EQ(perfcounter->get(l_rgw_kms_error_transient), 0);
  EXPECT_EQ(perfcounter->get(l_rgw_kms_error_permanent), 0);
  EXPECT_EQ(perfcounter->get(l_rgw_kms_error_secret_store), 0);
  EXPECT_TRUE(cct->_conf->rgw_crypt_s3_kms_cache_enabled);
  ASSERT_EQ(cache_return, test_val);

  cache_return.clear();
  ASSERT_EQ(uut->do_cache(&no_dpp, "testing", test_key, [&](std::string&) -> int {
    EXPECT_TRUE(false) << "fetch must not be called";
    return -2342;
  }, cache_return, null_yield), 0);
  EXPECT_EQ(perfcounter->get(l_rgw_kms_error_transient), 0);
  EXPECT_EQ(perfcounter->get(l_rgw_kms_error_permanent), 0);
  EXPECT_EQ(perfcounter->get(l_rgw_kms_error_secret_store), 0);
  EXPECT_TRUE(cct->_conf->rgw_crypt_s3_kms_cache_enabled);
  ASSERT_EQ(cache_return, test_val);
}


class TestSSEKMSWithTestingKMS : public ::testing::Test {
 protected:
  CephContext* cct = g_ceph_context;
  const NoDoutPrefix no_dpp{cct, ceph_subsys_rgw};
  std::map<std::string, bufferlist> attrs = {
      {RGW_ATTR_CRYPT_KEYID,
       []() {
         bufferlist bl;
         bl.append("foo");
         return bl;
       }()},
      {RGW_ATTR_CRYPT_KEYSEL, []() {
         // AES_ECB(32*"#").decrypt(32*"*")
         bufferlist bl;
         bl.append(
             "\xc6\xb1/\x12\xdc\xf7"
             "e"
             "\xe3;\xea\x14\xa4x\x1f"
             "bX"
             "\xc6\xb1/\x12\xdc\xf7"
             "e"
             "\xe3;\xea\x14\xa4x\x1f"
             "bX");
         return bl;
       }()}};
  rgw::kms::KMSCache* uut;
  PerfCounters* cache_perf = nullptr;

  void SetUp() override {
    // Simulate RGW app KMSCache livecyle. A single instance started
    // during app init and may be disabled via config.
    uut = &cct->lookup_or_create_singleton_object<rgw::kms::KMSCache>(
        "TestSSEKMSWithTestingKMS::kms-cache", false, cct);
    cct->get_perfcounters_collection()->with_counters(
        [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
          for (const auto& i : by_path) {
            const auto& perf_counters = i.second.perf_counters;
            if (perf_counters->get_name() == "kms-cache") {
              cache_perf = perf_counters;
              return;
            }
          }
        });

    ASSERT_NE(perfcounter, nullptr);
    ASSERT_NE(cache_perf, nullptr);
  }

  void TearDown() override {
    JSONFormatter f(true);
    cache_perf->dump_formatted(&f, false, select_labeled_t::labeled);
    f.flush(std::cout);
    cct->get_perfcounters_collection()->with_counters(
        [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
          for (const auto& i : by_path) {
            const auto& perf_counters = i.second.perf_counters;
            if (perf_counters->get_name() == "rgw") {
              auto [sum, count] =
                  perf_counters->get_tavg_ns(l_rgw_kms_fetch_lat);
              fmt::println(
                  std::cout,
                  "RGW KMS perf counters: err_trans={} err_perm={} err_sec={} "
                  "avg_fetch_lat={} fetch_cnt={}",
                  perf_counters->get(l_rgw_kms_error_transient),
                  perf_counters->get(l_rgw_kms_error_permanent),
                  perf_counters->get(l_rgw_kms_error_secret_store),
                  std::chrono::nanoseconds(sum / count), count);
              return;
            }
          }
        });
    uut->clear_cache();
  }

  void test_do_reconstitue() {
    std::string actual_key;
    const int ret = reconstitute_actual_key_from_kms(
        &no_dpp, attrs, uut, null_yield, actual_key);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(actual_key, "********************************");
    ASSERT_EQ(perfcounter->get(l_rgw_kms_error_secret_store), 0);
    ASSERT_EQ(perfcounter->get(l_rgw_kms_error_permanent), 0);
    ASSERT_EQ(perfcounter->get(l_rgw_kms_error_transient), 0);
  }
};

TEST_F(
    TestSSEKMSWithTestingKMS, TestReconstituteActualKeyFromKMSBasicsDefault) {
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 0);
  ASSERT_TRUE(cct->_conf->rgw_crypt_s3_kms_cache_enabled);
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::clear)), 0);
  EXPECT_EQ(
      cache_perf->get(static_cast<int>(webcache::Metric::capacity)),
      cct->_conf->rgw_crypt_s3_kms_cache_max_size);
}

TEST_F(
    TestSSEKMSWithTestingKMS,
    TestReconstituteActualKeyFromKMSBasicsWithoutCache) {
  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "false");
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 1);
  test_do_reconstitue();
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 0);
  ASSERT_FALSE(cct->_conf->rgw_crypt_s3_kms_cache_enabled);
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 2);
}

TEST_F(
    TestSSEKMSWithTestingKMS, TestReconstituteActualKeyFromKMSBasicsWithCache) {
  ASSERT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 2);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "true");
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 3);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);

  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 3);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);

  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 3);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 2);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);
}

TEST_F(TestSSEKMSWithTestingKMS, TestRuntimeEnableDisable) {
  ASSERT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 3);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "true");
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 4);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "false");
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 5);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "true");
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 5);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);

  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "false");
  uut->clear_cache();
  test_do_reconstitue();
  EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 6);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 0);
  EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 0);
}

class TestSSES3WithTestingKMS : public ::testing::Test {
 protected:
  CephContext* cct = g_ceph_context;
  const NoDoutPrefix no_dpp{cct, ceph_subsys_rgw};
  std::map<std::string, bufferlist> attrs = {
      {RGW_ATTR_CRYPT_KEYID,
       []() {
         bufferlist bl;
         bl.append("foo");
         return bl;
       }()},
      {RGW_ATTR_CRYPT_KEYSEL, []() {
         // AES_ECB(32*"#").decrypt(32*"*")
         bufferlist bl;
         bl.append(
             "\xc6\xb1/\x12\xdc\xf7"
             "e"
             "\xe3;\xea\x14\xa4x\x1f"
             "bX"
             "\xc6\xb1/\x12\xdc\xf7"
             "e"
             "\xe3;\xea\x14\xa4x\x1f"
             "bX");
         return bl;
       }()}};
  rgw::kms::KMSCache* uut;
  PerfCounters* cache_perf = nullptr;

  void SetUp() override {
    // Simulate RGW app KMSCache livecyle. A single instance started
    // during app init and may be disabled via config.
    uut = &cct->lookup_or_create_singleton_object<rgw::kms::KMSCache>(
        "TestSSEKMSWithTestingKMS::kms-cache", false, cct);
    cct->get_perfcounters_collection()->with_counters(
        [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
          for (const auto& i : by_path) {
            const auto& perf_counters = i.second.perf_counters;
            if (perf_counters->get_name() == "kms-cache") {
              cache_perf = perf_counters;
              return;
            }
          }
        });

    ASSERT_NE(perfcounter, nullptr);
    ASSERT_NE(cache_perf, nullptr);
  }

  void TearDown() override {
    JSONFormatter f(true);
    cache_perf->dump_formatted(&f, false, select_labeled_t::labeled);
    f.flush(std::cout);
    cct->get_perfcounters_collection()->with_counters(
        [&](const PerfCountersCollectionImpl::CounterMap& by_path) {
          for (const auto& i : by_path) {
            const auto& perf_counters = i.second.perf_counters;
            if (perf_counters->get_name() == "rgw") {
              auto [sum, count] =
                  perf_counters->get_tavg_ns(l_rgw_kms_fetch_lat);
              fmt::println(
                  std::cout,
                  "RGW KMS perf counters: err_trans={} err_perm={} err_sec={} "
                  "avg_fetch_lat={} fetch_cnt={}",
                  perf_counters->get(l_rgw_kms_error_transient),
                  perf_counters->get(l_rgw_kms_error_permanent),
                  perf_counters->get(l_rgw_kms_error_secret_store),
                  std::chrono::nanoseconds(sum / count), count);
              return;
            }
          }
        });
    uut->clear_cache();
  }

  void test_do_reconstitue() {
    std::string actual_key;
    const int ret = reconstitute_actual_key_from_kms(
        &no_dpp, attrs, uut, null_yield, actual_key);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(actual_key, "********************************");
    ASSERT_EQ(perfcounter->get(l_rgw_kms_error_secret_store), 0);
    ASSERT_EQ(perfcounter->get(l_rgw_kms_error_permanent), 0);
    ASSERT_EQ(perfcounter->get(l_rgw_kms_error_transient), 0);
  }
};

TEST_F(
    TestSSES3WithTestingKMS, CreateMakeReconsitute) {
  const std::string bucket_key_id = "foo";
  cct->_conf.set_val("rgw_crypt_s3_kms_cache_enabled", "false");

  // essentially a noop - bucket key is part of config
  ASSERT_EQ(create_sse_s3_bucket_key(&no_dpp, bucket_key_id, null_yield), 0);

  // creates DEK
  std::string new_key;
  ASSERT_EQ(make_actual_key_from_sse_s3(&no_dpp, attrs, uut, null_yield, new_key), 0);
  ASSERT_EQ(attrs[RGW_ATTR_CRYPT_DATAKEY].length(), AES_256_KEYSIZE);
  ASSERT_EQ(new_key.length(), AES_256_KEYSIZE);
  std::string new_dek = attrs[RGW_ATTR_CRYPT_DATAKEY].to_str();
  ASSERT_NE(new_key, new_dek);
  ASSERT_NE(new_key, std::string(AES_256_KEYSIZE, '\0'));
  ASSERT_NE(new_dek, std::string(AES_256_KEYSIZE, '\0'));

  std::string reconsitute_key;
  ASSERT_EQ(reconstitute_actual_key_from_sse_s3(&no_dpp, attrs, uut, null_yield, reconsitute_key), 0);
  ASSERT_EQ(attrs[RGW_ATTR_CRYPT_DATAKEY].length(), AES_256_KEYSIZE);
  ASSERT_EQ(reconsitute_key.length(), AES_256_KEYSIZE);

  std::string reconsitute_dek = attrs[RGW_ATTR_CRYPT_DATAKEY].to_str();
  ASSERT_NE(reconsitute_dek, reconsitute_key);
  ASSERT_EQ(new_key, reconsitute_key);
  ASSERT_EQ(new_dek, reconsitute_dek);
  // XXX add tests testing the cache integration
  // EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 0);
  // ASSERT_TRUE(cct->_conf->rgw_crypt_s3_kms_cache_enabled);
  // test_do_reconstitue();
  // EXPECT_EQ(perfcounter->get_tavg_ns(l_rgw_kms_fetch_lat).second, 1);
  // EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::hit)), 0);
  // EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::miss)), 1);
  // EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::size)), 1);
  // EXPECT_EQ(cache_perf->get(static_cast<int>(webcache::Metric::clear)), 0);
  // EXPECT_EQ(
  //     cache_perf->get(static_cast<int>(webcache::Metric::capacity)),
  //     cct->_conf->rgw_crypt_s3_kms_cache_max_size);
}


int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  std::map<std::string, std::string> defaults{
    {"rgw_crypt_s3_kms_backend", RGW_SSE_KMS_BACKEND_TESTING},  // SSE-KMS
    {"rgw_crypt_sse_s3_backend", RGW_SSE_KMS_BACKEND_TESTING},  // SSE-S3
      {"rgw_crypt_s3_kms_encryption_keys",
       "foo=IyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyM="},
      {"debug_rgw", "20"}};
  auto cct = global_init(
      &defaults, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  rgw_perf_start(g_ceph_context);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
