// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs;

namespace fs = std::filesystem;

const static std::string TEST_DIR = "rgw_sfs_tests";
const std::uintmax_t SIZE_1MB = 1024 * 1024;

// See https://stackoverflow.com/questions/16190078/how-to-atomically-update-a-maximum-value
template <typename T>
void update_maximum(std::atomic<T>& maximum_value, T const& value) noexcept {
  T prev_value = maximum_value;
  while (prev_value < value &&
         !maximum_value.compare_exchange_weak(prev_value, value)) {
  }
}

class TestSFSWALCheckpoint : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;
  const fs::path test_dir;
  std::unique_ptr<rgw::sal::SFStore> store;
  BucketRef bucket;

  TestSFSWALCheckpoint()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        test_dir(fs::temp_directory_path() / TEST_DIR) {
    fs::create_directory(test_dir);
    cct->_conf.set_val("rgw_sfs_data_path", test_dir);
    cct->_log->start();
  }

  ~TestSFSWALCheckpoint() override {
    store.reset();
    fs::remove_all(test_dir);
  }

  // Ordinarily this would just go in the TestSFSWALCheckpoint constructor.
  // Unfortunately our tests need to tweak config settings that must be
  // done _before_ the SFStore is created so that they're in place when
  // DBConn's storage->on_open handler is invoked, so each test has to
  // call this function explicitly.
  void init_store() {
    store.reset(new rgw::sal::SFStore(cct.get(), test_dir));

    sqlite::SQLiteUsers users(store->db_conn);
    sqlite::DBOPUserInfo user;
    user.uinfo.user_id.id = "testuser";
    user.uinfo.display_name = "display_name";
    users.store_user(user);

    sqlite::SQLiteBuckets db_buckets(store->db_conn);
    sqlite::DBOPBucketInfo db_binfo;
    db_binfo.binfo.bucket = rgw_bucket("", "testbucket", "1234");
    db_binfo.binfo.owner = rgw_user("testuser");
    db_binfo.binfo.creation_time = ceph::real_clock::now();
    db_binfo.binfo.placement_rule = rgw_placement_rule();
    db_binfo.binfo.zonegroup = "";
    db_binfo.deleted = false;
    db_buckets.store_bucket(db_binfo);
    RGWUserInfo bucket_owner;

    bucket = std::make_shared<Bucket>(
        cct.get(), store.get(), db_binfo.binfo, bucket_owner, db_binfo.battrs,
        db_binfo.mtime
    );
  }

  // This will spawn num_threads threads, each creating num_objects objects,
  // and will record and return the maximum size the WAL reaches while this
  // is ongoing.
  std::uintmax_t multithread_object_create(
      size_t num_threads, size_t num_objects
  ) {
    std::atomic<std::uintmax_t> max_wal_size{0};
    fs::path wal(test_dir / "s3gw.db-wal");

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
      std::thread t([&, i]() {
        for (size_t j = 0; j < num_objects; ++j) {
	  std::unique_ptr<Object> obj;
          while (!obj) {
            obj = bucket->create_version(rgw_obj_key(
                "object-" + std::to_string(i) + "-" + std::to_string(j)
            ));
          }
          obj->metadata_finish(store.get(), false);
          update_maximum(max_wal_size, fs::file_size(wal));
        }
      });
      threads.push_back(std::move(t));
    }
    for (size_t i = 0; i < num_threads; ++i) {
      threads[i].join();
    }
    return max_wal_size;
  }
};

// This test proves that we have a problem with WAL growth.
// If this test ever *fails* it means the WAL growth problem
// has been unexpectedly fixed by some other change that
// doesn't involve our SFS checkpoint mechanism.
TEST_F(TestSFSWALCheckpoint, confirm_wal_explosion) {
  cct->_conf.set_val("rgw_sfs_wal_checkpoint_use_sqlite_default", "true");
  cct->_conf.set_val("rgw_sfs_wal_size_limit", "-1");
  init_store();

  // Using the SQLite default checkpointing mechanism with
  // multiple threads writing a couple thousand objects
  // should easily push us past a few hundred megabytes
  // quite quickly.
  std::uintmax_t max_wal_size =
      multithread_object_create(std::thread::hardware_concurrency(), 2000);
  EXPECT_GT(max_wal_size, SIZE_1MB * 300);

  // The fact that we have no size limit set means the WAL
  // won't be truncated even when the last writer completes,
  // so it should *still* be huge now.
  EXPECT_EQ(fs::file_size(test_dir / "s3gw.db-wal"), max_wal_size);
}

// This test proves the WAL growth problem has been fixed
// by our SFS checkpoint mechanism.
TEST_F(TestSFSWALCheckpoint, test_wal_checkpoint) {
  init_store();

  // Using our SFS checkpoint mechanism, the WAL may exceed
  // 16MB while writing, because the trunacte checkpoints
  // don't always succeed, but it shouldn't go over by much.
  // We're allowing 32MB here for some extra wiggle room
  // just in case.
  std::uintmax_t max_wal_size =
      multithread_object_create(std::thread::hardware_concurrency(), 2000);
  EXPECT_LT(max_wal_size, SIZE_1MB * 32);

  // Once the writes are all done, the WAL should be finally
  // truncated to something less than 16MB.
  EXPECT_LT(fs::file_size(test_dir / "s3gw.db-wal"), SIZE_1MB * 16);
}
