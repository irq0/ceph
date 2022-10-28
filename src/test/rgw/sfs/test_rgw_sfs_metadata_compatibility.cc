// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/ceph_time.h"

#include "rgw/rgw_sal_sfs.h"

#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/conversion_utils.h"

// test cases
#include "compatibility_test_cases/columns_deleted.h"
#include "compatibility_test_cases/columns_added.h"
#include "compatibility_test_cases/optional_columns_added.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

/*
  HINT
  s3gw.db will create here: /tmp/rgw_sfs_tests
*/

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
namespace metadata_tests = rgw::test::metadata;

const static std::string TEST_DIR = "rgw_sfs_tests";


class TestSFSMetadataCompatibility : public ::testing::Test {
protected:
  void SetUp() override {
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
  }

  void TearDown() override {
    fs::current_path(fs::temp_directory_path());
    fs::remove_all(TEST_DIR);
  }

public:
  static std::string getTestDir() {
    auto test_dir = fs::temp_directory_path() / TEST_DIR;
    return test_dir.string();
  }

  static fs::path getDBFullPath(const std::string & base_dir) {
    auto db_full_name = "s3gw.db";
    auto db_full_path = fs::path(base_dir) /  db_full_name;
    return db_full_path;
  }

  static fs::path getDBFullPath() {
    return getDBFullPath(getTestDir());
  }

};

TEST_F(TestSFSMetadataCompatibility, ColumnsAdded) {
  // checks adding extra columns.
  // as the columns added have no default value and they cannot be empty it
  // will throw an exception.
  auto test_db =
    std::make_shared<metadata_tests::columns_added::TestDB>(getDBFullPath());
  test_db->addData();

  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  ASSERT_THROW(new rgw::sal::SFStore(ceph_context.get(), getTestDir()),
                sqlite_sync_exception);
  try {
    new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  } catch (const std::exception & e) {
    // check the exception message
    // this time it doesn't point the tables because it tries to drop the
    // buckets table and as the objects table has a foreign key to buckets it
    // throws a foreign key error.
    EXPECT_STREQ("ERROR ACCESSING SFS METADATA. Metadata database might be corrupted or is no longer compatible",
                  e.what());
  }
  // check that original data was not altered
  EXPECT_TRUE(test_db->checkDataExists());
}

TEST_F(TestSFSMetadataCompatibility, OptionalColumnsAdded) {
  // checks adding extra columns.
  // as the columns added are optional no error should be thrown
  auto test_db =
    std::make_shared<metadata_tests::optional_columns_added::TestDB>(getDBFullPath());
  test_db->addData();

  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  ASSERT_NO_THROW(new rgw::sal::SFStore(ceph_context.get(), getTestDir()));
  // check that original data was not altered
  EXPECT_TRUE(test_db->checkDataExists());
}

TEST_F(TestSFSMetadataCompatibility, ColumnsDeleted) {
  // creates a db that has extra columns.
  // when the SFS database tries to sync it will detect columns removed
  // in its schema
  auto test_db =
    std::make_shared<metadata_tests::columns_deleted::TestDB>(getDBFullPath());
  test_db->addData();

  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  // check that it throws a sqlite_sync_exception
  ASSERT_THROW(new rgw::sal::SFStore(ceph_context.get(), getTestDir()),
                sqlite_sync_exception);
  try {
    new rgw::sal::SFStore(ceph_context.get(), getTestDir());
  } catch (const std::exception & e) {
    // check the exception message
    EXPECT_STREQ("ERROR ACCESSING SFS METADATA. Tables: [ objects versioned_objects ] are no longer compatible.",
                  e.what());
  }
  // check that original data was not altered
  EXPECT_TRUE(test_db->checkDataExists());
}
