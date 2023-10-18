/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string_view>

#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/random_string.h"
#include "driver/sfs/object_state.h"
#include "driver/sfs/sqlite/objects/object_definitions.h"
#include "driver/sfs/sqlite/versioned_object/versioned_object_definitions.h"
#include "driver/sfs/version_type.h"
#include "gtest/gtest.h"
#include "rgw/driver/sfs/sqlite/sqlite_list.h"
#include "rgw/rgw_perf_counters.h"
#include "test/rgw/sfs/rgw_sfs_utils.h"

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;

class TestSFSList : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;
  const fs::path database_directory;

  std::unique_ptr<rgw::sal::SFStore> store;
  DBConnRef dbconn;

  TestSFSList()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        database_directory(create_database_directory()) {
    cct->_conf.set_val("rgw_sfs_data_path", database_directory);
    cct->_conf.set_val("rgw_sfs_sqlite_profile", "1");
    cct->_conf.set_val("rgw_sfs_sqlite_profile_slowlog_time", "0");
    cct->_log->set_stderr_level(5, 5);
    cct->_log->start();
    rgw_perf_start(cct.get());
  }
  void SetUp() override {
    ASSERT_TRUE(fs::exists(database_directory)) << database_directory;
    dbconn = std::make_shared<DBConn>(cct.get());
    store.reset(new rgw::sal::SFStore(cct.get(), database_directory));

    SQLiteUsers users(dbconn);
    DBOPUserInfo user;
    user.uinfo.user_id.id = "testuser";
    user.uinfo.display_name = "display_name";
    users.store_user(user);

    SQLiteBuckets db_buckets(dbconn);
    DBOPBucketInfo db_binfo;
    db_binfo.binfo.bucket = rgw_bucket("", "testbucket", "testbucket");
    db_binfo.binfo.owner = rgw_user("testuser");
    db_binfo.binfo.creation_time = ceph::real_clock::now();
    db_binfo.binfo.placement_rule = rgw_placement_rule();
    db_binfo.binfo.zonegroup = "zone";
    db_binfo.deleted = false;
    db_buckets.store_bucket(db_binfo);
    RGWUserInfo bucket_owner;
  }
  void TearDown() override {
    if (::testing::Test::HasFailure()) {
      dump_db();
    }
    store.reset();
    dbconn.reset();
    fs::remove_all(database_directory);
  }

  fs::path create_database_directory() const {
    const std::string rand = gen_rand_alphanumeric(cct.get(), 23);
    const auto result{fs::temp_directory_path() / rand};
    fs::create_directory(result);
    return result;
  }

  std::pair<DBObject, DBVersionedObject> add_obj_single_ver(
      const std::string& prefix = "", rgw::sal::sfs::ObjectState version_state =
                                          rgw::sal::sfs::ObjectState::COMMITTED
  ) const {
    std::string name(prefix);
    name.append(gen_rand_alphanumeric(cct.get(), 23));
    const auto obj = create_test_object("testbucket", name);
    SQLiteObjects os(dbconn);
    os.store_object(obj);
    auto ver = create_test_versionedobject(obj.uuid, "testversion");
    ver.object_state = version_state;
    SQLiteVersionedObjects vos(dbconn);
    vos.insert_versioned_object(ver);
    return std::make_pair(obj, ver);
  }

  void dump_db() {
    auto storage = dbconn->get_storage();
    lderr(cct.get()) << "Dumping objects:" << dendl;
    for (const auto& row : storage.get_all<DBObject>()) {
      lderr(cct.get()) << row << dendl;
    }
    lderr(cct.get()) << "Dumping versioned objects:" << dendl;
    for (const auto& row : storage.get_all<DBVersionedObject>()) {
      lderr(cct.get()) << row << dendl;
    }
  }

  rgw_bucket_dir_entry make_dentry_with_name(const std::string& name) {
    rgw_bucket_dir_entry e;
    e.key.name = name;
    return e;
  }

  SQLiteList make_uut() { return SQLiteList(dbconn); }
};

class TestSFSListObjectsAndVersions
    : public TestSFSList,
      public testing::WithParamInterface<std::string> {
 protected:
  bool uut_list(
      const std::string& bucket_id, const std::string& prefix,
      const std::string& start_after_object_name, size_t max,
      std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available = nullptr
  ) {
    if (GetParam() == "objects") {
      const auto uut = make_uut();
      return uut.objects(
          bucket_id, prefix, start_after_object_name, max, out,
          out_more_available
      );
    } else if (GetParam() == "versions") {
      const auto uut = make_uut();
      return uut.versions(
          bucket_id, prefix, start_after_object_name, max, out,
          out_more_available
      );
    } else {
      throw std::runtime_error("implement me");
    }
  }
};

TEST_P(TestSFSListObjectsAndVersions, empty__lists_nothing) {
  std::vector<rgw_bucket_dir_entry> results;
  ASSERT_TRUE(uut_list("testbucket", "", "", 10, results));
  ASSERT_EQ(results.size(), 0);
}

TEST_P(TestSFSListObjectsAndVersions, single_object__plain_list_returns_it) {
  // See also s3-test bucket_list_return_data
  const auto obj = add_obj_single_ver();
  std::vector<rgw_bucket_dir_entry> results;
  ASSERT_TRUE(uut_list("testbucket", "", "", 100, results));
  EXPECT_EQ(results.size(), 1);
  const auto e = results[0];
  EXPECT_EQ(e.key.name, obj.first.name);
  EXPECT_EQ(e.meta.mtime, obj.second.mtime);
  EXPECT_EQ(e.meta.etag, obj.second.etag);
  EXPECT_EQ(e.meta.size, obj.second.size);
  EXPECT_EQ(e.meta.accounted_size, obj.second.size);
}

TEST_P(TestSFSListObjectsAndVersions, never_returns_more_than_max) {
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut_list("testbucket", "", "", 2, results));
  ASSERT_EQ(results.size(), 2);
}

TEST_P(TestSFSListObjectsAndVersions, result_key_names_is_sorted_asc) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut_list("testbucket", "", "", 1000, results));

  std::vector<rgw_bucket_dir_entry> expected(results);
  std::sort(expected.begin(), expected.end(), [](const auto& a, const auto& b) {
    return a.key.name < b.key.name;
  });
  for (size_t i = 0; i < results.size(); i++) {
    EXPECT_EQ(results[i].key.name, expected[i].key.name);
  }
}

TEST_P(TestSFSListObjectsAndVersions, prefix_search_returns_only_prefixed) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver("aaa/");
  add_obj_single_ver("aaa/");
  add_obj_single_ver("aaa/");
  add_obj_single_ver("XXX/");
  add_obj_single_ver("XXX/");

  ASSERT_TRUE(uut_list("testbucket", "aaa/", "", 1000, results));
  for (size_t i = 0; i < results.size(); i++) {
    EXPECT_TRUE(results[i].key.name.starts_with("aaa/"));
  }
}

TEST_P(TestSFSListObjectsAndVersions, start_after_object_name) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver("aaa");
  add_obj_single_ver("bbb");
  add_obj_single_ver("ccc");
  const auto after_this = add_obj_single_ver("ddd");
  add_obj_single_ver("eee");

  ASSERT_TRUE(uut_list("testbucket", "", after_this.first.name, 1000, results));
  ASSERT_EQ(results.size(), 1);
  EXPECT_TRUE(results[0].key.name.starts_with("eee"));
}

TEST_P(TestSFSListObjectsAndVersions, more_avail__false_if_all) {
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{true};
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut_list("testbucket", "", "", 2, results, &more_avail));
  EXPECT_EQ(results.size(), 2);
  EXPECT_FALSE(more_avail);
}

TEST_P(TestSFSListObjectsAndVersions, more_avail__true_if_more) {
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{false};
  add_obj_single_ver();
  add_obj_single_ver();
  add_obj_single_ver();

  ASSERT_TRUE(uut_list("testbucket", "", "", 2, results, &more_avail));
  EXPECT_EQ(results.size(), 2);
  EXPECT_TRUE(more_avail);
}

TEST_P(TestSFSListObjectsAndVersions, more_avail__max_zero_bucket_empty) {
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{false};
  ASSERT_TRUE(uut_list("testbucket", "", "", 0, results, &more_avail));
  EXPECT_EQ(results.size(), 0);
  EXPECT_FALSE(more_avail);
}

TEST_P(TestSFSListObjectsAndVersions, more_avail__max_zero_bucket_not_empty) {
  std::vector<rgw_bucket_dir_entry> results;
  bool more_avail{true};
  add_obj_single_ver();
  ASSERT_TRUE(uut_list("testbucket", "", "", 0, results, &more_avail));
  EXPECT_EQ(results.size(), 0);
  EXPECT_TRUE(more_avail);
}

TEST_P(TestSFSListObjectsAndVersions, wildcard_in_prefix_do_not_match) {
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver("$");
  ASSERT_TRUE(uut_list("testbucket", "%", "", 1000, results));
  ASSERT_EQ(results.size(), 0);
}

TEST_P(
    TestSFSListObjectsAndVersions, prefix_matches_dont_interprete_wildcards
) {
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver("___$");
  add_obj_single_ver("$__$");
  ASSERT_TRUE(uut_list("testbucket", "___$", "", 1000, results));
  ASSERT_EQ(results.size(), 1);
  EXPECT_TRUE(results[0].key.name.starts_with("___$"));
}

INSTANTIATE_TEST_SUITE_P(
    ObjectsVersions, TestSFSListObjectsAndVersions,
    testing::Values("objects", "versions"),
    [](const testing::TestParamInfo<TestSFSListObjectsAndVersions::ParamType>&
           info) { return info.param; }

);

TEST_F(TestSFSList, objects__does_not_return_objects_with_delete_marker) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  std::pair<DBObject, DBVersionedObject> oov = add_obj_single_ver();
  std::pair<DBObject, DBVersionedObject> expected_result = add_obj_single_ver();

  auto del = create_test_versionedobject(oov.first.uuid, "deletemarker");
  del.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  del.version_type = rgw::sal::sfs::VersionType::DELETE_MARKER;
  SQLiteVersionedObjects vos(dbconn);
  vos.insert_versioned_object(del);

  ASSERT_TRUE(uut.objects("testbucket", "", "", 1000, results));
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].key.name, expected_result.first.name);
}

TEST_F(TestSFSList, versions__returns_instances) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;
  add_obj_single_ver();
  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 1);
  EXPECT_FALSE(results[0].key.instance.empty());
}

TEST_F(TestSFSList, versions__returns_versions_and_delete_markers) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;

  std::pair<DBObject, DBVersionedObject> oov = add_obj_single_ver();
  auto del = create_test_versionedobject(oov.first.uuid, "deletemarker");
  del.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  del.version_type = rgw::sal::sfs::VersionType::DELETE_MARKER;
  SQLiteVersionedObjects vos(dbconn);
  vos.insert_versioned_object(del);

  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 2);

  EXPECT_TRUE(results[0].flags & rgw_bucket_dir_entry::FLAG_VER);
  EXPECT_TRUE(results[0].is_delete_marker());
  EXPECT_TRUE(results[1].flags & rgw_bucket_dir_entry::FLAG_VER);
  EXPECT_TRUE(results[1].is_valid());
}

TEST_F(TestSFSList, versions__correctly_sorts_and_marks_latest_version) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;

  const auto obj = create_test_object("testbucket", "obj");
  SQLiteObjects os(dbconn);
  SQLiteVersionedObjects vos(dbconn);
  os.store_object(obj);
  auto latest = create_test_versionedobject(obj.uuid, "latest");
  latest.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  latest.commit_time = ceph::real_time(
      std::chrono::nanoseconds(std::numeric_limits<int64_t>::max())
  );
  auto between = create_test_versionedobject(obj.uuid, "between");
  between.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  between.commit_time = ceph::real_clock::now();
  auto first = create_test_versionedobject(obj.uuid, "first");
  first.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  first.commit_time = ceph::real_time::min();
  vos.insert_versioned_object(latest);
  vos.insert_versioned_object(first);
  vos.insert_versioned_object(between);

  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 3);

  EXPECT_EQ(results[0].key.instance, "latest");
  EXPECT_TRUE(results[0].is_current());
  EXPECT_EQ(results[1].key.instance, "between");
  EXPECT_FALSE(results[1].is_current());
  EXPECT_EQ(results[2].key.instance, "first");
  EXPECT_FALSE(results[2].is_current());
}

TEST_F(TestSFSList, versions__there_is_latest_with_multiple_versions) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;

  const auto obj1 = create_test_object("testbucket", "test1/a");
  const auto obj2 = create_test_object("testbucket", "test2/abc");
  SQLiteObjects os(dbconn);
  SQLiteVersionedObjects vos(dbconn);
  os.store_object(obj1);
  os.store_object(obj2);
  std::array<rgw::sal::sfs::sqlite::DBVersionedObject, 3> vers_obj1 = {
      create_test_versionedobject(
          obj1.uuid, gen_rand_alphanumeric(cct.get(), 23)
      ),
      create_test_versionedobject(
          obj1.uuid, gen_rand_alphanumeric(cct.get(), 23)
      ),
      create_test_versionedobject(
          obj1.uuid, gen_rand_alphanumeric(cct.get(), 23)
      )};
  for (size_t i = 0; i < vers_obj1.size(); i++) {
    vers_obj1[i].object_state = rgw::sal::sfs::ObjectState::COMMITTED;
    vers_obj1[i].commit_time = ceph::real_time(std::chrono::seconds(i));
    vos.insert_versioned_object(vers_obj1[i]);
  }
  std::array<rgw::sal::sfs::sqlite::DBVersionedObject, 3> vers_obj2 = {
      create_test_versionedobject(
          obj2.uuid, gen_rand_alphanumeric(cct.get(), 23)
      ),
      create_test_versionedobject(
          obj2.uuid, gen_rand_alphanumeric(cct.get(), 23)
      ),
      create_test_versionedobject(
          obj2.uuid, gen_rand_alphanumeric(cct.get(), 23)
      )};
  for (size_t i = 0; i < vers_obj2.size(); i++) {
    vers_obj2[i].object_state = rgw::sal::sfs::ObjectState::COMMITTED;
    vers_obj2[i].commit_time = ceph::real_time(std::chrono::seconds(i + 10));
    vos.insert_versioned_object(vers_obj2[i]);
  }
  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 6);
  EXPECT_TRUE(results[0].is_current());
  EXPECT_FALSE(results[1].is_current());
  EXPECT_FALSE(results[2].is_current());
  EXPECT_TRUE(results[3].is_current());
  EXPECT_FALSE(results[4].is_current());
  EXPECT_FALSE(results[5].is_current());
}

TEST_F(TestSFSList, versions__only_one_latest) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;

  const auto obj = create_test_object("testbucket", "test1/a");
  SQLiteObjects os(dbconn);
  SQLiteVersionedObjects vos(dbconn);
  os.store_object(obj);
  auto vo1 = create_test_versionedobject(
      obj.uuid, gen_rand_alphanumeric(cct.get(), 23)
  );
  vo1.commit_time = ceph::real_time(std::chrono::seconds(2342));
  vo1.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  auto vo2 = create_test_versionedobject(
      obj.uuid, gen_rand_alphanumeric(cct.get(), 23)
  );
  vo2.commit_time = vo1.commit_time;
  vo2.object_state = vo1.object_state;
  vos.insert_versioned_object(vo1);
  vos.insert_versioned_object(vo2);

  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 2);
  std::vector<bool> current_flags;
  for (const auto& result : results) {
    current_flags.emplace_back(result.is_current());
  }
  EXPECT_THAT(current_flags, ::testing::UnorderedElementsAre(true, false));
}

TEST_F(TestSFSList, versions__delete_marker_latest) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;

  const auto obj = create_test_object("testbucket", "test1/a");
  SQLiteObjects os(dbconn);
  SQLiteVersionedObjects vos(dbconn);
  os.store_object(obj);
  auto vo1 = create_test_versionedobject(
      obj.uuid, gen_rand_alphanumeric(cct.get(), 23)
  );
  vo1.commit_time = ceph::real_time(std::chrono::seconds(1));
  vo1.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  vo1.version_type = rgw::sal::sfs::VersionType::REGULAR;
  auto dm = create_test_versionedobject(
      obj.uuid, gen_rand_alphanumeric(cct.get(), 23)
  );
  dm.commit_time = vo1.commit_time + std::chrono::seconds(1);
  dm.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  dm.version_type = rgw::sal::sfs::VersionType::DELETE_MARKER;

  vos.insert_versioned_object(vo1);
  vos.insert_versioned_object(dm);

  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 2);
  EXPECT_TRUE(results[0].is_delete_marker());
  EXPECT_TRUE(results[0].is_current());
  EXPECT_FALSE(results[1].is_delete_marker());
  EXPECT_FALSE(results[1].is_current());
}

TEST_F(TestSFSList, versions__delete_marker_is_not_latest) {
  const auto uut = make_uut();
  std::vector<rgw_bucket_dir_entry> results;

  const auto obj = create_test_object("testbucket", "test1/a");
  SQLiteObjects os(dbconn);
  SQLiteVersionedObjects vos(dbconn);
  os.store_object(obj);
  auto vo1 = create_test_versionedobject(
      obj.uuid, gen_rand_alphanumeric(cct.get(), 23)
  );
  vo1.commit_time = ceph::real_time(std::chrono::seconds(1));
  vo1.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  vo1.version_type = rgw::sal::sfs::VersionType::REGULAR;
  auto dm = create_test_versionedobject(
      obj.uuid, gen_rand_alphanumeric(cct.get(), 23)
  );
  dm.commit_time = vo1.commit_time + std::chrono::seconds(1);
  dm.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  dm.version_type = rgw::sal::sfs::VersionType::DELETE_MARKER;

  vos.insert_versioned_object(vo1);
  vos.insert_versioned_object(dm);

  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 2);
  EXPECT_TRUE(results[0].is_delete_marker());
  EXPECT_TRUE(results[0].is_current());
  EXPECT_FALSE(results[1].is_delete_marker());
  EXPECT_FALSE(results[1].is_current());

  // now we add another version on top of the delete marker
  auto new_version = create_test_versionedobject(
      obj.uuid, gen_rand_alphanumeric(cct.get(), 23)
  );
  new_version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  new_version.version_type = rgw::sal::sfs::VersionType::REGULAR;
  new_version.commit_time = dm.commit_time + std::chrono::seconds(1);
  vos.insert_versioned_object(new_version);

  // delete marker should not be the last now
  results.clear();
  ASSERT_TRUE(uut.versions("testbucket", "", "", 1000, results));
  ASSERT_EQ(results.size(), 3);
  EXPECT_FALSE(results[0].is_delete_marker());
  EXPECT_TRUE(results[0].is_current());
  EXPECT_TRUE(results[1].is_delete_marker());
  EXPECT_FALSE(results[1].is_current());
  EXPECT_FALSE(results[2].is_delete_marker());
  EXPECT_FALSE(results[2].is_current());
}

TEST_F(TestSFSList, roll_up_example) {
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("sample.foo"),
      make_dentry_with_name("photos/2006/January/sample.jpg"),
      make_dentry_with_name("photos/2006/February/sample2.jpg"),
      make_dentry_with_name("photos/2006/February/sample3.jpg"),
      make_dentry_with_name("photos/2006/February/sample4.jpg")};
  const auto expected_still_exists = objects[0];
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "/", objects, prefixes, out);
  EXPECT_EQ(prefixes.size(), 1);
  EXPECT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].key.name, expected_still_exists.key.name);
  EXPECT_THAT(
      prefixes, ::testing::ElementsAre(::testing::Pair("photos/", true))
  );
}

TEST_F(TestSFSList, roll_up_empty) {
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects;
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "/", objects, prefixes, out);
  EXPECT_EQ(prefixes.size(), 0);
  EXPECT_EQ(out.size(), 0);
}

TEST_F(TestSFSList, roll_up_no_such_delim_in_equals_out) {
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("prefix/aaa"), make_dentry_with_name("prefix/bbb"),
      make_dentry_with_name("prefix/ccc")};
  const auto expected_still_exists = objects[0];
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "$", objects, prefixes, out);
  ASSERT_EQ(prefixes.size(), 0);
  ASSERT_EQ(out.size(), objects.size());
  for (size_t i = 0; i < objects.size(); i++) {
    EXPECT_EQ(objects[i].key.name, out[i].key.name);
  }
}

TEST_F(TestSFSList, roll_up_multi_delim_group_by_first) {
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("prefix/aaa/1"),
      make_dentry_with_name("prefix/bbb/2"),
      make_dentry_with_name("prefix/ccc/3")};
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "/", objects, prefixes, out);
  EXPECT_EQ(prefixes.size(), 1);
  EXPECT_EQ(out.size(), 0);
  EXPECT_THAT(
      prefixes, ::testing::ElementsAre(::testing::Pair("prefix/", true))
  );
}

TEST_F(TestSFSList, roll_up_multi_prefixes) {
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("a/1"), make_dentry_with_name("b/2"),
      make_dentry_with_name("c/3")};
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "/", objects, prefixes, out);
  EXPECT_EQ(prefixes.size(), 3);
  EXPECT_EQ(out.size(), 0);
  EXPECT_THAT(
      prefixes, ::testing::ElementsAre(
                    ::testing::Pair("a/", true), ::testing::Pair("b/", true),
                    ::testing::Pair("c/", true)
                )
  );
}

TEST_F(TestSFSList, roll_up_empty_delimiter_prefix_is_copy) {
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("a"), make_dentry_with_name("b"),
      make_dentry_with_name("c")};
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "", objects, prefixes, out);
  ASSERT_EQ(prefixes.size(), 0);
  ASSERT_EQ(out.size(), objects.size());
  for (size_t i = 0; i < objects.size(); i++) {
    EXPECT_EQ(objects[i].key.name, out[i].key.name);
  }
}

TEST_F(TestSFSList, roll_up_starts_after_prefix) {
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("prefix/xxx"),
      make_dentry_with_name("prefix/yyy/0"),
      make_dentry_with_name("something/else")};
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("prefix/", "/", objects, prefixes, out);
  ASSERT_EQ(prefixes.size(), 1);
  EXPECT_THAT(
      prefixes, ::testing::ElementsAre(::testing::Pair("prefix/yyy/", true))
  );
  EXPECT_EQ(out[0].key.name, "prefix/xxx");
}

TEST_F(TestSFSList, roll_up_a_multichar_delimiters_work) {
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("sample.foo"),
      make_dentry_with_name("photosDeLiM2006DeLiMJanuaryDeLiMsample.jpg"),
      make_dentry_with_name("photosDeLiM2006DeLiMFebruaryDeLiMsample2.jpg"),
      make_dentry_with_name("photosDeLiM2006DeLiMFebruaryDeLiMsample3.jpg"),
      make_dentry_with_name("photosDeLiM2006DeLiMFebruaryDeLiMsample4.jpg")};
  const auto expected_still_exists = objects[0];
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "DeLiM", objects, prefixes, out);
  EXPECT_EQ(prefixes.size(), 1);
  EXPECT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].key.name, expected_still_exists.key.name);
  EXPECT_THAT(
      prefixes, ::testing::ElementsAre(::testing::Pair("photosDeLiM", true))
  );
}

TEST_F(TestSFSList, roll_up_delim_must_follow_prefix) {
  const auto uut = make_uut();
  const std::vector<rgw_bucket_dir_entry> objects{
      make_dentry_with_name("prefix"), make_dentry_with_name("prefixDELIM"),
      make_dentry_with_name("prefixDELIMsomething"),
      make_dentry_with_name("prefixSOMETHING")};
  std::map<std::string, bool> prefixes;
  std::vector<rgw_bucket_dir_entry> out;

  uut.roll_up_common_prefixes("", "DELIM", objects, prefixes, out);
  ASSERT_EQ(prefixes.size(), 1);
  EXPECT_THAT(
      prefixes, ::testing::ElementsAre(::testing::Pair("prefixDELIM", true))
  );
  ASSERT_EQ(out.size(), 2);
  EXPECT_EQ(out[0].key.name, "prefix");
  EXPECT_EQ(out[1].key.name, "prefixSOMETHING");
}
