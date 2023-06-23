// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <memory>
#include <random>
#include <stdexcept>

#include "common/ceph_context.h"
#include "driver/sfs/object_state.h"
#include "driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "driver/sfs/sqlite/versioned_object/versioned_object_definitions.h"
#include "driver/sfs/version_type.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_conversions.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/driver/sfs/types.h"
#include "rgw/rgw_sal_sfs.h"
#include "rgw_common.h"
#include "rgw_placement_types.h"

using namespace rgw::sal::sfs;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

class TestSFSObjectStateMachine : public ::testing::Test {
 protected:
  CephContext* cct;

  std::unique_ptr<rgw::sal::SFStore> store;
  sqlite::DBOPBucketInfo db_binfo;
  RGWUserInfo bucket_owner;
  BucketRef bucket;

  void SetUp() override {
    cct = (new CephContext(CEPH_ENTITY_TYPE_ANY))->get();
    cct->_conf.set_val("rgw_sfs_data_path", getTestDir());
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
    store.reset(new rgw::sal::SFStore(cct, getTestDir()));
    sqlite::SQLiteUsers users(dbconn());
    sqlite::DBOPUserInfo user;
    user.uinfo.user_id.id = "testuser";
    user.uinfo.display_name = "display_name";
    users.store_user(user);

    sqlite::SQLiteBuckets db_buckets(dbconn());
    db_binfo.binfo.bucket = rgw_bucket("", "testbucket", "1234");
    db_binfo.binfo.owner = rgw_user("testuser");
    db_binfo.binfo.creation_time = ceph::real_clock::now();
    db_binfo.binfo.placement_rule = rgw_placement_rule();
    db_binfo.binfo.zonegroup = "zone";
    db_binfo.deleted = false;
    db_buckets.store_bucket(db_binfo);

    bucket = std::make_shared<Bucket>(
        cct, store.get(), db_binfo.binfo, bucket_owner, db_binfo.battrs
    );
  }

  void TearDown() override {
    fs::current_path(fs::temp_directory_path());
    fs::remove_all(TEST_DIR);
  }

  std::string getTestDir() const {
    auto test_dir = fs::temp_directory_path() / TEST_DIR;
    return test_dir.string();
  }

  fs::path getDBFullPath(const std::string& base_dir) const {
    auto db_full_name = "s3gw.db";
    auto db_full_path = fs::path(base_dir) / db_full_name;
    return db_full_path;
  }

  fs::path getDBFullPath() const { return getDBFullPath(getTestDir()); }
  sqlite::DBConnRef dbconn() { return std::make_shared<sqlite::DBConn>(cct); }
  sqlite::Storage storage() { return dbconn()->get_storage(); }
  ObjectState database_object_state(ObjectRef obj) {
    return storage()
        .select(
            &sqlite::DBVersionedObject::object_state,
            sqlite_orm::where(sqlite_orm::is_equal(
                &sqlite::DBVersionedObject::id, obj->version_id
            ))
        )
        .back();
  }
  VersionType database_version_type(ObjectRef obj) {
    return storage()
        .select(
            &sqlite::DBVersionedObject::version_type,
            sqlite_orm::where(sqlite_orm::is_equal(
                &sqlite::DBVersionedObject::id, obj->version_id
            ))
        )
        .back();
  }
  int database_number_of_versions(ObjectRef obj) {
    return storage()
        .select(
            sqlite_orm::count(),
            sqlite_orm::where(sqlite_orm::is_equal(
                &sqlite::DBVersionedObject::object_id, obj->path.get_uuid()
            ))
        )
        .back();
  }
  auto database_get_versions_as_id_type_state(ObjectRef obj) {
    return storage().select(
        sqlite_orm::columns(
            &sqlite::DBVersionedObject::version_id,
            &sqlite::DBVersionedObject::version_type,
            &sqlite::DBVersionedObject::object_state
        ),
        sqlite_orm::where(sqlite_orm::is_equal(
            &sqlite::DBVersionedObject::object_id, obj->path.get_uuid()
        )),
        sqlite_orm::order_by(&sqlite::DBVersionedObject::id).asc()
    );
  }
};

TEST_F(TestSFSObjectStateMachine, object_start_in_open) {
  const auto object = bucket->create_version(rgw_obj_key("foo", "bar"));
  EXPECT_EQ(database_object_state(object), ObjectState::OPEN);
  EXPECT_EQ(database_version_type(object), VersionType::REGULAR);
  EXPECT_EQ(database_number_of_versions(object), 1);
}

TEST_F(TestSFSObjectStateMachine, multiple_open_versions_are_ok) {
  const int max_obj = 42;
  for (int i = 0; i < max_obj; i++) {
    const auto object = bucket->create_version(
        rgw_obj_key("name", fmt::format("instance_{}", i))
    );
    ASSERT_EQ(database_object_state(object), ObjectState::OPEN);
    ASSERT_EQ(database_number_of_versions(object), i + 1);
  }
}

TEST_F(TestSFSObjectStateMachine, non_committed_objects_are_invisible_to_get) {
  const auto object = bucket->create_version(rgw_obj_key("foo", "bar"));
  EXPECT_THROW(bucket->get(rgw_obj_key("foo", "bar")), UnknownObjectException);
  EXPECT_EQ(database_number_of_versions(object), 1);
}

TEST_F(
    TestSFSObjectStateMachine,
    unversioned__delete_object_soft_deletes_latest_open_version
) {
  const auto object = bucket->create_version(rgw_obj_key("foo"));
  std::string unused;
  ASSERT_TRUE(bucket->delete_object(object, rgw_obj_key("foo"), false, unused));
  EXPECT_EQ(database_object_state(object), ObjectState::DELETED);
  EXPECT_EQ(database_version_type(object), VersionType::REGULAR);
}

TEST_F(
    TestSFSObjectStateMachine,
    unversioned__a_deleted_version_cannot_be_committed
) {
  const auto object = bucket->create_version(rgw_obj_key("foo"));
  std::string unused;
  ASSERT_TRUE(bucket->delete_object(object, rgw_obj_key("foo"), false, unused));
  ASSERT_EQ(database_object_state(object), ObjectState::DELETED);
  EXPECT_FALSE(object->metadata_finish(store.get(), false));
  EXPECT_EQ(database_object_state(object), ObjectState::DELETED);
}

TEST_F(
    TestSFSObjectStateMachine, versioned__a_deleted_version_cannot_be_committed
) {
  const auto object = bucket->create_version(rgw_obj_key("foo", "bar"));
  std::string unused;
  ASSERT_TRUE(
      bucket->delete_object(object, rgw_obj_key("foo", "bar"), false, unused)
  );
  ASSERT_EQ(database_object_state(object), ObjectState::DELETED);
  EXPECT_FALSE(object->metadata_finish(store.get(), true));
  EXPECT_EQ(database_object_state(object), ObjectState::DELETED);
}

TEST_F(
    TestSFSObjectStateMachine, unversioned__commit_deletes_all_committed_versions
) {
  const std::array<ObjectRef, 3> objects = {
      bucket->create_version(rgw_obj_key("foo", "version1")),
      bucket->create_version(rgw_obj_key("foo", "version2")),
      bucket->create_version(rgw_obj_key("foo", "version3")),
  };
  ASSERT_EQ(database_object_state(objects[0]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(objects[0]->metadata_finish(store.get(), false));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::COMMITTED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::OPEN);
}

TEST_F(
    TestSFSObjectStateMachine, unversioned__last_committer_wins
) {
  const std::array<ObjectRef, 3> objects = {
      bucket->create_version(rgw_obj_key("foo", "version1")),
      bucket->create_version(rgw_obj_key("foo", "version2")),
      bucket->create_version(rgw_obj_key("foo", "version3")),
  };
  ASSERT_EQ(database_object_state(objects[0]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(objects[0]->metadata_finish(store.get(), false));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::COMMITTED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(objects[1]->metadata_finish(store.get(), false));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::DELETED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::COMMITTED);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(objects[2]->metadata_finish(store.get(), false));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::DELETED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::DELETED);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::COMMITTED);
}

TEST_F(
    TestSFSObjectStateMachine,
    unversioned__commit_on_deleted_by_another_commit_fails
) {
  const std::array<ObjectRef, 3> objects = {
      bucket->create_version(rgw_obj_key("foo", "version1")),
      bucket->create_version(rgw_obj_key("foo", "version2")),
      bucket->create_version(rgw_obj_key("foo", "version3")),
  };
  ASSERT_TRUE(objects[0]->metadata_finish(store.get(), false));
  ASSERT_EQ(database_object_state(objects[0]), ObjectState::COMMITTED);
  ASSERT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(objects[1]->metadata_finish(store.get(), false));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::DELETED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::COMMITTED);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_FALSE(objects[0]->metadata_finish(store.get(), false));
  ASSERT_EQ(database_object_state(objects[0]), ObjectState::DELETED);
  ASSERT_EQ(database_object_state(objects[1]), ObjectState::COMMITTED);
  ASSERT_EQ(database_object_state(objects[2]), ObjectState::OPEN);
}

TEST_F(
    TestSFSObjectStateMachine,
    versioned__commit_does_not_change_any_other_version_state
) {
  const std::array<ObjectRef, 3> objects = {
      bucket->create_version(rgw_obj_key("foo", "version1")),
      bucket->create_version(rgw_obj_key("foo", "version2")),
      bucket->create_version(rgw_obj_key("foo", "version3"))};
  ASSERT_EQ(database_object_state(objects[0]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(objects[0]->metadata_finish(store.get(), true));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::COMMITTED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(objects[2]->metadata_finish(store.get(), true));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::COMMITTED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::COMMITTED);
}

TEST_F(
    TestSFSObjectStateMachine,
    versioned__delete_does_not_change_any_other_version_state
) {
  std::string unused;
  const std::array<ObjectRef, 3> objects = {
      bucket->create_version(rgw_obj_key("foo", "version1")),
      bucket->create_version(rgw_obj_key("foo", "version2")),
      bucket->create_version(rgw_obj_key("foo", "version3"))};
  ASSERT_TRUE(objects[0]->metadata_finish(store.get(), true));
  ASSERT_EQ(database_object_state(objects[0]), ObjectState::COMMITTED);
  ASSERT_EQ(database_object_state(objects[1]), ObjectState::OPEN);
  ASSERT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(bucket->delete_object(
      objects[1], rgw_obj_key("foo", "version2"), true, unused
  ));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::COMMITTED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::DELETED);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::OPEN);

  EXPECT_TRUE(bucket->delete_object(
      objects[0], rgw_obj_key("foo", "version1"), true, unused
  ));
  EXPECT_EQ(database_object_state(objects[0]), ObjectState::DELETED);
  EXPECT_EQ(database_object_state(objects[1]), ObjectState::DELETED);
  EXPECT_EQ(database_object_state(objects[2]), ObjectState::OPEN);
}

TEST_F(
    TestSFSObjectStateMachine,
    unversioned__delete_soft_deletes_latest_committed_version
) {
  const auto object = bucket->create_version(rgw_obj_key("foo"));
  object->metadata_finish(store.get(), false);
  std::string unused;
  EXPECT_TRUE(bucket->delete_object(object, rgw_obj_key("foo"), false, unused));
  EXPECT_EQ(database_object_state(object), ObjectState::DELETED);
  EXPECT_EQ(database_version_type(object), VersionType::REGULAR);
  EXPECT_EQ(database_number_of_versions(object), 1);
}


class TestSFSVersionedDeleteMarkerTests : public TestSFSObjectStateMachine,
					  public testing::WithParamInterface<ObjectState> {
};

TEST_P(
    TestSFSVersionedDeleteMarkerTests,
    deleting_object_creates_delete_marker
) {
  const auto object = bucket->create_version(rgw_obj_key("foo", "VERSION"));
  const ObjectState initial_state = GetParam();
  switch (initial_state) {
    case ObjectState::COMMITTED:
      object->metadata_finish(store.get(), true);
      break;
    default:
      break;
  }
  ASSERT_EQ(database_object_state(object), initial_state);
  ASSERT_EQ(database_version_type(object), VersionType::REGULAR);

  std::string delete_marker_id;
  ASSERT_TRUE(bucket->delete_object(object, rgw_obj_key("foo"), true, delete_marker_id));
  const auto versions = database_get_versions_as_id_type_state(object);
  EXPECT_FALSE(delete_marker_id.empty());
  ASSERT_EQ(versions.size(), 2);
  EXPECT_EQ(std::get<0>(versions[0]), "VERSION");
  EXPECT_EQ(std::get<1>(versions[0]), VersionType::REGULAR);
  EXPECT_EQ(std::get<2>(versions[0]), initial_state);
  EXPECT_EQ(std::get<0>(versions[1]), delete_marker_id);
  EXPECT_EQ(std::get<1>(versions[1]), VersionType::DELETE_MARKER);
  EXPECT_EQ(std::get<2>(versions[1]), ObjectState::COMMITTED);
}

INSTANTIATE_TEST_SUITE_P(OpenAndCommitted,
			 TestSFSVersionedDeleteMarkerTests,
			testing::Values(ObjectState::OPEN, ObjectState::COMMITTED));

TEST_F(
    TestSFSObjectStateMachine,
    versioned__deleting_version_does_not_create_delete_marker
) {
  const auto object = bucket->create_version(rgw_obj_key("foo", "VERSION"));
  object->metadata_finish(store.get(), false);
  ASSERT_EQ(database_object_state(object), ObjectState::COMMITTED);
  ASSERT_EQ(database_version_type(object), VersionType::REGULAR);

  std::string delete_marker_id;
  EXPECT_TRUE(bucket->delete_object(
      object, rgw_obj_key("foo", "VERSION"), true, delete_marker_id
  ));
  EXPECT_TRUE(object->deleted);
  EXPECT_EQ(database_object_state(object), ObjectState::DELETED);
  EXPECT_EQ(database_version_type(object), VersionType::REGULAR);
  EXPECT_NE(delete_marker_id, object->instance);
  EXPECT_EQ("", delete_marker_id);
  EXPECT_EQ(database_number_of_versions(object), 1);
}

TEST_F(TestSFSObjectStateMachine, non_committed_are_invisible_to_get_all) {
  const auto object = bucket->create_version(rgw_obj_key("foo", "bar"));
  const auto all = bucket->get_all();
  ASSERT_EQ(all.size(), 0);
}

TEST_F(TestSFSObjectStateMachine, metadata_finish_makes_visible_to_get) {
  auto object = bucket->create_version(rgw_obj_key("foo", "bar"));
  object->metadata_finish(store.get(), false);
  auto obj_gotten = bucket->get(rgw_obj_key("foo", "bar"));
  EXPECT_EQ(object->name, obj_gotten->name);
  EXPECT_EQ(object->instance, obj_gotten->instance);
}

TEST_F(TestSFSObjectStateMachine, metadata_finish_makes_visible_to_get_all) {
  auto object = bucket->create_version(rgw_obj_key("foo", "bar"));
  object->metadata_finish(store.get(), false);
  const auto all = bucket->get_all();
  EXPECT_EQ(all.size(), 1);
  EXPECT_EQ(all.front()->name, "foo");
}

TEST_F(TestSFSObjectStateMachine, flush_attrs_does_not_commit) {
  const auto object = bucket->create_version(rgw_obj_key("foo", "bar"));
  object->metadata_flush_attrs(store.get());
  ASSERT_EQ(database_object_state(object), ObjectState::OPEN);
}
