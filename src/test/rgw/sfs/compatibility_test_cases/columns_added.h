#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/conversion_utils.h"

using namespace rgw::sal::sfs::sqlite;

namespace rgw::test::metadata::columns_added {

struct DBTestUser {
  std::string user_id;
  std::optional<std::string> tenant;
  std::optional<std::string> ns;
  std::optional<std::string> display_name;
  std::optional<std::string> user_email;
  std::optional<BLOB> access_keys;
  std::optional<BLOB> swift_keys;
  std::optional<BLOB> sub_users;
  std::optional<unsigned char> suspended;
  std::optional<int> max_buckets;
  std::optional<int> op_mask;
  std::optional<BLOB> user_caps;
  std::optional<int> admin;
  std::optional<int> system;
  std::optional<std::string> placement_name;
  std::optional<std::string> placement_storage_class;
  std::optional<BLOB> placement_tags;
  std::optional<BLOB> bucke_quota;
  std::optional<BLOB> temp_url_keys;
  std::optional<BLOB> user_quota;
  std::optional<int> type;
  std::optional<BLOB> mfa_ids;
  std::optional<std::string> assumed_role_arn;
  std::optional<BLOB> user_attrs;
  std::optional<int> user_version;
  std::optional<std::string> user_version_tag;
};

struct DBTestAccessKey {
  int id;
  std::string access_key;
  std::string user_id;
};

struct DBTestBucket {
  std::string bucket_name;
  std::string bucket_id;
  std::optional<std::string> tenant;
  std::optional<std::string> marker;
  std::optional<int> size;
  std::optional<int> size_rounded;
  std::optional<BLOB> creation_time;
  std::optional<int> count;
  std::optional<std::string> placement_name;
  std::optional<std::string> placement_storage_class;
  std::string owner_id;
  std::optional<uint32_t> flags;
  std::optional<std::string> zone_group;
  std::optional<bool> has_instance_obj;
  std::optional<BLOB> quota;
  std::optional<bool> requester_pays;
  std::optional<bool> has_website;
  std::optional<BLOB> website_conf;
  std::optional<bool> swift_versioning;
  std::optional<std::string> swift_ver_location;
  std::optional<BLOB> mdsearch_config;
  std::optional<std::string> new_bucket_instance_id;
  std::optional<BLOB> object_lock;
  std::optional<BLOB> sync_policy_info_groups;
  std::optional<BLOB> bucket_attrs;
  std::optional<int> bucket_version;
  std::optional<std::string> bucket_version_tag;
  std::optional<BLOB> mtime;
  // this column will be added
  // bool deleted;
};

struct DBTestObject {
  std::string object_id;
  std::string bucket_id;
  std::string name;
  std::optional<size_t> size;
  std::optional<std::string> etag;
  std::optional<BLOB> mtime;
  std::optional<BLOB> set_mtime;
  std::optional<BLOB> delete_at_time;
  std::optional<BLOB> attrs;
  std::optional<BLOB> acls;
};

struct DBTestVersionedObject {
  uint id;
  std::string object_id;
  std::string checksum;
  BLOB deletion_time;
  size_t size;
  BLOB creation_time;
  uint object_state;
  std::string version_id;
  std::string etag;
};

inline auto _make_test_storage(const std::string &path) {
  return sqlite_orm::make_storage(path,
    sqlite_orm::make_table(std::string(USERS_TABLE),
          sqlite_orm::make_column("user_id", &DBTestUser::user_id, sqlite_orm::primary_key()),
          sqlite_orm::make_column("tenant", &DBTestUser::tenant),
          sqlite_orm::make_column("ns", &DBTestUser::ns),
          sqlite_orm::make_column("display_name", &DBTestUser::display_name),
          sqlite_orm::make_column("user_email", &DBTestUser::user_email),
          sqlite_orm::make_column("access_keys", &DBTestUser::access_keys),
          sqlite_orm::make_column("swift_keys", &DBTestUser::swift_keys),
          sqlite_orm::make_column("sub_users", &DBTestUser::sub_users),
          sqlite_orm::make_column("suspended", &DBTestUser::suspended),
          sqlite_orm::make_column("max_buckets", &DBTestUser::max_buckets),
          sqlite_orm::make_column("op_mask", &DBTestUser::op_mask),
          sqlite_orm::make_column("user_caps", &DBTestUser::user_caps),
          sqlite_orm::make_column("admin", &DBTestUser::admin),
          sqlite_orm::make_column("system", &DBTestUser::system),
          sqlite_orm::make_column("placement_name", &DBTestUser::placement_name),
          sqlite_orm::make_column("placement_storage_class", &DBTestUser::placement_storage_class),
          sqlite_orm::make_column("placement_tags", &DBTestUser::placement_tags),
          sqlite_orm::make_column("bucke_quota", &DBTestUser::bucke_quota),
          sqlite_orm::make_column("temp_url_keys", &DBTestUser::temp_url_keys),
          sqlite_orm::make_column("user_quota", &DBTestUser::user_quota),
          sqlite_orm::make_column("type", &DBTestUser::type),
          sqlite_orm::make_column("mfa_ids", &DBTestUser::mfa_ids),
          sqlite_orm::make_column("assumed_role_arn", &DBTestUser::assumed_role_arn),
          sqlite_orm::make_column("user_attrs", &DBTestUser::user_attrs),
          sqlite_orm::make_column("user_version", &DBTestUser::user_version),
          sqlite_orm::make_column("user_version_tag", &DBTestUser::user_version_tag)),
    sqlite_orm::make_table(std::string(BUCKETS_TABLE),
          sqlite_orm::make_column("bucket_id", &DBTestBucket::bucket_id, sqlite_orm::primary_key()),
          sqlite_orm::make_column("bucket_name", &DBTestBucket::bucket_name),
          sqlite_orm::make_column("tenant", &DBTestBucket::tenant),
          sqlite_orm::make_column("marker", &DBTestBucket::marker),
          sqlite_orm::make_column("owner_id", &DBTestBucket::owner_id),
          sqlite_orm::make_column("flags", &DBTestBucket::flags),
          sqlite_orm::make_column("zone_group", &DBTestBucket::zone_group),
          sqlite_orm::make_column("quota", &DBTestBucket::quota),
          sqlite_orm::make_column("creation_time", &DBTestBucket::creation_time),
          sqlite_orm::make_column("placement_name", &DBTestBucket::placement_name),
          sqlite_orm::make_column("placement_storage_class", &DBTestBucket::placement_storage_class),
          // this column will be added
          // sqlite_orm::make_column("deleted", &DBTestBucket::deleted),
          sqlite_orm::make_column("bucket_attrs", &DBTestBucket::bucket_attrs),
          sqlite_orm::foreign_key(&DBTestBucket::owner_id).references(&DBTestUser::user_id)),
    sqlite_orm::make_table(std::string(OBJECTS_TABLE),
          sqlite_orm::make_column("object_id", &DBTestObject::object_id, sqlite_orm::primary_key()),
          sqlite_orm::make_column("bucket_id", &DBTestObject::bucket_id),
          sqlite_orm::make_column("name", &DBTestObject::name),
          sqlite_orm::make_column("size", &DBTestObject::size),
          sqlite_orm::make_column("etag", &DBTestObject::etag),
          sqlite_orm::make_column("mtime", &DBTestObject::mtime),
          sqlite_orm::make_column("set_mtime", &DBTestObject::set_mtime),
          sqlite_orm::make_column("delete_at_time", &DBTestObject::delete_at_time),
          sqlite_orm::make_column("attrs", &DBTestObject::attrs),
          sqlite_orm::make_column("acls", &DBTestObject::acls),
          sqlite_orm::foreign_key(&DBTestObject::bucket_id).references(&DBTestBucket::bucket_id)),
    sqlite_orm::make_table(std::string(VERSIONED_OBJECTS_TABLE),
          sqlite_orm::make_column("id", &DBTestVersionedObject::id, sqlite_orm::autoincrement(), sqlite_orm::primary_key()),
          sqlite_orm::make_column("object_id", &DBTestVersionedObject::object_id),
          sqlite_orm::make_column("checksum", &DBTestVersionedObject::checksum),
          sqlite_orm::make_column("deletion_time", &DBTestVersionedObject::deletion_time),
          sqlite_orm::make_column("size", &DBTestVersionedObject::size),
          sqlite_orm::make_column("creation_time", &DBTestVersionedObject::creation_time),
          sqlite_orm::make_column("object_state", &DBTestVersionedObject::object_state),
          sqlite_orm::make_column("version_id", &DBTestVersionedObject::version_id),
          sqlite_orm::make_column("etag", &DBTestVersionedObject::etag),
          sqlite_orm::foreign_key(&DBTestVersionedObject::object_id).references(&DBTestObject::object_id)),
    sqlite_orm::make_table(std::string(ACCESS_KEYS),
          sqlite_orm::make_column("id", &DBAccessKey::id, sqlite_orm::autoincrement(), sqlite_orm::primary_key()),
          sqlite_orm::make_column("access_key", &DBAccessKey::access_key),
          sqlite_orm::make_column("user_id", &DBAccessKey::user_id),
          sqlite_orm::foreign_key(&DBAccessKey::user_id).references(&DBTestUser::user_id))
  );
}

using TestStorage = decltype(_make_test_storage(""));

struct TestDB {
  TestStorage storage;
  DBTestUser test_user;
  DBTestBucket test_bucket;
  DBTestObject test_object;
  DBTestVersionedObject test_version;

  explicit TestDB(const std::string & db_full_path)
    : storage(_make_test_storage(db_full_path))
  {
    storage.open_forever();
    storage.busy_timeout(5000);
    storage.sync_schema();
  }

  DBTestUser get_test_user() {
    DBTestUser user;
    user.user_id = "user1";
    return user;
  }

  DBTestBucket get_test_bucket() {
    DBTestBucket bucket;
    bucket.bucket_id = "bucket1_id";
    bucket.bucket_name = "bucket1";
    bucket.owner_id = "user1";
    return bucket;
  }

  DBTestObject get_test_object() {
    DBTestObject object;
    object.object_id = "9f06d9d3-307f-4c98-865b-cd3b087acc4f";
    object.bucket_id = "bucket1_id";
    object.name = "object_name";
    return object;
  }

  DBTestVersionedObject get_test_version() {
    DBTestVersionedObject version;
    version.id = 1;
    version.object_id = "9f06d9d3-307f-4c98-865b-cd3b087acc4f";
    version.checksum = "checksum1";
    encode_blob(ceph::real_clock::now(), version.deletion_time);
    version.size = rand();
    encode_blob(ceph::real_clock::now(), version.creation_time);
    version.object_state = 2;
    version.version_id = "version_id_1";
    return version;
  }

  bool compareUser(const DBTestUser & user1, const DBTestUser & user2) {
    if (user1.user_id != user2.user_id) return false;
    return true;
  }

  bool compareBucket(const DBTestBucket & bucket1, const DBTestBucket & bucket2) {
    if (bucket1.bucket_id != bucket2.bucket_id) return false;
    if (bucket1.bucket_name != bucket2.bucket_name) return false;
    if (bucket1.owner_id != bucket2.owner_id) return false;
    return true;
  }

  bool compareObject(const DBTestObject & obj1, const DBTestObject & obj2) {
    if (obj1.object_id != obj2.object_id) return false;
    if (obj1.bucket_id != obj2.bucket_id) return false;
    if (obj1.name != obj2.name) return false;
    return true;
  }

  bool compareVersion(const DBTestVersionedObject & v1,
                      const DBTestVersionedObject & v2) {
    if (v1.version_id != v2.version_id) return false;
    if (v1.object_id != v2.object_id) return false;
    if (v1.checksum != v2.checksum) return false;
    if (v1.deletion_time != v2.deletion_time) return false;
    if (v1.size != v2.size) return false;
    if (v1.creation_time != v2.creation_time) return false;
    if (v1.object_state != v2.object_state) return false;
    if (v1.version_id != v2.version_id) return false;
    return true;
  }

  void addData() {
    test_user = get_test_user();
    storage.replace(test_user);

    test_bucket = get_test_bucket();
    storage.replace(test_bucket);

    test_object = get_test_object();
    storage.replace(test_object);

    test_version = get_test_version();
    storage.insert(test_version);
  }

  bool checkDataExists() {
    auto buckets = storage.get_all<DBTestBucket>();
    if (buckets.size() != 1) return false;

    auto users = storage.get_all<DBTestUser>();
    if (users.size() != 1) return false;

    auto objs = storage.get_all<DBTestObject>();
    if (objs.size() != 1) return false;

    auto versions = storage.get_all<DBTestVersionedObject>();
    if (versions.size() != 1) return false;

    auto bucket = storage.get_pointer<DBTestBucket>(test_bucket.bucket_id);
    if (!bucket) return false;
    if (!compareBucket(*bucket, test_bucket)) return false;

    auto user = storage.get_pointer<DBTestUser>(test_user.user_id);
    if (!user) return false;
    if (!compareUser(*user, test_user)) return false;

    auto object = storage.get_pointer<DBTestObject>(test_object.object_id);
    if (!object) return false;
    if (!compareObject(*object, test_object)) return false;

    auto version = storage.get_pointer<DBTestVersionedObject>(test_version.id);
    if (!version) return false;
    if (!compareVersion(*version, test_version)) return false;

    return true;
  }

};

} //  namespace rgw::test::metadata::new_columns_added
