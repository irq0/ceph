// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "sqlite_versioned_objects.h"

#include <stdexcept>

#include "driver/sfs/object_state.h"
#include "rgw/driver/sfs/uuid_path.h"
#include "versioned_object/versioned_object_definitions.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteVersionedObjects::SQLiteVersionedObjects(DBConnRef _conn) : conn(_conn) {}

std::optional<DBVersionedObject> SQLiteVersionedObjects::get_versioned_object(
    uint id, bool filter_deleted
) const {
  auto storage = conn->get_storage();
  auto object = storage.get_pointer<DBVersionedObject>(id);
  std::optional<DBVersionedObject> ret_value;
  if (object) {
    if (!filter_deleted || object->object_state != ObjectState::DELETED) {
      ret_value = *object;
    }
  }
  return ret_value;
}

std::optional<DBVersionedObject> SQLiteVersionedObjects::get_versioned_object(
    const std::string& version_id, bool filter_deleted
) const {
  auto storage = conn->get_storage();
  auto versioned_objects = storage.get_all<DBVersionedObject>(
      where(c(&DBVersionedObject::version_id) = version_id)
  );
  ceph_assert(versioned_objects.size() <= 1);
  std::optional<DBVersionedObject> ret_value;
  if (versioned_objects.size()) {
    if (!filter_deleted ||
        versioned_objects[0].object_state != ObjectState::DELETED) {
      ret_value = versioned_objects[0];
    }
  }
  return ret_value;
}

std::optional<DBVersionedObject>
SQLiteVersionedObjects::get_committed_versioned_object(
    const std::string& bucket_id, const std::string& object_name,
    const std::string& version_id
) const {
  if (version_id.empty()) {
    return get_committed_versioned_object_last_version(bucket_id, object_name);
  }
  return get_committed_versioned_object_specific_version(
      bucket_id, object_name, version_id
  );
}

DBObjectsListItems SQLiteVersionedObjects::list_last_versioned_objects(
    const std::string& bucket_id
) const {
  auto storage = conn->get_storage();
  auto results = storage.select(
      columns(
          &DBObject::uuid, &DBObject::name, &DBVersionedObject::version_id,
          max(&DBVersionedObject::commit_time), max(&DBVersionedObject::id),
          &DBVersionedObject::size, &DBVersionedObject::etag,
          &DBVersionedObject::mtime, &DBVersionedObject::delete_time,
          &DBVersionedObject::attrs, &DBVersionedObject::version_type,
          &DBVersionedObject::object_state
      ),
      inner_join<DBObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBObject::bucket_id, bucket_id) and
          is_not_equal(&DBVersionedObject::object_state, ObjectState::DELETED)
      ),
      group_by(&DBObject::uuid), order_by(&DBVersionedObject::create_time).asc()
  );
  return results;
}

uint SQLiteVersionedObjects::insert_versioned_object(
    const DBVersionedObject& object
) const {
  auto storage = conn->get_storage();
  return storage.insert(object);
}

void SQLiteVersionedObjects::store_versioned_object(
    const DBVersionedObject& object
) const {
  auto storage = conn->get_storage();
  storage.update(object);
}

bool SQLiteVersionedObjects::store_versioned_object_if_state(
    const DBVersionedObject& object, std::vector<ObjectState> allowed_states
) const {
  auto storage = conn->get_storage();
  auto transaction = storage.transaction_guard();
  const std::vector<int> version_in_allowed_state = storage.select(
      count(&DBVersionedObject::id),
      where(
          in(&DBVersionedObject::object_state, allowed_states) and
          is_equal(&DBVersionedObject::id, object.id)
      )
  );
  if (version_in_allowed_state[0] == 0) {
    // TODO(https://github.com/aquarist-labs/s3gw/issues/563) error handling
    // throw exception (will be caught later in the sfs logic)
    transaction.rollback();
    return false;
  }
  storage.update(object);
  transaction.commit();
  return true;
}

void SQLiteVersionedObjects::store_versioned_object_delete_rest_transact(
    const DBVersionedObject& object
) const {
  try {
    auto storage = conn->get_storage();
    auto transaction = storage.transaction_guard();
    storage.update(object);
    // soft delete the rest of this object
    storage.update_all(
        set(c(&DBVersionedObject::object_state) = ObjectState::DELETED),
        where(
            is_equal(&DBVersionedObject::object_id, object.object_id) and
            is_not_equal(&DBVersionedObject::id, object.id)
        )
    );
    transaction.commit();
  } catch (const std::system_error& e) {
    // TODO(https://github.com/aquarist-labs/s3gw/issues/563) error handling
    // throw exception (will be caught later in the sfs logic)
    throw(e);
  }
}

bool SQLiteVersionedObjects::
    store_versioned_object_delete_committed_transact_if_state(
        const DBVersionedObject& object, std::vector<ObjectState> allowed_states
    ) const {
  try {
    auto storage = conn->get_storage();
    auto transaction = storage.transaction_guard();
    const auto version_in_allowed_state = storage.select(
        count(), where(
                     in(&DBVersionedObject::object_state, allowed_states) and
                     is_equal(&DBVersionedObject::id, object.id)
                 )
    );

    if (version_in_allowed_state[0] == 0) {
      // TODO(https://github.com/aquarist-labs/s3gw/issues/563) error handling
      // throw exception (will be caught later in the sfs logic)
      transaction.rollback();
      return false;
    }
    storage.update(object);
    // soft delete all other _COMMITTED_ versions. Leave OPEN versions
    // alone, as they may be an in progress write racing us.
    storage.update_all(
        set(c(&DBVersionedObject::object_state) = ObjectState::DELETED),
        where(
            is_equal(&DBVersionedObject::object_id, object.object_id) and
            is_equal(
                &DBVersionedObject::object_state, ObjectState::COMMITTED
            ) and
            is_not_equal(&DBVersionedObject::id, object.id)
        )
    );
    transaction.commit();
  } catch (const std::system_error& e) {
    // TODO(https://github.com/aquarist-labs/s3gw/issues/563) error handling
    // throw exception (will be caught later in the sfs logic)
    return false;
  }
  return true;
}

void SQLiteVersionedObjects::remove_versioned_object(uint id) const {
  auto storage = conn->get_storage();
  storage.remove<DBVersionedObject>(id);
}

std::vector<uint> SQLiteVersionedObjects::get_versioned_object_ids(
    bool filter_deleted
) const {
  auto storage = conn->get_storage();
  if (filter_deleted) {
    return storage.select(
        &DBVersionedObject::id,
        where(
            is_not_equal(&DBVersionedObject::object_state, ObjectState::DELETED)
        )
    );
  }
  return storage.select(&DBVersionedObject::id);
}

std::vector<uint> SQLiteVersionedObjects::get_versioned_object_ids(
    const uuid_d& object_id, bool filter_deleted
) const {
  auto storage = conn->get_storage();
  auto uuid = object_id.to_string();
  if (filter_deleted) {
    return storage.select(
        &DBVersionedObject::id,
        where(
            is_equal(&DBVersionedObject::object_id, uuid) and
            is_not_equal(&DBVersionedObject::object_state, ObjectState::DELETED)
        )
    );
  }
  return storage.select(
      &DBVersionedObject::id, where(c(&DBVersionedObject::object_id) = uuid)
  );
}

std::vector<DBVersionedObject> SQLiteVersionedObjects::get_versioned_objects(
    const uuid_d& object_id, bool filter_deleted
) const {
  auto storage = conn->get_storage();
  auto uuid = object_id.to_string();
  if (filter_deleted) {
    return storage.get_all<DBVersionedObject>(
        where(
            is_equal(&DBVersionedObject::object_id, uuid) and
            is_not_equal(&DBVersionedObject::object_state, ObjectState::DELETED)
        ),
        order_by(&DBVersionedObject::commit_time).desc()
    );
  }
  return storage.get_all<DBVersionedObject>(
      where(c(&DBVersionedObject::object_id) = uuid)
  );
}

std::optional<DBVersionedObject>
SQLiteVersionedObjects::get_last_versioned_object(
    const uuid_d& object_id, bool filter_deleted
) const {
  auto storage = conn->get_storage();
  std::vector<std::tuple<uint, std::unique_ptr<ceph::real_time>>>
      max_commit_time_ids;
  // we are looking for the ids that match the object_id with the highest
  // commit_time and we want to get the highest id.
  if (filter_deleted) {
    max_commit_time_ids = storage.select(
        columns(&DBVersionedObject::id, max(&DBVersionedObject::commit_time)),
        where(
            is_equal(&DBVersionedObject::object_id, object_id) and
            is_not_equal(&DBVersionedObject::object_state, ObjectState::DELETED)
        ),
        group_by(&DBVersionedObject::id),
        order_by(&DBVersionedObject::id).desc()
    );
  } else {
    max_commit_time_ids = storage.select(
        columns(&DBVersionedObject::id, max(&DBVersionedObject::commit_time)),
        where(is_equal(&DBVersionedObject::object_id, object_id)),
        group_by(&DBVersionedObject::id),
        order_by(&DBVersionedObject::id).desc()
    );
  }

  // if found, value we are looking for is in the first position of the results
  // because we ordered descending in the query
  auto found_value = max_commit_time_ids.size() &&
                     std::get<1>(max_commit_time_ids[0]) != nullptr;

  std::optional<DBVersionedObject> ret_value;
  if (found_value) {
    auto last_version_id = std::get<0>(max_commit_time_ids[0]);
    auto last_version = storage.get_pointer<DBVersionedObject>(last_version_id);
    if (last_version) {
      ret_value = *last_version;
    }
  }
  return ret_value;
}

std::optional<DBVersionedObject>
SQLiteVersionedObjects::delete_version_and_get_previous_transact(uint id) {
  try {
    auto storage = conn->get_storage();
    auto transaction = storage.transaction_guard();
    auto version = storage.get_pointer<DBVersionedObject>(id);
    std::optional<DBVersionedObject> ret_value;
    if (version != nullptr) {
      auto object_id = version->object_id;
      storage.remove<DBVersionedObject>(id);
      // get the last version of the object now
      auto max_commit_time_ids = storage.select(
          columns(&DBVersionedObject::id, max(&DBVersionedObject::commit_time)),
          where(
              is_equal(&DBVersionedObject::object_id, object_id) and
              is_not_equal(
                  &DBVersionedObject::object_state, ObjectState::DELETED
              )
          ),
          group_by(&DBVersionedObject::id),
          order_by(&DBVersionedObject::id).desc()
      );
      auto found_value = max_commit_time_ids.size() &&
                         std::get<1>(max_commit_time_ids[0]) != nullptr;
      if (found_value) {
        // if not value is found could be, for example, if lifecycle deleted all
        // non current versions before.
        auto last_version_id = std::get<0>(max_commit_time_ids[0]);
        auto last_version =
            storage.get_pointer<DBVersionedObject>(last_version_id);
        if (last_version) {
          ret_value = *last_version;
        }
      }
      transaction.commit();
    }
    return ret_value;
  } catch (const std::system_error& e) {
    // throw exception (will be caught later in the sfs logic)
    // TODO revisit this when error handling is defined
    throw(e);
  }
}

uint SQLiteVersionedObjects::add_delete_marker_transact(
    const uuid_d& object_id, const std::string& delete_marker_id, bool& added
) const {
  uint ret_id{0};
  added = false;
  try {
    auto storage = conn->get_storage();
    auto transaction = storage.transaction_guard();
    auto max_commit_time_ids = storage.select(
        columns(&DBVersionedObject::id, max(&DBVersionedObject::commit_time)),
        where(
            is_equal(&DBVersionedObject::object_id, object_id) and
            is_not_equal(&DBVersionedObject::object_state, ObjectState::DELETED)
        ),
        group_by(&DBVersionedObject::id),
        order_by(&DBVersionedObject::id).desc()
    );
    // if found, value we are looking for is in the first position of the results
    // because we ordered descending in the query
    auto found_value = max_commit_time_ids.size() &&
                       std::get<1>(max_commit_time_ids[0]) != nullptr;
    if (found_value) {
      auto last_version_id = std::get<0>(max_commit_time_ids[0]);
      auto last_version =
          storage.get_pointer<DBVersionedObject>(last_version_id);
      if (last_version &&
          (last_version->object_state == ObjectState::COMMITTED ||
           last_version->object_state == ObjectState::OPEN) &&
          last_version->version_type == VersionType::REGULAR) {
        auto now = ceph::real_clock::now();
        last_version->version_type = VersionType::DELETE_MARKER;
        last_version->object_state = ObjectState::COMMITTED;
        last_version->delete_time = now;
        last_version->mtime = now;
        last_version->version_id = delete_marker_id;
        ret_id = storage.insert(*last_version);
        added = true;
        // only commit if the delete maker was indeed inserted.
        // the rest of calls in this transaction are read operations
        transaction.commit();
      }
    }
  } catch (const std::system_error& e) {
    // throw exception (will be caught later in the sfs logic)
    // TODO revisit this when error handling is defined
    throw(e);
  }
  return ret_id;
}

std::optional<DBVersionedObject>
SQLiteVersionedObjects::get_committed_versioned_object_specific_version(
    const std::string& bucket_id, const std::string& object_name,
    const std::string& version_id
) const {
  auto storage = conn->get_storage();
  auto ids = storage.select(
      &DBVersionedObject::id,
      inner_join<DBObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED) and
          is_equal(&DBObject::bucket_id, bucket_id) and
          is_equal(&DBObject::name, object_name) and
          is_equal(&DBVersionedObject::version_id, version_id)
      )
  );
  // TODO return an error if this returns more than 1 version?
  // Only 1 object with no deleted versions should be present
  // revisit this ceph_assert after error handling is defined
  ceph_assert(ids.size() <= 1);
  std::optional<DBVersionedObject> ret_value;
  if (ids.size() > 0) {
    auto version = storage.get_pointer<DBVersionedObject>(ids[0]);
    if (version != nullptr) {
      ret_value = *version;
    }
  }
  return ret_value;
}

std::optional<DBVersionedObject>
SQLiteVersionedObjects::get_committed_versioned_object_last_version(
    const std::string& bucket_id, const std::string& object_name
) const {
  // we don't have a version_id, so return the last available one that is
  // committed
  auto storage = conn->get_storage();
  auto max_commit_time_ids = storage.select(
      columns(&DBVersionedObject::id, max(&DBVersionedObject::commit_time)),
      inner_join<DBObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBObject::bucket_id, bucket_id) and
          is_equal(&DBObject::name, object_name) and
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED)
      ),
      group_by(&DBVersionedObject::id), order_by(&DBVersionedObject::id).desc()
  );
  auto found_value = max_commit_time_ids.size() &&
                     std::get<1>(max_commit_time_ids[0]) != nullptr;
  std::optional<DBVersionedObject> ret_value;
  if (found_value) {
    // if not value is found could be, for example, if lifecycle deleted all
    // non current versions before.
    auto last_version_id = std::get<0>(max_commit_time_ids[0]);
    auto last_version = storage.get_pointer<DBVersionedObject>(last_version_id);
    if (last_version) {
      ret_value = *last_version;
    }
  }
  return ret_value;
}

std::optional<DBVersionedObject>
SQLiteVersionedObjects::create_new_versioned_object_transact(
    const std::string& bucket_id, const std::string& object_name,
    const std::string& version_id
) const {
  std::optional<DBVersionedObject> ret_value;
  try {
    auto storage = conn->get_storage();
    auto transaction = storage.transaction_guard();
    auto objs = storage.select(
        columns(&DBObject::uuid),
        inner_join<DBVersionedObject>(
            on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
        ),
        where(
            is_not_equal(
                &DBVersionedObject::object_state, ObjectState::DELETED
            ) and
            is_equal(&DBObject::bucket_id, bucket_id) and
            is_equal(&DBObject::name, object_name)
        ),
        group_by(&DBObject::uuid)
    );
    // should return none or 1
    // TODO revisit this ceph_assert after error handling is defined
    ceph_assert(objs.size() <= 1);
    DBObject obj;
    obj.name = object_name;
    obj.bucket_id = bucket_id;
    if (objs.size() == 0) {
      // object does not exist
      // create it
      obj.uuid.generate_random();
      storage.replace(obj);
    } else {
      obj.uuid = std::get<0>(objs[0]);
    }
    // create the version now
    DBVersionedObject version;
    version.object_id = obj.uuid;
    version.object_state = ObjectState::OPEN;
    version.version_type = VersionType::REGULAR;
    version.version_id = version_id;
    version.create_time = ceph::real_clock::now();
    version.id = storage.insert(version);
    transaction.commit();
    ret_value = version;
  } catch (const std::system_error& e) {
    // throw exception (will be caught later in the sfs logic)
    // TODO revisit this when error handling is defined
    throw(e);
  }
  return ret_value;
}

}  // namespace rgw::sal::sfs::sqlite
