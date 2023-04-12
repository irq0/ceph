// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
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
#include "sqlite_lifecycle.h"

using namespace sqlite_orm;
namespace rgw::sal::sfs::sqlite {

SQLiteLifecycle::SQLiteLifecycle(DBConnRef _conn) : conn(_conn) {}

DBOPLCHead SQLiteLifecycle::get_head(const std::string& oid) const {
  auto storage = conn->get_storage();
  auto head = storage->get_pointer<DBOPLCHead>(oid);
  DBOPLCHead ret_value;
  if (head) {
    ret_value = *head;
  } else {
    // there's still no head.
    // LC was not executed yet.
    // create an empty entry
    DBOPLCHead new_head{oid, "", 0};
    storage->replace(new_head);
    ret_value = new_head;
  }
  return ret_value;
}

void SQLiteLifecycle::store_head(const DBOPLCHead& head) const {
  auto storage = conn->get_storage();
  storage->replace(head);
}

void SQLiteLifecycle::remove_head(const std::string& oid) const {
  auto storage = conn->get_storage();
  storage->remove<DBOPLCHead>(oid);
}

std::optional<DBOPLCEntry> SQLiteLifecycle::get_entry(
    const std::string& oid, const std::string& marker
) const {
  auto storage = conn->get_storage();
  auto db_entry = storage->get_pointer<DBOPLCEntry>(oid, marker);
  std::optional<DBOPLCEntry> ret_value;
  if (db_entry) {
    ret_value = *db_entry;
  }
  return ret_value;
}

std::optional<DBOPLCEntry> SQLiteLifecycle::get_next_entry(
    const std::string& oid, const std::string& marker
) const {
  auto storage = conn->get_storage();
  auto db_entries = storage->get_all<DBOPLCEntry>(
      where(
          is_equal(&DBOPLCEntry::lc_index, oid) and
          greater_than(&DBOPLCEntry::bucket_name, marker)
      ),
      order_by(&DBOPLCEntry::bucket_name).asc(), limit(1)
  );
  std::optional<DBOPLCEntry> ret_value;
  // should return 1 entry or none
  if (db_entries.size() == 1) {
    ret_value = db_entries[0];
  }
  return ret_value;
}

void SQLiteLifecycle::store_entry(const DBOPLCEntry& entry) const {
  auto storage = conn->get_storage();
  storage->replace(entry);
}

void SQLiteLifecycle::remove_entry(
    const std::string& oid, const std::string& marker
) const {
  auto storage = conn->get_storage();
  storage->remove<DBOPLCEntry>(oid, marker);
}

std::vector<DBOPLCEntry> SQLiteLifecycle::list_entries(
    const std::string& oid, const std::string& marker, uint32_t max_entries
) const {
  auto storage = conn->get_storage();
  return storage->get_all<DBOPLCEntry>(
      where(
          is_equal(&DBOPLCEntry::lc_index, oid) and
          greater_than(&DBOPLCEntry::bucket_name, marker)
      ),
      order_by(&DBOPLCEntry::bucket_name).asc(), limit(max_entries)
  );
}

}  // namespace rgw::sal::sfs::sqlite
