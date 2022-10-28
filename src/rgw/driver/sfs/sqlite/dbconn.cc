#include "dbconn.h"

#include <filesystem>

namespace fs = std::filesystem;
namespace orm = sqlite_orm;

namespace rgw::sal::sfs::sqlite  {

std::string get_temporary_db_path(CephContext *ctt) {
    auto rgw_sfs_path = ctt->_conf.get_val<std::string>("rgw_sfs_data_path");
    auto tmp_db_name = std::string(SCHEMA_DB_NAME) + "_tmp";
    auto db_path =
      std::filesystem::path(rgw_sfs_path) / std::string(tmp_db_name);
    return db_path.string();
}

void DBConn::check_metadata_is_compatible(CephContext *ctt) {
    // create a copy of the actual metadata
    fs::copy(getDBPath(ctt), get_temporary_db_path(ctt));

    // try to sync the storage based on the temporary db
    // in case something goes wrong show possible errors and return
    auto test_storage = _make_storage(get_temporary_db_path(ctt));
    test_storage.open_forever();
    test_storage.busy_timeout(5000);
    bool sync_error = false;
    std::string result_message;
    try {
        auto sync_res = test_storage.sync_schema();
        std::vector<std::string> non_compatible_tables;
        for (auto const& [table_name, sync_result] : sync_res) {
            if (sync_result == orm::sync_schema_result::dropped_and_recreated) {
                // this result is aggresive as it drops the table and
                // recreates it.
                // Data loss is expected and we should warn the user and
                // stop the final sync in the real database.
                result_message += "Table: [" + table_name +
                                  "] is no longer compatible. ";
                non_compatible_tables.push_back(table_name);
            }
        }
        if (non_compatible_tables.size() > 0) {
            sync_error = true;
            result_message = "Tables: [ ";
            for (auto const& table: non_compatible_tables) {
                result_message += table + " ";
            }
            result_message += "] are no longer compatible.";
        }
    } catch (std::exception & e) {
        // check for any other errors (foreign keys constrains, etc...)
        result_message = "Metadata database might be corrupted or is no longer compatible";
        sync_error = true;
    }
    // remove the temporary db
    fs::remove(get_temporary_db_path(ctt));

    // if there was a sync issue, throw an exception
    if (sync_error) {
        throw sqlite_sync_exception("ERROR ACCESSING SFS METADATA. " +
                                    result_message);
    }
}

}  // namespace rgw::sal::sfs::sqlite
