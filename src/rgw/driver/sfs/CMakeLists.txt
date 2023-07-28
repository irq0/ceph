# need to update cmake version here
cmake_minimum_required(VERSION 3.14.0)
project(sfs)

option(USE_SFS "Enable SFS" ON)

find_package(SQLite3 REQUIRED)

# These flags should not be necessary because radosgw is linked with sqlite
# library via system package imported by cmake with find_package().
# We keep these because dbstore backend is doing that in its CMakeLists.txt
# and we decided to conform with it.
set(SQLITE_COMPILE_FLAGS "-DSQLITE_THREADSAFE=1")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SQLITE_COMPILE_FLAGS}")

set(sfs_srcs
    sqlite/sqlite_users.cc
    sqlite/sqlite_buckets.cc
    sqlite/sqlite_objects.cc
    sqlite/sqlite_versioned_objects.cc
    sqlite/sqlite_lifecycle.cc
    sqlite/sqlite_multipart.cc
    sqlite/users/users_conversions.cc
    sqlite/buckets/bucket_conversions.cc
    sqlite/buckets/multipart_conversions.cc
    sqlite/dbconn.cc
    sqlite/errors.cc
    bucket.cc
    multipart.cc
    object.cc
    user.cc
    types.cc
    zone.cc
    writer.cc
    sfs_bucket.cc
    sfs_gc.cc
    sfs_user.cc
    sfs_lc.cc
    )

add_library(sfs STATIC ${sfs_srcs})
target_compile_options(sfs PRIVATE
   -Werror -Wextra
   -Wunused-function
   -Wunused-result
   -Wshadow
   -Wno-error=unused-parameter
   -Wno-error=unqualified-std-cast-call)
# Mark ceph includes system to not see "our" warnings there
target_include_directories(sfs
    SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src/fmt/include"
    SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src/rgw"
    SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src"
    SYSTEM PRIVATE "${CMAKE_SOURCE_DIR}/src/include")
set(link_targets spawn)

if(WITH_JAEGER)
  list(APPEND link_targets jaeger_base)
endif()

set(CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} ${link_targets} rgw_common sqlite3 pthread)
target_link_libraries(sfs PRIVATE ${CMAKE_LINK_LIBRARIES})
