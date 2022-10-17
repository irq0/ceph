#need to update cmake version here
cmake_minimum_required(VERSION 3.14.0)
project(sfs)

option(USE_SFS "Enable SFS" ON)

find_package(SQLite3 REQUIRED)

#These flags should not be necessary because radosgw is linked with sqlite
#library via system package imported by cmake with find_package().
#We keep these because dbstore backend is doing that in its CMakeLists.txt
#and we decided to conform with it.
set(SQLITE_COMPILE_FLAGS "-DSQLITE_THREADSAFE=1")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SQLITE_COMPILE_FLAGS}")

set(CMAKE_INCLUDE_DIR ${CMAKE_INCLUDE_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/sqlite")
include_directories(${CMAKE_INCLUDE_DIR})

set(sfs_srcs
    sqlite/sqlite_users.cc
    sqlite/sqlite_buckets.cc
    sqlite/sqlite_objects.cc
    sqlite/sqlite_versioned_objects.cc
    sqlite/users/users_conversions.cc
    sqlite/buckets/bucket_conversions.cc
    sqlite/objects/object_conversions.cc
    sqlite/versioned_object/versioned_object_conversions.cc
    sqlite/dbconn.cc
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
    )

add_library(sfs STATIC ${sfs_srcs})
target_include_directories(sfs PUBLIC "${CMAKE_SOURCE_DIR}/src/fmt/include")
target_include_directories(sfs PUBLIC "${CMAKE_SOURCE_DIR}/src/rgw")
set(link_targets spawn)
if(WITH_JAEGER)
  list(APPEND link_targets jaeger_base)
endif()

set(CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} ${link_targets} rgw_common sqlite3 pthread)
target_link_libraries(sfs PUBLIC ${CMAKE_LINK_LIBRARIES})
