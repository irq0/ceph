cmake_minimum_required(VERSION 3.14.0)
project(filestore)

set (CMAKE_INCLUDE_DIR ${CMAKE_INCLUDE_DIR}
"${CMAKE_CURRENT_SOURCE_DIR}/common" )

set(libfilestore_srcs
  filestore/rgw_filestore_types.cc
  filestore/Transaction.cc
  filestore/ObjectStore.cc
  filestore/BtrfsFileStoreBackend.cc
  filestore/FileJournal.cc
  filestore/FileStore.cc
  filestore/JournalThrottle.cc
  filestore/GenericFileStoreBackend.cc
  filestore/JournalingObjectStore.cc
  filestore/HashIndex.cc
  filestore/IndexManager.cc
  filestore/LFNIndex.cc
  filestore/WBThrottle.cc
  filestore/os_xattr.c
  filestore/fs/FS.cc)

if(WITH_ZBD)
  list(APPEND libfilestore_srcs
    bluestore/ZonedFreelistManager.cc
    bluestore/ZonedAllocator.cc)
endif()

if(HAVE_LIBXFS)
  list(APPEND libfilestore_srcs
    filestore/XfsFileStoreBackend.cc
    filestore/fs/XFS.cc)
endif()

if(HAVE_LIBZFS)
  add_library(filestore_zfs_objs OBJECT
    filestore/ZFSFileStoreBackend.cc
    filestore/fs/ZFS.cc)
  target_include_directories(filestore_zfs_objs SYSTEM PRIVATE
    ${ZFS_INCLUDE_DIRS})
  list(APPEND libfilestore_srcs $<TARGET_OBJECTS:filestore_zfs_objs>)
endif()

add_library(rgwfilestore STATIC ${libfilestore_srcs})
target_link_libraries(rgwfilestore blk)

target_link_libraries(rgwfilestore spawn)

if(HAVE_LIBZFS)
  target_link_libraries(rgwfilestore ${ZFS_LIBRARIES})
endif()

if(WITH_JAEGER)
  add_dependencies(rgwfilestore jaeger_base)
  target_link_libraries(rgwfilestore jaeger_base)
endif()
