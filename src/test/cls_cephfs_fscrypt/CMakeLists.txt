add_executable(ceph_test_cls_cephfs_fscrypt
  test_cls_cephfs_fscrypt.cc)
target_link_libraries(ceph_test_cls_cephfs_fscrypt
  librados
  global
  radostest-cxx
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )
install(TARGETS
  ceph_test_cls_cephfs_fscrypt
  DESTINATION ${CMAKE_INSTALL_BINDIR})
