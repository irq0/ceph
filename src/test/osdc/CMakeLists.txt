add_executable(ceph_test_objectcacher_stress
  object_cacher_stress.cc
  FakeWriteback.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_stress
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_objectcacher_misc
  object_cacher_misc.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_misc
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_misc
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# unittest_objecter_instance
add_executable(unittest_objecter_instance
  objecter_instance.cc
  )
add_ceph_unittest(unittest_objecter_instance)
target_link_libraries(unittest_objecter_instance global)
