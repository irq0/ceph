#!/bin/sh

set -e

# https://cmake.org/cmake/help/latest/variable/CMAKE_BUILD_TYPE.html
# Release: Your typical release build with no debugging information and full optimization.
# MinSizeRel: A special Release build optimized for size rather than speed.
# RelWithDebInfo: Same as Release, but with debugging information.
# Debug: Usually a classic debug build including debugging information, no optimization etc.
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-"Debug"}

CEPH_DIR=$(realpath ${CEPH_DIR:-"/srv/ceph"})
SFS_CCACHE_DIR=${SFS_CCACHE_DIR:-"${CEPH_DIR}/build.ccache"}
WITH_TESTS=${WITH_TESTS:-"OFF"}
RUN_TESTS=${RUN_TESTS:-"OFF"}
WITH_RADOSGW_DBSTORE=${WITH_RADOSGW_DBSTORE:-"OFF"}

CEPH_CMAKE_ARGS=(
  "-DCMAKE_C_COMPILER=gcc-11"
  "-DCMAKE_CXX_COMPILER=g++-11"
  "-DENABLE_GIT_VERSION=ON"
  "-DWITH_PYTHON3=3"
  "-DWITH_CCACHE=ON"
  "-DWITH_TESTS=ON"
  "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  "-DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
  "-DWITH_RADOSGW_KAFKA_ENDPOINT=OFF"
  "-DWITH_RADOSGW_SELECT_PARQUET=OFF"
  "-DWITH_RADOSGW_MOTR=OFF"
  "-DWITH_RADOSGW_DBSTORE=${WITH_RADOSGW_DBSTORE}"
  "-DWITH_RADOSGW_LUA_PACKAGES=OFF"
  "-DWITH_MANPAGE=OFF"
  "-DWITH_OPENLDAP=OFF"
  "-DWITH_LTTNG=OFF"
  "-DWITH_RDMA=OFF"
  "-DWITH_SYSTEM_BOOST=ON"
  ${CEPH_CMAKE_ARGS}
)
NPROC=${NPROC:-$(nproc --ignore=2)}

build_radosgw() {
  echo "Building radosgw ..."
  echo "CEPH_DIR=${CEPH_DIR}"
  echo "NPROC=${NPROC}"
  echo "CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  echo "CCACHE_DIR=${SFS_CCACHE_DIR}"

  export CCACHE_DIR=${SFS_CCACHE_DIR}
  if [ ! -d "${CCACHE_DIR}" ]; then
    echo "ccache dir not found, create."
    mkdir "${CCACHE_DIR}"
    echo "Created by build-radosgw container" > \
      "${CCACHE_DIR}/README"
  fi

  cd ${CEPH_DIR}

  # This is necessary since git v2.35.2 because of CVE-2022-24765
  # but we have to continue in case CEPH_DIR is not a git repo
  # Since git 2.36 the the wildcard '*' is also accepted
  git config --global --add safe.directory "*" || true

  if [ -d "build" ]; then
      cd build/
      cmake -DBOOST_J=${NPROC} ${CEPH_CMAKE_ARGS[@]} ..
  else
      ./do_cmake.sh ${CEPH_CMAKE_ARGS[@]}
      cd build/
  fi

  ninja -j${NPROC} bin/radosgw
}

strip_radosgw() {
  [ "${CMAKE_BUILD_TYPE}" == "Debug" -o "${CMAKE_BUILD_TYPE}" == "RelWithDebInfo" ] && return 0

  echo "Stripping files ..."
  strip --strip-debug --strip-unneeded \
    --remove-section=.comment --remove-section=.note.* \
    --keep-section=.GCC.command.line \
    ${CEPH_DIR}/build/bin/radosgw \
    ${CEPH_DIR}/build/lib/*.so
}

build_radosgw_tests() {
  echo "Building radosgw tests..."

  export CCACHE_DIR=${SFS_CCACHE_DIR}
  if [ ! -d "${CCACHE_DIR}" ]; then
    echo "ccache dir not found, create."
    mkdir "${CCACHE_DIR}"
    echo "Created by build-radosgw container" > \
      "${CCACHE_DIR}/README"
  fi

  cd ${CEPH_DIR}

  # This is necessary since git v2.35.2 because of CVE-2022-24765
  # but we have to continue in case CEPH_DIR is not a git repo
  git config --global --add safe.directory "*" || true

  if [ -d "build" ]; then
      cd build/
      cmake -DBOOST_J=${NPROC} ${CEPH_CMAKE_ARGS[@]} ..
  else
      ./do_cmake.sh ${CEPH_CMAKE_ARGS[@]}
      cd build/
  fi

  for unit_test in "${UNIT_TESTS[@]}"
  do
    ninja -j${NPROC} bin/${unit_test}
  done
}

run_radosgw_tests() {
  cd ${CEPH_DIR}
  for unit_test in "${UNIT_TESTS[@]}"
  do
    build/bin/${unit_test}
  done
}

pushd .
build_radosgw
strip_radosgw
if [ "${WITH_TESTS}" == "ON" ]; then
  # discover tests from build.ninja so we don't need to update this after adding
  # a new unit test
  # SFS unittests should be named unittest_rgw_sfs*
  TESTS_FROM_NINJA_BUILD=`grep "build unittest_rgw_sfs" build.ninja | awk '{print $2}' | sed 's/:$//g'`

  UNIT_TESTS=(
    ${TESTS_FROM_NINJA_BUILD}
  )
  build_radosgw_tests
  if [ "${RUN_TESTS}" == "ON" ]; then
    run_radosgw_tests
  fi
fi
popd

exit 0
