#!/bin/bash

set -e

# https://cmake.org/cmake/help/latest/variable/CMAKE_BUILD_TYPE.html
# Release: Your typical release build with no debugging information and full optimization.
# MinSizeRel: A special Release build optimized for size rather than speed.
# RelWithDebInfo: Same as Release, but with debugging information.
# Debug: Usually a classic debug build including debugging information, no optimization etc.
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-"Debug"}

CEPH_DIR="$(realpath "${CEPH_DIR:-"/srv/ceph"}")"
SFS_BUILD_DIR="$(realpath "${SFS_BUILD_DIR:-"${CEPH_DIR}/build"}")"
SFS_CCACHE_DIR="$(realpath "${SFS_CCACHE_DIR:-"${CEPH_DIR}/build.ccache"}")"

WITH_TESTS=${WITH_TESTS:-"OFF"}
RUN_TESTS=${RUN_TESTS:-"OFF"}
UNIT_TESTS=()

ENABLE_GIT_VERSION=${ENABLE_GIT_VERSION:-"ON"}

WITH_RADOSGW_DBSTORE=${WITH_RADOSGW_DBSTORE:-"OFF"}
ALLOCATOR=${ALLOCATOR:-"tcmalloc"}
WITH_SYSTEM_BOOST=${WITH_SYSTEM_BOOST:-"ON"}
WITH_JAEGER=${WITH_JAEGER:-"OFF"}

WITH_ASAN=${WITH_ASAN:-"OFF"}
WITH_ASAN_LEAK=${WITH_ASAN_LEAK:-"OFF"}
WITH_TSAN=${WITH_TSAN:-"OFF"}
WITH_UBSAN=${WITH_UBSAN:-"OFF"}


NPROC=${NPROC:-$(nproc --ignore=2)}

CC=${CC:-"gcc-11"}
CXX=${CXX:-"g++-11"}

CEPH_CMAKE_ARGS=(
  "-GNinja"
  "-DBOOST_J=${NPROC}"
  "-DCMAKE_C_COMPILER=${CC}"
  "-DCMAKE_CXX_COMPILER=${CXX}"
  "-DENABLE_GIT_VERSION=${ENABLE_GIT_VERSION}"
  "-DWITH_PYTHON3=3"
  "-DWITH_CCACHE=ON"
  "-DWITH_TESTS=${WITH_TESTS}"
  "-DALLOCATOR=${ALLOCATOR}"
  "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  "-DCMAKE_EXPORT_COMPILE_COMMANDS=YES"
  "-DWITH_JAEGER=${WITH_JAEGER}"
  "-DWITH_LTTNG=OFF"
  "-DWITH_MANPAGE=OFF"
  "-DWITH_OPENLDAP=OFF"
  "-DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
  "-DWITH_RADOSGW_DBSTORE=${WITH_RADOSGW_DBSTORE}"
  "-DWITH_RADOSGW_KAFKA_ENDPOINT=OFF"
  "-DWITH_RADOSGW_LUA_PACKAGES=OFF"
  "-DWITH_RADOSGW_MOTR=OFF"
  "-DWITH_RADOSGW_SELECT_PARQUET=OFF"
  "-DWITH_RDMA=OFF"
  "-DWITH_SYSTEM_BOOST=${WITH_SYSTEM_BOOST}"
  "-DWITH_ASAN=${WITH_ASAN}"
  "-DWITH_ASAN_LEAK=${WITH_ASAN_LEAK}"
  "-DWITH_TSAN=${WITH_TSAN}"
  "-DWITH_UBSAN=${WITH_UBSAN}"
  "${CEPH_CMAKE_ARGS[@]}"
)


_configure() {
  echo "Building radosgw ..."
  echo "CEPH_DIR=${CEPH_DIR}"
  echo "NPROC=${NPROC}"
  echo "CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  echo "BUILD_DIR=${SFS_BUILD_DIR}"
  echo "CCACHE_DIR=${SFS_CCACHE_DIR}"
  for arg in "${CEPH_CMAKE_ARGS[@]}" ; do
    echo "${arg}"
  done

  if [ ! -d "${SFS_BUILD_DIR}" ] ; then
    echo "build dir not found, create."
    mkdir -p  "${SFS_BUILD_DIR}"
  fi

  export CCACHE_DIR=${SFS_CCACHE_DIR}
  if [ ! -d "${CCACHE_DIR}" ]; then
    echo "ccache dir not found, create."
    mkdir -p "${CCACHE_DIR}"
    echo "Created by build-radosgw container" > \
      "${CCACHE_DIR}/README"
  fi

  # This is necessary since git v2.35.2 because of CVE-2022-24765
  # but we have to continue in case CEPH_DIR is not a git repo
  # Since git 2.36 the the wildcard '*' is also accepted
  if ! git config --global safe.directory > /dev/null ; then
    git config --global --add safe.directory "*" || true
  fi

  pushd "${SFS_BUILD_DIR}"

  cmake "${CEPH_CMAKE_ARGS[@]}" "${CEPH_DIR}"

  popd
}


_build() {
  pushd "${SFS_BUILD_DIR}"

  ninja -j "${NPROC}" bin/radosgw crypto_plugins

  if [ "${WITH_TESTS}" == "ON" ] ; then
    # discover tests from build.ninja so we don't need to update this after
    # adding a new unit test
    # SFS unittests should be named unittest_rgw_sfs_*
    # SFS unittests should be named unittest_rgw_s3gw_*
    IFS=" " read -r -a \
      UNIT_TESTS <<< "$(grep -E "build unittest_rgw_sfs_|build unittest_rgw_s3gw_" build.ninja \
                          | awk 'BEGIN {ORS=" "}; {print $4}')"
    ninja -j "${NPROC}" "${UNIT_TESTS[@]}"
  fi

  popd
}


_strip() {
  [ "${CMAKE_BUILD_TYPE}" == "Debug" ] \
    || [ "${CMAKE_BUILD_TYPE}" == "RelWithDebInfo" ] \
    && return 0

  echo "Stripping files ..."
  strip \
    --strip-debug \
    --strip-unneeded \
    --remove-section=.comment \
    --keep-section=.GCC.command.line \
    "${CEPH_DIR}"/build/bin/radosgw \
    "${CEPH_DIR}"/build/lib/*.so
}


_run_tests() {
  [ "${RUN_TESTS}" == "ON" ] || return 0

  echo "Running tests..."
  for unit_test in "${UNIT_TESTS[@]}"
  do
    echo "Running...${SFS_BUILD_DIR}/${unit_test}"
    "${SFS_BUILD_DIR}/${unit_test}"
  done
}


pushd .

_configure
_build
_strip
_run_tests

popd
