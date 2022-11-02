#
# spec file for package s3gw
#
# Copyright (C) 2004-2019 The Ceph Project Developers. See COPYING file
# at the top-level directory of this distribution and at
# https://github.com/ceph/ceph/blob/master/COPYING
#
# All modifications and additions to the file contributed by third parties
# remain the property of their copyright owners, unless otherwise agreed
# upon.
#
# This file is under the GNU Lesser General Public License, version 2.1
#
# Please submit bugfixes or comments via http://tracker.ceph.com/
#

#################################################################################
# conditional build section
#
# please read this for explanation of bcond syntax:
# https://rpm-software-management.github.io/rpm/manual/conditionalbuilds.html
#################################################################################
%bcond_with make_check
%bcond_with zbd
%bcond_with cmake_verbose_logging
%bcond_with ceph_test_package
%ifarch s390
%bcond_with tcmalloc
%else
%bcond_without tcmalloc
%endif
%bcond_without rbd_ssd_cache
%ifarch x86_64
%bcond_without rbd_rwl_cache
%else
%bcond_with rbd_rwl_cache
%endif
%if 0%{?fedora} || 0%{?rhel}
%if 0%{?rhel} < 9
%bcond_with system_pmdk
%else
%bcond_without system_pmdk
%endif
%bcond_without selinux
%bcond_without cephfs_java
%bcond_without amqp_endpoint
%bcond_without kafka_endpoint
%bcond_without lttng
%bcond_without libradosstriper
%bcond_without ocf
%global luarocks_package_name luarocks
%bcond_without lua_packages
%global _remote_tarball_prefix https://download.ceph.com/tarballs/
%endif
%if 0%{?suse_version}
%bcond_without system_pmdk
%bcond_with amqp_endpoint
%bcond_with cephfs_java
%bcond_with kafka_endpoint
%bcond_with libradosstriper
%ifarch x86_64 aarch64 ppc64le
%bcond_without lttng
%else
%bcond_with lttng
%endif
%bcond_with ocf
%bcond_with selinux
#Compat macro for _fillupdir macro introduced in Nov 2017
%if ! %{defined _fillupdir}
%global _fillupdir /var/adm/fillup-templates
%endif
#luarocks
%if 0%{?is_opensuse}
# openSUSE
%bcond_without lua_packages
%if 0%{?sle_version}
# openSUSE Leap
%global luarocks_package_name lua53-luarocks
%else
# openSUSE Tumbleweed
%global luarocks_package_name lua54-luarocks
%endif
%else
# SLE
%bcond_with lua_packages
%endif
%endif
%bcond_with seastar
%if 0%{?suse_version}
%bcond_with jaeger
%else
%bcond_without jaeger
%endif
%if 0%{?fedora} || 0%{?suse_version} >= 1500
# distros that ship cmd2 and/or colorama
%bcond_without cephfs_shell
%else
# distros that do _not_ ship cmd2/colorama
%bcond_with cephfs_shell
%endif
%bcond_with system_arrow
%bcond_with system_utf8proc
%if 0%{?fedora} || 0%{?suse_version} || 0%{?rhel} >= 8
%global weak_deps 1
%endif
%if %{with selinux}
# get selinux policy version
# Force 0.0.0 policy version for centos builds to avoid repository sync issues between rhel and centos
%if 0%{?centos}
%global _selinux_policy_version 0.0.0
%else
%{!?_selinux_policy_version: %global _selinux_policy_version 0.0.0}
%endif
%endif

%{!?_udevrulesdir: %global _udevrulesdir /lib/udev/rules.d}
%{!?tmpfiles_create: %global tmpfiles_create systemd-tmpfiles --create}
%{!?python3_pkgversion: %global python3_pkgversion 3}
%{!?python3_version_nodots: %global python3_version_nodots 3}
%{!?python3_version: %global python3_version 3}

%if ! 0%{?suse_version}
# use multi-threaded xz compression: xz level 7 using ncpus threads
%global _source_payload w7T%{_smp_build_ncpus}.xzdio
%global _binary_payload w7T%{_smp_build_ncpus}.xzdio
%endif

%define smp_limit_mem_per_job() %( \
  kb_per_job=%1 \
  kb_total=$(head -3 /proc/meminfo | sed -n 's/MemAvailable:\\s*\\(.*\\) kB.*/\\1/p') \
  jobs=$(( $kb_total / $kb_per_job )) \
  [ $jobs -lt 1 ] && jobs=1 \
  echo $jobs )

%if 0%{?_smp_ncpus_max} == 0
%if 0%{?__isa_bits} == 32
# 32-bit builds can use 3G memory max, which is not enough even for -j2
%global _smp_ncpus_max 1
%else
# 3.0 GiB mem per job
# SUSE distros use limit_build in the place of smp_limit_mem_per_job, please
# be sure to update it (in the build section, below) as well when changing this
# number.
%global _smp_ncpus_max %{smp_limit_mem_per_job 3000000}
%endif
%endif

%if 0%{with seastar}
# disable -specs=/usr/lib/rpm/redhat/redhat-annobin-cc1, as gcc-toolset-{9,10}-annobin
# do not provide gcc-annobin.so anymore, despite that they provide annobin.so. but
# redhat-rpm-config still passes -fplugin=gcc-annobin to the compiler.
%undefine _annotated_build
%endif

#################################################################################
# main package definition
#################################################################################
Name:		s3gw
Version:	WILL_BE_OVERWRITTEN_BY_OBS
Release:	0%{?dist}
%if 0%{?fedora} || 0%{?rhel}
Epoch:		2
%endif

# define _epoch_prefix macro which will expand to the empty string if epoch is
# undefined
%global _epoch_prefix %{?epoch:%{epoch}:}

Summary:	Standalone S3 Gateway
License:	LGPL-2.1 and LGPL-3.0 and CC-BY-SA-3.0 and GPL-2.0 and BSL-1.0 and BSD-3-Clause and MIT
%if 0%{?suse_version}
Group:		System/Filesystems
%endif
URL:		https://aquarist-labs.io/s3gw/
Source0:	ceph-%{version}.tar
%if 0%{?suse_version}
# _insert_obs_source_lines_here
ExclusiveArch:  x86_64 aarch64 ppc64le s390x
%endif
PreReq: permissions
Conflicts:     ceph-radosgw
Conflicts:     librados2
Requires: group(www)
Requires(post):	binutils
%if 0%{with cephfs_java}
BuildRequires:	java-devel
BuildRequires:	jpackage-utils
BuildRequires:	sharutils
%endif
%if 0%{with selinux}
BuildRequires:	checkpolicy
BuildRequires:	selinux-policy-devel
%endif
BuildRequires:	gperf
BuildRequires:  cmake > 3.5
BuildRequires:	fuse-devel
%if 0%{with seastar} && 0%{?rhel}
BuildRequires:	gcc-toolset-10-gcc-c++ >= 10.3.1-1.2
%else
%if 0%{?suse_version}
BuildRequires:	gcc11-c++
%else
BuildRequires:	gcc-c++
%endif
%endif
%if 0%{with tcmalloc}
# libprofiler did not build on ppc64le until 2.7.90
%if 0%{?fedora} || 0%{?rhel} >= 8
BuildRequires:	gperftools-devel >= 2.7.90
%endif
%if 0%{?rhel} && 0%{?rhel} < 8
BuildRequires:	gperftools-devel >= 2.6.1
%endif
%if 0%{?suse_version}
BuildRequires:	gperftools-devel >= 2.4
%endif
%endif
BuildRequires:	libaio-devel
BuildRequires:	libblkid-devel >= 2.17
BuildRequires:	cryptsetup-devel
BuildRequires:	libcurl-devel
BuildRequires:	libcap-ng-devel
BuildRequires:	fmt-devel >= 6.2.1
BuildRequires:	pkgconfig(libudev)
BuildRequires:	libnl3-devel
BuildRequires:	liboath-devel
BuildRequires:	libtool
BuildRequires:	libxml2-devel
BuildRequires:	make
BuildRequires:	ncurses-devel
BuildRequires:	libicu-devel
BuildRequires:	patch
BuildRequires:	perl
BuildRequires:	pkgconfig
BuildRequires:  procps
BuildRequires:	python%{python3_pkgversion}
BuildRequires:	python%{python3_pkgversion}-devel
BuildRequires:	python%{python3_pkgversion}-setuptools
BuildRequires:	python%{python3_pkgversion}-Cython
BuildRequires:	snappy-devel
BuildRequires:	sqlite-devel
BuildRequires:	sudo
BuildRequires:	pkgconfig(udev)
BuildRequires:	valgrind-devel
BuildRequires:	which
BuildRequires:	xfsprogs-devel
BuildRequires:	xmlstarlet
BuildRequires:	nasm
BuildRequires:	lua-devel
%if 0%{with seastar} || 0%{with jaeger}
BuildRequires:  yaml-cpp-devel >= 0.6
%endif
%if 0%{with amqp_endpoint}
BuildRequires:  librabbitmq-devel
%endif
%if 0%{with kafka_endpoint}
BuildRequires:  librdkafka-devel
%endif
%if 0%{with lua_packages}
BuildRequires:  %{luarocks_package_name}
%endif
%if 0%{with make_check}
BuildRequires:  hostname
BuildRequires:  jq
BuildRequires:	libuuid-devel
BuildRequires:	python%{python3_pkgversion}-bcrypt
BuildRequires:	python%{python3_pkgversion}-pecan
BuildRequires:	python%{python3_pkgversion}-requests
BuildRequires:	python%{python3_pkgversion}-dateutil
BuildRequires:	python%{python3_pkgversion}-coverage
BuildRequires:	python%{python3_pkgversion}-pyOpenSSL
BuildRequires:	socat
%endif
%if 0%{with zbd}
BuildRequires:  libzbd-devel
%endif
%if 0%{?suse_version}
BuildRequires:  libthrift-devel >= 0.13.0
%else
BuildRequires:  thrift-devel >= 0.13.0
%endif
BuildRequires:  re2-devel
%if 0%{with jaeger}
BuildRequires:  bison
BuildRequires:  flex
%if 0%{?fedora} || 0%{?rhel}
BuildRequires:  json-devel
%endif
%if 0%{?suse_version}
BuildRequires:  nlohmann_json-devel
%endif
BuildRequires:  libevent-devel
%endif
%if 0%{with system_pmdk}
BuildRequires:  libpmem-devel
BuildRequires:  libpmemobj-devel
%endif
%if 0%{with system_arrow}
BuildRequires:  arrow-devel
BuildRequires:  parquet-devel
%endif
%if 0%{with system_utf8proc}
BuildRequires:  utf8proc-devel
%endif
%if 0%{with seastar}
BuildRequires:  c-ares-devel
BuildRequires:  gnutls-devel
BuildRequires:  hwloc-devel
BuildRequires:  libpciaccess-devel
BuildRequires:  lksctp-tools-devel
BuildRequires:  ragel
BuildRequires:  systemtap-sdt-devel
%if 0%{?fedora}
BuildRequires:  libubsan
BuildRequires:  libasan
BuildRequires:  libatomic
%endif
%if 0%{?rhel}
BuildRequires:  gcc-toolset-10-annobin
BuildRequires:  gcc-toolset-10-libubsan-devel
BuildRequires:  gcc-toolset-10-libasan-devel
BuildRequires:  gcc-toolset-10-libatomic-devel
%endif
%endif
#################################################################################
# distro-conditional dependencies
#################################################################################
%if 0%{?suse_version}
BuildRequires:  pkgconfig(systemd)
BuildRequires:	systemd-rpm-macros
%{?systemd_requires}
PreReq:		%fillup_prereq
BuildRequires:	fdupes
BuildRequires:  memory-constraints
BuildRequires:	net-tools
BuildRequires:	libbz2-devel
BuildRequires:	mozilla-nss-devel
BuildRequires:	keyutils-devel
BuildRequires:  libopenssl-devel
BuildRequires:  ninja
BuildRequires:  openldap2-devel
#BuildRequires:  krb5
#BuildRequires:  krb5-devel
BuildRequires:  cunit-devel
BuildRequires:	python%{python3_pkgversion}-PrettyTable
BuildRequires:	python%{python3_pkgversion}-PyYAML
BuildRequires:	python%{python3_pkgversion}-Sphinx
BuildRequires:  rdma-core-devel
BuildRequires:	liblz4-devel >= 1.7
# for prometheus-alerts
BuildRequires:  golang-github-prometheus-prometheus
%endif
%if 0%{?fedora} || 0%{?rhel}
Requires:	systemd
BuildRequires:  boost-random
BuildRequires:	nss-devel
BuildRequires:	keyutils-libs-devel
BuildRequires:	libibverbs-devel
BuildRequires:  librdmacm-devel
BuildRequires:  ninja-build
BuildRequires:  openldap-devel
#BuildRequires:  krb5-devel
BuildRequires:  openssl-devel
BuildRequires:  CUnit-devel
BuildRequires:	python%{python3_pkgversion}-devel
BuildRequires:	python%{python3_pkgversion}-prettytable
BuildRequires:	python%{python3_pkgversion}-pyyaml
BuildRequires:	python%{python3_pkgversion}-sphinx
BuildRequires:	lz4-devel >= 1.7
%endif
# distro-conditional make check dependencies
%if 0%{with make_check}
BuildRequires:	golang
%if 0%{?fedora} || 0%{?rhel}
BuildRequires:	golang-github-prometheus
BuildRequires:	libtool-ltdl-devel
BuildRequires:	xmlsec1
BuildRequires:	xmlsec1-devel
%ifarch x86_64
BuildRequires:	xmlsec1-nss
%endif
BuildRequires:	xmlsec1-openssl
BuildRequires:	xmlsec1-openssl-devel
BuildRequires:	python%{python3_pkgversion}-cherrypy
BuildRequires:	python%{python3_pkgversion}-jwt
BuildRequires:	python%{python3_pkgversion}-routes
BuildRequires:	python%{python3_pkgversion}-scipy
BuildRequires:	python%{python3_pkgversion}-werkzeug
BuildRequires:	python%{python3_pkgversion}-pyOpenSSL
%endif
%if 0%{?suse_version}
BuildRequires:	golang-github-prometheus-prometheus
BuildRequires:	libxmlsec1-1
BuildRequires:	libxmlsec1-nss1
BuildRequires:	libxmlsec1-openssl1
BuildRequires:	python%{python3_pkgversion}-CherryPy
BuildRequires:	python%{python3_pkgversion}-PyJWT
BuildRequires:	python%{python3_pkgversion}-Routes
BuildRequires:	python%{python3_pkgversion}-Werkzeug
BuildRequires:	python%{python3_pkgversion}-numpy-devel
BuildRequires:	xmlsec1-devel
BuildRequires:	xmlsec1-openssl-devel
%endif
%endif
# lttng and babeltrace for rbd-replay-prep
%if %{with lttng}
%if 0%{?fedora} || 0%{?rhel}
BuildRequires:	lttng-ust-devel
BuildRequires:	libbabeltrace-devel
%endif
%if 0%{?suse_version}
BuildRequires:	lttng-ust-devel
BuildRequires:  babeltrace-devel
%endif
%endif
%if 0%{?suse_version}
BuildRequires:	libexpat-devel
%endif
%if 0%{?rhel} || 0%{?fedora}
BuildRequires:	expat-devel
%endif
#hardened-cc1
%if 0%{?fedora} || 0%{?rhel}
BuildRequires:  redhat-rpm-config
%endif
%if 0%{with seastar}
%if 0%{?fedora} || 0%{?rhel}
BuildRequires:  cryptopp-devel
BuildRequires:  numactl-devel
%endif
%if 0%{?suse_version}
BuildRequires:  libcryptopp-devel
BuildRequires:  libnuma-devel
%endif
%endif
%if 0%{?rhel} >= 8
BuildRequires:  /usr/bin/pathfix.py
%endif
BuildRequires:  libboost_atomic-devel-impl >= 1.79
BuildRequires:  libboost_context-devel-impl >= 1.79
BuildRequires:  libboost_coroutine-devel-impl >= 1.79
BuildRequires:  libboost_filesystem-devel-impl >= 1.79
BuildRequires:  libboost_iostreams-devel-impl >= 1.79
BuildRequires:  libboost_program_options-devel-impl >= 1.79
BuildRequires:  libboost_python3-devel-impl >= 1.79
BuildRequires:  libboost_random-devel-impl >= 1.79
BuildRequires:  libboost_regex-devel-impl >= 1.79
BuildRequires:  libboost_system-devel-impl >= 1.79
BuildRequires:  libboost_thread-devel-impl >= 1.79
BuildRequires:  libsqliteorm
BuildRequires:  patchelf

%description
Everything you need to run Ceph's RADOS Gateway, without RADOS, backed by a
regular filesystem instead.


#################################################################################
# common
#################################################################################
%prep
%autosetup -p1 -n ceph-%{version}

%build
# Disable lto on systems that do not support symver attribute
# See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=48200 for details
%if ( 0%{?rhel} && 0%{?rhel} < 9 ) || ( 0%{?suse_version} && 0%{?suse_version} <= 1500 )
%define _lto_cflags %{nil}
%endif

%if 0%{with seastar} && 0%{?rhel}
. /opt/rh/gcc-toolset-10/enable
%endif

%if 0%{with cephfs_java}
# Find jni.h
for i in /usr/{lib64,lib}/jvm/java/include{,/linux}; do
    [ -d $i ] && java_inc="$java_inc -I$i"
done
%endif

%if 0%{?suse_version}
%limit_build -m 3000
%endif

export CPPFLAGS="$java_inc"
export CFLAGS="$RPM_OPT_FLAGS"
export CXXFLAGS="$RPM_OPT_FLAGS"
export LDFLAGS="$RPM_LD_FLAGS"

%if 0%{with seastar}
# seastar uses longjmp() to implement coroutine. and this annoys longjmp_chk()
export CXXFLAGS=$(echo $RPM_OPT_FLAGS | sed -e 's/-Wp,-D_FORTIFY_SOURCE=2//g')
# remove from CFLAGS too because it causes the arrow submodule to fail with:
#   warning _FORTIFY_SOURCE requires compiling with optimization (-O)
export CFLAGS=$(echo $RPM_OPT_FLAGS | sed -e 's/-Wp,-D_FORTIFY_SOURCE=2//g')
%endif

env | sort

%{?!_vpath_builddir:%global _vpath_builddir %{_target_platform}}

# TODO: drop this step once we can use `cmake -B`
mkdir -p %{_vpath_builddir}
pushd %{_vpath_builddir}
cmake .. \
%if 0%{?suse_version}
    -DCMAKE_C_COMPILER=gcc-11 \
    -DCMAKE_CXX_COMPILER=g++-11 \
%endif
    -DCMAKE_INSTALL_PREFIX=%{_prefix} \
    -DCMAKE_INSTALL_LIBDIR:PATH=%{_libdir} \
    -DCMAKE_INSTALL_LIBEXECDIR:PATH=%{_libexecdir} \
    -DCMAKE_INSTALL_LOCALSTATEDIR:PATH=%{_localstatedir} \
    -DCMAKE_INSTALL_SYSCONFDIR:PATH=%{_sysconfdir} \
    -DCMAKE_INSTALL_MANDIR:PATH=%{_mandir} \
    -DCMAKE_INSTALL_DOCDIR:PATH=%{_docdir}/ceph \
    -DCMAKE_INSTALL_INCLUDEDIR:PATH=%{_includedir} \
    -DSYSTEMD_SYSTEM_UNIT_DIR:PATH=%{_unitdir} \
    -DWITH_MANPAGE:BOOL=ON \
    -DWITH_PYTHON3:STRING=%{python3_version} \
    -DWITH_MGR_DASHBOARD_FRONTEND:BOOL=OFF \
%if 0%{?suse_version}
    -DWITH_RADOSGW_SELECT_PARQUET:BOOL=OFF \
%endif
%if 0%{without ceph_test_package}
    -DWITH_TESTS:BOOL=OFF \
%endif
%if 0%{with cephfs_java}
    -DJAVA_HOME=%{java_home} \
    -DJAVA_LIB_INSTALL_DIR=%{_jnidir} \
    -DWITH_CEPHFS_JAVA:BOOL=ON \
%endif
%if 0%{with selinux}
    -DWITH_SELINUX:BOOL=ON \
%endif
%if %{with lttng}
    -DWITH_LTTNG:BOOL=ON \
    -DWITH_BABELTRACE:BOOL=ON \
%else
    -DWITH_LTTNG:BOOL=OFF \
    -DWITH_BABELTRACE:BOOL=OFF \
%endif
    $CEPH_EXTRA_CMAKE_ARGS \
%if 0%{with ocf}
    -DWITH_OCF:BOOL=ON \
%endif
%if 0%{with cephfs_shell}
    -DWITH_CEPHFS_SHELL:BOOL=ON \
%endif
%if 0%{with libradosstriper}
    -DWITH_LIBRADOSSTRIPER:BOOL=ON \
%else
    -DWITH_LIBRADOSSTRIPER:BOOL=OFF \
%endif
%if 0%{with amqp_endpoint}
    -DWITH_RADOSGW_AMQP_ENDPOINT:BOOL=ON \
%else
    -DWITH_RADOSGW_AMQP_ENDPOINT:BOOL=OFF \
%endif
%if 0%{with kafka_endpoint}
    -DWITH_RADOSGW_KAFKA_ENDPOINT:BOOL=ON \
%else
    -DWITH_RADOSGW_KAFKA_ENDPOINT:BOOL=OFF \
%endif
%if 0%{without lua_packages}
    -DWITH_RADOSGW_LUA_PACKAGES:BOOL=OFF \
%endif
%if 0%{with zbd}
    -DWITH_ZBD:BOOL=ON \
%endif
%if 0%{with cmake_verbose_logging}
    -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON \
%endif
%if 0%{with rbd_rwl_cache}
    -DWITH_RBD_RWL:BOOL=ON \
%endif
%if 0%{with rbd_ssd_cache}
    -DWITH_RBD_SSD_CACHE:BOOL=ON \
%endif
%if 0%{with system_pmdk}
    -DWITH_SYSTEM_PMDK:BOOL=ON \
%endif
%if 0%{without jaeger}
    -DWITH_JAEGER:BOOL=OFF \
%endif
%if 0%{?suse_version}
    -DBOOST_J:STRING=%{jobs} \
%else
    -DBOOST_J:STRING=%{_smp_build_ncpus} \
%endif
%if 0%{?rhel}
    -DWITH_FMT_HEADER_ONLY:BOOL=ON \
%endif
%if 0%{with system_arrow}
    -DWITH_SYSTEM_ARROW:BOOL=ON \
%endif
%if 0%{with system_utf8proc}
    -DWITH_SYSTEM_UTF8PROC:BOOL=ON \
%endif
    -DWITH_GRAFANA:BOOL=ON \
    -DWITH_SYSTEM_BOOST=ON \
    -DWITH_LIBURING=OFF \
    -DENABLE_GIT_VERSION=OFF \
    -GNinja

%if %{with cmake_verbose_logging}
cat ./CMakeFiles/CMakeOutput.log
cat ./CMakeFiles/CMakeError.log
%endif

%if 0%{?suse_version}
ninja -j%{jobs} bin/radosgw
%else
ninja  bin/radosgw
%endif

popd

%if 0%{with make_check}
%check
# run in-tree unittests
pushd %{_vpath_builddir}
ctest %{_smp_mflags}
popd
%endif


%install
pushd %{_vpath_builddir}
mkdir -p %{buildroot}%{_localstatedir}/lib/s3gw
install -m 0755 -D bin/radosgw --target-directory %{buildroot}%{_bindir}
install -m 0644 -D lib/libradosgw.so.2.0.0 --target-directory %{buildroot}%{_libdir}
install -m 0644 -D lib/librados.so.2.0.0 --target-directory %{buildroot}%{_libdir}
install -m 0644 -D lib/libceph-common.so.2 --target-directory %{buildroot}%{_libdir}
# Because we're not using cmake to install, we need to strip RPATH manually
for f in %{buildroot}%{_bindir}/* %{buildroot}%{_libdir}/* ; do
        patchelf --remove-rpath $f
done
# Add .so.2 symlinks
ln -s libradosgw.so.2.0.0 %{buildroot}%{_libdir}/libradosgw.so.2
ln -s librados.so.2.0.0 %{buildroot}%{_libdir}/librados.so.2
popd

install -m 0644 -D systemd/s3gw.service --target-directory %{buildroot}%{_unitdir}
%if 0%{?suse_version}
install -m 0644 -D etc/sysconfig/s3gw %{buildroot}%{_fillupdir}/sysconfig.%{name}
mkdir -p %{buildroot}%{_sbindir}
ln -s service %{buildroot}%{_sbindir}/rcs3gw
%else
install -m 0644 -D etc/sysconfig/s3gw %{buildroot}%{_sysconfdir}/sysconfig/%{name}
%endif


%clean
rm -rf %{buildroot}
# built binaries are no longer necessary at this point,
# but are consuming ~17GB of disk in the build environment
rm -rf %{_vpath_builddir}

#################################################################################
# files and systemd scriptlets
#################################################################################
%files
%attr(750,root,www) %{_bindir}/radosgw
%{_libdir}/*so*
%if 0%{?suse_version}
%{_fillupdir}/sysconfig.*
%{_sbindir}/rcs3gw
%else
%{_sysconfdir}/sysconfig/%{name}
%endif
%{_unitdir}/s3gw.service
%attr(750,s3gw,s3gw) %dir %{_localstatedir}/lib/s3gw/

%pre
%service_add_pre s3gw.service
%if 0%{?suse_version}
if ! getent group s3gw >/dev/null ; then
    groupadd -r s3gw 2>/dev/null || :
fi
if ! getent passwd s3gw >/dev/null ; then
    # probably want home directory to be /var/lib/s3gw, and also use that for data
    useradd -r -g s3gw -s /sbin/nologin -c 's3gw user' -d /var/lib/empty s3gw 2>/dev/null || :
fi
%endif

%post
%service_add_post s3gw.service
/sbin/ldconfig
%set_permissions %{_bindir}/radosgw
# Workaround for s3gw creating db by default with root ownership
# (see https://github.com/aquarist-labs/s3gw/issues/194)
S3GW_DB=%{_localstatedir}/lib/s3gw/s3gw.db
if ! [ -f "$S3GW_DB" ]; then
    touch $S3GW_DB
    chown s3gw:s3gw $S3GW_DB
    chmod 640 $S3GW_DB
fi
%fillup_only

%preun
%service_del_preun s3gw.service

%postun
%service_del_postun s3gw.service
/sbin/ldconfig

%verifyscript
%verify_permissions -e %{_bindir}/radosgw


%changelog
