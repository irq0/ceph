#!/bin/bash
#
# Copyright 2022 SUSE, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[[ -n "${TRACE}" ]] && set -x
set +e

testpath=$(mktemp -q -d sfs.XXXX --tmpdir=/tmp)
s3cfg=${testpath}/s3cfg
url="${1}"
retcode="0"

usage() {
  cat << EOF
usage: $0 ADDRESS[:PORT[/LOCATION]]
EOF
}

setup() {
  cat > "${s3cfg}" << EOF
[default]
access_key = test
secret_key = test
host_base = ${url}/
host_bucket = ${url}/%(bucket)
signurl_use_https = False
use_https = False
signature_v2 = True
signurl_use_https = False
EOF

  pushd "${testpath}" > /dev/null || exit 1
}

cleanup() {
  popd > /dev/null || exit 1

  rm -rf "${testpath}"
}

success() {
  [[ "$(caller 1 | cut -d' ' -f2)" == "main" ]] && \
    echo -e "$(caller 1) \e[0;32mSUCCESS\e[0m"
}

failure() {
  [[ "$(caller 1 | cut -d' ' -f2)" == "main" ]] && \
    echo -e "$(caller 1) \e[1;31mFAILURE\e[0m"
  retcode="1"
}

# this function is meant to be called indirectly from expect_success
# it will ignore a failure, but not fail on success, unlike expect_failure,
# which will fail on success
# shellcheck disable=SC2317
ignore_failure() {
  "$@" || true
}

expect_failure() {
  if "$@" > /dev/null 2>&1 ; then failure ; else success ; fi
}

expect_success() {
  if "$@" > /dev/null 2>&1 ; then success ; else failure ; fi
}

expect_count() {
  local count="$1"
  shift
  local cmd=("$@")
  mapfile -t lst < <("${cmd[@]}")
  [[ "${#lst[@]}" -eq "$count" ]] || failure
  success
}

expect_in_bucket() {
  local bucket="$1"
  local objs=( "$@" )
  mapfile -t lst < <(s3 ls -r "s3://${bucket}")
  for obj in "${objs[@]}" ; do
    echo "${lst[@]}" | grep -q "$obj" || failure
  done
  success
}

# this function is meant to be called indirectly from expect_success/failure
# shellcheck disable=SC2317
compare_file_hash() {
  local aaa="$1"
  local bbb="$2"
  aaa_md5=$(md5sum -b "$aaa" | cut -f1 -d' ')
  bbb_md5=$(md5sum -b "$bbb" | cut -f1 -d' ')
  [[ "$aaa_md5" == "$bbb_md5" ]]
}

s3() {
  s3cmd -c "${s3cfg}" "$@"
}

[[ -z "${url}" ]] && usage && exit 1

setup

# Please note: rgw will refuse bucket names with upper case letters.
# This is due to amazon s3's bucket naming restrictions.
# See:
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
#
bucket="sfs-test-$(echo $RANDOM | sha1sum | head -c 4 | tr -dc a-z0-9)"

expect_success s3 ls "s3://"
expect_success s3 mb "s3://${bucket}"
expect_success s3 ls "s3://${bucket}"
expect_failure s3 ls "s3://${bucket}-dne"

dd if=/dev/random bs=1k count=1k of=obj1.bin status=none
dd if=/dev/random bs=1k count=2k of=obj2.bin status=none

expect_success s3 put "obj1.bin" "s3://${bucket}/"
expect_success s3 put "obj1.bin" "s3://${bucket}/obj1.bin"
expect_success s3 put "obj1.bin" "s3://${bucket}/obj1.bin.2"
expect_success s3 put "obj1.bin" "s3://${bucket}/my/obj1.bin"
expect_success s3 get "s3://${bucket}/obj1.bin" "obj1.bin.local"

expect_success compare_file_hash "obj1.bin" "obj1.bin.local"

expect_failure s3 get "s3://${bucket}/dne.bin"

expect_count 3 s3 ls -r "s3://${bucket}"
expect_in_bucket "${bucket}" "obj1.bin" "obj1.bin.2" "my/obj1.bin"

expect_success s3 rm "s3://${bucket}/obj1.bin.2"
expect_count 2 s3 ls -r "s3://${bucket}"
expect_in_bucket "${bucket}" "obj1.bin" "my/obj1.bin"

expect_success s3 put "obj2.bin" "s3://${bucket}/obj2.bin"
expect_success s3 put "obj1.bin" "s3://${bucket}/obj2.bin"
expect_success s3 get "s3://${bucket}/obj2.bin" "obj2.bin.local"

expect_failure compare_file_hash "obj2.bin" "obj2.bin.local"
expect_success compare_file_hash "obj1.bin" "obj2.bin.local"

do_copy() {
  local from_bucket="$1"
  local to_bucket="$2"

  # For now this operation fails. While the copy actually succeeds, s3cmd then
  # tries to perform an ACL operation on the bucket/object, and that fails.
  # We need to ensure the object is there instead, and check it matches in
  # contents.
  expect_success \
    ignore_failure \
    s3 cp "s3://${from_bucket}/obj1.bin" "s3://${to_bucket}/obj1.bin.copy"

  expect_in_bucket "$to_bucket" "obj1.bin.copy"
  expect_success \
    s3 get "s3://${to_bucket}/obj1.bin.copy" "obj1.bin.copy.${to_bucket}"
  expect_success compare_file_hash "obj1.bin" "obj1.bin.copy.${to_bucket}"
  success
}

# copy from $bucket/obj to $bucket/obj.copy
do_copy "${bucket}" "${bucket}"

# copy from $bucket/obj to $newbucket/obj.copy
newbucket="${bucket}-2"
expect_success s3 mb "s3://${newbucket}"
do_copy "${bucket}" "${newbucket}"

# delete the bucket contents
expect_success s3 del --recursive --force "s3://${bucket}"

# list the bucket, it should be empty
expect_count 0 s3 ls -r "s3://${bucket}"

# remove the bucket
expect_success s3 rb "s3://${bucket}"

# should no longer be available
expect_failure s3 ls -r "s3://${bucket}"

cleanup
exit "$retcode"
