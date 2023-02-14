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
#
# --------------------
#
# This file is a collection of helper functions for testing s3gw.
# Source this in your shell(scripts) to make use of it, don't execute as script.


# wait_for_http_200
#
# Takes a url and loops until the server responding at that url returns an HTTP
# status 200 (ok), or up to one minute.
function wait_for_http_200 {
  local url="$1"

  for _ in {1..60} ; do
    if [[ $(curl -s -o/dev/null -w '%{http_code}' "$url") == "200" ]] ; then
      return
    fi
    sleep 1
  done
  exit 1
}
