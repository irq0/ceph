#!/usr/bin/env python3
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

import string
import unittest
import boto3
import botocore
import random

ACCESS_KEY = "test"
SECRET_KEY = "test"
URL = "http://127.0.0.1:7480"


class UnsupportedFeaturesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s3c = boto3.client(  # type: ignore
            "s3",
            endpoint_url=URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )

    def tearDown(self) -> None:
        self.s3c.close()

    def test_cannot_suspend_versioning(self):
        """
        As we don't support versioning suspended buckets, enabling
        it must raise an NotImplemented error
        """
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(42))
        resp = self.s3c.create_bucket(Bucket=name)
        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            self.s3c.put_bucket_versioning(
                Bucket=name,
                VersioningConfiguration={
                    "MFADelete": "Disabled",
                    "Status": "Suspended",
                },
            )
        self.assertEqual(cm.exception.response["Error"]["Code"], "NotImplemented")

    def test_cannot_suspend_enabled_versioning(self):
        """
        As we don't support versioning suspended buckets, enabling
        on a versioned bucket must raise an NotImplemented error
        """
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(42))
        self.s3c.create_bucket(Bucket=name)
        self.s3c.put_bucket_versioning(
            Bucket=name,
            VersioningConfiguration={"MFADelete": "Disabled", "Status": "Enabled"},
        )
        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            self.s3c.put_bucket_versioning(
                Bucket=name,
                VersioningConfiguration={
                    "MFADelete": "Disabled",
                    "Status": "Suspended",
                },
            )
        self.assertEqual(cm.exception.response["Error"]["Code"], "NotImplemented")
