#!/usr/bin/env python3
#
# Copyright 2023 SUSE, LLC.
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

import unittest
import sys
import boto3, botocore
import tempfile
import time
from datetime import datetime
from datetime import timedelta


class ObjectLockingTests(unittest.TestCase):
    ACCESS_KEY = "test"
    SECRET_KEY = "test"
    URL = "http://127.0.0.1:7480"

    BUCKET_NAME_1 = "bobjlockenabled1"

    ObjVersions = {}

    def setUp(self):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=ObjectLockingTests.URL,
            aws_access_key_id=ObjectLockingTests.ACCESS_KEY,
            aws_secret_access_key=ObjectLockingTests.SECRET_KEY,
        )

        self.s3 = boto3.resource(
            "s3",
            endpoint_url=ObjectLockingTests.URL,
            aws_access_key_id=ObjectLockingTests.ACCESS_KEY,
            aws_secret_access_key=ObjectLockingTests.SECRET_KEY,
        )

        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.s3_client.close()
        self.test_dir.cleanup()

    def ensure_bucket(self, bucket_name, objLockEnabled):
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError:
            self.s3_client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=objLockEnabled
            )

    def test_object_locking_create_bucket(self):
        self.ensure_bucket(ObjectLockingTests.BUCKET_NAME_1, True)
        response = self.s3_client.get_bucket_versioning(
            Bucket=ObjectLockingTests.BUCKET_NAME_1
        )
        self.assertTrue(response["Status"] == "Enabled")
        response = self.s3_client.get_object_lock_configuration(
            Bucket=ObjectLockingTests.BUCKET_NAME_1
        )
        self.assertTrue(
            response["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"
        )

        try:
            self.s3_client.put_bucket_versioning(
                Bucket=ObjectLockingTests.BUCKET_NAME_1,
                VersioningConfiguration={"MFADelete": "Enabled", "Status": "Suspended"},
            )
            self.fail("cannot suspend versioning on ObjectLockEnabled bucket")
        except botocore.exceptions.ClientError as error:
            self.assertTrue(error.response["ResponseMetadata"]["HTTPStatusCode"] == 409)

    def check_object_retention(self, response, mode, amount, type):
        self.assertTrue(response["Retention"]["Mode"] == mode)
        againstDate = datetime.utcnow() + timedelta(
            days=amount if type == "Days" else amount * 365
        )
        checkDate = response["Retention"]["RetainUntilDate"]
        self.assertTrue(checkDate.year == againstDate.year)
        self.assertTrue(checkDate.month == againstDate.month)
        self.assertTrue(checkDate.day == againstDate.day)

    def test_object_locking_bucket_object_lock_default_configuration(self):
        self.ensure_bucket(ObjectLockingTests.BUCKET_NAME_1, True)

        # set GOVERNANCE DefaultRetention over bucket

        self.s3_client.put_object_lock_configuration(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 7}},
            },
        )

        response = self.s3_client.get_object_lock_configuration(
            Bucket=ObjectLockingTests.BUCKET_NAME_1
        )
        self.assertTrue(
            response["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"
        )
        self.assertTrue(
            response["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Mode"]
            == "GOVERNANCE"
        )
        self.assertTrue(
            response["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Days"] == 7
        )

        self.s3_client.put_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Key="key.1", Body="data.1"
        )

        response = self.s3_client.list_object_versions(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Prefix="key.1"
        )

        for version in response["Versions"]:
            if version["Key"] == "key.1" and version["IsLatest"] == True:
                self.ObjVersions["key.1.1"] = version["VersionId"]
                print(self.ObjVersions["key.1.1"])

        response = self.s3_client.get_object_retention(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.1"],
        )

        self.check_object_retention(response, "GOVERNANCE", 7, "Days")

        # set COMPLIANCE DefaultRetention over bucket

        self.s3_client.put_object_lock_configuration(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Years": 1}},
            },
        )

        response = self.s3_client.get_object_lock_configuration(
            Bucket=ObjectLockingTests.BUCKET_NAME_1
        )
        self.assertTrue(
            response["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"
        )
        self.assertTrue(
            response["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Mode"]
            == "COMPLIANCE"
        )
        self.assertTrue(
            response["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Years"]
            == 1
        )

        self.s3_client.put_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Key="key.1", Body="data.2"
        )

        response = self.s3_client.list_object_versions(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Prefix="key.1"
        )

        for version in response["Versions"]:
            if version["Key"] == "key.1" and version["IsLatest"] == True:
                self.ObjVersions["key.1.2"] = version["VersionId"]
                print(self.ObjVersions["key.1.2"])

        response = self.s3_client.get_object_retention(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.2"],
        )

        self.check_object_retention(response, "COMPLIANCE", 1, "Years")

        # # check version key.1.1 still has the GOVERNANCE retention

        response = self.s3_client.get_object_retention(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.1"],
        )

        self.check_object_retention(response, "GOVERNANCE", 7, "Days")

    def test_object_locking_object_compliance(self):
        self.ensure_bucket(ObjectLockingTests.BUCKET_NAME_1, True)

        retainUntilDate = datetime.utcnow() + timedelta(days=6)

        self.s3_client.put_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            Body="data.3",
            ObjectLockMode="COMPLIANCE",
            ObjectLockRetainUntilDate=retainUntilDate,
        )

        response = self.s3_client.list_object_versions(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Prefix="key.1"
        )

        for version in response["Versions"]:
            if version["Key"] == "key.1" and version["IsLatest"] == True:
                self.ObjVersions["key.1.3"] = version["VersionId"]
                print(self.ObjVersions["key.1.3"])

        response = self.s3_client.get_object_retention(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.3"],
        )

        self.check_object_retention(response, "COMPLIANCE", 6, "Days")

        try:
            response = self.s3_client.delete_object(
                Bucket=ObjectLockingTests.BUCKET_NAME_1,
                Key="key.1",
                VersionId=self.ObjVersions["key.1.3"],
            )
        except botocore.exceptions.ClientError as error:
            self.assertTrue(error.response["ResponseMetadata"]["HTTPStatusCode"] == 403)
            self.assertTrue(error.response["Error"]["Code"] == "AccessDenied")
            self.assertTrue(
                error.response["Error"]["Message"] == "forbidden by object lock"
            )

        # test deletion is allowed after retainUntilDate expires

        retainUntilDate = datetime.utcnow() + timedelta(seconds=1)

        self.s3_client.put_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            Body="data.3.1",
            ObjectLockMode="COMPLIANCE",
            ObjectLockRetainUntilDate=retainUntilDate,
        )

        response = self.s3_client.list_object_versions(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Prefix="key.1"
        )

        for version in response["Versions"]:
            if version["Key"] == "key.1" and version["IsLatest"] == True:
                self.ObjVersions["key.1.3.1"] = version["VersionId"]
                print(self.ObjVersions["key.1.3.1"])

        # let's wait 2 seconds so that retainUntilDate expires.
        time.sleep(2)

        response = self.s3_client.delete_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.3.1"],
        )

    def test_object_locking_object_governance(self):
        self.ensure_bucket(ObjectLockingTests.BUCKET_NAME_1, True)

        retainUntilDate = datetime.utcnow() + timedelta(days=13)

        self.s3_client.put_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            Body="data.4",
            ObjectLockMode="GOVERNANCE",
            ObjectLockRetainUntilDate=retainUntilDate,
        )

        response = self.s3_client.list_object_versions(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Prefix="key.1"
        )

        for version in response["Versions"]:
            if version["Key"] == "key.1" and version["IsLatest"] == True:
                self.ObjVersions["key.1.4"] = version["VersionId"]
                print(self.ObjVersions["key.1.4"])

        response = self.s3_client.get_object_retention(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.4"],
        )

        self.check_object_retention(response, "GOVERNANCE", 13, "Days")

        try:
            response = self.s3_client.delete_object(
                Bucket=ObjectLockingTests.BUCKET_NAME_1,
                Key="key.1",
                VersionId=self.ObjVersions["key.1.4"],
            )
        except botocore.exceptions.ClientError as error:
            self.assertTrue(error.response["ResponseMetadata"]["HTTPStatusCode"] == 403)
            self.assertTrue(error.response["Error"]["Code"] == "AccessDenied")
            self.assertTrue(
                error.response["Error"]["Message"] == "forbidden by object lock"
            )

        # test deletion is allowed with BypassGovernanceRetention

        response = self.s3_client.delete_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.4"],
            BypassGovernanceRetention=True,
        )

        self.assertTrue(response["ResponseMetadata"]["HTTPStatusCode"] == 204)

        # test deletion is allowed after retainUntilDate expires

        retainUntilDate = datetime.utcnow() + timedelta(seconds=1)

        self.s3_client.put_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            Body="data.4.1",
            ObjectLockMode="GOVERNANCE",
            ObjectLockRetainUntilDate=retainUntilDate,
        )

        response = self.s3_client.list_object_versions(
            Bucket=ObjectLockingTests.BUCKET_NAME_1, Prefix="key.1"
        )

        for version in response["Versions"]:
            if version["Key"] == "key.1" and version["IsLatest"] == True:
                self.ObjVersions["key.1.4.1"] = version["VersionId"]
                print(self.ObjVersions["key.1.4.1"])

        # let's wait 2 seconds so that retainUntilDate expires.
        time.sleep(2)

        response = self.s3_client.delete_object(
            Bucket=ObjectLockingTests.BUCKET_NAME_1,
            Key="key.1",
            VersionId=self.ObjVersions["key.1.4.1"],
        )

if __name__ == "__main__":
    if len(sys.argv) == 2:
        address_port = sys.argv.pop()
        ObjectLockingTests.URL = "http://{0}".format(address_port)
        unittest.main()
    else:
        print("usage: {0} ADDRESS:PORT".format(sys.argv[0]))
