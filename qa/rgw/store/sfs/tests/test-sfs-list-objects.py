# Copyright 2023 SUSE, LLC.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import sys
import boto3, botocore
import random
import string
import tempfile
import os
import filecmp

class ListObjectsTests(unittest.TestCase):
    ACCESS_KEY='test'
    SECRET_KEY='test'
    URL='http://127.0.0.1:7480'
    BUCKET_NAME_LENGTH=8
    OBJECT_NAME_LENGTH=10

    def setUp(self):
        self.s3_client = boto3.client('s3',
                                endpoint_url=ListObjectsTests.URL,
                                aws_access_key_id='test',
                                aws_secret_access_key='test')

        self.s3 = boto3.resource('s3',
                                endpoint_url=ListObjectsTests.URL,
                                aws_access_key_id='test',
                                aws_secret_access_key='test')

        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.s3_client.close()
        self.test_dir.cleanup()

    def get_random_name(self, length) -> str:
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def get_random_bucket_name(self) -> str:
        return self.get_random_name(ListObjectsTests.BUCKET_NAME_LENGTH)

    def get_random_object_name(self) -> str:
        return self.get_random_name(ListObjectsTests.OBJECT_NAME_LENGTH)

    def generate_random_file(self, path, size=4):
        # size passed is in mb
        size = size * 1024 * 1024
        with open(path, 'wb') as fout:
            fout.write(os.urandom(size))

    def assert_bucket_exists(self, bucket_name):
        response = self.s3_client.list_buckets()
        found = False
        for bucket in response['Buckets']:
            if (bucket['Name'] ==  bucket_name):
                found = True
        self.assertTrue(found)

    def upload_file_and_check(self, bucket_name, object_name):
        test_file_path = os.path.join(self.test_dir.name, object_name)
        self.generate_random_file(test_file_path)
        # upload the file
        self.s3_client.upload_file(test_file_path, bucket_name, object_name)

        # get the file and compare with the original
        test_file_path_check = os.path.join(self.test_dir.name, 'test_file_check.bin')
        self.s3_client.download_file(bucket_name, object_name, test_file_path_check)
        self.assertTrue(filecmp.cmp(test_file_path, test_file_path_check, shallow=False))

    def check_list_response_prefix(self, list_resp, objects_expected, prefix):
        self.assertTrue('ResponseMetadata' in list_resp)
        self.assertTrue('HTTPStatusCode' in list_resp['ResponseMetadata'])
        self.assertTrue(200, list_resp['ResponseMetadata']['HTTPStatusCode'])
        if (len(objects_expected) > 0):
            self.assertTrue('Contents' in list_resp)
            self.assertEqual(len(list_resp['Contents']), len(objects_expected))
            found_objects = []
            for obj in list_resp['Contents']:
                self.assertTrue('Key' in obj)
                if (obj['Key'] in objects_expected):
                    found_objects.append(obj['Key'])
            self.assertEqual(len(found_objects), len(objects_expected))
        else:
            self.assertFalse('Contents' in list_resp)
        # check the prefix in the response
        self.assertTrue('Prefix' in list_resp)
        self.assertEqual(list_resp['Prefix'], prefix)

    def test_list_objects_no_prefix(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        # objects to upload
        objects = [
            'prefix_obj1.bin',
            'prefix_obj2.bin',
            'prefix_obj3.bin',
            'prefix_obj4.bin',
            'test_file_1.bin',
            'another_file.bin'
        ]
        for obj in objects:
            self.upload_file_and_check(bucket_name, obj)
        # list all objects
        objs_ret = self.s3_client.list_objects(Bucket=bucket_name)
        self.check_list_response_prefix(objs_ret, objects, '')

    def test_list_objects_with_prefix(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        # objects to upload
        objects = [
            'prefix_obj1.bin',
            'prefix_obj2.bin',
            'prefix_obj3.bin',
            'prefix_obj4.bin',
            'test_file_1.bin',
            'another_file.bin'
        ]
        for obj in objects:
            self.upload_file_and_check(bucket_name, obj)
        # list all objects with prefix equal to 'prefix'
        objs_ret = self.s3_client.list_objects(Bucket=bucket_name, Prefix='prefix')
        expected_objects = [
            'prefix_obj1.bin',
            'prefix_obj2.bin',
            'prefix_obj3.bin',
            'prefix_obj4.bin'
        ]
        self.check_list_response_prefix(objs_ret, expected_objects, 'prefix')

        # check with 'test' prefix
        objs_ret = self.s3_client.list_objects(Bucket=bucket_name, Prefix='test')
        expected_objects = [
            'test_file_1.bin'
        ]
        self.check_list_response_prefix(objs_ret, expected_objects, 'test')

        # check with 'another' prefix
        objs_ret = self.s3_client.list_objects(Bucket=bucket_name, Prefix='another')
        expected_objects = [
            'another_file.bin'
        ]
        self.check_list_response_prefix(objs_ret, expected_objects, 'another')

        # list all objects with prefix equal to 'pr'
        objs_ret = self.s3_client.list_objects(Bucket=bucket_name, Prefix='pr')
        expected_objects = [
            'prefix_obj1.bin',
            'prefix_obj2.bin',
            'prefix_obj3.bin',
            'prefix_obj4.bin'
        ]
        self.check_list_response_prefix(objs_ret, expected_objects, 'pr')

        # list all objects with prefix equal to 'nothing'
        objs_ret = self.s3_client.list_objects(Bucket=bucket_name, Prefix='nothing')
        expected_objects = []
        self.check_list_response_prefix(objs_ret, expected_objects, 'nothing')


if __name__ == '__main__':
    if len(sys.argv) == 2:
        address_port = sys.argv.pop()
        ListObjectsTests.URL = 'http://{0}'.format(address_port)
        unittest.main()
    else:
        print ('usage: {0} ADDRESS:PORT'.format(sys.argv[0]))
