# test/test_sdb.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

#!! this unit test require that setting.py with the folowing constants is created
# access_key and secret_key

import time
import unittest
from unittest import TestCase
from test.settings import access_key, secret_key, account_number
from awsutils.s3client import S3Client

#helper
class S3_HELPER:
    BUCKET_NAME = ""
    OBJECT= b"1234567890123456789"


class S3ClientMethodTesting(TestCase):

    def setUp(self):
        self.s3 = S3Client("s3.amazonaws.com", access_key, secret_key)
        S3_HELPER.BUCKET_NAME = "aws_unittest_bucket_8500"#%s" % (int(time.time()))
        try:
            result = self.s3.getBucket(S3_HELPER.BUCKET_NAME)
            self.assertTrue(self.cleanup_objects(S3_HELPER.BUCKET_NAME))
        except Exception as e:
            result =  self.s3.putBucket(S3_HELPER.BUCKET_NAME)


    def tearDown(self):
        pass


    def create_bucket_policy(self, my_bucket_name):
        principal = {}
        principal['AWS'] = "%s"% account_number
        statement = {}
        statement['Sid'] = "Stmt%s"% (int(time.time()))
        statement['Action'] = "s3:*"
        statement['Effect'] = "Allow"
        statement['Resource'] = "*"
        statement['Principal'] = principal
        statement['Resource'] = "arn:aws:s3:::%s"%my_bucket_name
        policy = {}
        policy['Id'] = "Policy%s"% (int(time.time()))
        policy['Statement'] = statement
        return policy

    def cleanup_bucket(self, bucket_name):
        #deletes all objects from the bucket and the bucket itself
        self.assertTrue(self.cleanup_objects(bucket_name))
        try:
            self.s3.deleteBucket(bucket_name)
        except Exception as e:
            pass


    def cleanup_objects(self, bucket_name):
        no_more_objects = False
        my_obj = []
        my_objects = self.listObjects(bucket_name)
        if my_objects.__len__() > 0:
            for obj_name in my_objects:
                my_obj.append(obj_name['Key'])
            no_more_objects = self.s3.deleteMultipleObjects(bucket_name, my_obj)
        else:
            no_more_objects = True
        return no_more_objects


    def listObjects(self, bucket_name):
        bucket_object = []
        myobjects = self.s3.getBucket(bucket_name)
        if myobjects.__len__() > 5:
            if isinstance(myobjects['Contents'], dict):
                bucket_object.append(myobjects['Contents'])
            if isinstance(myobjects['Contents'], list):
                for myobject_ in myobjects['Contents']:
                    bucket_object.append(myobject_)
        return bucket_object


    def test_Object_create_enum_delete(self):
        #add 4 objects
        self.s3.putObject(S3_HELPER.BUCKET_NAME, "aws_unittest_object0_%s" % (int(time.time())),S3_HELPER.OBJECT)
        self.s3.putObject(S3_HELPER.BUCKET_NAME, "aws_unittest_object1_%s" % (int(time.time())),S3_HELPER.OBJECT)
        self.s3.putObject(S3_HELPER.BUCKET_NAME, "aws_unittest_object2_%s" % (int(time.time())),S3_HELPER.OBJECT)
        self.s3.putObject(S3_HELPER.BUCKET_NAME, "aws_unittest_object3_%s" % (int(time.time())),S3_HELPER.OBJECT)
        #verify if they are in the bucket
        myobjects = self.listObjects(S3_HELPER.BUCKET_NAME)
        self.assertEqual(myobjects.__len__(), 4)
        #test delete the last object from the list
        self.s3.deleteObject(S3_HELPER.BUCKET_NAME, myobjects[-1]['Key'])
        myobjects = self.listObjects(S3_HELPER.BUCKET_NAME)
        self.assertEqual(myobjects.__len__(), 3)
        #test multiple delete
        self.assertTrue(self.cleanup_objects(S3_HELPER.BUCKET_NAME))


    def test_Object_head(self):
        #TODO: more meta data should be tested when putObject/headObject will be extended
        my_object_name = "aws_unittest_object_head_%s" % (int(time.time()))
        self.s3.putObject(S3_HELPER.BUCKET_NAME, my_object_name, S3_HELPER.OBJECT, x_amz_server_side_encryption = "AES256")
        result = self.s3.headObject(S3_HELPER.BUCKET_NAME, my_object_name)
        self.assertEqual(result['x-amz-server-side-encryption'],'AES256')

    def test_Object_PutCopy(self):
        source_object_name =  "aws_unittest_object_%s" % (int(time.time()))
        self.s3.putObject(S3_HELPER.BUCKET_NAME, source_object_name, S3_HELPER.OBJECT, x_amz_server_side_encryption = "AES256")
        result = self.s3.getObject(S3_HELPER.BUCKET_NAME, source_object_name)
        original_ETag = result['ETag']
        #create another bucket, where the object will be copied
        destination_object_name = "aws_unittest_object_destination%s" % (int(time.time()))
        destination_bucket_name = "awsl_unittest_bucket_8500_destination"
        try:
            result = self.s3.getBucket(destination_bucket_name)
            self.assertTrue(self.cleanup_objects(destination_bucket_name))
        except Exception as e:
            result =  self.s3.putBucket(destination_bucket_name)
        result = self.s3.putObjectCopy(destination_bucket_name, destination_object_name, S3_HELPER.BUCKET_NAME,
                              source_object_name, x_amz_server_side_encryption= "AES256")
        self.assertEqual(original_ETag, result['ETag'])
        #cleanup
        self.cleanup_bucket(destination_bucket_name)


    @unittest.skip("test_bucket_policy - skipped until putBucketPolicy will be finalized")
    def test_bucket_policy(self):
        #TODO: verify this test when putBucketPolicy will be finished
        policy = self.create_bucket_policy(S3_HELPER.BUCKET_NAME)
        result = self.s3.putBucketPolicy(S3_HELPER.BUCKET_NAME, policy)
        self.assertEqual(result, "/"+S3_HELPER.BUCKET_NAME)


    @unittest.skip("test_list_all - skipped, uncomment this if you want to see the whole bucket/object tree, first-1000")
    def test_list_all(self):
        result = self.s3.getService()
        for mybucket in result['Buckets']['Bucket']:
            #we have the bucket names
            mybucket_name = mybucket['Name']
            print(mybucket_name)
            myobjects = self.s3.getBucket(mybucket['Name'])
            if myobjects.__len__() > 5:
                if isinstance(myobjects['Contents'], dict):
                    myobject = myobjects['Contents']['ETag']
                    print("     ", myobject)
                if isinstance(myobjects['Contents'], list):
                    for myobject_ in myobjects['Contents']:
                        myobject = myobject_['ETag']
                        print("     ", myobject)
            else:
                print("     It has no objects")
        #print(result)

    @unittest.skip("test_delete_all - skipped use this with caution, it will delete every bucket from S3 (full cleanup)\
                    first-1000")
    def test_delete_all(self):
        result = self.s3.getService()
        for mybucket in result['Buckets']['Bucket']:
            #we have the bucket names, we can delete all the objects from that bucket
            self.assertTrue(self.cleanup_objects(mybucket['Name']))
            try:
                #TODO: verify the exception given during the bucket delete
                self.s3.deleteBucket(mybucket['Name'])
            except Exception as e:
                pass
        print("done")



if __name__ == '__main__':
    unittest.main()