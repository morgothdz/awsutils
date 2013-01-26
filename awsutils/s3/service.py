# awsutils/s3/service.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.s3client import S3Client
from awsutils.s3.bucket import S3Bucket

class S3Service:
    def __init__(self, s3client):
        """
        @param s3client: the s3client to be used for communication
        @type s3client: S3Client
        """
        self.s3client = s3client

    def getBucket(self, name):
        """
        @param name: the name of the bucket
        @type name: str
        @rtype: None or S3Bucket
        """
        buckets = self.getBuckets()
        if name in buckets:
            return buckets[name]
        return None

    def getBuckets(self):
        """
        @rtype: dict[] S3Bucket
        """
        data = self.s3client.getService()
        buckets = data['Buckets']['Bucket']
        result = {}
        for bucket in buckets:
            result[bucket['Name']] = S3Bucket(name=bucket['Name'], s3client=self.s3client, owner=data['Owner'],
                                              creationdate=bucket['CreationDate'])
        return result


