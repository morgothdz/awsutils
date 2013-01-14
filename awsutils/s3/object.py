# awsutils/s3/object.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


class S3Object():
    def __init__(self, name, bucketname, s3client):
        self.name = name
        self.bucket = bucketname
        self.s3client = s3client

    def delete(self):
        self.s3client.deleteObject(bucketname=self.bucketname, objectname=self.name)

    def getValue(self):
        #TODO: implement
        pass

    def getValueToFile(self, filename):
        #TODO: implement
        pass

    def setValue(self, value):
        #TODO: implement
        pass

    def setValueFromFile(self, filename):
        #TODO: implement
        pass

    def __repr__(self):
        return self.name