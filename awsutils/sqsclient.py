# awsutils/s3/sqsclient.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.client import AWSClient
from awsutils.utils.auth import SIGNATURE_V4_HEADERS

class SQSClient(AWSClient):

    def _getRegion(self):
        try:
            return self.__region
        except:
            self.__region = self.host[4:-14]
            return self.__region

    def listQueues(self, queueNamePrefix=None):
        query = {'Action':'ListQueues', 'Version':'2012-11-05'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=self._getRegion(), service='sqs')
        result = []
        data = data['ListQueuesResponse']['ListQueuesResult']
        if 'QueueUrl' in data:
            result = data['QueueUrl']
            if not isinstance(result, list):
                result = [result]
        return result

    def getQueueUrl(self, queueName, queueOwnerAWSAccountId=None):
        query = {'Action':'GetQueueUrl', 'QueueName':queueName, 'Version':'2012-11-05'}
        if queueOwnerAWSAccountId is not None:
            query['QueueOwnerAWSAccountId'] = queueOwnerAWSAccountId
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=self._getRegion(), service='sqs')
        data = data['GetQueueUrlResponse']['GetQueueUrlResult']
        if 'QueueUrl' in data:
            return data['QueueUrl']
        return None

    def receiveMessage(self, attributes=None, maxNumberOfMessages=None, visibilityTimeout=None, waitTimeSeconds=None):
        """
        attributes
            All-returns all values.
            SenderId -returns the AWS account number (or the IP address, if anonymous access is allowed) of the sender.
            SentTimestamp -returns the time when the message was sent (epoch time in milliseconds).
            ApproximateReceiveCount -returns the number of times a message has been received but not deleted.
            ApproximateFirstReceiveTimestamp -returns the time when the message was first received (epoch time in milliseconds).

https://sqs.eu-west-1.amazonaws.com/292654169181/test
        http://sqs.us-east-1.amazonaws.com/123456789012/testQueue/
?Action=ReceiveMessage
&MaxNumberOfMessages=5
&VisibilityTimeout=15
&AttributeName=All;
&Version=2009-02-01
&SignatureMethod=HmacSHA256
&Expires=2009-04-18T22%3A52%3A43PST
&AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE
&SignatureVersion=2
&Signature=Dqlp3Sd6ljTUA9Uf6SGtEExwUQEXAMPLE
        """
        pass