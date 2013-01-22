# awsutils/s3/sqsclient.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import hashlib
from awsutils.client import AWSClient
from awsutils.utils.auth import SIGNATURE_V4_HEADERS

class SQSUserInputException(Exception):
    pass
class SQSHashCheckException(Exception):
    pass

class SQSClient(AWSClient):
    def addPermission(self, queueuri, permissions, endpoint=None):
        #TODO: implement
        pass

    def changeMessageVisibility(self, queueuri, receiptHandle, visibilityTimeout, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        if visibilityTimeout > 43200:
            raise SQSUserInputException('visibilityTimeout too big (max 43200 seconds)')
        query = {'Action': 'ChangeMessageVisibility', 'ReceiptHandle':receiptHandle,
                 'VisibilityTimeout':visibilityTimeout, 'Version': '2012-11-05'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint)
        data['ChangeMessageVisibilityResponse']

    def changeMessageVisibilityBatch(self, queueuri, endpoint=None):
        #TODO: implement
        """
        &ChangeMessageVisibilityBatchRequestEntry.1.Id=change_visibility_msg_2
        &ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle=Your_Receipt_Handle
        &ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=45
        """

    def listQueues(self, queueNamePrefix=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'ListQueues', 'Version': '2012-11-05'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint)
        result = []
        data = data['ListQueuesResponse']['ListQueuesResult']
        if 'QueueUrl' in data:
            urls = data['QueueUrl']
            if not isinstance(urls, list):
                urls = [urls]
        result = []
        for url in urls:
            if url[7] == '/': url = url[8:]
            else: url = url[7:]
            url = url.split('/')
            result.append({'endpoint':url[0], 'accNumber':url[1], 'qName':url[2]})
        return result

    def getQueueUrl(self, queueName, queueOwnerAWSAccountId=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'GetQueueUrl', 'QueueName': queueName, 'Version': '2012-11-05'}
        if queueOwnerAWSAccountId is not None:
            query['QueueOwnerAWSAccountId'] = queueOwnerAWSAccountId
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint)
        data = data['GetQueueUrlResponse']['GetQueueUrlResult']
        if 'QueueUrl' in data:
            url = data['QueueUrl']
            if url[7] == '/': url = url[8:]
            else: url = url[7:]
            url = url.split('/')
            return {'endpoint':url[0], 'accNumber':url[1], 'qName':url[2]}
        return None


    def receiveMessage(self, qName, accNumber, attributes=None, maxNumberOfMessages=None, visibilityTimeout=None,
                       waitTimeSeconds=None, endpoint=None):
        """
        attributes
            All-returns all values.
            SenderId -returns the AWS account number (or the IP address, if anonymous access is allowed) of the sender.
            SentTimestamp -returns the time when the message was sent (epoch time in milliseconds).
            ApproximateReceiveCount -returns the number of times a message has been received but not deleted.
            ApproximateFirstReceiveTimestamp -returns the time when the message was first received (epoch time in milliseconds).
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'ReceiveMessage', 'Version': '2012-11-05'}
        if maxNumberOfMessages is not None:
            query['MaxNumberOfMessages'] = maxNumberOfMessages
        if visibilityTimeout is not None:
            query['VisibilityTimeout'] = visibilityTimeout
        if waitTimeSeconds is not None:
            query['WaitTimeSeconds'] = waitTimeSeconds
        if isinstance(attributes, str):
            query['AttributeName.1'] = attributes
        elif isinstance(attributes, list):
            for i in range(0, len(attributes)):
                query['AttributeName.%d' % (i + 1,)] = attributes[i]

        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint,
                                                        uri="/%s/%s"%(accNumber, qName))
        data = data['ReceiveMessageResponse']['ReceiveMessageResult']
        if not isinstance(data, dict):
            return []
        #example {'Body': 'sometext', 'ReceiptHandle': 'gH2...2e4=', 'MD5OfBody': '9d...dc', 'MessageId': '..'},
        if isinstance(data['Message'], dict):
            return data['Message']
        return data['Message']

    def deleteMessage(self, qName, accNumber, receiptHandle, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DeleteMessage', 'ReceiptHandle':receiptHandle, 'Version': '2012-11-05'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint,
                                                        uri="/%s/%s"%(accNumber, qName))
        data['DeleteMessageResponse']

    def sendMessage(self, qName, accNumber, messageBody, delaySeconds=None, endpoint=None, hashcheck=False):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'SendMessage', 'MessageBody':messageBody, 'Version': '2012-11-05'}
        if delaySeconds is not None:
            if delaySeconds > 900:
                raise SQSUserInputException('delaySeconds too big (max 900 seconds)')
            query['DelaySeconds']= delaySeconds
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint,
                                                        uri="/%s/%s"%(accNumber, qName))
        md5message =  data['SendMessageResponse']['SendMessageResult']['MD5OfMessageBody']
        if hashcheck:
            messageBody = messageBody.encode(encoding='utf8')
            md5 = hashlib.md5()
            md5.update(messageBody)
            md5calculated = md5.hexdigest()
            if md5message != md5calculated:
                raise SQSHashCheckException()
