# awsutils/s3/sqsclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import hashlib
from awsutils.client import AWSClient, UserInputException, IntegrityCheckException
from awsutils.iamclient import IAMClient
from awsutils.utils.auth import SIGNATURE_V4_HEADERS

class SQSClient(AWSClient):
    def __init__(self, endpoint, access_key, secret_key, accNumber=None, secure=False):
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure)
        #try to retrieve the curent user's account number
        if accNumber is None:
            iam = IAMClient(access_key, secret_key)
            userinfo = iam.getUser()
            self.accNumber = userinfo['UserId']
            iam.closeConnections()
            iam = None
        else:
            self.accNumber = accNumber

    def addPermission(self, qName, accNumber, permissions, endpoint=None):
        #TODO: implement
        pass

    def changeMessageVisibility(self, qName, receiptHandle, visibilityTimeout, accNumber=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        if accNumber is None: accNumber = self.accNumber
        if visibilityTimeout > 43200:
            raise UserInputException('param visibilityTimeout too big (max 43200 seconds)')
        query = {'Action': 'ChangeMessageVisibility', 'ReceiptHandle': receiptHandle,
                 'VisibilityTimeout': visibilityTimeout, 'Version': '2012-11-05'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint,
                                                        uri="/%s/%s" % (accNumber, qName))
        data['ChangeMessageVisibilityResponse']

    def changeMessageVisibilityBatch(self, qname, accNumber=None, endpoint=None):
        #TODO: implement
        """
        &ChangeMessageVisibilityBatchRequestEntry.1.Id=change_visibility_msg_2
        &ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle=Your_Receipt_Handle
        &ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=45
        """

    def createQueue(self, queueName, delaySeconds=None, maximumMessageSize=None, messageRetentionPeriod=None,
                    receiveMessageWaitTimeSeconds=None, visibilityTimeout=None, policy=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'CreateQueue', 'QueueName': queueName, 'Version': '2012-11-05'}
        i = 1
        if delaySeconds is not None:
            if not isinstance(delaySeconds, int) or not 0 <= delaySeconds <= 900:
                raise UserInputException("param delaySeconds sould be an integer between 0 and 900 (15 minutes)")
            query["Attribute.%d.Name"%(i,)] = "DelaySeconds"
            query["Attribute.%d.Value"%(i,)] = delaySeconds
            i += 1

        if maximumMessageSize is not None:
            if not isinstance(maximumMessageSize, int) or not 1024 <= maximumMessageSize <= 65536:
                raise UserInputException("param maximumMessageSize sould be an integer between 1024 and 65536 (64 KiB)")
            query["Attribute.%d.Name"%(i,)] = "MaximumMessageSize"
            query["Attribute.%d.Value"%(i,)] = maximumMessageSize
            i += 1

        if messageRetentionPeriod is not None:
            if not isinstance(messageRetentionPeriod, int) or not 60 <= messageRetentionPeriod <= 1209600:
                raise UserInputException(
                    "param messageRetentionPeriod sould be an integer between 60 (1 minute) and 1209600 (14 days)")
            query["Attribute.%d.Name"%(i,)] = "MessageRetentionPeriod"
            query["Attribute.%d.Value"%(i,)] = messageRetentionPeriod
            i += 1

        if receiveMessageWaitTimeSeconds is not None:
            if not isinstance(receiveMessageWaitTimeSeconds, int) or not 0 <= receiveMessageWaitTimeSeconds <= 20:
                raise UserInputException(
                    "param receiveMessageWaitTimeSeconds sould be an integer between 0 and 20 (secconds)")
            query["Attribute.%d.Name"%(i,)] = "ReceiveMessageWaitTimeSeconds"
            query["Attribute.%d.Value"%(i,)] = receiveMessageWaitTimeSeconds
            i += 1

        if visibilityTimeout is not None:
            if not isinstance(visibilityTimeout, int) or not 1024 <= visibilityTimeout <= 65536:
                raise UserInputException("param visibilityTimeout sould be an integer between 0 and 43200 (12 hours)")
            query["Attribute.%d.Name"%(i,)] = "VisibilityTimeout"
            query["Attribute.%d.Value"%(i,)] = visibilityTimeout
            i += 1

        #TODO: implement policy
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint)
        url = data['CreateQueueResponse']['CreateQueueResult']['QueueUrl']
        if url[7] == '/': url = url[8:]
        else: url = url[7:]
        url = url.split('/')
        return {'endpoint': url[0], 'accNumber': url[1], 'qName': url[2]}


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
            result.append({'endpoint': url[0], 'accNumber': url[1], 'qName': url[2]})
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
            return {'endpoint': url[0], 'accNumber': url[1], 'qName': url[2]}
        return None


    def receiveMessage(self, qName, attributes=None, maxNumberOfMessages=None, visibilityTimeout=None,
                       waitTimeSeconds=None, accNumber=None, endpoint=None):
        """
        attributes
            All-returns all values.
            SenderId -returns the AWS account number (or the IP address, if anonymous access is allowed) of the sender.
            SentTimestamp -returns the time when the message was sent (epoch time in milliseconds).
            ApproximateReceiveCount -returns the number of times a message has been received but not deleted.
            ApproximateFirstReceiveTimestamp -returns the time when the message was first received (epoch time in milliseconds).
        """
        if endpoint is None: endpoint = self.endpoint
        if accNumber is None: accNumber = self.accNumber
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
                                                        uri="/%s/%s" % (accNumber, qName))
        data = data['ReceiveMessageResponse']['ReceiveMessageResult']
        if not isinstance(data, dict):
            return []
            #example {'Body': 'sometext', 'ReceiptHandle': 'gH2...2e4=', 'MD5OfBody': '9d...dc', 'MessageId': '..'},
        if isinstance(data['Message'], dict):
            return data['Message']
        return data['Message']

    def deleteMessage(self, qName, receiptHandle, accNumber=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        if accNumber is None: accNumber = self.accNumber
        query = {'Action': 'DeleteMessage', 'ReceiptHandle': receiptHandle, 'Version': '2012-11-05'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint,
                                                        uri="/%s/%s" % (accNumber, qName))
        data['DeleteMessageResponse']

    def deleteMessageBatch(self, qName, receiptHandles, accNumber=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        if accNumber is None: accNumber = self.accNumber
        query = {'Action': 'DeleteMessageBatch', 'Version': '2012-11-05'}
        i = 1
        for receiptHandle in receiptHandles:
            query['DeleteMessageBatchRequestEntry.%d.Id'%(i,)] = 'id-%d'%(i,)
            query['DeleteMessageBatchRequestEntry.%d.ReceiptHandle'%(i,)] = receiptHandle
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint,
                                                        uri="/%s/%s" % (accNumber, qName))
        data = data['<DeleteMessageBatchResponse']['DeleteMessageBatchResult']
        if isinstance(data, str):
            return []
        data = data['DeleteMessageBatchResultEntry']
        if isinstance(data, dict):
            return data['Id']
        result = []
        for item in data:
            result.append(item['Id'])
        return result

    def sendMessage(self, qName, messageBody, delaySeconds=None, accNumber=None, endpoint=None, hashcheck=False):
        if endpoint is None: endpoint = self.endpoint
        if accNumber is None: accNumber = self.accNumber
        query = {'Action': 'SendMessage', 'MessageBody': messageBody, 'Version': '2012-11-05'}
        if delaySeconds is not None:
            if delaySeconds > 900:
                raise UserInputException('param delaySeconds too big (max 900 seconds)')
            query['DelaySeconds'] = delaySeconds
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                                                        region=endpoint[4:-14], service='sqs', host=endpoint,
                                                        uri="/%s/%s" % (accNumber, qName))
        md5message = data['SendMessageResponse']['SendMessageResult']['MD5OfMessageBody']
        if hashcheck:
            messageBody = messageBody.encode(encoding='utf8')
            md5 = hashlib.md5()
            md5.update(messageBody)
            md5calculated = md5.hexdigest()
            if md5message != md5calculated:
                raise IntegrityCheckException("sendMessage unexpected MD5OfMessageBody received",
                                              md5message, md5calculated)
