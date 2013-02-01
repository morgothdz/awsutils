# awsutils/s3/sqsclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import hashlib, json
import awsutils.exceptions.sqs
from awsutils.exceptions.aws import UserInputException, IntegrityCheckException, extractExceptionsFromModule2Dicitonary
from awsutils.utils.auth import urlquote
from awsutils.awsclient import AWSClient
from awsutils.iamclient import IAMClient
from awsutils.utils.auth import SIGNATURE_V4_HEADERS

SQS_PERMISSIONS = {'*', 'SendMessage', 'ReceiveMessage', 'DeleteMessage', 'ChangeMessageVisibility',
                   'GetQueueAttributes', 'GetQueueUrl'}

SQS_QUEUE_ATTRIBUTES = {'All', 'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible',
                        'ApproximateNumberOfMessagesDelayed', 'VisibilityTimeout', 'CreatedTimestamp',
                        'LastModifiedTimestamp', 'Policy', 'MaximumMessageSize', 'MessageRetentionPeriod',
                        'QueueArn', 'ReceiveMessageWaitTimeSeconds', 'DelaySeconds'}

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

    def addPermission(self, qName, label, permissions):
        """
        @param qName: the sqs queue name
        @type qName: str
        @param permissions: dictionary {accoundid:actionname}
        @type permissions: dict
        """
        query ={'Action':'AddPermission', 'Label':label, 'Version':'2012-11-05'}
        i = 1
        for accountid in permissions:
            if permissions[accountid] not in SQS_PERMISSIONS:
                raise UserInputException('unknown action')
            query['AWSAccountId.%d'%(i,)] = accountid
            query['ActionName.%d'%(i,)] = permissions[accountid]
            i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                            region=self.endpoint[4:-14], service='sqs', host=self.endpoint,
                            uri="/%s/%s" % (self.accNumber, qName))
        data['awsresponse']['AddPermissionResponse']

    def removePermission(self, qName, label):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query ={'Action':'RemovePermission', 'Label':label, 'Version':'2012-11-05'}
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,
                            region=self.endpoint[4:-14], service='sqs', host=self.endpoint,
                            uri="/%s/%s" % (self.accNumber, qName))
        data['awsresponse']['RemovePermissionResponse']

    def changeMessageVisibility(self, qName, receiptHandle, visibilityTimeout):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        if visibilityTimeout > 43200:
            raise UserInputException('param visibilityTimeout too big (max 43200 seconds)')
        query = {'Action': 'ChangeMessageVisibility', 'ReceiptHandle': receiptHandle,
                 'VisibilityTimeout': visibilityTimeout, 'Version': '2012-11-05'}
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        return data['awsresponse']['ChangeMessageVisibilityResponse']

    def changeMessageVisibilityBatch(self, qName, receipts):
        """
        @param qName: the sqs queue name
        @type qName: str
        @param receipts: dictionary {receiptHandle:visibilityTimeout}
        @type receipts: dict
        """
        query ={'Action':'ChangeMessageVisibilityBatch', 'Version': '2012-11-05'}
        i = 1
        for receiptHandle in receipts:
            if receipts[receiptHandle] > 43200:
                raise UserInputException('param visibilityTimeout too big (max 43200 seconds)')
            query['ChangeMessageVisibilityBatchRequestEntry.%s.Id'%(i,)] = 'msg%s'%(i,)
            query['ChangeMessageVisibilityBatchRequestEntry.%s.ReceiptHandle'%(i,)] = receiptHandle
            query['ChangeMessageVisibilityBatchRequestEntry.%s.VisibilityTimeout'%(i,)] = receipts[receiptHandle]
            i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        return data['awsresponse']['ChangeMessageVisibilityBatchResponse']

    def createQueue(self, qName, delaySeconds=None, maximumMessageSize=None, messageRetentionPeriod=None,
                    receiveMessageWaitTimeSeconds=None, visibilityTimeout=None, policy=None):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query = {'Action': 'CreateQueue', 'QueueName': qName, 'Version': '2012-11-05'}
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

        if policy is not None:
            query["Attribute.%d.Name"%(i,)] = "Policy"
            query["Attribute.%d.Value"%(i,)] = urlquote(policy)
            i += 1

        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint)
        url = data['awsresponse']['CreateQueueResponse']['CreateQueueResult']['QueueUrl']
        if url[7] == '/': url = url[8:]
        else: url = url[7:]
        url = url.split('/')
        return {'endpoint': url[0], 'accNumber': url[1], 'qName': url[2]}

    def deleteQueue(self, qName):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query ={'Action':'DeleteQueue', 'Version': '2012-11-05'}
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        data['awsresponse']['DeleteQueueResponse']

    def setQueueAttributes(self, qName, delaySeconds=None, maximumMessageSize=None, messageRetentionPeriod=None,
                           receiveMessageWaitTimeSeconds=None, visibilityTimeout=None, policy=None):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query ={'Action':'SetQueueAttributes', 'Version': '2012-11-05'}

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

        if policy is not None:
            query["Attribute.%d.Name"%(i,)] = "Policy"
            query["Attribute.%d.Value"%(i,)] = urlquote(policy)
            i += 1

        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
            service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        data['awsresponse']['SetQueueAttributesResponse']

    def getQueueAttributes(self, qName, attributes = 'All'):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query ={'Action':'GetQueueAttributes', 'Version': '2012-11-05'}
        i = 1
        if isinstance(attributes, str):
            query['AttributeName.1'] = attributes
        else:
            for attribute in attributes:
                if attribute not in SQS_QUEUE_ATTRIBUTES:
                    raise UserInputException('unknown attribute')
                query['AttributeName.%d'%(i,)] = attribute
                i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query,region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        return data['awsresponse']['GetQueueAttributesResponse']['GetQueueAttributesResult']['Attribute']


    def listQueues(self, queueNamePrefix=None):
        """
        @param queueNamePrefix: the sqs queue name prefix
        @type queueNamePrefix: str
        """
        query = {'Action': 'ListQueues', 'Version': '2012-11-05'}
        if queueNamePrefix is not None:
            query['QueueNamePrefix'] = queueNamePrefix
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint)
        result = []
        data = data['awsresponse']['ListQueuesResponse']['ListQueuesResult']
        urls = []
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

    def getQueueUrl(self, qName, queueOwnerAWSAccountId=None):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query = {'Action': 'GetQueueUrl', 'QueueName': qName, 'Version': '2012-11-05'}
        if queueOwnerAWSAccountId is not None:
            query['QueueOwnerAWSAccountId'] = queueOwnerAWSAccountId
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint)
        data = data['awsresponse']['GetQueueUrlResponse']['GetQueueUrlResult']
        if 'QueueUrl' in data:
            url = data['QueueUrl']
            if url[7] == '/': url = url[8:]
            else: url = url[7:]
            url = url.split('/')
            return {'endpoint': url[0], 'accNumber': url[1], 'qName': url[2]}
        return None


    def receiveMessage(self, qName, attributes=None, maxNumberOfMessages=None, visibilityTimeout=None,
                       waitTimeSeconds=None):
        """
        @param qName: the sqs queue name
        @type qName: str
        attributes
            All-returns all values.
            SenderId -returns the AWS account number (or the IP address, if anonymous access is allowed) of the sender.
            SentTimestamp -returns the time when the message was sent (epoch time in milliseconds).
            ApproximateReceiveCount -returns the number of times a message has been received but not deleted.
            ApproximateFirstReceiveTimestamp -returns the time when the message was first received (epoch time in milliseconds).
        """
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

        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        data = data['awsresponse']['ReceiveMessageResponse']['ReceiveMessageResult']
        if not isinstance(data, dict):
            return []
        if isinstance(data['Message'], dict):
            return [data['Message']]
        return data['Message']

    def deleteMessage(self, qName, receiptHandle):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query = {'Action': 'DeleteMessage', 'ReceiptHandle': receiptHandle, 'Version': '2012-11-05'}
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        data['awsresponse']['DeleteMessageResponse']

    def deleteMessageBatch(self, qName, receiptHandles):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query = {'Action': 'DeleteMessageBatch', 'Version': '2012-11-05'}
        i = 1
        for receiptHandle in receiptHandles:
            query['DeleteMessageBatchRequestEntry.%d.Id'%(i,)] = 'id-%d'%(i,)
            query['DeleteMessageBatchRequestEntry.%d.ReceiptHandle'%(i,)] = receiptHandle
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        data = data['awsresponse']['DeleteMessageBatchResponse']['DeleteMessageBatchResult']
        if isinstance(data, str):
            return []
        data = data['DeleteMessageBatchResultEntry']
        if isinstance(data, dict):
            return data['Id']
        result = []
        for item in data:
            result.append(item['Id'])
        return result

    def sendMessage(self, qName, messageBody, delaySeconds=None, hashcheck=False):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query = {'Action': 'SendMessage', 'MessageBody': messageBody, 'Version': '2012-11-05'}
        if delaySeconds is not None:
            if delaySeconds > 900:
                raise UserInputException('param delaySeconds too big (max 900 seconds)')
            query['DelaySeconds'] = delaySeconds
        data = self.request(method="GET", signmethod=SIGNATURE_V4_HEADERS, query=query, region=self.endpoint[4:-14],
                             service='sqs', host=self.endpoint, uri="/%s/%s" % (self.accNumber, qName))
        md5message = data['awsresponse']['SendMessageResponse']['SendMessageResult']['MD5OfMessageBody']
        if hashcheck:
            messageBody = messageBody.encode(encoding='utf8')
            md5 = hashlib.md5()
            md5.update(messageBody)
            md5calculated = md5.hexdigest()
            if md5message != md5calculated:
                raise IntegrityCheckException("sendMessage unexpected MD5OfMessageBody received",
                                              md5message, md5calculated)

    #================================== helper functionality ===========================================================
    EXCEPTIONS = extractExceptionsFromModule2Dicitonary('awsutils.exceptions.sqs',
                                                         awsutils.exceptions.sqs.SQSException)
    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
        if 'ErrorResponse' in awsresponse and 'Error' in awsresponse['ErrorResponse']:
            error = awsresponse['ErrorResponse']['Error']
            if error['Code'].replace('.','_') in self.EXCEPTIONS:
                raise self.EXCEPTIONS[error['Code'].replace('.','_')](awsresponse, httpstatus, httpreason, httpheaders)
            else:
                raise awsutils.exceptions.sqs.SQSException(awsresponse, httpstatus, httpreason, httpheaders)
