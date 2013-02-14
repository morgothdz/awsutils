# awsutils/tornado/sqsclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import functools, collections
import tornado.gen
from awsutils.tornado.awsclient import AWSClient
from awsutils.iamclient import IAMClient
from awsutils.exceptions.aws import UserInputException, extractExceptionsFromModule2Dicitonary
import awsutils.exceptions.sqs
from awsutils.utils.auth import SIGNATURE_V4_HEADERS, SIGNATURE_V2

class SQSClient(AWSClient):

    def __init__(self, endpoint, access_key, secret_key, _ioloop=None, accNumber=None, secure=False):
        """
        @type endpoint: the amazon endpoint of the service
        @type endpoint: str
        @type access_key: amazon access key
        @type access_key: str
        @type secret_key: amazon secret key
        @type secret_key: str
        @type secure: use https
        @type accNumber: the amazon user id (account number), if None the client will try to fetch the info with IAMClient
        @type accNumber: str
        @type secure: bool
        @type _ioloop: the tornado ioloop for processing the events
        @type _ioloop: tornado.ioloop.IOLoop
        """
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure, _ioloop = _ioloop)
        #try to retrieve the curent user's account number
        if accNumber is None:
            #TODO, we should do this asynchronous
            iam = IAMClient(access_key, secret_key)
            userinfo = iam.getUser()
            self.accNumber = userinfo['UserId']
            iam.closeConnections()
            iam = None
        else:
            self.accNumber = accNumber

    @tornado.gen.engine
    def receiveMessage(self, callback, qName, attributes=None, maxNumberOfMessages=None, visibilityTimeout=None,
                       waitTimeSeconds=None):
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

        data = yield tornado.gen.Task(self.request, query=query, uri="/%s/%s" % (self.accNumber, qName),
                                      signmethod=SIGNATURE_V4_HEADERS, region=self.endpoint[4:-14], service='sqs')
        data = data['data']['ReceiveMessageResponse']['ReceiveMessageResult']
        if not isinstance(data, dict):
            data = None
        elif isinstance(data['Message'], dict):
            data = [data['Message']]
        else:
            data = data['Message']

        self._ioloop.add_callback(functools.partial(callback, data))

    @tornado.gen.engine
    def deleteMessage(self, callback, qName, receiptHandle):
        query = {'Action': 'DeleteMessage', 'ReceiptHandle': receiptHandle, 'Version': '2012-11-05'}
        data = yield tornado.gen.Task(self.request, query=query, uri="/%s/%s" % (self.accNumber, qName),
                                      signmethod=SIGNATURE_V4_HEADERS, region=self.endpoint[4:-14], service='sqs')
        data['data']['DeleteMessageResponse']
        self._ioloop.add_callback(functools.partial(callback, True))

    @tornado.gen.engine
    def sendMessage(self, callback, qName, messageBody, delaySeconds=None, hashcheck=False):
        query = {'Action': 'SendMessage', 'MessageBody': messageBody, 'Version': '2012-11-05'}
        if delaySeconds is not None:
            if delaySeconds > 900:
                raise UserInputException('param delaySeconds too big (max 900 seconds)')
            query['DelaySeconds'] = delaySeconds
        data = yield tornado.gen.Task(self.request, query=query, uri="/%s/%s" % (self.accNumber, qName),
                                      signmethod=SIGNATURE_V4_HEADERS, region=self.endpoint[4:-14], service='sqs')
        data['data']['SendMessageResponse']['SendMessageResult']['MD5OfMessageBody']
        self._ioloop.add_callback(functools.partial(callback, True))

    @tornado.gen.engine
    def changeMessageVisibility(self, callback, qName, receiptHandle, visibilityTimeout):
        """
        @param qName: the sqs queue name
        @type qName: str
        """
        query = {'Action': 'ChangeMessageVisibility', 'ReceiptHandle': receiptHandle,
                 'VisibilityTimeout': visibilityTimeout, 'Version': '2012-11-05'}
        data = yield tornado.gen.Task(self.request, query=query, uri="/%s/%s" % (self.accNumber, qName),
                                      signmethod=SIGNATURE_V4_HEADERS, region=self.endpoint[4:-14], service='sqs')
        data['data']['ChangeMessageVisibilityResponse']
        self._ioloop.add_callback(functools.partial(callback, True))


    #================================== helper functionality ===========================================================

    EXCEPTIONS = extractExceptionsFromModule2Dicitonary('awsutils.exceptions.sqs', awsutils.exceptions.sqs.SQSException)

    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
        if 'ErrorResponse' in awsresponse and 'Error' in awsresponse['ErrorResponse']:
            error = awsresponse['ErrorResponse']['Error']
            if error['Code'].replace('.','_') in self.EXCEPTIONS:
                raise self.EXCEPTIONS[error['Code'].replace('.','_')](awsresponse, httpstatus, httpreason, httpheaders)
            else:
                raise awsutils.exceptions.sqs.SQSException(awsresponse, httpstatus, httpreason, httpheaders)