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
from awsutils.utils.auth import SIGNATURE_V4_HEADERS


class SQSClient(AWSClient):

    def __init__(self, endpoint, access_key, secret_key, _ioloop=None, accNumber=None, secure=False):
        self._ioloop = _ioloop
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure)
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
        #TODO: implement
        pass

    @tornado.gen.engine
    def deleteMessage(self, callback, qName, receiptHandle):
        query = {'Action': 'DeleteMessage', 'ReceiptHandle': receiptHandle, 'Version': '2012-11-05'}
        status, data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V4_HEADERS)
        if status is True:
            try:
                data['awsresponse']['DeleteMessageResponse']
            except Exception as e:
                stauts = False
                data = e
        self._ioloop.add_callback(functools.partial(callback, status, data))

    @tornado.gen.engine
    def sendMessage(self, callback, qName, messageBody, delaySeconds=None, hashcheck=False):
        #TODO: implement
        pass


    #================================== helper functionality ===========================================================

    EXCEPTIONS = extractExceptionsFromModule2Dicitonary('awsutils.exceptions.sdb', awsutils.exceptions.sqs.SDBException)

    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
        if 'ErrorResponse' in awsresponse and 'Error' in awsresponse['ErrorResponse']:
            error = awsresponse['ErrorResponse']['Error']
            if error['Code'].replace('.','_') in self.EXCEPTIONS:
                raise self.EXCEPTIONS[error['Code'].replace('.','_')](awsresponse, httpstatus, httpreason, httpheaders)
            else:
                raise awsutils.exceptions.sqs.SQSException(awsresponse, httpstatus, httpreason, httpheaders)