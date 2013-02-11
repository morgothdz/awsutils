# awsutils/sqs/service.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.exceptions.sqs import AWS_SimpleQueueService_NonExistentQueue
from awsutils.sqs.queue import SQSQueue

class SQSService:
    def __init__(self, sqsclient):
        """
        @param sqsclient: the s3client to be used for communication
        @type sqsclient: SQSClient
        """
        self.sqsclient = sqsclient

    def getQuery(self, qName):
        try:
            attributes = self.sqsclient.getQueueAttributes(qName)
        except AWS_SimpleQueueService_NonExistentQueue:
            return None
        query = SQSQueue(qName, self.sqsclient, False)
        for attribute in attributes:
            setattr(query, attribute['Name'], attribute['Value'])
        return query

    def createQueue(self, qName, delaySeconds=None, maximumMessageSize=None, messageRetentionPeriod=None,
                    receiveMessageWaitTimeSeconds=None, visibilityTimeout=None, policy=None):
        self.sqsclient.createQueue(qName, delaySeconds, maximumMessageSize, messageRetentionPeriod,
                                   receiveMessageWaitTimeSeconds, visibilityTimeout, policy)
        return self.getQuery(qName)

    def deleteQueue(self, qName):
        self.sqsclient.deleteQueue(qName)