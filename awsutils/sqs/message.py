# awsutils/sqs/message.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time
from awsutils.exceptions.aws import UserInputException

class SQSMessage:
    def __init__(self, messageBody=None, queue=None):
        self.messageBody = messageBody
        self.receiptHandle = None
        self.queue = None

    def getBody(self):
        return self.messageBody

    def setBody(self, messageBody):
        self.messageBody = messageBody

    def delete(self):
        if self.queue is None:
            raise UserInputException('This message does not belong to any queue')
        self.queue.sqsclient.deleteMessage(self.queue.qName, self.receiptHandle)

    def visibilityTimeoutLeft(self):
        if self.queue is None:
            raise UserInputException('This message does not belong to any queue')
        return self.VisibilityTimeout - (time.time() - self.receptionTimestamp)

    def changeVisibility(self, visibilityTimeout):
        if self.queue is None:
            raise UserInputException('This message does not belong to any queue')
        if self.receiptHandle is None:
            raise UserInputException('This message does not have a receipt handle')
        self.queue.lsqsclient.changeMessageVisibility(self.queue.qName, self.receiptHandle, visibilityTimeout)
        self.VisibilityTimeout = visibilityTimeout

    def __repr__(self):
        return 'SQSMessage: ' + repr(self.__dict__)
