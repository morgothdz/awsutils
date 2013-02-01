# awsutils/sqs/queue.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time
from awsutils.exceptions.aws import UserInputException
from awsutils.sqs.message import SQSMessage

class SQSQueue:
    COOLDOWN_BETWEEN_RECEIVEMESSAGES = 1

    def __init__(self, qName, sqsservice):
        """
        @param qName: the SQS queue name
        @type qName: str
        @param sqsservice: the s3client to be used for communication
        @type sqsservice: SQSService
        """
        self.qName = qName
        self.sqsservice = sqsservice

    def refresh(self):
        """
        Refresh the queue attributes
        """
        attributes = self.sqsservice.sqsclient.getQueueAttributes(self.qName)
        for attribute in attributes:
            setattr(self, attribute['Name'], attribute['Value'])

    def send(self, message, delaySeconds=None):
        """
        @type message: SQSMessage
        @type delaySeconds: int
        """
        if message.messageBody is None:
            raise UserInputException('this message has no body')
        self.sqsservice.sqsclient.sendMessage(self.qName, message.messageBody, delaySeconds)

    def delete(self, message):
        if message.receiptHandle is None:
            raise UserInputException('this message does not have receiptHandle set')
        if message.queue != self:
            raise UserInputException('this message does not belong to this queue')
        self.sqsservice.sqsclient.deleteMessage(message.queue.qName, message.receiptHandle)
        message.queue = None
        message.receiptHandle = None

    def receive(self, attributes='All', maxNumberOfMessages=1, visibilityTimeout=None, waitTimeSeconds=None):
        """
        @type visibilityTimeout: int
        @type waitTimeSeconds: int
        @type maxNumberOfMessages: int
        """
        if waitTimeSeconds is None or waitTimeSeconds <= 20:
            messages = self.sqsservice.sqsclient.receiveMessage(self.qName, attributes, maxNumberOfMessages,
                                                              visibilityTimeout, waitTimeSeconds)
        else:
            starttime = int(time.time())
            while True:
                remainingtime = waitTimeSeconds - (int(time.time()) - starttime)
                _waitTimeSeconds = min(remainingtime, 20)
                if _waitTimeSeconds <= 0:
                    return None
                print(time.time(), waitTimeSeconds, _waitTimeSeconds)
                messages = self.sqsservice.sqsclient.receiveMessage(self.qName, attributes, maxNumberOfMessages,                                                                    visibilityTimeout, _waitTimeSeconds)
                if messages != []:
                    break
                time.sleep(self.COOLDOWN_BETWEEN_RECEIVEMESSAGES)

        result = []
        if visibilityTimeout == None:
            visibilityTimeout = int(self.VisibilityTimeout)
        for item in messages:
            message = SQSMessage(item['Body'])
            message.id = item['MessageId']
            message.receiptHandle = item['ReceiptHandle']
            message.queue = self
            message.visibilityTimeout = visibilityTimeout
            message.receptionTimestamp = time.time()
            for attribute in item['Attribute']:
                setattr(message, attribute['Name'], attribute['Value'])
            if maxNumberOfMessages == 0:
                return message
            result.append(message)
        return result


    def __repr__(self):
        return "SQSQueue: " + repr(self.__dict__)




