# awsutils/sqs/queue.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, logging
from awsutils.exceptions.aws import UserInputException
from awsutils.sqs.message import SQSMessage

class SQSQueue:
    COOLDOWN_BETWEEN_RECEIVEMESSAGES = 1

    def __init__(self, qName, sqsclient, loadAttributes=True):
        """
        @param qName: the SQS queue name
        @type qName: str
        @param sqsclient: the s3client to be used for communication
        @type sqsclient: SQSClient
        """
        self.VisibilityTimeout = 60
        self.qName = qName
        self.sqsclient = sqsclient
        self.logger = logging.getLogger("%s.%s" % (type(self).__module__, type(self).__name__))
        self.logger.addHandler(logging.NullHandler())
        if loadAttributes:
            self.refresh()

    def refresh(self):
        """
        Refresh the queue attributes
        """
        attributes = self.sqsclient.getQueueAttributes(self.qName)
        for attribute in attributes:
            setattr(self, attribute['Name'], attribute['Value'])

    def send(self, message, delaySeconds=None):
        """
        @type message: SQSMessage
        @type delaySeconds: int
        """
        if message.messageBody is None:
            raise UserInputException('this message has no body')
        self.sqsclient.sendMessage(self.qName, message.messageBody, delaySeconds)

    def delete(self, message):
        if message.receiptHandle is None:
            raise UserInputException('this message does not have receiptHandle set')
        if message.queue != self:
            raise UserInputException('this message does not belong to this queue')
        self.sqsclient.deleteMessage(message.queue.qName, message.receiptHandle)
        message.queue = None
        message.receiptHandle = None

    def receive(self, attributes='All', maxNumberOfMessages=1, visibilityTimeout=None, waitTimeSeconds=None):
        """
        @type visibilityTimeout: int
        @type waitTimeSeconds: int
        @type maxNumberOfMessages: int
        """
        messages = []
        if waitTimeSeconds is None or waitTimeSeconds <= 20:
            messages = self.sqsclient.receiveMessage(self.qName, attributes, maxNumberOfMessages,
                                                     visibilityTimeout, waitTimeSeconds)
        else:
            starttime = int(time.time())
            while True:
                remainingtime = waitTimeSeconds - (int(time.time()) - starttime)
                _waitTimeSeconds = min(remainingtime, 20)
                if _waitTimeSeconds <= 0:
                    return None
                print(time.time(), waitTimeSeconds, _waitTimeSeconds)
                messages = self.sqsclient.receiveMessage(self.qName, attributes, maxNumberOfMessages,
                                                         visibilityTimeout, _waitTimeSeconds)
                if messages != []:
                    break
                time.sleep(self.COOLDOWN_BETWEEN_RECEIVEMESSAGES)

        result = []
        if visibilityTimeout is None:
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

    def messages(self, attributes='All', visibilityTimeout=None, autodelete=True, neverfail=True):
        """
        Generator to retrieve infinitely messages from a queue
        @param visibilityTimeout:
        @type visibilityTimeout: int
        @param autodelete: the message deletion will be attempted upon release
        @type autodelete: bool
        @param neverfail: don't fail on aws exceptions
        @type neverfail: bool
        @rtype: SQSMessage
        """
        while True:
            try:
                items = self.sqsclient.receiveMessage(self.qName, attributes,
                                                      maxNumberOfMessages = 1,
                                                      visibilityTimeout = visibilityTimeout,
                                                      waitTimeSeconds = 20)
            except Exception as e:
                if neverfail:
                    self.logger.warn('receiveMessage error [%s]', e)
                    time.sleep(self.COOLDOWN_BETWEEN_RECEIVEMESSAGES)
                    continue
                raise

            if items == []:
                time.sleep(self.COOLDOWN_BETWEEN_RECEIVEMESSAGES)
                continue

            item = items[0]

            message = SQSMessage(item['Body'])
            message.id = item['MessageId']
            message.receiptHandle = item['ReceiptHandle']
            message.queue = self
            message.visibilityTimeout = visibilityTimeout
            message.receptionTimestamp = time.time()
            for attribute in item['Attribute']:
                setattr(message, attribute['Name'], attribute['Value'])

            yield message

            if autodelete:
                timeleft = message.visibilityTimeoutLeft()
                if timeleft < 0:
                    self.logger.warn('Missed the message deletion by %s s. %s', -1*timeleft, message)
                else:
                    try:
                        self.delete(message)
                    except Exception as e:
                        if neverfail:
                            self.logger.warn("Could not delete the message %s [%s]", message, e)
                        raise

    def __repr__(self):
        return "SQSQueue: " + repr(self.__dict__)




