# test/test_sqs.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, unittest
from awsutils.sqsclient import SQSClient
from awsutils.sqs.service import SQSService

#!! this unit test require that setting.py with the folowing constants is created
from test.settings import access_key, secret_key, account_number, access_key_2nd, secret_key_2nd, account_number_2nd

class SQS:
    CLIENT = None
    CLEAN_Q = True
    QNAME = ""

class SQSClientMethodTesting(unittest.TestCase):

    def setUp(self):
        if SQS.CLIENT is None:
            time.sleep(2)
            SQS.QNAME = "aws_unittest_queue_%s" % (int(time.time()))
            self.sqs = SQSClient('sqs.eu-west-1.amazonaws.com', access_key, secret_key)
            self.sqs.createQueue(SQS.QNAME)
            SQS.CLIENT = self.sqs
        else:
            time.sleep(2)
            self.sqs = SQS.CLIENT

    def tearDown(self):
        if SQS.CLEAN_Q:
            #cleanup any queues created by the unittest
            time.sleep(2)
            try:
                createdqueues = self.sqs.listQueues(queueNamePrefix='aws_unittest_queue_')
                if createdqueues is not None:
                    #print(createdqueues)
                    for queue in createdqueues:
                        try:
                            self.sqs.deleteQueue(queue['qName'])
                        except Exception as e:
                            #deleted queues will survive for a time in listQueues but deleteQueue once more on them will fail
                            print("Failed to clean up queue - tearDown", e)
            except Exception as e:
                print("nothing to cleanup")
                pass
            SQS.CLIENT = None
        else:
            pass


    def test_createQueue(self):
        SQS.CLEAN_Q = True
        qName = "aws_unittest_queue_%s" % (int(time.time()))
        self.expected_qresponse = {'endpoint': 'sqs.eu-west-1.amazonaws.com', 'qName': qName,
                                   "accNumber": account_number}
        result = self.sqs.createQueue(qName)
        self.assertDictEqual(result, self.expected_qresponse)
        #cleanup
        try:
            self.sqs.deleteQueue(qName)
        except Exception as e:
            self.fail("Failed to delete Queue - test_createQueue")
        SQS.CLEAN_Q = False

    def test_getQueueUrl(self):
        SQS.CLEAN_QUEUE = True
        #get the url for the specific queue
        result = self.sqs.getQueueUrl(SQS.QNAME)
        self.assertIsNotNone(result)
        #fail to get the url for a non existent queue
        qName = "test"
        try:
            self.sqs.getQueueUrl(qName)
        except Exception as e:
            self.assertEqual(e.args[0]['ErrorResponse']['Error']['Message'],
                             'The specified queue does not exist for this wsdl version.')
        SQS.CLEAN_QUEUE = False

    def test_setQueueAttributes(self):
        SQS.CLEAN_QUEUE = True
        self.expected_attributes = {'DelaySeconds': 50, 'MaximumMessageSize': 2048, 'MessageRetentionPeriod': 70,
                                    'ReceiveMessageWaitTimeSeconds': 10, 'VisibilityTimeout': 60 }
        self.attributes = { 'DelaySeconds', 'MaximumMessageSize', 'MessageRetentionPeriod',
                            'ReceiveMessageWaitTimeSeconds','VisibilityTimeout' }

        self.sqs.setQueueAttributes(SQS.QNAME,delaySeconds=self.expected_attributes['DelaySeconds'],
                                    maximumMessageSize=self.expected_attributes['MaximumMessageSize'],
                                    messageRetentionPeriod=self.expected_attributes['MessageRetentionPeriod'],
                                    receiveMessageWaitTimeSeconds=self.expected_attributes['ReceiveMessageWaitTimeSeconds'],
                                    visibilityTimeout=self.expected_attributes['VisibilityTimeout'] )
        time.sleep(2)
        result = self.sqs.getQueueAttributes(SQS.QNAME, self.attributes)
        self.assertGreater(len(result), 0)
        result_attributes = {}
        for attrib in result:
            attrib_name = attrib['Name']
            attrib_value = int(attrib['Value'])
            result_attributes.update({attrib_name:attrib_value})
        self.assertDictEqual(result_attributes, self.expected_attributes)
        #cleanup needed so SQS.CLEAN_QUEUE will remain True

    def test_getQueueAttributes(self):
        SQS.CLEAN_QUEUE = True

        #get the attributes of the created queue
        result = self.sqs.getQueueAttributes(SQS.QNAME)
        self.assertGreater(len(result), 0)

        qName = "test"
        #should fail to get the attributes to the non existing queue
        try:
            result = self.sqs.getQueueAttributes(qName)
        except Exception as e:
            self.assertEqual(e.args[0]['ErrorResponse']['Error']['Message'],
                             'The specified queue does not exist for this wsdl version.')
        SQS.CLEAN_QUEUE = False


    def test_add_remove_Permission(self):
        SQS.CLEAN_QUEUE = True
        try:
            self.sqs.addPermission(SQS.QNAME, "Second Account Messages", {str(account_number_2nd):'*'})
        except Exception as e:
            print(e.args[0]['ErrorResponse']['Error']['Message'])
        try:
            self.sqs.removePermission(SQS.QNAME, "Second Account Messages")
        except Exception as e:
            self.fail("remove permission failed")
        qName = "test"
        try:
            self.sqs.removePermission(qName, "Label")
            self.fail("wrong qName")
        except Exception as e:
            self.failureException
        try:
            self.sqs.rselfemovePermission((SQS.QNAME, "Wrong Label"))
            self.fail("wrong label")
        except Exception as e:
            self.failureException

        try:
            self.sqs.addPermission(SQS.QNAME, "Second Account Messages", {str(account_number_2nd):'*'})
        except Exception as e:
            print(e.args[0]['ErrorResponse']['Error']['Message'])

        #cleanup needed
        #SQS.CLEAN_QUEUE = True


        #postpone this part (verify if the second account can access this queue
        #sqs2nd = SQSClient('sqs.eu-west-1.amazonaws.com', access_key_2nd, secret_key_2nd)
        #result = sqs2nd.sendMessage(SQS.QNAME, "small message")
        #try:
            #sqs2nd.sendMessalusage(SQS.QNAME,"this is the body from the second user")
        #except Exception as e:
            #print(e)
        #self.assertIsNotNone(result_2nd)

    def test_send_receive_Message(self):
        SQS.CLEAN_QUEUE = True
        self.messageBody="This is the message Body No.1"
        try:
            self.sqs.sendMessage(SQS.QNAME, self.messageBody)
        except Exception as e:
            self.fail("sendMessage Failed - test_send_receive_Message")
        result = self.sqs.receiveMessage(SQS.QNAME)
        self.assertEqual(self.messageBody, result[0]['Body'])
        result = self.sqs.receiveMessage(SQS.QNAME)
        if result != []:
            self.fail("second message received")
        #cleanup needed SQS.CLEAN_QUEUE = True


    def test_remove_Message(self):
        SQS.CLEAN_QUEUE = True
        self.messageBody="This is the message Body No.0"
        try:
            self.sqs.sendMessage(SQS.QNAME, self.messageBody)
        except Exception as e:
            self.fail("sendMessage Failed")
        result = self.sqs.receiveMessage(SQS.QNAME)
        try:
            self.sqs.deleteMessage(SQS.QNAME,result[0]['ReceiptHandle'])
        except Exception as e:
            self.fail("delete Message Failed")
        result = self.sqs.getQueueAttributes(SQS.QNAME, attributes={'ApproximateNumberOfMessages'})
        self.assertEqual(0, int(result['Value']))
        #cleanup needed -> SQS.CLEAN_QUEUE = True


    def test_deleteMessageBatch(self):
        SQS.CLEAN_QUEUE = True
        self.messageBody0="This is the message Body No.0"
        self.messageBody1="This is the message Body No.1"
        self.messageBody2="This is the message Body No.2"
        receiptHandlers = []
        try:
            self.sqs.sendMessage(SQS.QNAME, self.messageBody0)
        except Exception as e:
            self.fail("sendMessage0 Failed")
        try:
            self.sqs.sendMessage(SQS.QNAME, self.messageBody1)
        except Exception as e:
            self.fail("sendMessage1 Failed")
        try:
            self.sqs.sendMessage(SQS.QNAME, self.messageBody2)
        except Exception as e:
            self.fail("sendMessage2 Failed")

        #receive the messages to get their receiptHandle
        result = self.sqs.receiveMessage(SQS.QNAME)
        receiptHandlers.append(result[0]['ReceiptHandle'])
        result = self.sqs.receiveMessage(SQS.QNAME)
        receiptHandlers.append(result[0]['ReceiptHandle'])
        result = self.sqs.receiveMessage(SQS.QNAME)
        receiptHandlers.append(result[0]['ReceiptHandle'])

        result = self.sqs.deleteMessageBatch(SQS.QNAME, receiptHandlers )
        expected_result=['id-1', 'id-2', 'id-3']
        expected_result.sort()
        if isinstance(result,list):
            result.sort()
        self.assertListEqual(expected_result, result)
        SQS.CLEAN_QUEUE = False

    @unittest.skip("skipped until SendMessageBatch is not implemented")
    def test_Message_Batch(self):
        sqs = SQSClient('sqs.eu-west-1.amazonaws.com', access_key, secret_key)
        self.messageBody0="This is the message Body No.0"
        self.messageBody1="This is the message Body No.1"
        self.messageBody2="This is the message Body No.2"
        #TODO
        pass

if __name__ == '__main__':
    unittest.main()