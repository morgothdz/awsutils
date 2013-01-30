import time
from unittest import TestCase
from awsutils.sqsclient import SQSClient

#!! this unit test require that setting.py with the folowing constants is created
from test.settings import access_key, secret_key

class SQSClientMethodTesting(TestCase):
    def setUp(self):
        self.sqs = SQSClient('sqs.us-east-1.amazonaws.com', access_key, secret_key)

    def tearDown(self):
        #cleanup any queues created by the unittest
        createdqueues = self.sqs.listQueues(queueNamePrefix = 'awsutilsunittest_')
        print(createdqueues)
        for queue in createdqueues:
            try:
                print('deleting', queue)
                self.sqs.deleteQueue(queue['qName'])
            except Exception as e:
                print("Failed to clean up queue", queue, e)
        self.sqs = None

    def test_queuemanagementsimple(self):
        newqueuename = "awsutilsunittest_%s"%(int(time.time()))
        self.sqs.createQueue(newqueuename)
        createdqueues = self.sqs.listQueues(queueNamePrefix = 'awsutilsunittest_')
        for queue in createdqueues:
            if queue['qName'] == newqueuename:
                break
        else:
            self.fail('Created queue named %s not found'%(newqueuename))
        self.sqs.deleteQueue(newqueuename)

        #TODO: check createQueue with params and read back the params
        #getQueueAttributes getQueueUrl

    def test_queuepermissions(self):
        #TODO:
        pass
        #need a third party accoundid
