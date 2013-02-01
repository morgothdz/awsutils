# test/test_sqs.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, unittest
from awsutils.sqsclient import SQSClient
from awsutils.sqs.service import SQSService

#!! this unit test require that setting.py with the folowing constants is created
from test.settings import access_key, secret_key

class SQSClientMethodTesting(unittest.TestCase):
    def setUp(self):
        self.sqs = SQSClient('sqs.us-east-1.amazonaws.com', access_key, secret_key)

    def tearDown(self):
        return
        #cleanup any queues created by the unittest
        createdqueues = self.sqs.listQueues(queueNamePrefix = 'awsutilsunittest_')
        print(createdqueues)
        for queue in createdqueues:
            try:
                print('deleting', queue)
                self.sqs.deleteQueue(queue['qName'])
            except Exception as e:
                #deleted queues will survive for a time if listQueues but deleteQueue once more on them will fail
                print("Failed to clean up queue", queue, e)
        self.sqs = None

    @unittest.skip("skip")
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

    @unittest.skip("skip")
    def test_queuepermissions(self):
        #TODO:
        pass
        #need a third party accoundid

    def test_high_level(self):
        sqs = SQSService(self.sqs)
        #sqs.createQuery('awsutilsunittest_1')
        query = sqs.getQuery('awsutilsunittest_1')
        message = query.receive(waitTimeSeconds=60)
        print(message)


if __name__ == '__main__':
    import logging, sys
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter('%(levelname)s %(name)s %(message)s'))

    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console)

    unittest.main()