# test/test_sdb.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

#!! this unit test require that setting.py with the folowing constants is created
# access_key and secret_key

import time
import unittest
from unittest import TestCase
from test.settings import access_key, secret_key
from awsutils.sdbclient import SimpleDBClient

#helper
class SDB:
    CLIENT = None
    CLEANUP = False
    DOMAIN_NAME = ""
    TEST_FAILED = True


class SDBClientMethodTesting(TestCase):

    def setUp(self):
        if SDB.CLIENT is None:
            self.sdb = SimpleDBClient('sdb.amazonaws.com', access_key, secret_key)
            domainList = self.sdb.listDomains()
            self.assertIsInstance(domainList, list)
            searchedName = "aws_unittest"
            for domain in domainList:
                if "aws_unittest" in domain:
                    try:
                        self.sdb.deleteDomain(domain)
                    except Exception as e:
                        self.fail("Failed to delete domain - setUp()")
            SDB.DOMAIN_NAME = "aws_unittest_%s" % (int(time.time()))
            self.sdb.createDomain(SDB.DOMAIN_NAME)
            SDB.CLIENT = self.sdb
        else:
            self.sdb = SDB.CLIENT


    def tearDown(self):
        if SDB.TEST_FAILED:
            domainList = self.sdb.listDomains()
            self.assertIsInstance(domainList, list)
            searchedName = "aws_unittest"
            for domain in domainList:
                if "aws_unittest" in domain:
                    try:
                        self.sdb.deleteDomain(domain)
                    except Exception as e:
                        self.fail("Failed to delete domain - tearDown()")
            SDB.DOMAIN_NAME = "aws_unittest_%s" % (int(time.time()))
            self.sdb.createDomain(SDB.DOMAIN_NAME)
            SDB.CLIENT = self.sdb
        else:
            pass


    def test_putAttributes(self):
        #if the test will fail (the test will exit with SDB.TEST_FAILED beeing True)
        #   the tearDonw() will clean up the data
        SDB.TEST_FAILED = True
        attributes = {"Name1": "1", "Name2": "2", "Name3": "3"}
        try:
            self.sdb.putAttributes(SDB.DOMAIN_NAME, "ItemName1", attributes)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed PutAttributes")
        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName1")
        if isinstance(result, tuple):
            expected_result_tuple = (('Name1', '1'), ('Name2', '2'), ('Name3', '3'))
            if result.__len__() != 3:
                self.fail("Attribute count doesn't match")
            for each_attr in result:
                if each_attr not in expected_result_tuple:
                    self.fail("Attribute Error")
        else:
            self.fail("Failed GetAttributes")

        attributes = {"Name": "0"}
        try:
            self.sdb.putAttributes(SDB.DOMAIN_NAME, "ItemName2", attributes)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed PutAttributes")
        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName2")
        if isinstance(result, tuple):
            expected_result_tuple = ('Name', '0')
            if result.__len__() != 2:   #here we have only a simple tuple
                self.fail("Attribute count doesn't match")
            for each_attr in result:
                if each_attr not in expected_result_tuple:
                    self.fail("Attribute Error")
        else:
            self.fail("Failed GetAttributes -  isinstance not tuple")

        #cleanup attributes
        try:
            self.sdb.batchDeleteAttributes(SDB.DOMAIN_NAME, {"ItemName1": {}, "ItemName2": {}})
        except Exception as e:
            self.fail("Failed DeleteAttributes")
        SDB.TEST_FAILED = False


    def test_getAttributes(self):
        SDB.TEST_FAILED = True
        attributes = {"Name1": "1", "Name2": "2", "Name3": "3"}
        try:
            self.sdb.putAttributes(SDB.DOMAIN_NAME, "ItemName", attributes)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed PutAttributes")
        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName")
        if isinstance(result, tuple):
            expected_result_tuple = (('Name1', '1'), ('Name2', '2'), ('Name3', '3'))
            if result.__len__() != 3:
                self.fail("Attribute count doesn't match")
            for each_attr in result:
                if each_attr not in expected_result_tuple:
                    self.fail("Attribute Error")
        else:
            self.fail("Failed GetAttributes")
        #cleanup attributes
        try:
            self.sdb.deleteAttributes(SDB.DOMAIN_NAME,"ItemName",{})
        except Exception as e:
            self.fail("Failed DeleteAttributes - test_getAttributes")
        SDB.TEST_FAILED = False


    def test_deleteAttributes(self):
        SDB.TEST_FAILED = True
        attributes = {"Name1": "1", "Name2": "2", "Name3": "3"}
        try:
            self.sdb.putAttributes(SDB.DOMAIN_NAME, "ItemName", attributes)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed PutAttributes")
        try:
            self.sdb.deleteAttributes(SDB.DOMAIN_NAME, "ItemName", {"Name1": '1'}, )
            time.sleep(2)
        except Exception as e:
            self.fail("Failed DeleteAttributes")
        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName")
        if isinstance(result, tuple):
            expected_result_tuple = (('Name2', '2'), ('Name3', '3'))
            if result.__len__() != 2:
                self.fail("Attribute count doesn't match")
            for each_attr in result:
                if each_attr not in expected_result_tuple:
                    self.fail("Attribute Error")
        else:
            self.fail("Failed GetAttributes")

        try:
            self.sdb.deleteAttributes(SDB.DOMAIN_NAME, "ItemName", {})
        except Exception as e:
            self.fail("Failed deleteAttributes - test_deleteAttributes")
        SDB.TEST_FAILED = False


    def test_select(self):
        SDB.TEST_FAILED = True
        #result = self.sdb.domainMetadata(SDB.DOMAIN_NAME)
        self.createmultipleAttributes()
        expected_items = ["ItemName1", "ItemName2"]
        select = "SELECT Color from %s" % SDB.DOMAIN_NAME
        result = self.sdb.select(select)
        for item in result:
            if "Attribute" in item.keys():
                if item['Name'] not in expected_items:
                    self.fail("Failed")
        #cleanup attributes
        try:
            self.sdb.batchDeleteAttributes(SDB.DOMAIN_NAME, {"ItemName1":{}, "ItemName2":{}, "ItemName3":{}})
        except Exception as e:
            self.fail("Failed DeleteAttributes - test_select")
        SDB.TEST_FAILED = False


    def test_domainMetadata(self):
        SDB.TEST_FAILED = True
        result = self.sdb.domainMetadata(SDB.DOMAIN_NAME)
        self.createmultipleAttributes()
        time.sleep(3)
        result = self.sdb.domainMetadata(SDB.DOMAIN_NAME)
        self.assertEqual(result['AttributeNameCount'], '3')
        self.assertEqual(result['AttributeNamesSizeBytes'], '15')
        self.assertEqual(result['AttributeValueCount'], '8')
        self.assertEqual(result['AttributeValuesSizeBytes'], '37')
        self.assertEqual(result['ItemCount'], '3')
        self.assertTrue('ItemNamesSizeBytes' in result)
        self.assertTrue('Timestamp' in result)

        #cleanup attributes
        try:
            self.sdb.batchDeleteAttributes(SDB.DOMAIN_NAME, {"ItemName1":{}, "ItemName2":{}, "ItemName3":{}})
        except Exception as e:
            self.fail("Failed DeleteAttributes - test_domainMetadata")
        SDB.TEST_FAILED = False


    def test_batchPutAttribute(self):
        SDB.TEST_FAILED = True
        attributes1 = {"Color": "Blue", "Sound": "loud", "Taste": "sweet"}
        attributes2 = {"Color": "Green", "Sound": "quiet", "Taste": "hot"}
        attributes3 = {"Sound": "normal", "Taste": "sweet"}
        items = {"ItemName1": attributes1, "ItemName2": attributes2, "ItemName3": attributes3}
        try:
            self.sdb.batchPutAttributes(SDB.DOMAIN_NAME, items)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed batchPutAttribute")

        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName1")
        if isinstance(result, tuple):
            expected_result_tuple = (("Color", "Blue"), ("Sound", "loud"), ("Taste", "sweet"))
            if result.__len__() != 3:
                self.fail("Attribute count doesn't match - ItemName1")
            for each_attr in result:
                if each_attr not in expected_result_tuple:
                    self.fail("Attribute Error - ItemName1")
        else:
            self.fail("Failed GetAttributes - ItemName1")

        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName2")
        if isinstance(result, tuple):
            expected_result_tuple = (("Color", "Green"), ("Sound", "quiet"), ("Taste", "hot"))
            if result.__len__() != 3:
                self.fail("Attribute count doesn't match - ItemName2")
            for each_attr in result:
                if each_attr not in expected_result_tuple:
                    self.fail("Attribute Error - ItemName2")
        else:
            self.fail("Failed GetAttributes ItemName2")

        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName3")
        if isinstance(result, tuple):
            expected_result_tuple = (("Sound", "normal"), ("Taste", "sweet"))
            if result.__len__() != 2:
                self.fail("Attribute count doesn't match - ItemName3")
            for each_attr in result:
                if each_attr not in expected_result_tuple:
                    self.fail("Attribute Error - ItemName3")
        else:
            self.fail("Failed GetAttributes ItemName3")

        #cleanup attributes
        try:
            self.sdb.batchDeleteAttributes(SDB.DOMAIN_NAME, {"ItemName1":{}, "ItemName2":{}, "ItemName3":{}})
        except Exception as e:
            self.fail("Failed DeleteAttributes - test_batchPutAttribute")
        SDB.TEST_FAILED = False


    def test_batchDeleteAttributes(self):
        SDB.TEST_FAILED = True
        self.createmultipleAttributes()
        #verify the batchDeleteAttributes with intem and attributes
        try:
            self.sdb.batchDeleteAttributes(SDB.DOMAIN_NAME,{"ItemName1":{"Color":"Blue"},
                                                            "ItemName2":{"Sound":"quiet"}})
            time.sleep(2)
        except Exception as e:
            self.fail("Failed batchDeleteAttributes %s"%e)
        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName1")
        expected_attributes = (("Sound","loud"), ("Taste","sweet"))
        if result.__len__() != 2:
            self.fail("Attribute count doesn't match - ItemName1 - test_batchDeleteAttributes")
        for each_attr in result:
            if each_attr not in expected_attributes:
                self.fail("Attribute Error - ItemName1 - test_batchDeleteAttributes")

        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName2")
        expected_attributes = (("Color","Green"), ("Taste","hot"))
        if result.__len__() != 2:
            self.fail("Attribute count doesn't match - ItemName2 - test_batchDeleteAttributes")
        for each_attr in result:
            if each_attr not in expected_attributes:
                self.fail("Attribute Error - ItemName2 - test_batchDeleteAttributes")

        result = self.sdb.getAttributes(SDB.DOMAIN_NAME, "ItemName3")
        expected_attributes = (("Sound","normal"), ("Taste","sweet"))
        if result.__len__() != 2:
            self.fail("Attribute count doesn't match - ItemName3 - test_batchDeleteAttributes")
        for each_attr in result:
            if each_attr not in expected_attributes:
                self.fail("Attribute Error - ItemName3 - test_batchDeleteAttributes")

        #verify the batchDeleteAttributes with intems only
        try:
            self.sdb.batchDeleteAttributes(SDB.DOMAIN_NAME, {"ItemName1":{}, "ItemName2":{}, "ItemName3":{}})
        except Exception as e:
            self.fail("Failed DeleteAttributes - test_batchDeleteAttributes")
        time.sleep(3)
        result = self.sdb.domainMetadata(SDB.DOMAIN_NAME)
        self.assertEqual(result['AttributeNameCount'], '0')
        self.assertEqual(result['AttributeNamesSizeBytes'], '0')
        self.assertEqual(result['AttributeValueCount'], '0')
        self.assertEqual(result['AttributeValuesSizeBytes'], '0')
        self.assertEqual(result['ItemCount'], '0')
        self.assertEqual(result['ItemNamesSizeBytes'], '0')
        SDB.TEST_FAILED = False


    def createmultipleAttributes(self):
        attributes1 = {"Color":"Blue", "Sound":"loud", "Taste":"sweet"}
        attributes2 = {"Color":"Green", "Sound":"quiet", "Taste":"hot"}
        attributes3 = {"Sound":"normal", "Taste":"sweet"}
        try:
            self.sdb.putAttributes(SDB.DOMAIN_NAME, "ItemName1", attributes1)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed PutAttributes")
        try:
            self.sdb.putAttributes(SDB.DOMAIN_NAME, "ItemName2", attributes2)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed PutAttributes")
        try:
            self.sdb.putAttributes(SDB.DOMAIN_NAME, "ItemName3", attributes3)
            time.sleep(2)
        except Exception as e:
            self.fail("Failed PutAttributes")


if __name__ == '__main__':
    unittest.main()
