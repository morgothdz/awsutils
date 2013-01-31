# test/test_sdb.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

#!! this unit test require that setting.py with the folowing constants is created

import time
import unittest
from unittest import TestCase
from test.settings import access_key, secret_key
from awsutils.sdbclient import SimpleDBClient, AWSSDBException_NoSuchDomain

class SDBClientMethodTesting(TestCase):
    def setUp(self):
        self.sdb = SimpleDBClient('sdb.amazonaws.com', access_key, secret_key)

    def test_1(self):
        #create a domain
        newdomainename = "awsutilsunittest_%s"%(int(time.time()))

        self.sdb.createDomain(newdomainename)

        #idempotent will not fail
        self.sdb.createDomain(newdomainename)

        domainnames = self.sdb.listDomains()
        self.assertIsInstance(domainnames, list)
        if newdomainename not in domainnames:
            self.fail("can't find domain %s just created"%(newdomainename,))

        meta = self.sdb.domainMetadata(newdomainename)
        self.assertIsInstance(meta, dict)

        self.assertTrue('Timestamp' in meta)
        self.assertTrue('AttributeValueCount' in meta)
        self.assertTrue('AttributeValuesSizeBytes' in meta)
        self.assertTrue('ItemNamesSizeBytes' in meta)
        self.assertTrue('AttributeNameCount' in meta)
        self.assertTrue('ItemCount' in meta)
        self.assertTrue('AttributeNamesSizeBytes' in meta)

        self.sdb.deleteDomain(newdomainename)

        with self.assertRaises(AWSSDBException_NoSuchDomain):
            self.sdb.domainMetadata(newdomainename)

if __name__ == '__main__':
    unittest.main()
