# awsutils/tornado/sdbclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import functools, collections
import tornado.gen
from awsutils.tornado.awsclient import AWSClient
from awsutils.exceptions.aws import UserInputException, extractExceptionsFromModule2Dicitonary
import awsutils.exceptions.sdb
from awsutils.utils.auth import SIGNATURE_V2

class SimpleDbClient(AWSClient):
    VERSION = '2009-04-15'

    def __init__(self, endpoint, access_key, secret_key, _ioloop=None, secure=False):
        """
        @type endpoint: the amazon endpoint of the service
        @type endpoint: str
        @type access_key: amazon access key
        @type access_key: str
        @type secret_key: amazon secret key
        @type secret_key: str
        @type secure: use https
        @type secure: bool
        @type _ioloop: the tornado ioloop for processing the events
        @type _ioloop: tornado.ioloop.IOLoop
        """
        self.boxUssage = 0
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure, _ioloop = _ioloop)

    @tornado.gen.engine
    def select(self, callback, selectExpression, consistentRead = False, nextToken = None):
        """
        The Select operation returns a set of Attributes for ItemNames that match the select expression
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_Select.html
        @type callback: callback function params: (status, data)
        @type callback: function
        @type selectExpression: the expression used to query the domain
        @type selectExpression: str
        @type consistentRead: when set to true, ensures that the most recent data is returned
        @type consistentRead: bool
        @type nextToken: where to start the next list of ItemNames
        @type nextToken: str
        @return: a list of dictionaries
        @rtype: list
        """
        query = {'Action':'Select', 'SelectExpression':selectExpression, 'Version': self.VERSION}
        if consistentRead:
            query['ConsistentRead'] = consistentRead
        if nextToken is not None:
            query['NextToken'] = nextToken

        data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V2)

        data = data['data']
        self.boxUssage += float(data['SelectResponse']['ResponseMetadata']['BoxUsage'])
        selectresult = data['SelectResponse']['SelectResult']['Item']
        if isinstance(selectresult, dict): selectresult = [selectresult]
        self._ioloop.add_callback(functools.partial(callback, selectresult))


    @tornado.gen.engine
    def getAttributes(self, callback, domainName, itemName, attributeName=None, consistentRead=None, endpoint=None):
        """
        Get the atributes for itemName in domainName
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_GetAttributes.html
        @type callback: callback function params: (status, data)
        @type callback: function
        @param domainName: the name of the domain
        @type domainName: str
        @param itemName: the name of the item
        @type itemName: str
        @param attributeName: the name of the attribute
        @type attributeName: str
        @param consistentRead: when set to true, ensures that the most recent data is returned
        @type consistentRead: bool
        @return: a tuple of tuples (key, value)
        @rtype: tuple
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'GetAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': self.VERSION}
        if attributeName is not None:
            query['AttributeName'] = attributeName
        if consistentRead is not None:
            query['ConsistentRead'] = consistentRead

        data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V2)

        data = data['data']
        self.boxUssage += float(data['GetAttributesResponse']['ResponseMetadata']['BoxUsage'])
        data = data['GetAttributesResponse']['GetAttributesResult']

        if isinstance(data, str):
            data = None
        else:
            data = data['Attribute']
            if isinstance(data, dict): data = (data['Name'], data['Value'])
            else: data = tuple((attr['Name'],attr['Value']) for attr in data)

        self._ioloop.add_callback(functools.partial(callback, data))


    @tornado.gen.engine
    def putAttributes(self, callback, domainName, itemName, attributes, expected=None, endpoint=None):
        """
        Set/modify atributes for itemName in domainName
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_PutAttributes.html
        @type callback: callback function params: (status, data)
        @type callback: function
        @param domainName: the name of the domain
        @type domainName: str
        @param itemName: the name of the item
        @type itemName: str
        @param attributes: the new attributes
                      ex: {"someattributename" : "somevalue",
                          "someotherattributename" : ("somevalue", True) #=> indicates force owerwrite
                          }
                      or (("someattributename", "somevalue"), ("someotherattrname", "somevalue", True))
        @type attributes: dict, tuple
        @param expected: manipulate the attributes only if this attributes exist
                      ex: {"someattributename" : ("somevalue", 1),
                          "someotherattributename" : ("somevalue", 0)}
        @type expected: dict
        @return: returns True on success or raise exceptions
        @rtype: bool
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'PutAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': self.VERSION}
        i = 1
        if isinstance(attributes, dict): attributes = attributes.items()
        for attribute in attributes:
            value = attribute[1]
            query['Attribute.%d.Name'%(i,)] = attribute[0]
            if isinstance(value, tuple) or isinstance(value, list):
                query['Attribute.%d.Value'%(i,)] = value[0]
                query['Attribute.%d.Replace'%(i,)] = value[1]
            else:
                if not isinstance(value, str): value = repr(value)
                query['Attribute.%d.Value'%(i,)] = value
            i += 1

        if expected is not None:
            i = 1
            for name in expected:
                query['Expected.%d.Name'%(i,)] = name
                query['Expected.%d.Value'%(i,)] = expected[name]
                query['Expected.%d.Exists'%(i,)] = expected[name]
                i += 1

        data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V2)
        self.boxUssage += float(data['data']['PutAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self._ioloop.add_callback(functools.partial(callback, True))

    @tornado.gen.engine
    def deleteAttributes(self, callback, domainName, itemName, attributes=None, expected=None, endpoint=None):
        """
        Delete atributes from itemName in domainName
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_DeleteAttributes.html
        @type callback: callback function params: (status, data)
        @type callback: function
        @param domainName: the name of the domain
        @type domainName: str
        @param itemName: the name of the item
        @type itemName: str
        @param attributes: the new attributes
                      ex: {"someattributename" : 1,
                          "someotherattributename" : 0
                          }
        @type attributes: dict
        @param expected: manipulate the attributes only if this attributes exist
                      ex: {"someattributename" : ("somevalue", 1),
                          "someotherattributename" : ("somevalue", 0)}
        @type expected: dict
        @return: returns True on success or raise exceptions
        @rtype: bool
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DeleteAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': self.VERSION}
        if attributes is not None:
            i = 1
            for name in attributes:
                query['Attribute.%d.Name'%(i,)] = name
                value = attributes[name]
                query['Attribute.%d.Value'%(i,)] = value
                i += 1
        if expected is not None:
            i = 1
            for name in expected:
                query['Expected.%d.Name'%(i,)] = name
                query['Expected.%d.Value'%(i,)] = expected[name][0]
                query['Expected.%d.Exists'%(i,)] = expected[name][1]
                i += 1

        data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V2)
        self.boxUssage += float(data['data']['DeleteAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self._ioloop.add_callback(functools.partial(callback, True))


    @tornado.gen.engine
    def batchDeleteAttributes(self, callback, domainName, items, endpoint=None):
        """
        Delete atributes for multiple items
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_BatchDeleteAttributes.html
        @type callback: callback function params: (status, data)
        @type callback: function
        @param domainName: the name of the domain
        @type domainName: str
        @param items: items to have their attributes deleted
                      ex: {"someItemname":{"someattributename" : 0,
                                       "someotherattributename" : 0}
                        "someotherItemname":{"someattributename" : 1}}
        @type items: dict
        @return: returns True on success or raise exceptions
        @rtype: bool
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'BatchDeleteAttributes', 'DomainName':domainName, 'Version': self.VERSION}
        i = 1
        for itemName in items:
            query['Item.%d.ItemName'%(i,)] = itemName
            a = 1
            for attributeName in items[itemName]:
                query['Item.%d.Attribute.%d.name'%(i,a)] = attributeName
                query['Item.%d.Attribute.%d.value'%(i,a)] = items[itemName][attributeName]
                a += 1
            i += 1

        data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V2)
        self.boxUssage += float(data['data']['BatchDeleteAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self._ioloop.add_callback(functools.partial(callback, True))

    @tornado.gen.engine
    def batchPutAttributes(self, callback, domainName, items, endpoint=None):
        """
        Set/modify atributes for multiple items from a given domain
        http://docs.aw.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_BatchPutAttributes.html
        @type callback: callback function params: (status, data)
        @type callback: function
        @param domainName: the name of the domain
        @type domainName: str
        @param items: items to have their attributes manipulated
                      ex: {"someItemname":{"someattributename" : "somevalue",
                                       "someotherattributename" : ("somevalue", True) #=> indicates force owerwrite
                                       }
                        "someotherItemname":{"someattributename" : "somevalue"}}
        @type items: dict
        @return: returns True on success or raise exceptions
        @rtype: bool
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'BatchPutAttributes', 'DomainName':domainName, 'Version': '2009-04-15'}
        i = 1
        for itemName in items:
            if i > 25:
                raise UserInputException('25 item limit per BatchPutAttributes operation exceded')
            query['Item.%d.ItemName'%(i,)] = itemName
            a = 1
            for attributeName in items[itemName]:
                if a > 256:
                    raise UserInputException('256 attribute name-value pairs per item exceded')
                query['Item.%d.Attribute.%d.name'%(i,a)] = attributeName
                v = items[itemName][attributeName]
                if isinstance(v, collections.Iterable):
                    query['Item.%d.Attribute.%d.value'%(i,a)] = v[0]
                    query['Item.%d.Attribute.%d.replace'%(i,a)] = v[1]
                else:
                    query['Item.%d.Attribute.%d.value'%(i,a)] = v
                a += 1
            i += 1

        data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V2)
        self.boxUssage += float(data['data']['BatchPutAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self._ioloop.add_callback(functools.partial(callback, True))


    #================================== helper functionality ===========================================================
    EXCEPTIONS = extractExceptionsFromModule2Dicitonary('awsutils.exceptions.sdb',
                                                        awsutils.exceptions.sdb.SDBException)

    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
        if 'Response' in awsresponse and 'Errors' in awsresponse['Response']:
            #raise the first error we found
            errors = awsresponse['Response']['Errors']['Error']
            if isinstance(errors, dict): errors = [errors]
            for error in errors:
                self.boxUssage += float(error['BoxUsage'])
            for error in errors:
                if error['Code'].replace('.','_') in self.EXCEPTIONS:
                    raise self.EXCEPTIONS[error['Code'].replace('.','_')](awsresponse, httpstatus, httpreason, httpheaders)
            else:
                raise awsutils.exceptions.sdb.SDBException(awsresponse, httpstatus, httpreason, httpheaders)
