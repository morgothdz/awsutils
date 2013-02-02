# awsutils/tornado/sdbclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import functools
import tornado.gen
from awsutils.tornado.awsclient import AWSClient
from awsutils.exceptions.aws import UserInputException, extractExceptionsFromModule2Dicitonary
import awsutils.exceptions.sdb
import awsutils.utils.auth as auth

class SimpleDbClient(AWSClient):
    VERSION = '2009-04-15'
    EXCEPTIONS = extractExceptionsFromModule2Dicitonary('awsutils.exceptions.sdb',
                                                        awsutils.exceptions.sdb.SDBException)

    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.boxUssage = 0
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure)

    def checkForErrors(self, awsresponse, httpstatus):
        if 'Response' in awsresponse and 'Errors' in awsresponse['Response']:
            #raise the first error we found
            errors = awsresponse['Response']['Errors']['Error']
            if isinstance(errors, dict): errors = [errors]
            for error in errors:
                self.boxUssage += error['BoxUsage']
            for error in errors:
                if error['Code'].replace('.','_') in self.EXCEPTIONS:
                    raise self.EXCEPTIONS[error['Code'].replace('.','_')](awsresponse, httpstatus,
                        httpreason, httpheaders)
            else:
                raise awsutils.exceptions.sdb.SDBException(awsresponse, httpstatus, httpreason, httpheaders)

    @tornado.gen.engine
    def select(self, callback, domainName, selectExpression, consistentRead = False, nextToken = None):
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
        """
        query = {}
        query['Action'] = 'Select'
        query['DomainName'] = domainName
        query['SelectExpression'] = selectExpression
        if consistentRead:
            query['ConsistentRead'] = consistentRead
        if nextToken is not None:
            query['NextToken'] = nextToken
        query['Version'] = self.VERSION

        status, data = yield tornado.gen.Task(self.request, query=query, signmethod=auth.SIGNATURE_V2)

        if status is True:
            try:
                data = data['data']
                self.boxUssage += float(data['SelectResponse']['ResponseMetadata']['BoxUsage'])
                selectresult = data['SelectResponse']['SelectResult']['Item']
                if isinstance(selectresult, dict): selectresult = [selectresult]
                data = selectresult
            except Exception as e:
                stauts = False
                data = e

        self._ioloop.add_callback(functools.partial(callback, status, data))


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
        @return: a list of attributes
        @rtype: list
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'GetAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': '2009-04-15'}
        if attributeName is not None:
            query['AttributeName'] = attributeName
        if consistentRead is not None:
            query['ConsistentRead'] = consistentRead

        status, data = yield tornado.gen.Task(self.request, query=query, signmethod=auth.SIGNATURE_V2)

        if status is True:
            try:
                data = data['data']
                self.boxUssage += float(data['GetAttributesResponse']['ResponseMetadata']['BoxUsage'])
                data = data['GetAttributesResponse']['GetAttributesResult']
                if isinstance(data, str):
                    data = None
                else:
                    data = data['Attribute']
                    if isinstance(data, dict): data = [data]
            except Exception as e:
                stauts = False
                data = e

        self._ioloop.add_callback(functools.partial(callback, status, data))
