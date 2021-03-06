# awsutils/sdbclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import collections
from awsutils.exceptions.aws import UserInputException, extractExceptionsFromModule2Dicitonary
from awsutils.awsclient import AWSClient
from awsutils.utils.auth import SIGNATURE_V2
import awsutils.exceptions.sdb

class SimpleDBClient(AWSClient):

    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.boxUssage = 0
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure)

    def batchDeleteAttributes(self, domainName, items, endpoint=None):
        """
        Delete atributes for multiple items
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_BatchDeleteAttributes.html
        @param domainName: the name of the domain
        @type domainName: str
        @param items: items to have their attributes deleted
                      ex: {"someItemname":{"someattributename" : "somevalue",
                                       "someotherattributename" : "somevalue"}
                        "someotherItemname":{"someattributename" : "somevalue"}}
        @type items: dict
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'BatchDeleteAttributes', 'DomainName':domainName, 'Version': '2009-04-15'}
        i = 1
        for itemName in items:
            query['Item.%d.ItemName'%(i,)] = itemName
            a = 1
            for attributeName in items[itemName]:
                query['Item.%d.Attribute.%d.Name'%(i,a)] = attributeName
                query['Item.%d.Attribute.%d.Value'%(i,a)] = items[itemName][attributeName]
                a += 1
            i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['BatchDeleteAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def batchPutAttributes(self, domainName, items, endpoint=None):
        """
        Set/modify atributes for multiple items from a given domain
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_BatchPutAttributes.html
        @param domainName: the name of the domain
        @type domainName: str
        @param items: items to have their attributes manipulated
                      ex: {"someItemname":{"someattributename" : "somevalue",
                                       "someotherattributename" : ("somevalue", True) #=> indicates force owerwrite
                                       }
                        "someotherItemname":{"someattributename" : "somevalue"}}
        @type items: dict
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
                query['Item.%d.Attribute.%d.Name'%(i,a)] = attributeName
                v = items[itemName][attributeName]
                if isinstance(v, (list, tuple)):
                    query['Item.%d.Attribute.%d.Value'%(i,a)] = v[0]
                    query['Item.%d.Attribute.%d.Replace'%(i,a)] = v[1]
                else:
                    query['Item.%d.Attribute.%d.Value'%(i,a)] = v
                a += 1
            i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['BatchPutAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def createDomain(self, domainName, endpoint=None):
        """
        Creates a domain
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_CreateDomain.html
        @param domainName: the name of the domain
        @type domainName: str
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'CreateDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['CreateDomainResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def deleteDomain(self, domainName, endpoint=None):
        """
        Deletes a domain
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_DeleteDomain.html
        @param domainName: the name of the domain
        @type domainName: str
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DeleteDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['DeleteDomainResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def domainMetadata(self, domainName, endpoint=None):
        """
        Returns information about the domain
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_DomainMetadata.html
        @param domainName: the name of the domain
        @type domainName: str
        @return: the meta result
        @rtype: dict
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DomainMetadata', 'DomainName':domainName, 'Version': '2009-04-15'}
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['DomainMetadataResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
        return data['DomainMetadataResponse']['DomainMetadataResult']

    def listDomains(self, maxNumberOfDomains=None, nextToken=None, endpoint=None):
        """
        Lists all domains associated with the Access Key ID
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_ListDomains.html
        @param maxNumberOfDomains: maximum number of domain names to be want returned (max 100)
        @type maxNumberOfDomains: int
        @param nextToken: where to start the next list of domain names
        @type nextToken: str
        @return: a list of domains
        @rtype: list
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'ListDomains', 'Version': '2009-04-15'}
        if maxNumberOfDomains is not None:
            if maxNumberOfDomains > 100:
                raise UserInputException('The maximum number of domain names is 100')
            query['MaxNumberOfDomains'] = maxNumberOfDomains
        if nextToken is not None:
            query['NextToken'] = nextToken
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['ListDomainsResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
        data = data['ListDomainsResponse']['ListDomainsResult']
        result = []
        if 'DomainName' in data:
            result = data['DomainName']
            if not isinstance(result, list):
                result = [result]
        return result

    def select(self, selectExpression, consistentRead=None, nextToken=None, endpoint=None):
        """
        The Select operation returns a set of Attributes for ItemNames that match the select expression
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_Select.html
        @type selectExpression: the expression used to query the domain
        @type selectExpression: str
        @type consistentRead: when set to true, ensures that the most recent data is returned
        @type consistentRead: bool
        @type nextToken: where to start the next list of ItemNames
        @type nextToken: str
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'Select', 'SelectExpression':selectExpression, 'Version': '2009-04-15'}
        if consistentRead is not None:
            query['ConsistentRead'] = consistentRead
        if nextToken is not None:
            query['NextToken'] = nextToken
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['SelectResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
        data = data['SelectResponse']['SelectResult']
        if isinstance(data, str): return []
        data = data['Item']
        if isinstance(data, dict): return [data]
        return data

    def getAttributes(self, domainName, itemName, attributeName=None, consistentRead=None, endpoint=None):
        """
        Get the atributes for itemName in domainName
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_GetAttributes.html
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
        query = {'Action': 'GetAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': '2009-04-15'}
        if attributeName is not None:
            query['AttributeName'] = attributeName
        if consistentRead is not None:
            query['ConsistentRead'] = consistentRead
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['GetAttributesResponse']['ResponseMetadata']['BoxUsage'])
        data = data['GetAttributesResponse']['GetAttributesResult']
        if isinstance(data, str):
            return None
        data = data['Attribute']
        if isinstance(data, dict): data = (data['Name'], data['Value'])
        else: data = tuple((attr['Name'],attr['Value']) for attr in data)
        return data

    def putAttributes(self, domainName, itemName, attributes, expected=None, endpoint=None):
        """
        Set/modify atributes for itemName in domainName
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_PutAttributes.html
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
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'PutAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': '2009-04-15'}
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
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['PutAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage


    def deleteAttributes(self, domainName, itemName, attributes, expected=None, endpoint=None):
        """
        Delete atributes from itemName in domainName
        http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_DeleteAttributes.html
        @param domainName: the name of the domain
        @type domainName: str
        @param itemName: the name of the item
        @type itemName: str
        @param attributes: the new attributes
                      ex: {"someattributename" : value,
                          "someotherattributename" : value
                          }
        @type attributes: dict
        @param expected: manipulate the attributes only if this attributes exist
                      ex: {"someattributename" : ("somevalue", 1),
                          "someotherattributename" : ("somevalue", 0)}
        @type expected: dict
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DeleteAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': '2009-04-15'}
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
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['awsresponse']
        boxUsage = float(data['DeleteAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    #================================== helper functionality ===========================================================

    EXCEPTIONS = extractExceptionsFromModule2Dicitonary('awsutils.exceptions.sdb',
                                                         awsutils.exceptions.sdb.SDBException)

    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
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
