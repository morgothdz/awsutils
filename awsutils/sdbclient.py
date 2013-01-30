# awsutils/sdbclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.client import AWSClient, UserInputException
from awsutils.utils.auth import SIGNATURE_V2

class SimpleDBClient(AWSClient):
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.boxUssage = 0
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure)

    def batchDeleteAttributes(self, domainName, items, endpoint=None):
        """
        items example {"someItemname" :
                                    {"someattributename" : "somevalue",
                                    "someotherattributename" : "somevalue"
                                     }
                        "someotherItemname":{"someattributename" : "somevalue"}
                        }
        """
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'BatchDeleteAttributes', 'DomainName':domainName, 'Version': '2009-04-15'}
        i = 1
        for itemName in items:
            query['Item.%d.ItemName'%(i,)] = itemName
            a = 1
            for attributeName in items[itemName]:
                query['Item.%d.Attribute.%d.name'%(i,a)] = attributeName
                query['Item.%d.Attribute.%d.value'%(i,a)] = items[itemName][attributeName]
                a += 1
            i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['BatchDeleteAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def batchPutAttributes(self, domainName, items, endpoint=None):
        """
        items example {"someItemname" :
                                    {"someattributename" : "somevalue"
                                     "someotherattributename" : ("somevalue", True) => if we want the attribute to be overwritten
                                     }
                        "someotherItemname":{"someattributename" : "somevalue"}
                        }
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
                if isinstance(v, list):
                    query['Item.%d.Attribute.%d.value'%(i,a)] = v[0]
                    query['Item.%d.Attribute.%d.replace'%(i,a)] = v[1]
                else:
                    query['Item.%d.Attribute.%d.value'%(i,a)] = v
                a += 1
            i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['BatchPutAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def createDomain(self, domainName, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'CreateDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['CreateDomainResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def deleteDomain(self, domainName, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DeleteDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['DeleteDomainResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def domainMetadata(self, domainName, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DomainMetadata', 'DomainName':domainName, 'Version': '2009-04-15'}
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['DomainMetadataResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
        return data['awsresponse']['DomainMetadataResponse']['DomainMetadataResult']

    def listDomains(self, maxNumberOfDomains=None, nextToken=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'ListDomains', 'Version': '2009-04-15'}
        if maxNumberOfDomains is not None:
            if maxNumberOfDomains > 100:
                raise UserInputException('The maximum number of domain names is 100')
            query['MaxNumberOfDomains'] = maxNumberOfDomains
        if nextToken is not None:
            query['NextToken'] = nextToken
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['ListDomainsResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
        data = data['awsresponse']['ListDomainsResponse']['ListDomainsResult']
        result = []
        if 'DomainName' in data:
            result = data['DomainName']
            if not isinstance(result, list):
                result = [result]
        return result

    def select(self, selectExpression, consistentRead=None, nextToken=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'Select', 'SelectExpression':selectExpression, 'Version': '2009-04-15'}
        if consistentRead is not None:
            query['ConsistentRead'] = consistentRead
        if nextToken is not None:
            query['NextToken'] = nextToken
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['SelectResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
        data = data['awsresponse']['SelectResponse']['SelectResult']
        if isinstance(data, str): return []
        data = data['Item']
        if isinstance(data, dict): return [data]
        return data

    def getAttributes(self, domainName, itemName, attributeName=None, consistentRead=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'GetAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': '2009-04-15'}
        if attributeName is not None:
            query['AttributeName'] = attributeName
        if consistentRead is not None:
            query['ConsistentRead'] = consistentRead
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['GetAttributesResponse']['ResponseMetadata']['BoxUsage'])
        data = data['awsresponse']['GetAttributesResponse']['GetAttributesResult']
        if isinstance(data, str):
            return None
        data = data['Attribute']
        if isinstance(data, dict): return [data]
        return data

    def putAttributes(self, domainName, itemName, attributes, expected=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'PutAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': '2009-04-15'}
        i = 1
        for name in attributes:
            query['Attribute.%d.Name'%(i,)] = name
            query['Attribute.%d.Value'%(i,)] = attributes[name]
            i += 1
        if expected is not None:
            i = 1
            for name in expected:
                query['Expected.%d.Name'%(i,)] = name
                query['Expected.%d.Value'%(i,)] = expected[name][0]
                query['Expected.%d.Exists'%(i,)] = expected[name][1]
                i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['PutAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage


    def deleteAttributes(self, domainName, itemName, attributes, expected=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DeleteAttributes', 'ItemName': itemName, 'DomainName':domainName, 'Version': '2009-04-15'}
        i = 1
        for name in attributes:
            query['Attribute.%d.Name'%(i,)] = name
            query['Attribute.%d.Value'%(i,)] = attributes[name]
            i += 1
        if expected is not None:
            i = 1
            for name in expected:
                query['Expected.%d.Name'%(i,)] = name
                query['Expected.%d.Value'%(i,)] = expected[name][0]
                query['Expected.%d.Exists'%(i,)] = expected[name][1]
                i += 1
        data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['awsresponse']['DeleteAttributesResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
