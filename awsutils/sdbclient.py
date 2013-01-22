

from awsutils.client import AWSClient
from awsutils.utils.auth import SIGNATURE_V2

class SimpleDBUserInputException(Exception):
    pass

class SimpleDBClient(AWSClient):
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.boxUssage = 0
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure)

    def createDomain(self, domainName, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'CreateDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['CreateDomainResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def deleteDomain(self, domainName, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'DeleteDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['DeleteDomainResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage

    def listDomains(self, maxNumberOfDomains=None, nextToken=None, endpoint=None):
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'ListDomains', 'Version': '2009-04-15'}
        if maxNumberOfDomains is not None:
            if maxNumberOfDomains > 100:
                raise SimpleDBUserInputException('The maximum number of domain names is 100')
            query['MaxNumberOfDomains'] = maxNumberOfDomains
        if nextToken is not None:
            query['NextToken'] = nextToken
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
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
        if endpoint is None: endpoint = self.endpoint
        query = {'Action': 'Select', 'SelectExpression':selectExpression, 'Version': '2009-04-15'}
        if consistentRead is not None:
            query['ConsistentRead'] = consistentRead
        if nextToken is not None:
            query['NextToken'] = nextToken
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        boxUsage = float(data['SelectResponse']['ResponseMetadata']['BoxUsage'])
        self.boxUssage += boxUsage
        data = data['SelectResponse']['SelectResult']
        if isinstance(data, str): return []
        if isinstance(data, dict): return [data]
        return data
