

from awsutils.client import AWSClient
from awsutils.utils.auth import SIGNATURE_V2

class SimpleDBUserInputException(Exception):
    pass

class SimpleDBClient(AWSClient):
    def createDomain(self, domainName, endpoint=None):
        if endpoint is None: endpoint = self.host
        query = {'Action': 'CreateDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data['CreateDomainResponse']

    def deleteDomain(self, domainName, endpoint=None):
        if endpoint is None: endpoint = self.host
        query = {'Action': 'DeleteDomain', 'DomainName':domainName, 'Version': '2009-04-15'}
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data['DeleteDomainResponse']

    def listDomains(self, maxNumberOfDomains=None, nextToken=None, endpoint=None):
        if endpoint is None: endpoint = self.host
        query = {'Action': 'ListDomains', 'Version': '2009-04-15'}
        if maxNumberOfDomains is not None:
            if maxNumberOfDomains > 100:
                raise SimpleDBUserInputException('The maximum number of domain names is 100')
            query['MaxNumberOfDomains'] = maxNumberOfDomains
        if nextToken is not None:
            query['NextToken'] = nextToken
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query, host=endpoint)
        data = data['ListDomainsResponse']['ListDomainsResult']
        result = []
        if 'DomainName' in data:
            result = data['DomainName']
            if not isinstance(result, list):
                result = [result]
        return result
