# awsutils/tornado/sdbclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import functools
import tornado.gen
from awsutils.tornado.awsclient import AWSClient
import awsutils.utils.auth as auth

class SimpleDbClient(AWSClient):
    VERSION = '2009-04-15'

    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.boxUssage = 0
        AWSClient.__init__(self, endpoint, access_key, secret_key, secure)

    def checkForErrors(self, awsresponse, httpstatus):
        if 'Response' in awsresponse and 'Errors' in awsresponse['Response']:
            errors = awsresponse['Response']['Errors']['Error']
            if isinstance(errors, dict): errors = [errors]
            for error in errors:
                self.boxUssage += error['BoxUsage']
            return error[0]['Code'], error[0]['Message']
        return False, ''

    def _select(self, callback, result):
        if result['status'] == 'xml':
            try:
                awsresponse = result['awsresponse']
                self.boxUssage += float(awsresponse['SelectResponse']['ResponseMetadata']['BoxUsage'])
                selectresult = awsresponse['SelectResponse']['SelectResult']['Item']
                if isinstance(selectresult, dict): selectresult = [selectresult]
                self._ioloop.add_callback(functools.partial(callback, True, selectresult))
            except Exception as e:
                self._ioloop.add_callback(functools.partial(callback, False, e))
            return
        self._ioloop.add_callback(functools.partial(callback, False, result))

    @tornado.gen.engine
    def select(self, callback, domainName, selectExpression, consistentRead = False, nextToken = None):
        query = {}
        query['Action'] = 'Select'
        query['DomainName'] = domainName
        query['SelectExpression'] = selectExpression
        if consistentRead:
            query['ConsistentRead'] = consistentRead
        if nextToken is not None:
            query['NextToken'] = nextToken
        query['Version'] = self.VERSION
        self.request(callback=functools.partial(self._select, callback), query=query, signmethod=auth.SIGNATURE_V2)
