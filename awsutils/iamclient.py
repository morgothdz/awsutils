# awsutils/iamclient.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.client import AWSClient
from awsutils.utils.auth import SIGNATURE_V2, SIGNATURE_V4

class IAMClient(AWSClient):
    def __init__(self, access_key, secret_key):
        #IAM has only one endpoint
        #IAM calls should be always https
        AWSClient.__init__(self, 'iam.amazonaws.com', access_key, secret_key, secure=True)

    def getUser(self, userName=None):
        query = {'Action': 'GetUser', 'Version': '2010-05-08'}
        if userName is not None:
            query['UserName'] = userName
        _status, _reason, _headers, data = self.request(method="GET", signmethod=SIGNATURE_V2, query=query)
        return data['GetUserResponse']['GetUserResult']['User']