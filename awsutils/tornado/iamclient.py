import functools, collections
import tornado.gen
from awsutils.tornado.awsclient import AWSClient
from awsutils.exceptions.aws import UserInputException, extractExceptionsFromModule2Dicitonary
import awsutils.exceptions.sdb
from awsutils.utils.auth import SIGNATURE_V4_HEADERS

class SimpleDbClient(AWSClient):
    VERSION = '2010-05-08'

    def __init__(self, access_key, secret_key, _ioloop=None):
        AWSClient.__init__(self, 'iam.amazonaws.com', access_key, secret_key, secure=True, _ioloop = _ioloop)

    @tornado.gen.engine
    def getUser(self, callback, userName=None):
        query = {'Action': 'GetUser', 'Version': self.VERSION}
        if userName is not None:
            query['UserName'] = userName
        data = yield tornado.gen.Task(self.request, query=query, signmethod=SIGNATURE_V4_HEADERS)
        data = data['awsresponse']['GetUserResponse']['GetUserResult']['User']
        self._ioloop.add_callback(functools.partial(callback, data))