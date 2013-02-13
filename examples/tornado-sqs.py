

import tornado.ioloop
#!! this example requires the file /test/setting.py with the folowing constants is created
from awsutils.tornado.sqsclient import SQSClient
from test.settings import access_key, secret_key

sqsclient = SQSClient(endpoint='sqs.us-east-1.amazonaws.com', access_key=access_key, secret_key=secret_key, secure=False)

def renderResult(data):
    print("message received", data)

sqsclient.sendMessage(callback=renderResult, qName="test", messageBody="this is a test message")

tornado.ioloop.IOLoop.instance().start()