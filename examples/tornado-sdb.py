

import tornado.ioloop
#!! this example requires the file /test/setting.py with the folowing constants is created
from awsutils.tornado.sdbclient import SimpleDbClient
from test.settings import access_key, secret_key

sdbclient = SimpleDbClient(endpoint='sdb.amazonaws.com', access_key=access_key, secret_key=secret_key, secure=False)

def renderResult(data):
    print("message received", data)

sdbclient.select(callback=renderResult, selectExpression="SELECT * FROM `FOO`")

tornado.ioloop.IOLoop.instance().start()