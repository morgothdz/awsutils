import os, json, hashlib, time, functools, xml.sax
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen
import tornado.httpclient

from awsutils.utils.xmlhandler import AWSXMLHandler
import awsutils.utils.auth as auth

class AWSClient:
    def __init__(self, host, access_key, secret_key, secure=False, _ioloop=tornado.ioloop.IOLoop.instance()):
        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self._ioloop = _ioloop
        self.http_client = tornado.httpclient.AsyncHTTPClient()

    def streamingCallback(self, incrementalParser, collector, data):
        collector.append(data)
        if hasattr(incrementalParser.handler, 'exception'):
            return
        try:
            incrementalParser.feed(data)
        except Exception as e:
            incrementalParser.handler.exception = e

    def checkForErrors(self, awsresponse, httpstatus):
        pass

    @tornado.gen.engine
    def request(self, callback, endpoint=None, method='GET', uri='/', query=None, headers=None, statusexpected=None,
                body=None, signmethod=None, region=None, service=None, date=time.gmtime(), xmlexpected=True,
                connect_timeout=2, request_timeout=5):

        if statusexpected is None: statusexpected = [200]
        headers, query, body = auth.signRequest(access_key=self.access_key, secret_key=self.secret_key,
                                                endpoint=endpoint, region=region, service=service,
                                                signmethod=signmethod, date=date,
                                                uri=uri, method=method, headers=headers,
                                                query=query, body=body)

        awsresponse = []
        handler = AWSXMLHandler()
        incrementalParser = xml.sax.make_parser()
        incrementalParser.setContentHandler(handler)
        streamingCallback = functools.partial(self.streamingCallback, incrementalParser, awsresponse)

        protocol = 'https' if self.secure else 'http'
        request = tornado.httpclient.HTTPRequest("%s://%s/?%s" % (protocol, endpoint, auth.canonicalQueryString(query)),
                                                 headers=headers, body=body, streaming_callback=streamingCallback,
                                                 connect_timeout=connect_timeout, request_timeout=request_timeout)

        #TODO: timeout handling

        response = yield tornado.gen.Task(self.http_client.fetch, request)

        result = {'status':response.status, 'reason':response.reason, 'headers':dict(response.headers)}

        if not hasattr(handler, 'exception'):
            awsresponse = handler.getdict()
            errorType, message = self.checkForErrors(awsresponse, response.status)
            if errorType is not False:
                result['result'] = 'error'
                result['type'] = errorType
                result['message'] = message
                self._ioloop.add_callback(functools.partial(callback, result))
                return
            result['result'] = 'xml'
            #TODO: redirect handling
        else:
            result['result'] = 'raw'
            awsresponse = ''.join(awsresponse)

        if statusexpected is not True and response.status not in statusexpected:
            result['result'] = 'error'
            result['type'] = 'AWSStatusException'
            result['message'] = ''
            self._ioloop.add_callback(functools.partial(callback, result))
            return

        result['awsresponse'] = awsresponse
        self._ioloop.add_callback(functools.partial(callback, result))


