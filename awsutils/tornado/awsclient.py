# awsutils/tornado/awsclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, functools, xml
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen
import tornado.httpclient

from awsutils.utils.xmlhandler import AWSXMLHandler
import awsutils.utils.auth as auth

class AWSClient:
    def __init__(self, endpoint, access_key, secret_key, secure=False, _ioloop=tornado.ioloop.IOLoop.instance()):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self._ioloop = _ioloop
        self.count = {}
        self.http_client = tornado.httpclient.AsyncHTTPClient()

    def streamingCallback(self, incrementalParser, collector, data):
        collector.append(data)
        if hasattr(incrementalParser._cont_handler, 'exception'):
            return
        try:
            incrementalParser.feed(data)
        except Exception as e:
            incrementalParser._cont_handler.exception = e

    def checkForErrors(self, awsresponse, httpstatus):
        pass

    @tornado.gen.engine
    def request(self, callback, endpoint=None, method='GET', uri='/', query=None, headers=None, statusexpected=None,
                body=None, signmethod=None, region=None, service=None, date=time.gmtime(), xmlexpected=True,
                connect_timeout=2, request_timeout=5):

        if endpoint is None: endpoint = self.endpoint
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

        #counting the requests
        if method not in self.count:
            self.count[method] = 0
        self.count[method] += 1

        request = tornado.httpclient.HTTPRequest("%s://%s/?%s" % (protocol, endpoint, auth.canonicalQueryString(query)),
                                                 headers=headers, body=body, streaming_callback=streamingCallback,
                                                 connect_timeout=connect_timeout, request_timeout=request_timeout)

        #TODO: timeout handling

        response = yield tornado.gen.Task(self.http_client.fetch, request)

        result = {'code':response.code, 'headers':dict(response.headers)}

        if response.code == 599:
            result['data'] = response.error
            self._ioloop.add_callback(functools.partial(callback, False, result))
            return

        if not hasattr(handler, 'exception'):
            data = handler.getdict()
            try:
                self.checkForErrors(awsresponse, response.code)
            except Exception as e:
                result['data'] = e
                self._ioloop.add_callback(functools.partial(callback, (False, result)))
                return
            #TODO: redirect handling
        else:
            data = ''.join(awsresponse)
            if xmlexpected:
                self._ioloop.add_callback(functools.partial(callback, (False, result)))
                return

        if statusexpected is not True and response.code not in statusexpected:
            result['data'] = 'AWSStatusException'
            self._ioloop.add_callback(functools.partial(callback, (False, result)))
            return

        result['data'] = data
        self._ioloop.add_callback(functools.partial(callback, (True, result)))