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
from awsutils.exceptions.aws import AWSStatusException, AWSDataException
import awsutils.utils.auth as auth


class AWSClient:
    def __init__(self, endpoint, access_key, secret_key, secure=False, _ioloop=None):
        """
        @type endpoint: the amazon endpoint of the service
        @type endpoint: str
        @type access_key: amazon access key
        @type access_key: str
        @type secret_key: amazon secret key
        @type secret_key: str
        @type secure: use https
        @type secure: bool
        @type _ioloop: the tornado ioloop for processing the events
        @type _ioloop: tornado.ioloop.IOLoop
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        if _ioloop is None:
            _ioloop = tornado.ioloop.IOLoop.instance()
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

    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
        pass

    @tornado.gen.engine
    def request(self, callback, endpoint=None, method='GET', uri='/', query={}, headers={}, statusexpected=None,
                body=b'', signmethod=None, region=None, service=None, date=time.gmtime(), xmlexpected=True,
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

        if method not in ["POST", "PUT"]: body = None
        request = tornado.httpclient.HTTPRequest("%s://%s%s?%s" % (protocol, endpoint, uri, auth.canonicalQueryString(query)),
                                                 headers=headers, body=body, streaming_callback=streamingCallback,
                                                 connect_timeout=connect_timeout, request_timeout=request_timeout, method=method)

        #TODO: timeout handling

        response = yield tornado.gen.Task(self.http_client.fetch, request)

        resultdata = {'status':response.code, 'headers':dict(response.headers), 'data':None}

        if response.code == 599:
            resultdata['reason'] = response.reason
            raise AWSStatusException(resultdata)

        if not hasattr(handler, 'exception'):
            awsresponsexml = handler.getdict()
            self.checkForErrors(awsresponsexml, response.code, '', response.headers)
            #TODO: redirect handling
        else:
            if xmlexpected:
                raise AWSDataException('xml-expected')

        if xmlexpected:
            resultdata['data'] = awsresponsexml
        else:
            resultdata['data'] = b''.join(awsresponse)

        if statusexpected is not True and response.code not in statusexpected:
            raise AWSStatusException(resultdata)

        self._ioloop.add_callback(functools.partial(callback, resultdata))