# awsutils/s3/client.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, hmac, hashlib, io, base64, socket, tempfile
import http.client
import xml.sax

try:
    from select import poll as select_poll
    from select import POLLIN as select_POLLIN
except ImportError:  # Doesn't exist on OSX and other platforms
    from select import select

    select_poll = False

from awsutils.utils.xmlhandler import AWSXMLHandler
import awsutils.utils.auth as authutils


class AWSException(Exception):
    """exceptions raised on amazon error responses"""

    def __init__(self, status, reason, headers, response):
        self.status = status
        self.reason = reason
        self.headers = headers
        self.response = response

    def __repr__(self):
        return repr(self.__dict__)


class AWSDataException(Exception):
    def __init__(self, message, status, reason, headers, data):
        self.message = message
        self.status = status
        self.reason = reason
        self.headers = headers
        self.data = data

    def __repr__(self):
        return repr(self.__dict__)


class AWSTimeout(Exception):
    pass


class AWSPartialReception(Exception):
    """exceptions raised when there is partial data received from amazon,
    but the request itself failed at some point"""

    def __init__(self, status, reason, headers, data, sizeinfo, exception):
        self.status = status
        self.reason = reason
        self.headers = headers
        self.data = data
        self.sizeinfo = sizeinfo
        self.exception = exception

    def __repr__(self):
        return "AWSPartialReception [%d]" % (self.sizeinfo,)


class AWSClient:
    MAX_IN_MEMORY_READ_CHUNK_SIZE_FOR_RAW_DATA = 1024 * 1024
    HTTP_CONNECTION_RETRY_NUMBER = 3
    HTTP_RECEPTION_TIMEOUT = 30
    TEMP_DIR = '.'

    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.connections = {}

    def closeConnections(self):
        for connection in self.connections.values():
            connection.close()

    def is_connection_usable(self, httpconnection):
        sock = httpconnection.sock
        if sock is None:
            return False
            # if we have to read that means either that the connection was dropped, or that we have input data and that
            # is not a good thing in this scenario
        if not select_poll:
            return select([sock], [], [], 0.0)[0] == []
        p = select_poll()
        p.register(sock, select_POLLIN)
        for (fno, _ev) in p.poll(0.0):
            if fno == sock.fileno():
                return False
        return True

    def getConnection(self, destination, port=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        #TODO: if there is need we can implement here proxy and socks proxy support
        #TODO: for have this lib thread safe we should bind the current thread to the connection pool key so each thread will have separate connection
        if destination in self.connections:
            if self.is_connection_usable(self.connections[destination]) and (
                self.connections[destination]._HTTPConnection__state == http.client._CS_IDLE):
                return self.connections[destination]
                # this connection is bogus or dropped so close it
            self.connections[destination].close()

        print("connecting to", destination)
        if self.secure:
            #TODO: https contex checking
            conn = http.client.HTTPSConnection(destination, timeout=timeout)
        else:
            conn = http.client.HTTPConnection(destination, timeout=timeout)

        self.connections[destination] = conn
        return conn

    def request(self, method='GET', host=None, uri='/', headers=None, query=None, body=b'',
                region= None, service=None,
                expires=None,
                date=time.gmtime(),
                signmethod = None,
                statusexpected=None,
                xmlexpected=True,
                inputobject=None,
                operationtimeout=None,
                retry=None,
                receptiontimeout=None,
                _inputIOWrapper=None):

        if retry is None: retry = self.HTTP_CONNECTION_RETRY_NUMBER
        if receptiontimeout is None: receptiontimeout = self.HTTP_RECEPTION_TIMEOUT
        if host is None: host=self.endpoint

        _redirectcount = 0
        _retrycount = 0
        _outputiooffset = None

        if headers is None: headers = {}
        if query is None: query = {}
        if statusexpected is None: statusexpected = [200]
        headers['Connection'] = 'keep-alive'
        starttime = time.time()

        while True:
            # if request body is an io like object ensure that we set the pointer back to the start before retrying
            if hasattr(body, 'reset'):
                if _redirectcount > 0:
                    body.reset()
            else:
                try:
                    if _outputiooffset is None:
                        _outputiooffset = body.tell()
                    else:
                        body.seek(_outputiooffset, io.SEEK_SET)
                except:
                    pass

            conn = self.getConnection(destination=host, timeout=receptiontimeout)

            headers, query, body = authutils.signRequest(access_key=self.access_key, secret_key=self.secret_key,
                                                         endpoint=host, region=region, service=service,
                                                         signmethod=signmethod, date=date,
                                                         uri=uri, method=method, headers=headers,
                                                         query=query, body=body, expires=expires)

            print("Requesting", method, host, uri, headers, query)

            if query != {}:
                url = "%s?%s" % (uri, authutils.canonicalQueryString(query))
            else:
                url = uri

            try:
                conn.request(method=method, url=url, body=body, headers=headers)
                response = conn.getresponse()
            except Exception as _e:
                if _retrycount < retry:
                    _retrycount += 1
                    continue
                raise

            data = None

            if xmlexpected or ('Content-Type' in response.headers and
                               response.headers['Content-Type'] == 'application/xml'):
                handler = AWSXMLHandler()
                incr_parser = xml.sax.make_parser()
                incr_parser.setContentHandler(handler)

                doretry = False
                while True:
                    try:
                        if (operationtimeout is not None) and (time.time() - starttime > operationtimeout):
                            raise AWSTimeout('operation timeout')
                        data = response.read(amt=32768)
                    except:
                        # TODO: not all exception should retry, maybe an exception white list would be the way to go
                        if _retrycount < retry:
                            _retrycount += 1
                            doretry = True
                            break
                        raise
                    if len(data) == 0:
                        break
                    print(">>", data)
                    incr_parser.feed(data)
                if doretry:
                    continue

                awsresponse = handler.getdict()

                if 300 <= response.status < 400:
                    try:
                        #TODO: we should differentiate between temporary and permanent redirect,
                        #and handle correctly the second
                        if awsresponse['Error']['Code'] in ('TemporaryRedirect', 'PermanentRedirect'):
                            redirect = awsresponse['Error']['Endpoint']
                            if _redirectcount < 3:
                                _redirectcount += 1
                                host = redirect
                                continue
                    except:
                        pass

                if response.status not in statusexpected:
                    #TODO: maybe we should retry on some status?
                    raise AWSException(response.status, response.reason, dict(response.headers), awsresponse)

                return response.status, response.reason, dict(response.headers), awsresponse

            if (response.status not in statusexpected) or ('Content-Length' not in response.headers):
                #consume input
                try:
                    data = response.read(1024)
                    while response.read(32768) != b'':
                        pass
                except Exception as e:
                    print(e)

                if response.status not in statusexpected:
                    #TODO: maybe we should retry on some status?
                    raise AWSDataException('unexpected status', response.status, response.reason,
                        dict(response.headers), data)

                if 'Content-Length' not in response.headers:
                    raise AWSDataException('missing content length', response.status, response.reason,
                        dict(response.headers), data)

            #if we are here then most probably we want to download some data
            size = int(response.headers['Content-Length'])
            if response.status == 206:
                contentrange = response.headers['Content-Range']
                contentrange = contentrange.split(' ')[1].split('/')
                range = contentrange[0].split('-')
                sizeinfo = {'size': int(contentrange[1]), 'start': int(range[0]), 'end': int(range[1]), 'downloaded': 0}
            elif response.status == 200:
                sizeinfo = {'size': size, 'start': 0, 'end': size - 1, 'downloaded': 0}

            if inputobject is None:
                if size > self.MAX_IN_MEMORY_READ_CHUNK_SIZE_FOR_RAW_DATA:
                    inputobject = tempfile.TemporaryFile(mode="w+b", dir=self.TEMP_DIR, prefix='awstmp-')
                else:
                    inputobject = io.BytesIO()
                if _inputIOWrapper is not None:
                    inputobject = _inputIOWrapper(inputobject)

            ammount = 0

            while True:
                try:
                    if (operationtimeout is not None) and (time.time() - starttime > operationtimeout):
                        raise AWSTimeout('operation timeout')
                    data = response.read(32768)
                    ammount += len(data)
                    #print(ammount, sizeinfo['size'])

                except Exception as e:
                    if ammount > 0:
                        # don't loose partial data yet, it may be useful even in this situation
                        sizeinfo['downloaded'] = ammount
                        raise AWSPartialReception(status=response.status, reason=response.reason,
                            headers=dict(response.headers), data=inputobject, sizeinfo=sizeinfo,
                            exception=e)

                    # TODO: not all exception should retry, maybe an exception white list would be the way to go
                    if _retrycount < retry:
                        _retrycount += 1
                        break
                    raise

                if (data == b'') or (ammount > size):
                    return response.status, response.reason, dict(response.headers), sizeinfo, inputobject

                inputobject.write(data)

            # if we are here then we should retry
            continue
