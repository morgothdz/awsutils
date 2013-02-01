# awsutils/s3/client.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, io, socket, tempfile, logging
import http.client
import xml.sax

try:
    from select import poll as select_poll
    from select import POLLIN as select_POLLIN
except ImportError:  # Doesn't exist on OSX and other platforms
    from select import select

    select_poll = False

from awsutils.utils.xmlhandler import AWSXMLHandler
from awsutils.exceptions.aws import AWSTimeout, AWSDataException, AWSPartialReception, AWSStatusException
import awsutils.utils.auth as auth

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
        self.logger = logging.getLogger("%s.%s" % (type(self).__module__, type(self).__name__))
        self.logger.addHandler(logging.NullHandler())

    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
        """
        Checks for aws error responses, this should be overriden by each subclass
        """
        pass

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
        #TODO: for having this module thread safe we should bind the current thread to the connection pool key so each
        # thread will have separate connection
        if destination in self.connections:
            if self.is_connection_usable(self.connections[destination]) and (
                self.connections[destination]._HTTPConnection__state == http.client._CS_IDLE):
                return self.connections[destination]
                # this connection is bogus or dropped so close it
            self.connections[destination].close()

        self.logger.debug("connecting to %s", destination)
        if self.secure:
            #TODO: https context (certificate) checking
            conn = http.client.HTTPSConnection(destination, timeout=timeout)
        else:
            conn = http.client.HTTPConnection(destination, timeout=timeout)

        self.connections[destination] = conn
        return conn

    def request(self, method='GET', host=None, uri='/', headers=None, query=None, body=b'',
                region=None, service=None,
                expires=None,
                date=time.gmtime(),
                signmethod=None,
                statusexpected=None,
                xmlexpected=True,
                inputobject=None,
                operationtimeout=None,
                retry=None,
                receptiontimeout=None,
                _inputIOWrapper=None):

        if retry is None: retry = self.HTTP_CONNECTION_RETRY_NUMBER
        if receptiontimeout is None: receptiontimeout = self.HTTP_RECEPTION_TIMEOUT
        if host is None: host = self.endpoint

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

            headers, query, body = auth.signRequest(access_key=self.access_key, secret_key=self.secret_key,
                                                         endpoint=host, region=region, service=service,
                                                         signmethod=signmethod, date=date,
                                                         uri=uri, method=method, headers=headers,
                                                         query=query, body=body, expires=expires)

            self.logger.debug("Requesting %s %s %s query=%s headers=%s", method, host, uri, query, headers)

            if query != {}:
                url = "%s?%s" % (uri, auth.canonicalQueryString(query))
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
                               response.headers['Content-Type'] in ('application/xml', 'text/xml')):

                if ('Content-Length' not in response.headers) and ('Transfer-Encoding' not in response.headers or
                                                                   response.headers['Transfer-Encoding'] != 'chunked'):
                #every xml response should have 'Content-Length' set or 'Transfer-Encoding' = chunked
                #take a peek of the data then consume the rest of it
                    try:
                        data = response.read(1024)
                        while response.read(32768) != b'':
                            pass
                    except:
                        pass
                    resultdata = {'status':response.status, 'reason':response.reason, 'headers':dict(response.headers),
                                  'data':data, 'type':'error'}
                    raise AWSDataException('missing content length', resultdata)

                handler = AWSXMLHandler()
                incrementalParser = xml.sax.make_parser()
                incrementalParser.setContentHandler(handler)

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
                    self.logger.debug("Received >> %s", data)
                    incrementalParser.feed(data)
                if doretry:
                    continue

                awsresponse = handler.getdict()

                if awsresponse is not None:
                    if 300 <= response.status < 400:
                        try:
                            #TODO: we should differentiate between temporary and permanent redirect,
                            #and handle correctly the second
                            if awsresponse['Error']['Code'] in ('TemporaryRedirect', 'PermanentRedirect', 'Redirect'):
                                redirect = awsresponse['Error']['Endpoint']
                                if _redirectcount < 3:
                                    _redirectcount += 1
                                    host = redirect
                                    continue
                        except:
                            pass

                    if self.checkForErrors(awsresponse, response.status, response.reason, response.headers) is True:
                        if _retrycount < retry:
                            _retrycount += 1
                            doretry = True
                            break

                resultdata = {'status':response.status, 'reason':response.reason, 'headers':dict(response.headers),
                              'awsresponse':awsresponse, 'type':'xmldict'}

                if statusexpected is not True and response.status not in statusexpected:
                    raise AWSStatusException(resultdata)
                else:
                    return resultdata

            if statusexpected is not True and response.status not in statusexpected:
                #take a peek of the data then consume the rest of it
                try:
                    data = response.read(1024)
                    while response.read(32768) != b'':
                        pass
                except:
                    pass
                resultdata = {'status':response.status, 'reason':response.reason, 'headers':dict(response.headers),
                              'data':data, 'type':'error'}
                raise AWSStatusException(resultdata)

            if 'Content-Length' not in response.headers:
                #every non xml response should have  'Content-Length' set
                #take a peek of the data then consume the rest of it
                try:
                    data = response.read(1024)
                    while response.read(32768) != b'':
                        pass
                except:
                    pass
                if data == b'':
                    return {'status':response.status, 'reason':response.reason,
                            'headers':dict(response.headers), 'type':'empty'}

                raise AWSDataException('missing content length', {'status':response.status, 'reason':response.reason,
                                                                  'headers':dict(response.headers),'data':data,
                                                                  'type':'error'})

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
                    return {'status':response.status, 'reason':response.reason, 'headers':dict(response.headers),
                            'sizeinfo':sizeinfo, 'type':'raw', 'inputobject':inputobject}
                inputobject.write(data)

            # if we are here then we should retry
            continue
