# awsutils/s3/client.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, hmac, hashlib, io, base64, socket, tempfile
import http.client
import xml.sax
from collections import OrderedDict
from urllib.parse import quote_plus as urllib_parse_quote_plus

try:
    from select import poll as select_poll
    from select import POLLIN as select_POLLIN
except ImportError:  # Doesn't exist on OSX and other platforms
    from select import select

    select_poll = False

from awsutils.utils.xmlhandler import AWSXMLHandler

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
        return "AWSPartialReception [%d]"%(self.size,)

qsa_of_interest = {'acl', 'cors', 'defaultObjectAcl', 'location', 'logging',
                   'partNumber', 'policy', 'requestPayment', 'torrent',
                   'versioning', 'versionId', 'versions', 'website',
                   'uploads', 'uploadId', 'response-content-type',
                   'response-content-language', 'response-expires',
                   'response-cache-control', 'response-content-disposition',
                   'response-content-encoding', 'delete', 'lifecycle'}

def awsDate(now=time.gmtime()):
    return ('%s, %02d %s %04d %02d:%02d:%02d GMT' % (('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')[now.tm_wday],
                                                     now.tm_mday,
                                                     ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                                                      'Oct', 'Nov', 'Dec')[now.tm_mon - 1],
                                                     now.tm_year, now.tm_hour, now.tm_min, now.tm_sec))


class AWSClient:
    MAX_IN_MEMORY_READ_CHUNK_SIZE_FOR_RAW_DATA = 1024 * 1024

    def __init__(self, host, access_key, secret_key, secure=False, tmpdir='.'):
        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.connections = {}
        self.tmpdir = tmpdir

    def urlencode(self, query):
        result = []
        for key in query:
            value = query[key]
            key = urllib_parse_quote_plus(key, safe='', encoding=None, errors=None)
            if value is not None:
                if not isinstance(value, str):
                    value = "%s"%(value,)
                result.append(key + '=' + urllib_parse_quote_plus(value, safe='', encoding=None, errors=None))
            else:
                result.append(key)
        return "&".join(result)

    def prepareHeaders(self, method, path, headers=None, query=None, expires=None, date=awsDate(),
                       virtualhostedsubdomain=None):
        if headers is None: headers = {}
        if query is None: query = {}

        if virtualhostedsubdomain is not None: path = "/%s%s" % (virtualhostedsubdomain, path)

        headers["Date"] = date
        interesting_headers = {}

        for key in headers:
            lk = key.lower()
            if headers[key] is not None and (lk in ('content-md5', 'content-type', 'date') or lk.startswith('x-amz-')):
                interesting_headers[lk] = headers[key].strip()

        # these keys get empty strings if they don't exist
        if 'content-type' not in interesting_headers:
            interesting_headers['content-type'] = ''
        if 'content-md5' not in interesting_headers:
            interesting_headers['content-md5'] = ''

        # if you're using expires for query string auth, then it trumps date (and provider.date_header)
        if expires: interesting_headers['date'] = expires

        sorted_header_keys = sorted(interesting_headers)

        buf = [method]
        for key in sorted_header_keys:
            val = interesting_headers[key]
            if key.startswith('x-amz-'):
                buf.append("%s:%s" % (key, val))
            else:
                buf.append(val)

        qs = OrderedDict((sorted([item for item in query.items() if item[0] in qsa_of_interest], key=lambda t: t[0])))
        if len(qs) > 0:
            buf.append("%s?%s" % (path, self.urlencode(qs)))
        else:
            buf.append(path)

        buffer = "\n".join(buf).encode()
        #print("HMAC", buffer)
        _hmac = hmac.new(self.secret_key.encode(), digestmod=hashlib.sha1)
        _hmac.update(buffer)
        headers["Authorization"] = 'AWS %s:%s' % (self.access_key, base64.b64encode(_hmac.digest()).strip().decode())

        return headers

    def is_connection_usable(self, httpconnection):
        sock = httpconnection.sock
        if sock is None:
            return False
            # if we have to read that means either that the connection was dropped, or that we have input data and that is not a good thing in this scenario
        if select_poll == False:
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

    def request(self, method='GET', path='/', headers=None, query=None, body=None,
                endpoint=None,
                statusexpected=None,
                xmlexpected=True,
                retry=3,
                inputobject=None,
                operationtimeout=None,
                receptiontimeout=socket._GLOBAL_DEFAULT_TIMEOUT):

        _redircount = 0
        _retycount = 0
        _outputiooffset = None
        _inputiooffset = None

        if headers is None: headers = {}
        if query is None: query = {}
        if statusexpected is None: statusexpected = [200]
        headers['Connection'] = 'keep-alive'
        starttime = time.time()

        while True:
            
            # if request body is an io like object ensure that we set the pointer back to the start before retrying
            if hasattr(body, 'reset'):
                if _redircount > 0:
                    body.reset()
            else:
                try:
                    if _outputiooffset is None:
                        _outputiooffset = body.tell()
                    else:
                        body.seek(_outputiooffset, io.SEEK_SET)
                except:
                    pass
            
            # same for the inputbuffer
            try:
                if _inputiooffset is None:
                    _inputiooffset = inputobject.tell()
            except:
                pass

            print("Requesting", method, endpoint, path, headers, query)

            virtualhostedsubdomain = None
            if endpoint is None:
                headers['Host'] = self.host
            else:
                headers['Host'] = endpoint
                virtualhostedsubdomain = endpoint.split('.', 2)[0]

            if query != {}:
                url = "%s?%s" % (path, self.urlencode(query))
            else:
                url = path

            conn = self.getConnection(destination=headers['Host'], timeout=receptiontimeout)

            self.prepareHeaders(method, path, headers, query, virtualhostedsubdomain=virtualhostedsubdomain)

            try:
                conn.request(method=method, url=url, body=body, headers=headers)
                response = conn.getresponse()
            except Exception as _e:
                if _retycount < retry:
                    _retycount += 1
                    continue
                raise

            data = None

            if xmlexpected or ('Content-Type' in response.headers and response.headers['Content-Type'] == 'application/xml'):

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
                        if _retycount < retry:
                            _retycount += 1
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
                        if data['Error']['Code'] in ('TemporaryRedirect', 'PermanentRedirect'):
                            redirect = data['Error']['Endpoint']
                            if _redircount < 3:
                                _redircount += 1
                                endpoint = redirect
                                continue
                    except:
                        pass

                if response.status not in statusexpected:
                    # TODO: maybe we should retry on some status?
                    raise AWSException(response.status, response.reason, dict(response.headers), awsresponse)

                return response.status, response.reason, dict(response.headers), awsresponse

            if (response.status not in statusexpected) or ('Content-Length' not in response.headers):
                #consume input
                try:
                    data = response.read(1024)
                    while response.read(1024) != b'':
                        pass
                except Exception as e:
                    print(e)

                if response.status not in statusexpected:
                    #TODO: maybe we should retry on some status?
                    raise AWSDataException('unexpected status', response.status, response.reason, dict(response.headers), data)

                if 'Content-Length' not in response.headers:
                    raise AWSDataException('missing content length', response.status, response.reason, dict(response.headers), data)
                    """
                    size = len(data)
                    sizeinfo = {'size':size, 'start' : 0, 'end':size - 1, 'downloaded':0}
                    return response.status, response.reason, dict(response.headers), sizeinfo, data
                    """

            #if we are here then most probably we want to download some data
            size = int(response.headers['Content-Length'])
            if response.status == 206:
                contentrange = response.headers['Content-Range']
                contentrange = contentrange.split(' ')[1].split('/')
                range = contentrange[0].split('-')
                sizeinfo = {'size':int(contentrange[1]), 'start' : int(range[0]), 'end':int(range[1]), 'downloaded':0}
            elif response.status == 200:
                sizeinfo = {'size':size, 'start' : 0, 'end':size - 1, 'downloaded':0}

            if inputobject is None:
                _inputiooffset = 0
                if size > self.MAX_IN_MEMORY_READ_CHUNK_SIZE_FOR_RAW_DATA:
                    inputobject = tempfile.TemporaryFile(mode="w+b", dir=self.tmpdir, prefix='awstmp-')
                else:
                    inputobject = io.BytesIO()

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
                        raise AWSPartialReception(headers = dict(response.headers), data = inputobject, sizeinfo = sizeinfo, exception =e)

                    # TODO: not all exception should retry, maybe an exception white list would be the way to go
                    if _retycount < retry:
                        _retycount += 1
                        _inputiooffset += ammount
                        inputobject.seek(_inputiooffset, io.SEEK_SET)
                        break
                    raise

                if (data == b'') or (ammount > size):
                    return response.status, response.reason, dict(response.headers), sizeinfo, inputobject

                inputobject.write(data)

            # if we are here then we should retry
            continue
