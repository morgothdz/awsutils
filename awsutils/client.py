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
        return "AWSPartialReception [%d]" % (self.size,)

qsa_of_interest = {'acl', 'cors', 'defaultObjectAcl', 'location', 'logging',
                   'partNumber', 'policy', 'requestPayment', 'torrent',
                   'versioning', 'versionId', 'versions', 'website',
                   'uploads', 'uploadId', 'response-content-type',
                   'response-content-language', 'response-expires',
                   'response-cache-control', 'response-content-disposition',
                   'response-content-encoding', 'delete', 'lifecycle'}

_ALWAYS_SAFE = frozenset(b'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                         b'abcdefghijklmnopqrstuvwxyz'
                         b'0123456789'
                         b'_.-+')
_ALWAYS_SAFE_BYTES = bytes(_ALWAYS_SAFE)

def awsDate(now=time.gmtime()):
    return ('%s, %02d %s %04d %02d:%02d:%02d GMT' % (('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')[now.tm_wday],
                                                     now.tm_mday,
                                                     ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                                                      'Oct', 'Nov', 'Dec')[now.tm_mon - 1],
                                                     now.tm_year, now.tm_hour, now.tm_min, now.tm_sec))


class AWSClient:
    MAX_IN_MEMORY_READ_CHUNK_SIZE_FOR_RAW_DATA = 1024 * 1024
    HTTP_CONNECTION_RETRY_NUMBER = 3
    HTTP_RECEPTION_TIMEOUT = 30

    def __init__(self, host, access_key, secret_key, secure=False, tmpdir='.'):
        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.connections = {}
        self.tmpdir = tmpdir

    @classmethod
    def urlquote(cls, string):
        """
        Do not URL-encode any of the unreserved characters that RFC 3986 defines: A-Z, a-z, 0-9, hyphen ( - ), underscore ( _ ), period ( . ), and tilde ( ~ ).
        Percent-encode all other characters with %XY, where X and Y are hexadecimal characters (0-9 and uppercase A-F).
        Percent-encode extended UTF-8 characters in the form %XY%ZA....
        Percent-encode the space character as %20 (and not '+', as some encoding schemes do).
        """
        return ''.join([chr(char) if char in _ALWAYS_SAFE_BYTES else '%{:02X}'.format(b) for char in
                        string.encode('utf-8', 'strict')])

    @classmethod
    def canonicalQueryString(cls, query, whitelist=None):
        _query = sorted(query.items())
        result = []
        for item in _query:
            if whitelist is not None:
                if item[0] not in whitelist:
                    continue
            value = item[1]
            if value is not None:
                if not isinstance(value, str):
                    value = "%s" % (value,)
                result.append(cls.urlquote(item[0]) + '=' + cls.urlquote(value))
            else:
                result.append(cls.urlquote(item[0]))
        return "&".join(result)

    @classmethod
    def canonicalHeaders(cls, headers):
        _headers = []
        for key in headers:
            _headers[key.lower()] = headers[key].strip()
        _headers = sorted(_headers.items())
        result = []
        for item in sorted_headers:
            result.append("%s:%s" % (item[0], item[1]))
        return "\n".join(result), ';'.join([item[0].lower() for item in _headers])

    @classmethod
    def canonicalRequest(cls, method='GET', uri='/', headers=None, query=None, expires=None):
        interesting_headers = {}
        if headers is not None:
            for key in headers:
                lk = key.lower()
                if headers[key] is not None and (
                lk in ('content-md5', 'content-type', 'date') or lk.startswith('x-amz-')):
                    interesting_headers[lk] = headers[key].strip()
        if 'content-type' not in interesting_headers: interesting_headers['content-type'] = ''
        if 'content-md5' not in interesting_headers: interesting_headers['content-md5'] = ''
        if expires is not None: interesting_headers['date'] = expires
        sorted_header_keys = sorted(interesting_headers)
        result = [method]
        for key in sorted_header_keys:
            val = interesting_headers[key]
            if key.startswith('x-amz-'):
                result.append("%s:%s" % (key, val))
            else:
                result.append(val)

        cqs = cls.canonicalQueryString(query, whitelist=qsa_of_interest)
        if len(cqs) > 0:
            result.append("%s?%s" % (uri, cqs))
        else:
            result.append(uri)

        return "\n".join(result)

    @classmethod
    def canonicalRequestV4(cls, method='GET', uri='/', headers=None, query=None, body=b''):
        cheaders, signedheaders = cls.canonicalHeaders(headers)
        sha256 = hashlib.sha256()
        sha256.update(body)
        result = [method, uri, cls.canonicalQueryString(query), cheaders, signedheaders, sha256.hexdigest()]
        return "\n".join(result)

    def calculateV4Signature(self, date=time.gmtime(), uri='/', method='GET', headers=None, query=None, body=b''):
        simpledate = '%04d%02d%02d'%(date.tm_year, date.tm_mon, date.tm_mday)
        kDate = HMAC("AWS4" + self.secret_key, simpledate)
        kRegion = HMAC(kDate, self.region)
        kService = HMAC(kRegion, self.service)
        kSigning = HMAC(kService, "aws4_request")

        requestDate = '%04d%02d%02dT%02d%02%02Z'%(date.tm_year, date.tm_mon, date.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)
        credentialsScope = '/'.join([simpledate, self.region, self.service, 'aws4_request'])

        sha256 = hashlib.sha256()
        sha256.update(self.canonicalRequestV4(uri='/', method='GET', headers=None, query=None, body=b''))
        hashCanonicalRequest = sha256.hexdigest()

        stringToSign = ['AWS4-HMAC-SHA256', requestDate, credentialsScope, hashCanonicalRequest]
        stringToSign = "\n".join(stringToSign)

        signature = HMAC(kSigning, stringToSign)
        return signature

        """
        
        """

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

        cqs = self.canonicalQueryString(query, whitelist=qsa_of_interest)
        if len(cqs) > 0:
            buf.append("%s?%s" % (path, cqs))
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
                inputobject=None,
                operationtimeout=None,
                retry=None,
                receptiontimeout=None,
                _inputIOWrapper=None):
        if retry is None: retry = self.HTTP_CONNECTION_RETRY_NUMBER
        if receptiontimeout is None: receptiontimeout = self.HTTP_RECEPTION_TIMEOUT

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

            print("Requesting", method, endpoint, path, headers, query)

            virtualhostedsubdomain = None
            if endpoint is None:
                headers['Host'] = self.host
            else:
                headers['Host'] = endpoint
                virtualhostedsubdomain = endpoint.split('.', 2)[0]

            if query != {}:
                url = "%s?%s" % (path, AWSClient.canonicalQueryString(query))
            else:
                url = path

            conn = self.getConnection(destination=headers['Host'], timeout=receptiontimeout)

            self.prepareHeaders(method, path, headers, query, virtualhostedsubdomain=virtualhostedsubdomain)

            try:
                conn.request(method=method, url=url, body=body, headers=headers)
                response = conn.getresponse()
            except Exception as _e:
                if _retrycount < retry:
                    _retrycount += 1
                    continue
                raise

            data = None

            if xmlexpected or (
            'Content-Type' in response.headers and response.headers['Content-Type'] == 'application/xml'):
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
                        if data['Error']['Code'] in ('TemporaryRedirect', 'PermanentRedirect'):
                            redirect = data['Error']['Endpoint']
                            if _redirectcount < 3:
                                _redirectcount += 1
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
                sizeinfo = {'size': int(contentrange[1]), 'start': int(range[0]), 'end': int(range[1]), 'downloaded': 0}
            elif response.status == 200:
                sizeinfo = {'size': size, 'start': 0, 'end': size - 1, 'downloaded': 0}

            if inputobject is None:
                if size > self.MAX_IN_MEMORY_READ_CHUNK_SIZE_FOR_RAW_DATA:
                    inputobject = tempfile.TemporaryFile(mode="w+b", dir=self.tmpdir, prefix='awstmp-')
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
