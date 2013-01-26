# awsutils/utils/auth.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import time, hmac, hashlib, base64

QUERIES_OF_INTEREST = {'acl', 'cors', 'defaultObjectAcl', 'location', 'logging',
                       'partNumber', 'policy', 'requestPayment', 'torrent',
                       'versioning', 'versionId', 'versions', 'website',
                       'uploads', 'uploadId', 'response-content-type',
                       'response-content-language', 'response-expires',
                       'response-cache-control', 'response-content-disposition',
                       'response-content-encoding', 'delete', 'lifecycle'}

URLENCODE_SAFE = frozenset(b'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                           b'abcdefghijklmnopqrstuvwxyz'
                           b'0123456789'
                           b'_.-')

URLENCODE_SAFE_BYTES = bytes(URLENCODE_SAFE)

SIGNATURE_V2 = 0
SIGNATURE_V4 = 1
SIGNATURE_V4_HEADERS = 2
SIGNATURE_S3_REST = 3

S3_ENDPOINTS = {"s3.amazonaws.com", "s3-us-west-1.amazonaws.com",
                "s3-us-west-2.amazonaws.com", "s3-eu-west-1.amazonaws.com",
                "s3-ap-southeast-1.amazonaws.com", "s3-ap-southeast-2.amazonaws.com",
                "s3-ap-northeast-1.amazonaws.com", "s3.sa-east-1.amazonaws.com"}

def awsDate(date=time.gmtime()):
    return ('%s, %02d %s %04d %02d:%02d:%02d GMT' % (('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')[date.tm_wday],
                                                     date.tm_mday,
                                                     ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                                                      'Oct', 'Nov', 'Dec')[date.tm_mon - 1],
                                                     date.tm_year, date.tm_hour, date.tm_min, date.tm_sec))


def getISO8601date(date=time.gmtime()):
    return '%04d%02d%02d' % (date.tm_year, date.tm_mon, date.tm_mday)


def getISO8601dashedTime(date=time.gmtime()):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', date)


def getISO8601Time(date=time.gmtime()):
    return '%04d%02d%02dT%02d%02d%02dZ' % (
        date.tm_year, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec)


def urlquote(data):
    if isinstance(data, str):
        data = data.encode('utf-8', 'strict')
    return ''.join([chr(char) if char in URLENCODE_SAFE_BYTES else '%{:02X}'.format(char) for char in data])


def HMAC_SHA1(secret, data, hexdigest=True):
    if isinstance(secret, str):
        secret = secret.encode(encoding='utf8')
    if isinstance(data, str):
        data = data.encode(encoding='utf8')
    _hmac = hmac.new(secret, digestmod=hashlib.sha1)
    _hmac.update(data)
    if hexdigest:
        return _hmac.hexdigest()
    return _hmac.digest()


def HMAC_SHA256(secret, data, hexdigest=True):
    if isinstance(secret, str):
        secret = secret.encode(encoding='utf8')
    if isinstance(data, str):
        data = data.encode(encoding='utf8')
    _hmac = hmac.new(secret, digestmod=hashlib.sha256)
    _hmac.update(data)
    if hexdigest:
        return _hmac.hexdigest()
    return _hmac.digest()


def SHA256(data, hexdigest=True):
    if isinstance(data, str):
        data = data.encode(encoding='utf8')
    sha256 = hashlib.sha256()
    sha256.update(data)
    if hexdigest:
        return sha256.hexdigest()
    return sha256.update()


def canonicalQueryString(query, whitelist=None):
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
            result.append(urlquote(item[0]) + '=' + urlquote(value))
        else:
            result.append(urlquote(item[0]))
    return "&".join(result)


def canonicalRequestS3Rest(method='GET', uri='/', headers=None, query=None, expires=None):
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
    cqs = canonicalQueryString(query, whitelist=QUERIES_OF_INTEREST)
    if len(cqs) > 0:
        result.append("%s?%s" % (uri, cqs))
    else:
        result.append(uri)

    return "\n".join(result)


def canonicalHeaders(headers):
    _headers = {}
    for key in headers:
        _headers[key.lower()] = headers[key].strip()
    _headers = sorted(_headers.items())
    result = []
    for item in _headers:
        result.append("%s:%s" % (item[0], item[1]))
    return "\n".join(result)


def canonicalHeaderNames(headers):
    _headers = {}
    for key in headers:
        _headers[key.lower()] = headers[key].strip()
    _headers = sorted(_headers.items())
    return ';'.join([item[0].lower() for item in _headers])


def canonicalRequestV4(method='GET', uri='/', headers=None, query=None, body=b''):
    result = [method, uri, canonicalQueryString(query), canonicalHeaders(headers), '', canonicalHeaderNames(headers),
              SHA256(body)]
    return "\n".join(result)


def canonicalRequestV2(method='GET', uri='/', headers=None, query=None, body=b''):
    result = [method, headers['Host'], uri, canonicalQueryString(query)]
    return "\n".join(result)


def getAmzCredential(access_key, region, service, date=time.gmtime()):
    return '/'.join([access_key, getISO8601date(date), region, service, 'aws4_request'])


def calculateV2Signature(secret_key, date=time.gmtime(), uri='/', method='GET', headers=None, query=None, body=b''):
    stringToSign = canonicalRequestV2(uri=uri, method=method, headers=headers, query=query, body=body)
    signature = HMAC_SHA256(secret_key, stringToSign, hexdigest=False)
    return base64.b64encode(signature).decode()


def calculateV4Signature(secret_key, region, service, date=time.gmtime(), uri='/', method='GET', headers=None,
                         query=None,
                         body=b''):
    simpledate = '%04d%02d%02d' % (date.tm_year, date.tm_mon, date.tm_mday)
    kDate = HMAC_SHA256("AWS4" + secret_key, simpledate, hexdigest=False)
    kRegion = HMAC_SHA256(kDate, region, hexdigest=False)
    kService = HMAC_SHA256(kRegion, service, hexdigest=False)
    kSigning = HMAC_SHA256(kService, "aws4_request", hexdigest=False)
    requestDate = '%04d%02d%02dT%02d%02d%02dZ' % (
        date.tm_year, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec)
    credentialsScope = '/'.join([simpledate, region, service, 'aws4_request'])
    canonicalRequest = canonicalRequestV4(uri=uri, method=method, headers=headers, query=query, body=body)
    hashCanonicalRequest = SHA256(canonicalRequest)
    stringToSign = ['AWS4-HMAC-SHA256', requestDate, credentialsScope, hashCanonicalRequest]
    stringToSign = "\n".join(stringToSign)
    signature = HMAC_SHA256(kSigning, stringToSign)
    return signature


def signRequest(access_key, secret_key, endpoint, region=None, service=None,
                signmethod=None, date=time.gmtime(),
                uri='/', method='GET', headers=None,
                query=None, body=b'', expires=None):
    #signmethod v2, v4, v4headers
    date = time.gmtime()
    if headers is None: headers = {}
    headers['Host'] = endpoint
    headers['Date'] = getISO8601dashedTime(date)

    if signmethod == SIGNATURE_V2:
        if query is None: query = {}
        query['AWSAccessKeyId'] = access_key
        query['SignatureVersion'] = 2
        query['SignatureMethod'] = 'HmacSHA256'
        if expires is not None:
            query['Expires'] = getISO8601dashedTime(expires)
        else:
            query['Timestamp'] = getISO8601dashedTime(date)
        signature = calculateV2Signature(secret_key, date, uri, method, headers, query, body)
        query['Signature'] = signature

    elif signmethod == SIGNATURE_V4:

        if query is None: query = {}
        query['X-Amz-Date'] = getISO8601Time(date)
        query['X-Amz-Algorithm'] = 'AWS4-HMAC-SHA256'
        query['X-Amz-Credential'] = getAmzCredential(access_key, region, service, date)
        query['X-Amz-SignedHeaders'] = canonicalHeaderNames(headers)
        signature = calculateV4Signature(secret_key, region, service, date, uri, method, headers, query, body)
        query['X-Amz-Signature'] = signature

    elif signmethod == SIGNATURE_V4_HEADERS:
        signature = calculateV4Signature(secret_key, region, service, date, uri, method, headers, query, body)
        authorization = ['AWS4-HMAC-SHA256 Credential=',
                         getAmzCredential(access_key, region, service, date),
                         ', SignedHeaders=',
                         canonicalHeaderNames(headers),
                         ', Signature=',
                         signature]
        headers['Authorization'] = ''.join(authorization)

    elif signmethod == SIGNATURE_S3_REST:
        #detect if the bucket name is in the endpoint
        bucketname = None
        for region in S3_ENDPOINTS:
            if endpoint[1:].endswith(region):
                bucketname = endpoint[0:- (len(region) + 1)]
                break
        else:
            parts = endpoint.split('.')
            if len(parts) > 3:
                if "s3-external" in parts[-3]:
                    bucketname = '.'.join(parts[0:-3])
        if bucketname is not None:
            uri = '/' + bucketname + uri

        headers['Date'] = awsDate(date)
        canonicalRequest = canonicalRequestS3Rest(method=method, uri=uri, headers=headers, query=query,
                                                  expires=expires)

        print("canonicalRequest", canonicalRequest)

        headers["Authorization"] = 'AWS %s:%s' % (
            access_key,
            base64.b64encode(HMAC_SHA1(secret_key, canonicalRequest, hexdigest=False)).strip().decode())

    return headers, query, body