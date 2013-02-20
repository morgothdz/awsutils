# awsutils/tornado/sdbclient.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import functools
import tornado.gen
from awsutils.tornado.awsclient import AWSClient
from awsutils.exceptions.aws import UserInputException, extractExceptionsFromModule2Dicitonary
import awsutils.exceptions.s3
from awsutils.utils.auth import SIGNATURE_S3_REST
from awsutils.utils.auth import urlquote


class S3Client(AWSClient):
    @tornado.gen.engine
    def getObject(self, callback, bucketname, objectname, byterange=None, versionID=None,
                  if_modified_since=None, if_unmodified_since=None, if_match=None, if_none_match=None):

        query = {}
        if versionID is not None:
            query['vesionId'] = versionID
        headers = {}
        statusexpected = [404, 200]
        if byterange is not None:
            if len(byterange) > 1:
                headers['Range'] = "bytes=%d-%d" % (byterange[0], byterange[1])
            else:
                headers['Range'] = "bytes=%d-" % (byterange[0],)
            statusexpected = [200, 206]

        if if_modified_since is not None:
            # TODO: implement
            headers['If-Modified-Since'] = None
            statusexpected.append(304)
        if if_unmodified_since is not None:
            # TODO: implement
            headers['If-Unmodified-Since'] = None
            statusexpected.append(412)
        if if_match is not None:
            headers['If-Match'] = '"' + if_match + '"'
            statusexpected.append(412)
        if if_none_match is not None:
            headers['If-None-Match'] = '"' + if_none_match + '"'
            statusexpected.append(304)

        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)

        data = yield tornado.gen.Task(self.request, uri=uri + urlquote(objectname), endpoint=endpoint, query=query,
                                      statusexpected=statusexpected, headers=headers, xmlexpected=False,
                                      signmethod=SIGNATURE_S3_REST)

        self._ioloop.add_callback(functools.partial(callback, data))

    @tornado.gen.engine
    def headObject(self, callback, bucketname, objectname, versionID=None, byterange=None, if_modified_since=None,
                   if_unmodified_since=None, if_match=None, if_none_match=None):
        query = {}
        if versionID is not None:
            query['vesionId'] = versionID
        headers = {}
        statusexpected = [404, 200]
        if byterange is not None:
            if len(byterange) > 1:
                headers['Range'] = "bytes=%d-%d" % (byterange[0], byterange[1])
            else:
                headers['Range'] = "bytes=%d-" % (byterange[0],)
            statusexpected = [200, 206]

        if if_modified_since is not None:
            # TODO: implement
            headers['If-Modified-Since'] = None
            statusexpected.append(304)
        if if_unmodified_since is not None:
            # TODO: implement
            headers['If-Unmodified-Since'] = None
            statusexpected.append(412)
        if if_match is not None:
            headers['If-Match'] = '"' + if_match + '"'
            statusexpected.append(412)
        if if_none_match is not None:
            headers['If-None-Match'] = '"' + if_none_match + '"'
            statusexpected.append(304)

        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)

        data = yield tornado.gen.Task(self.request, uri=uri + urlquote(objectname), endpoint=endpoint, query=query,
                                      statusexpected=statusexpected, headers=headers, xmlexpected=False, method='HEAD',
                                      signmethod=SIGNATURE_S3_REST)

        self._ioloop.add_callback(functools.partial(callback, data))

    #================================== helper functionality ===========================================================
    def _buketname2PathAndEndpoint(self, bucketname):
        if bucketname != bucketname.lower():
            return "/" + bucketname + "/", self.endpoint
        return '/', bucketname + "." + self.endpoint

    EXCEPTIONS = extractExceptionsFromModule2Dicitonary('awsutils.exceptions.s3', awsutils.exceptions.s3.S3Exception)

    def checkForErrors(self, awsresponse, httpstatus, httpreason, httpheaders):
        if isinstance(awsresponse, dict):
            if 'Error' in awsresponse:
                if awsresponse['Error']['Code'] in self.EXCEPTIONS:
                    raise self.EXCEPTIONS[awsresponse['Error']['Code']](awsresponse, httpstatus, httpreason,
                                                                        httpheaders)
                else:
                    raise awsutils.exceptions.s3.S3Exception(awsresponse, httpstatus, httpreason, httpheaders)