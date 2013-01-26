# awsutils/s3/s3client.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import binascii, base64, json
from awsutils.client import AWSClient, UserInputException, IntegrityCheckException
from awsutils.utils.auth import SIGNATURE_S3_REST
from awsutils.utils.exceptions import generateExceptionDictionary


class AWSS3Exception_AccessDenied(Exception):
    #Access Denied
    HTTP_STATUS = 403

class AWSS3Exception_AccountProblem(Exception):
    #There is a problem with your AWS account that prevents the operation from completing successfully
    HTTP_STATUS = 403

class AWSS3Exception_AmbiguousGrantByEmailAddress(Exception):
    #The e-mail address you provided is associated with more than one account
    HTTP_STATUS = 400

class AWSS3Exception_BadDigest(Exception):
    #The Content-MD5 you specified did not match what we received
    HTTP_STATUS = 400

class AWSException_BucketAlreadyExists(Exception):
    #The requested bucket name is not available. The bucket namespace is shared by all users of the system
    HTTP_STATUS = 409

class AWSS3Exception_BucketAlreadyOwnedByYou(Exception):
    #Your previous request to create the named bucket succeeded and you already own it
    HTTP_STATUS = 409

class AWSS3Exception_BucketNotEmpty(Exception):
    #The bucket you tried to delete is not empty
    HTTP_STATUS = 409

class AWSS3Exception_CredentialsNotSupported(Exception):
    #This request does not support credentials
    HTTP_STATUS = 400

class AWSS3Exception_CredentialsNotSupported(Exception):
    #This request does not support credentials
    HTTP_STATUS = 400

class AWSS3Exception_CrossLocationLoggingProhibited(Exception):
    #Cross location logging not allowed. Buckets in one geographic location cannot log information to a
    #bucket in another location
    HTTP_STATUS = 403

class AWSS3Exception_EntityTooSmall(Exception):
    #Your proposed upload is smaller than the minimum allowed object size
    HTTP_STATUS = 400

class AWSS3Exception_EntityTooLarge(Exception):
    #Your proposed upload exceeds the maximum allowed object size
    HTTP_STATUS = 400

class AWSS3Exception_ExpiredToken(Exception):
    #The provided token has expired
    HTTP_STATUS = 400

class AWSS3Exception_IllegalVersioningConfigurationException(Exception):
    #Indicates that the Versioning configuration specified in the request is invalid
    HTTP_STATUS = 400

class AWSS3Exception_SignatureDoesNotMatch(Exception):
    #The request signature we calculated does not match the signature you provided
    HTTP_STATUS = 403

class S3Client(AWSClient):

    EXCEPTIONS = generateExceptionDictionary(__name__, exceptionprefix = 'AWSS3Exception_')

    #==================================== operations on the service ====================================================
    def getService(self):
        _status, _reason, _headers, data = self.request(method='GET', uri='/', signmethod=SIGNATURE_S3_REST)
        buckets = data['ListAllMyBucketsResult']['Buckets']['Bucket']
        if isinstance(buckets, dict): buckets = [buckets]
        return data['ListAllMyBucketsResult']

    #==================================== operations on the buckets ====================================================
    def deleteBucket(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        self.request(method="DELETE", uri=uri, host=endpoint, statusexpected=[204], signmethod=SIGNATURE_S3_REST)

    def deleteBucketCors(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        self.request(method="DELETE", uri=uri, host=endpoint, statusexpected=[204], query={'cors': None},
                     signmethod=SIGNATURE_S3_REST)

    def deleteBucketLifecycle(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        self.request(method="DELETE", uri=uri, host=endpoint, statusexpected=[204], query={'lifecycle': None},
                     signmethod=SIGNATURE_S3_REST)

    def deleteBucketPolicy(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        self.request(method="DELETE", uri=uri, host=endpoint, statusexpected=[204], query={'policy ': None},
                     signmethod=SIGNATURE_S3_REST)

    def deleteBucketTagging(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        self.request(method="DELETE", uri=uri, host=endpoint, statusexpected=[204], query={'tagging': None},
                     signmethod=SIGNATURE_S3_REST)

    def deleteBucketWebsite(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        self.request(method="DELETE", uri=uri, host=endpoint, statusexpected=[204], query={'website': None},
                     signmethod=SIGNATURE_S3_REST)

    def getBucket(self, bucketname, delimiter=None, marker=None, prefix=None, maxkeys=None):
        """
        List Objects
        """
        query = {}
        if delimiter is not None: query['delimiter'] = delimiter
        if prefix is not None: query['prefix'] = prefix
        if marker is not None: query['marker'] = marker
        if maxkeys is not None: query['max-keys'] = maxkeys
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri, host=endpoint, query=query,
                                                        signmethod=SIGNATURE_S3_REST)
        return data['ListBucketResult']

    def getBucketAcl(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri, host=endpoint, query={'acl': None},
                                                        signmethod=SIGNATURE_S3_REST)
        return data['AccessControlPolicy']

    def getBucketCors(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri, host=endpoint, query={'cors': None},
                                                        signmethod=SIGNATURE_S3_REST)
        return data['CORSConfiguration']

    def getBucketLifecycle(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri, host=endpoint, query={'lifecycle': None},
                                                        signmethod=SIGNATURE_S3_REST)
        return data['LifecycleConfiguration']

    def getBucketPolicy(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri, host=endpoint, query={'policy': None},
                                                        signmethod=SIGNATURE_S3_REST)
        return data['LifecycleConfiguration']

    def getBucketLocation(self, bucketname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri, host=endpoint, query={'location': None},
                                                        signmethod=SIGNATURE_S3_REST)
        location = data['LocationConstraint']
        if location == '':
            location = 'us-standard'
        return location

    def listMultipartUploads(self, bucketname, delimiter=None, max_uploads=None, key_marker=None, prefix=None,
                             upload_id_marker=None):
        query = {'uploads': None}
        if delimiter is not None: query['delimiter'] = delimiter
        if max_uploads is not None: query['max-uploads'] = max_uploads
        if key_marker is not None: query['key-marker'] = key_marker
        if prefix is not None: query['prefix'] = prefix
        if upload_id_marker is not None: query['upload-id-marker'] = upload_id_marker
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri, host=endpoint,
                                                        query=query, signmethod=SIGNATURE_S3_REST)
        return data['ListMultipartUploadsResult']

    def putBucketPolicy(self, bucketname, policy):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        if isinstance(policy, dict):
            policy = json.dumps(policy)
        _status, _reason, _headers, data = self.request(method="PUT", uri=uri, body=policy, host=endpoint,
                                                        statusexpected=[204], query={'policy': None},
                                                        signmethod=SIGNATURE_S3_REST)

    #==================================== operations on the objects ====================================================
    def deleteObject(self, bucketname, objectname, versionID=None, x_amz_mfa=None):
        headers = {}
        query = {}
        if x_amz_mfa is not None:
            headers['x-amz-mfa'] = x_amz_mfa
        if versionID is not None:
            query['vesionId'] = versionID

        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, headers, _data = self.request(method="DELETE", uri=uri + objectname, host=endpoint, query=query,
                                                        statusexpected=[204], signmethod=SIGNATURE_S3_REST)

        headers = dict((k, v) for k, v in headers.items() if k in ('x-amz-version-id', 'x-amz-delete-marker'))
        return headers

    def deleteMultipleObjects(self, bucketname, objects, x_amz_mfa=None):
        """ojects can be: a list of  strings [object names] 
                          a list of dictionaries with name and vesionId keys
                          a list of objects with name and vesionId attributes"""
        #TODO: implement
        pass

    def getObject(self, bucketname, objectname,
                  inputobject=None,
                  byterange=None,
                  versionID=None,
                  if_modified_since=None,
                  if_unmodified_since=None, if_match=None, if_none_match=None,
                  _doHeadRequest=False,
                  _inputIOWrapper=None):
        """
        range = list(start, end)
        inputbuffer = be any object implementing write(bytes)
        if no inputbuffer is provided then the response will be depending on the response size an io.BytesIO or a
        tempfile.TemporaryFile opened to mode w+b
        """
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
        status, _reason, headers, range, data = self.request(method="HEAD" if _doHeadRequest else "GET",
                                                             uri=uri + objectname,
                                                             headers=headers,
                                                             host=endpoint,
                                                             statusexpected=statusexpected,
                                                             query=query,
                                                             inputobject=inputobject,
                                                             xmlexpected=False,
                                                             _inputIOWrapper=_inputIOWrapper,
                                                             signmethod=SIGNATURE_S3_REST)

        result = dict((k, v) for k, v in headers.items() if k in ('ETag',
                                                                  'x-amz-delete-marker', 'x-amz-expiration',
                                                                  'x-amz-server-side-encryption',
                                                                  'x-amz-restore', 'x-amz-version-id',
                                                                  'x-amz-website-redirect-location')
        or k.startswith('x-amz-meta-'))
        result['status'] = status
        result['range'] = range
        if status in (200, 206) and not _doHeadRequest:
            result['data'] = data
        return result

    def headObject(self, bucketname, objectname, versionID=None, byterange=None, if_modified_since=None,
                   if_unmodified_since=None, if_match=None, if_none_match=None, inputbuffer=None):
        return self.getObject(bucketname, objectname, versionID, byterange, if_modified_since, if_unmodified_since,
                              if_match, if_none_match, inputbuffer, _doHeadRequest=True)

    def putObject(self, bucketname, objectname, value, objlen=None, md5digest=None,
                  meta=None,
                  x_amz_server_side_encryption=None, x_amz_storage_class=None,
                  x_amz_website_redirect_location=None,
                  expires=None,
                  # TODO: not handled yet
                  x_amz_acl=None,
                  x_amz_grant_read=None, x_amz_grant_write=None, x_amz_grant_read_acp=None, x_amz_grant_write_acp=None,
                  x_amz_grant_full_control=None):
        """
        expires: int Number of milliseconds before expiration
        meta: a list of meta strings
        x_amz_server_side_encryption valid values None, AES256
        x_amz_storage_class valid values None (STANDARD), REDUCED_REDUNDANCY
        x_amz_website_redirect_location see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPUT.html
        """
        headers = {}
        if expires is not None:
            headers['Expires'] = expires
        if x_amz_server_side_encryption is not None:
            headers['x-amz-server-side-encryption'] = x_amz_server_side_encryption
        if x_amz_storage_class is not None:
            headers['x-amz-storage-class'] = x_amz_storage_class
        if x_amz_website_redirect_location is not None:
            headers['x-amz-website-redirect-location'] = x_amz_website_redirect_location

        # TODO: implement true support for 'Expect':'100-continue'       
        if objlen is not None:
            headers['Content-Length'] = str(objlen)

        if md5digest is not None:
            headers['Content-MD5'] = base64.b64encode(md5digest).strip().decode()

        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, headers, _data = self.request(method="PUT", uri=uri + objectname, body=value,
                                                        headers=headers,
                                                        host=endpoint, signmethod=SIGNATURE_S3_REST)
        if md5digest is not None:
            if headers['ETag'][1:-1] != binascii.hexlify(md5digest).decode():
                raise IntegrityCheckException('putObject returned unexpected ETag value',
                                              headers['ETag'][1:-1], binascii.hexlify(md5digest).decode())

        return dict((k, v) for k, v in headers.items() if k in ('ETag',
                                                                'x-amz-expiration', 'x-amz-server-side-encryption',
                                                                'x-amz-version-id'))


    def putObjectCopy(self, bucketname, objectname, sourcebucketname, sourceobjectname, metadata=None,
                      x_amz_server_side_encryption=None,
                      x_amz_storage_class=None,
                      x_amz_website_redirect_location=None,
                      x_amz_metadata_directive=None,
                      x_amz_copy_source_if_match=None, x_amz_copy_source_if_none_match=None,
                      # TODO: not handled yet
                      x_amz_copy_source_if_unmodified_since=None, x_amz_copy_source_if_modified_since=None,
                      x_amz_acl=None,
                      x_amz_grant_read=None, x_amz_grant_write=None, x_amz_grant_read_acp=None,
                      x_amz_grant_write_acp=None, x_amz_grant_full_control=None):

        headers = {'x-amz-copy-source': "/%s/%s" % (sourcebucketname, sourceobjectname)}
        if x_amz_server_side_encryption is not None:
            headers['x-amz-server-side-encryption'] = x_amz_server_side_encryption
        if x_amz_storage_class is not None:
            headers['x-amz-storage-class'] = x_amz_storage_class
        if x_amz_website_redirect_location is not None:
            headers['x-amz-website-redirect-location'] = x_amz_website_redirect_location
        if metadata is not None:
            for name in metadata:
                headers['x-amz-meta-'+ name] = metadata[name]
                headers['x-amz-metadata-directive'] = 'REPLACE'
        if x_amz_metadata_directive is not None:
            if x_amz_metadata_directive not in ['COPY','REPLACE']:
                raise UserInputException('x_amz_metadata_directive valid values: COPY|REPLACE')
            headers['x-amz-metadata-directive'] = x_amz_metadata_directive
        if x_amz_copy_source_if_match is not None:
            headers['x-amz-copy-source-if-match'] = x_amz_copy_source_if_match
        if x_amz_copy_source_if_none_match is not None:
            headers['x-amz-copy-source-if-none-match'] = x_amz_copy_source_if_none_match


        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="PUT", uri=uri + objectname,
                                                        headers=headers,
                                                        host=endpoint, signmethod=SIGNATURE_S3_REST)
        return data['CopyObjectResult']

    def putObjectAcl(self, bucketname, objectname,
                     x_amz_acl=None,
                     x_amz_grant_read=None, x_amz_grant_write=None, x_amz_grant_read_acp=None,
                     x_amz_grant_write_acp=None, x_amz_grant_full_control=None):
        # TODO: implement
        pass

    def getObjectAcl(self, bucketname, objectname):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri,
                                                        host=endpoint, query={'acl': None},
                                                        signmethod=SIGNATURE_S3_REST)
        return data['AccessControlPolicy']


    def uploadOjectPart(self, bucketname, objectname, partnumber, uploadid, value, objlen=None, md5digest=None):
        headers = {}
        if objlen is not None:
            headers['Content-Length'] = str(objlen)

        if md5digest is not None:
            headers['Content-MD5'] = base64.b64encode(md5digest).strip().decode()

        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, headers, _data = self.request(method="PUT", uri=uri + objectname, body=value,
                                                        headers=headers,
                                                        host=endpoint,
                                                        query={"partNumber": partnumber, "uploadId": uploadid},
                                                        signmethod=SIGNATURE_S3_REST)
        if md5digest is not None:
            if headers['ETag'][1:-1] != binascii.hexlify(md5digest).decode():
                raise IntegrityCheckException('uploadOjectPart returned unexpected ETag value', headers['ETag'][1:-1],
                                              binascii.hexlify(md5digest).decode())
        return dict((k, v) for k, v in headers.items() if k in ('ETag'))

    def uploadPartCopy(self, bucketname, objectname, partnumber, uploadID, sourcebucketname, sourceobjectname,
                       byterange=None, x_amz_copy_source_if_match=None, x_amz_copy_source_if_none_match=None,
                       x_amz_copy_source_if_unmodified_since=None, x_amz_copy_source_if_modified_since=None):
        # TODO: implement
        pass

    def initiateMultipartUpload(self, bucketname, objectname,
                                meta=None,
                                x_amz_server_side_encryption=None,
                                x_amz_storage_class=None,
                                x_amz_website_redirect_location=None,
                                expires=None,
                                # TODO: not handled yet
                                x_amz_acl=None,
                                x_amz_grant_read=None, x_amz_grant_write=None, x_amz_grant_read_acp=None,
                                x_amz_grant_write_acp=None,
                                x_amz_grant_full_control=None
    ):
        """
        x_amz_server_side_encryption valid values None, AES256
        x_amz_storage_class valid values None (STANDARD), REDUCED_REDUNDANCY
        x_amz_website_redirect_location see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPUT.html
        """
        headers = {}
        if x_amz_server_side_encryption:
            headers['x-amz-server-side-encryption'] = x_amz_server_side_encryption
        if x_amz_storage_class:
            headers['x-amz-storage-class'] = x_amz_storage_class
        if x_amz_website_redirect_location:
            headers['x-amz-website-redirect-location'] = x_amz_website_redirect_location

        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="POST", uri=uri + objectname,
                                                        headers=headers, host=endpoint,
                                                        query={"uploads": None}, signmethod=SIGNATURE_S3_REST)
        data = data['InitiateMultipartUploadResult']
        if data['Bucket'] != bucketname or data['Key'] != objectname:
            raise IntegrityCheckException('unexpected bucket/key name received', (data['Bucket'],data['Key']),
                                          (bucketname, objectname))
        return data

    def completeMultipartUpload(self, bucketname, objectname, uploadId, parts):
        data = ["<CompleteMultipartUpload>"]
        for partnumber in parts:
            data.append("<Part><PartNumber>%s</PartNumber><ETag>%s</ETag></Part>" % (partnumber, parts[partnumber]))
        data.append("</CompleteMultipartUpload>")

        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="POST", uri=uri + objectname,
                                                        body="".join(data),
                                                        host=endpoint, query={"uploadId": uploadId},
                                                        signmethod=SIGNATURE_S3_REST)
        data = data['CompleteMultipartUploadResult']
        if data['Bucket'] != bucketname or data['Key'] != objectname:
            raise IntegrityCheckException('unexpected bucket/key name received', (data['Bucket'],data['Key']),
                                          (bucketname, objectname))
        return data

    def abortMultipartUpload(self, bucketname, objectname, uploadId):
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        self.request(method="DELETE", uri=uri, host=endpoint, statusexpected=[204], query={"uploadId": uploadId},
                     signmethod=SIGNATURE_S3_REST)

    def listParts(self, bucketname, objectname, uploadId, max_parts=None, part_number_marker=None):
        query = {"uploadId": uploadId}
        if max_parts is not None:
            query['max-parts'] = max_parts
        if part_number_marker is not None:
            query['part-number-marker'] = part_number_marker
        uri, endpoint = self._buketname2PathAndEndpoint(bucketname)
        _status, _reason, _headers, data = self.request(method="GET", uri=uri + objectname,
                                                        host=endpoint, query=query, signmethod=SIGNATURE_S3_REST)
        data = data['ListPartsResult']
        if data['Bucket'] != bucketname or data['Key'] != objectname:
            raise IntegrityCheckException('unexpected bucket/key name received', (data['Bucket'],data['Key']),
                                          (bucketname, objectname))
        return data

    #================================== helper functions ===============================================================
    def _buketname2PathAndEndpoint(self, bucketname):
        if bucketname != bucketname.lower():
            return  "/" + bucketname + "/", self.endpoint
        return '/', bucketname + "." + self.endpoint