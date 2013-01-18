# awsutils/s3/s3client.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import binascii, base64
from awsutils.client import AWSClient

class S3Exception(Exception):
    pass

class S3HashCheckException(Exception):
    pass

class S3IntegrityException(Exception):
    pass


class S3Client(AWSClient):
    #==================================== operations on the service =======================================
    def getService(self):
        _status, _reason, _headers, data = self.request(method='GET', path='/')
        buckets = data['ListAllMyBucketsResult']['Buckets']['Bucket']
        if isinstance(buckets, dict): buckets = [buckets]
        return data['ListAllMyBucketsResult']

    #==================================== operations on the buckets =======================================

    def getBucketlocation(self, bucketname):
        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/"
            endpoint = None
        else:
            path = '/'
            endpoint = bucketname + ".s3.amazonaws.com"
        _status, _reason, _headers, data = self.request(method="GET", path=path, endpoint=endpoint,
                                                        query={'location': None})
        location = data['LocationConstraint']
        if location == '':
            location = 'us-standard'
        return location

    def getBucket(self, bucketname, delimiter=None, marker=None, prefix=None, maxkeys=None):
        query = {}
        if delimiter is not None: query['delimiter'] = delimiter
        if prefix is not None: query['prefix'] = prefix
        if marker is not None: query['marker'] = marker
        if maxkeys is not None: query['max-keys'] = maxkeys

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/"
            endpoint = None
        else:
            path = '/'
            endpoint = bucketname + ".s3.amazonaws.com"

        _status, _reason, _headers, data = self.request(method="GET", path=path, endpoint=endpoint, query=query)
        return data['ListBucketResult']

    def listMultipartUploads(self, bucketname, delimiter=None, max_uploads=None, key_marker=None, prefix=None, upload_id_marker=None):
        query = {'uploads':None}
        if delimiter is not None: query['delimiter'] = delimiter
        if max_uploads is not None: query['max-uploads'] = max_uploads
        if key_marker is not None: query['key-marker'] = key_marker
        if prefix is not None: query['prefix'] = prefix
        if upload_id_marker is not None: query['upload-id-marker'] = upload_id_marker
        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/"
            endpoint = None
        else:
            path = '/'
            endpoint = bucketname + ".s3.amazonaws.com"
        _status, _reason, _headers, data = self.request(method="GET", path=path, endpoint=endpoint, query=query)
        return data['ListMultipartUploadsResult']



    #==================================== operations on the objects =======================================
    def deleteObject(self, bucketname, objectname, versionID=None, x_amz_mfa=None):
        headers = {}
        query = {}
        if x_amz_mfa is not None:
            headers['x-amz-mfa'] = x_amz_mfa
        if versionID is not None:
            query['vesionId'] = versionID

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"

        _status, _reason, headers, _data = self.request(method="DELETE", path=path,
                                                        endpoint=endpoint,
                                                        query=query,
                                                        statusexpected=[204])
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
        if no inputbuffer is provided then the response will be depending on the response size an io.BytesIO or a tempfile.TemporaryFile opened to mode w+b 
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

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"

        status, _reason, headers, range, data = self.request(method="HEAD" if _doHeadRequest else "GET",
                                                             path=path,
                                                             headers=headers,
                                                             endpoint=endpoint,
                                                             statusexpected=statusexpected,
                                                             query=query,
                                                             inputobject=inputobject,
                                                             xmlexpected=False,
                                                             _inputIOWrapper=_inputIOWrapper)

        result = dict((k, v) for k, v in headers.items() if k in ('ETag',
        'x-amz-delete-marker', 'x-amz-expiration', 'x-amz-server-side-encryption',
        'x-amz-restore', 'x-amz-version-id', 'x-amz-website-redirect-location')
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

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"

        _status, _reason, headers, _data = self.request(method="PUT", path=path, body=value,
                                                        headers=headers,
                                                        endpoint=endpoint)
        if md5digest is not None:
            if headers['ETag'][1:-1] != binascii.hexlify(md5digest).decode():
                raise S3Exception('put-object-invalid-md5')

        return dict((k, v) for k, v in headers.items() if k in ('ETag',
        'x-amz-expiration', 'x-amz-server-side-encryption', 'x-amz-version-id'))


    def putObjectCopy(self, bucketname, objectname, sourcebucketname, sourceobjectname,
                      x_amz_server_side_encryption=None,
                      x_amz_storage_class=None,
                      x_amz_website_redirect_location=None,
                      # TODO: not handled yet
                      x_amz_metadata_directive=None,
                      x_amz_copy_source_if_match=None, x_amz_copy_source_if_none_match=None,
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

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"

        _status, _reason, _headers, data = self.request(method="PUT", path=path,
                                                        headers=headers,
                                                        endpoint=endpoint)
        return data['CopyObjectResult']

    def putObjectAcl(self, bucketname, objectname,
                     x_amz_acl=None,
                     x_amz_grant_read=None, x_amz_grant_write=None, x_amz_grant_read_acp=None,
                     x_amz_grant_write_acp=None, x_amz_grant_full_control=None):
        # TODO: implement
        pass

    def getObjectAcl(self, bucketname, objectname):
        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"
        _status, _reason, _headers, data = self.request(method="GET", path=path,
                                                        endpoint=endpoint,
                                                        query={'acl': None})
        return data['AccessControlPolicy']


    def uploadOjectPart(self, bucketname, objectname, partnumber, uploadid, value, objlen=None, md5digest=None):
        headers = {}
        if objlen is not None:
            headers['Content-Length'] = str(objlen)

        if md5digest is not None:
            headers['Content-MD5'] = base64.b64encode(md5digest).strip().decode()

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"

        _status, _reason, headers, _data = self.request(method="PUT", path=path, body=value,
                                                        headers=headers,
                                                        endpoint=endpoint,
                                                        query={"partNumber": partnumber, "uploadId": uploadid})
        if md5digest is not None:
            if headers['ETag'][1:-1] != binascii.hexlify(md5digest).decode():
                raise S3Exception('put-object-invalid-md5')
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
                                x_amz_grant_read=None, x_amz_grant_write=None, x_amz_grant_read_acp=None, x_amz_grant_write_acp=None,
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

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"

        _status, _reason, _headers, data = self.request(method="POST", path=path,
                                                        headers=headers,
                                                        endpoint=endpoint,
                                                        query={"uploads": None})
        data = data['InitiateMultipartUploadResult']
        if data['Bucket'] != bucketname or data['Key'] != objectname:
            raise S3Exception('invalid-bucket-key', data, bucketname, objectname)
        return data

    def completeMultipartUpload(self, bucketname, objectname, uploadId, parts):
        data = ["<CompleteMultipartUpload>"]
        for partnumber in parts:
            data.append("<Part><PartNumber>%s</PartNumber><ETag>%s</ETag></Part>" % (partnumber, parts[partnumber]))
        data.append("</CompleteMultipartUpload>")

        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"

        _status, _reason, _headers, data = self.request(method="POST", path=path,
                                                        body="".join(data),
                                                        endpoint=endpoint,
                                                        query={"uploadId": uploadId})
        data = data['CompleteMultipartUploadResult']
        if data['Bucket'] != bucketname or data['Key'] != objectname:
            raise S3Exception('invalid-bucket-key', data, bucketname, objectname)
        return data

    def abortMultipartUpload(self, bucketname, objectname, uploadId):
        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"
        _status, _reason, _headers, _data = self.request(method="DELETE", path=path,
                                                         endpoint=endpoint,
                                                         statusexpected=[204],
                                                         query={"uploadId": uploadId})

    def listParts(self, bucketname, objectname, uploadId, max_parts=None, part_number_marker=None):
        query = {"uploadId": uploadId}
        if max_parts is not None:
            query['max-parts'] = max_parts
        if part_number_marker is not None:
            query['part-number-marker'] = part_number_marker
        if bucketname != bucketname.lower():
            path = "/" + bucketname + "/" + objectname
            endpoint = None
        else:
            path = '/' + objectname
            endpoint = bucketname + ".s3.amazonaws.com"
        _status, _reason, _headers, data = self.request(method="GET", path=path,
                                                        endpoint=endpoint,
                                                        query=query)
        data = data['ListPartsResult']
        if data['Bucket'] != bucketname or data['Key'] != objectname:
            raise S3Exception('invalid-bucket-key', data, bucketname, objectname)
        return data
