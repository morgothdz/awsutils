# awsutils/s3/bucket.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.s3.object import S3Object
from awsutils.utils.wrappers import SimpleWindowedFileObjectReadWrapper
from awsutils.client import AWSPartialReception

class S3Bucket():
    def __init__(self, name, s3client,
                 owner=None,
                 creationdate=None):
        """
        @param name: the name of the bucket
        @type name: str
        @param s3client: the s3 client class responsible with the s3 communication
        @param s3client: S3Client
        """
        self.name = name
        self.s3client = s3client
        self.owner = owner
        self.creationdate = creationdate

    def getObject(self, name):
        data = self.s3client.getBucket(bucketname=self.name, prefix=name, maxkeys=1)
        if 'Contents' in data[0]['Key'] == name:
            return S3Object(name=name, bucketname=self.name, s3client=self.s3client)
        else:
            return None

    def getObjects(self, delimiter=None, marker=None, prefix=None, _maxkeys=250):
        while True:
            data = self.s3client.getBucket(bucketname=self.name, delimiter=delimiter, marker=marker, prefix=prefix,
                maxkeys=_maxkeys)
            if 'Contents' not in data:
                break
            keys = data['Contents']
            if isinstance(keys, dict): keys = [keys]
            for key in keys:
                yield S3Object(name=key['Key'], bucketname=self.name, s3client=self.s3client)
                marker = key['Key']
            if data['IsTruncated'] != 'true':
                break

    def hasObject(self, objectname):
        """
        Checks for an object existence by a head request
        @param objectname: the name of the required object
        @type objectname: str
        """
        return self.s3client.headObject(self.name, objectname)['status'] == 200

    def getMultipartUploads(self, prefix=None, delimiter=None, _maxuploads=500):
        """
        Generator: list all or those selected by prefix and delimiter pending multipart uploads
        @param prefix: see s3 amazon doc for listMultipartUploads prefix parameter
        @type prefix: str
        @param delimiter: see s3 amazon doc for listMultipartUploads delimiter parameter
        @type delimiter: str
        @param _maxuploads: this parameter stets the size of items requested in a request,
                            has to be a good balance between memory consumption and less
                            HTTP request.
        @type _maxuploads: integer
        """
        key_marker = None
        upload_id_marker = None
        while True:
            data = self.s3client.listMultipartUploads(bucketname=self.name,
                prefix=prefix,
                delimiter=delimiter,
                key_marker=key_marker,
                upload_id_marker=upload_id_marker,
                max_uploads=_maxuploads)
            if 'Upload' not in data:
                break
            uploads = data['Upload']
            if isinstance(uploads, dict): uploads = [uploads]
            for upload in uploads:
                yield upload
            if data['IsTruncated'] != 'true':
                break
            upload_id_marker = data['NextUploadIdMarker']

    def abortPendingMultipartUploads(self, prefix=None, delimiter=None):
        """
        abort all pending multipart uploads or only those selected by prefix and delimiter
        @param prefix: see s3 amazon doc for listMultipartUploads prefix parameter
        @type prefix: str
        @param delimiter: see s3 amazon doc for listMultipartUploads delimiter parameter
        @type delimiter: str
        """
        for upload in self.getMultipartUploads(prefix=prefix, delimiter=delimiter):
            self.s3client.abortMultipartUpload(bucketname=self.name, objectname=upload['Key'],
                uploadId=upload['UploadId'])

    def uploadArbitrarySizedObject(self, objectname, outputobject, start=0, end=None, chunklen=5242880,
                                   hashcheck=False):
        """
        upload a file like object of arbitrary length, uses multipart upload for files bigger than 2 x chunklen
        @param objectname: the name of the object in the s3 bucket
        @type objectname: str
        @param outputobject: file like object opened inr "rb" mode (must provide the read, seek and tell methods)
        @param outputobject: object
        @param start: the start ofsset from where we upload the data
        @param start: int
        @param end: the last byte till we want to upload the data, if not provided means the end of the stream
        @param end: int
        @param chunklen: the chunk length for calculating the multipart upload file sizes
        @param chunklen: int
        @param hashcheck: the functions checks for the upload data integrity using md5 hashes (partially implemented)
        @param hashcheck: bool
        """
        if chunklen < 5242880:
            raise Exception("Your proposed upload is smaller than the minimum allowed (by amazon) size")

        wrappedobj = SimpleWindowedFileObjectReadWrapper(obj=outputobject, start=start, end=end, hashcheck=hashcheck)

        #if file size bigger than given threshold (the minimum allowed chunklength * 2) start process file as multipart
        if wrappedobj.size < chunklen * 2:
            result = self.s3client.putObject(bucketname=self.name, objectname=objectname, value=wrappedobj,
                objlen=wrappedobj.size)
            #TODO: hashcheck
            return result

        end = wrappedobj.end

        upload = self.s3client.initiateMultipartUpload(bucketname=self.name, objectname=objectname)
        try:
            parts = {}
            for partnumber in range(1, 10000):
                if chunklen * 2 > (end - start):
                    tosendlen = (end - start)
                else:
                    tosendlen = chunklen

                wrappedobj.resetBoundaries(start=start, end=start + tosendlen)
                if wrappedobj.size <= 0:
                    break

                result = self.s3client.uploadOjectPart(bucketname=self.name, objectname=objectname,
                    partnumber=partnumber, uploadid=upload['UploadId'],
                    value=wrappedobj,
                    objlen=wrappedobj.size)

                if hashcheck and result['ETag'][1:-1] != wrappedobj.getMd5HexDigest():
                    raise Exception('upload hash mismatch')

                print('uploadOjectPart', result, wrappedobj.getMd5HexDigest())
                parts[partnumber] = result['ETag']

                start += tosendlen

            result = self.s3client.completeMultipartUpload(bucketname=self.name, objectname=objectname,
                uploadId=upload['UploadId'], parts=parts)
            #TODO: global hashcheck
            print("completeMultipartUpload", result)
            print(wrappedobj.getGlobalMd5HexDigest())
        except Exception as e:
            print('uploadLargeObject', e)
            self.s3client.abortMultipartUpload(bucketname=self.name, objectname=objectname, uploadId=upload['UploadId'])


    def downloadArbitrarySizedObject(self, objectname, inputobject=None, hashcheck=False):
        #THIS IS WORK IN PROGRESS
        """
        download an object from s3 to a file like object, it's capable to resume interrupted uploads
        @param objectname: the name of the object in the s3 bucket
        @type name: str
        @param output: file like object opened in "w+b" mode (must provide the write, seek and tell methods), if not
                       provided the data is returned in a tempfile.TemporaryFile
        @param output: object
        @param hashcheck: the functions checks for the download data integrity using md5 hashes
        @param hashcheck: bool
        """
        offset = 0
        result = None
        if inputobject is None:
            inputobjectoffset = 0
        else:
            inputobjectoffset = inputobject.tell()

        while True:
            try:
                result = self.s3client.getObject(bucketname=self.name, objectname=objectname, inputobject=inputobject, byterange =(offset,))
                if inputobject is None:
                    inputobject = result['data']
                break

            except AWSPartialReception as e:
                print("partial crash", e)
                if inputobject is None:
                    inputobject = e.data
                offset += e.sizeinfo['downloaded'] + 1
                size = e.sizeinfo['size']
                if offset >= size:
                    break
        """
        print("result", result)


        import io
        inputobject.seek(0, io.SEEK_SET)
        with open("result", "wb") as result:
            while True:
                o = inputobject.read(1024)
                if o:
                    result.write(o)
                else:
                    break
        """







    def __repr__(self):
        return repr(self.__dict__)
