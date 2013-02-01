# awsutils/s3/bucket.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import os, hashlib, logging
from awsutils.s3.object import S3Object
from awsutils.utils.wrappers import SimpleWindowedFileObjectReadWrapper, SimpleMd5FileObjectWriteWrapper
from awsutils.exceptions.aws import AWSPartialReception, IntegrityCheckException, UserInputException

class S3Bucket():
    def __init__(self, name, s3client,
                 owner=None,
                 creationdate=None):
        """
        @param name: the name of the bucket
        @type name: str
        @param s3client: the s3 client class responsible with the s3 communication
        @type s3client: S3Client
        """
        self.name = name
        self.s3client = s3client
        self.owner = owner
        self.creationdate = creationdate
        self.logger = logging.getLogger("%s.%s" % (type(self).__module__, type(self).__name__))
        self.logger.addHandler(logging.NullHandler())

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
        @return:
        @rtype: boolean
        """
        return self.s3client.headObject(self.name, objectname)['status'] == 200

    def delObject(self, objectname):
        return self.s3client.deleteObject(self.name, objectname)

    def renameObject(self, objectname, newObjectname):
        self.s3client.putObjectCopy(self.name, newObjectname, self.name, objectname)
        self.s3client.deleteObject(self.name, objectname)

    def getMultipartUploads(self, prefix=None, delimiter=None, _maxuploads=500):
        """
        Generator: list all or those selected by prefix and delimiter pending multipart uploads
        @param prefix: see s3 amazon doc for listMultipartUploads prefix parameter
        @type prefix: str
        @param delimiter: see s3 amazon doc for listMultipartUploads delimiter parameter
        @type delimiter: str
        @param _maxuploads: this parameter stets the size of items requested per request,
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
                             can be also a filename
        @type outputobject: object
        @param start: the start ofsset from where we upload the data
        @type start: int
        @param end: the end offset to upload the data, if not provided means the end of the stream
        @type end: int
        @param chunklen: the chunk length for calculating the multipart upload file sizes
        @type chunklen: int
        @param hashcheck: the functions checks for the upload data integrity using md5 hashes (partially implemented)
        @type hashcheck: bool
        @return: {'ETag': '"hash"', 'Bucket': bucketname, 'Location': 'http://bucketname.s3.amazonaws.com/objectname',
                 'Key': objectname}
        @rtype: dict
        """
        if chunklen < 5242880:
            raise UserInputException("Your proposed upload is smaller than the minimum allowed (by amazon) size")

        closeoutputobject = False
        if isinstance(outputobject, str):
            if not os.path.isfile(outputobject):
                raise UserInputException("param outputobject is a string but points to nonexistent file")
            outputobject = open(outputobject, "rb")
            closeoutputobject = True

        wrappedobj = SimpleWindowedFileObjectReadWrapper(obj=outputobject, start=start, end=end, hashcheck=hashcheck)

        if wrappedobj.size / chunklen > 10000:
            raise UserInputException("More than 10000 objects needed to complete this upload")

        try:

            if wrappedobj.size < chunklen * 2:
                result = self.s3client.putObject(bucketname=self.name, objectname=objectname, value=wrappedobj,
                    objlen=wrappedobj.size)
                if hashcheck:
                    if result['ETag'][1:-1] != wrappedobj.getMd5HexDigest():
                        raise IntegrityCheckException('uploadArbitrarySizedObject.putObject unexpected ETag received',
                                                      result['ETag'][1:-1], wrappedobj.getMd5HexDigest())
                return result

            #multipart upload =>
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

                    size = wrappedobj.size

                    if size <= 0:
                        break

                    result = self.s3client.uploadOjectPart(bucketname=self.name,
                                                           objectname=objectname,
                                                           partnumber=partnumber, uploadid=upload['UploadId'],
                                                           value=wrappedobj,
                                                           objlen=size)

                    senthash = wrappedobj.getMd5HexDigest()
                    if hashcheck and result['ETag'][1:-1] != senthash:
                        raise IntegrityCheckException('uploadArbitrarySizedObject.uploadOjectPart unexpected '
                                                      'ETag received',
                                                      result['ETag'][1:-1], senthash)

                    parts[partnumber] = result['ETag']
                    start += tosendlen

                else:
                    raise Exception("we shouldn't reach this point")

                result = self.s3client.completeMultipartUpload(bucketname=self.name, objectname=objectname,
                                                               uploadId=upload['UploadId'], parts=parts)
                return result

            except Exception as e:
                self.s3client.abortMultipartUpload(bucketname=self.name, objectname=objectname, uploadId=upload['UploadId'])
                raise

        finally:
            if closeoutputobject:
                outputobject.close()


    def downloadArbitrarySizedObject(self, objectname, inputobject=None, hashcheck=False, byterange=None):
        """
        download an object from s3 to a file like object, it's capable to resume interrupted uploads
        @param objectname: the name of the object in the s3 bucket
        @type objectname: str
        @param inputobject: - file like object opened in "w+b" mode (must provide the write, seek and tell methods)
                            - file name where we want to receive the data "w+b" mode
                            - None so the data is returned in a tempfile.TemporaryFile "w+b" mode
        @type inputobject: object
        @param hashcheck: enable checking of the downloaded data integrity (won't work if the object is multipart upload)
        @type hashcheck: bool
        @param byterange: a list with the desired range to be downloaded. ex: (10,) or (100,1000)
        @type byterange: list
        @return: {'input': if the input object was autogenerated by the downloadArbitrarySizedObject
                  'range':{'start':download start offset, 'end':end offset, 'size':the object size on s3}}
        @rtype: dict
        """

        closeinputobject = False
        if isinstance(inputobject, str):
            inputobject = open(inputobject, "w+b")
            closeinputobject = True

        if hashcheck and byterange is not None:
            raise UserInputException("downloading ranges can't be hash checked")

        if hashcheck and inputobject is not None:
            _inputobject = SimpleMd5FileObjectWriteWrapper(inputobject)
        else:
            _inputobject = inputobject

        if byterange is not None:
            byterange = list(byterange)
            range = {'start':byterange[0], 'end':None, 'size':None}
        else:
            range = {'start':0, 'end':None, 'size':None}

        try:

            while True:
                try:
                    self.logger.debug("downloading byterange %s", byterange)
                    if byterange is not None:
                        if len(byterange) > 1:
                            if byterange[0] >= byterange[1]:
                                break
                        else:
                            if range['size'] is not None and byterange[0] >= range['size']:
                                break

                    result = self.s3client.getObject(bucketname=self.name, objectname=objectname,
                                                     inputobject=_inputobject, byterange=byterange,
                                                     _inputIOWrapper=SimpleMd5FileObjectWriteWrapper)
                    range['end'] = result['range']['end']
                    range['size'] = result['range']['size']
                    if _inputobject is None:
                        _inputobject = result['data']
                    break

                except AWSPartialReception as e:
                    #request crashed but we have partial data available, prepare for a resume
                    if _inputobject is None:
                        _inputobject = e.data
                    if byterange is None:
                        byterange = [0]
                    byterange[0] += e.sizeinfo['downloaded']
                    range['size'] = e.sizeinfo['size']
                    range['end'] = byterange[0]

                    if len(byterange) > 1 and byterange[1] >= e.sizeinfo['size']:
                        byterange = [byterange[0],]

        finally:
            if closeinputobject:
                _inputobject.close()

        if byterange is not None and len(byterange) > 1:
            if byterange[1] != range['end']:
                raise IntegrityCheckException('downloadArbitrarySizedObject: unexpected object size',
                                              byterange[1], range['end'])
        else:
            if range['size'] != range['end'] + 1:
                raise IntegrityCheckException('downloadArbitrarySizedObject: unexpected object size',
                                              byterange[1], range['end'] + 1)

        etag = result['ETag'][1:-1]
        if hashcheck:
            if '-' in etag:
                self.logger.debug("multipart upload, can't check for integrity by ETag")
            else:
                if etag != _inputobject.getMd5HexDigest():
                    raise IntegrityCheckException('downloadArbitrarySizedObject returned unexpected ETag',
                                                  etag, _inputobject.getMd5HexDigest())
            _inputobject = _inputobject.obj

        if closeinputobject:
            _inputobject = None

        return {'input':_inputobject, 'range':range}

    def __repr__(self):
        return repr(self.__dict__)
