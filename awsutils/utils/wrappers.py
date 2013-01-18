# awsutils/utils/wrappers.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import os, io, hashlib

class SimpleMd5FileObjectWriteWrapper:
    def __init__(self, obj):
        self.obj = obj
        self.reset()
    def write(self, data):
        self.md5.update(data)
        self.obj.write(data)
    def tell(self):
        self.obj.tell()
    def seek(self, offset, whence=io.SEEK_SET):
        #seeking will ruin the hash ...
        self.md5invalid = True
        self.obj.seek(offset, whence)
    def close(self):
        self.obj.close()
    def reset(self):
        self.md5invalid = False
        self.md5 = hashlib.md5()
    def getMd5Digest(self):
        return self.md5.digest()
    def getMd5HexDigest(self):
        return self.md5.hexdigest()


class SimpleWindowedFileObjectReadWrapper:

    def __init__(self, obj, start=0, end=None, mode="w+b", hashcheck = False):
        self.mode = mode
        self.obj = obj
        self.start = start

        self.hashcheck = hashcheck
        if hashcheck:
            self.md5 = hashlib.md5()

        if end is None:
            if hasattr(obj, 'end'):
                end = obj.end
            elif hasattr(obj, 'size'):
                end = obj.size
        if end is None:
            try:
                end = os.fstat(obj.fileno()).st_size
            except (AttributeError, OSError):
                pass
        if end is None:
            try:
                current_offset = obj.tell()
                obj.seek(0, io.SEEK_END)
                end = obj.tell()
                obj.seek(current_offset, io.SEEK_SET)
            except:
                pass
        if end is None:
            raise Exception("end can't be None")

        self.end = end
        self.size = end - start
        self.readed = 0

    def resetBoundaries(self, start=None, end=None):
        if start is not None and start != self.start:
            self.start = start
            self.obj.seek(self.start, io.SEEK_SET)
        if self.hashcheck:
            self.md5 = hashlib.md5()

        if end is not None:
            self.start = end

        self.size = end - start
        self.readed = 0

    def reset(self):
        if self.hashcheck:
            self.md5 = hashlib.md5()
        self.readed = 0
        self.obj.seek(self.start, io.SEEK_SET)

    def read(self, size=99999999999999999999):
        size = min(size, self.size - self.readed)
        if size > 0:
            data = self.obj.read(size)
            l = len(data)
            if l > 0:
                if self.hashcheck:
                    self.md5.update(data)
                self.readed += len(data)
            return data
        return b""

    def getMd5Digest(self):
        if self.hashcheck:
            return self.md5.digest()

    def getMd5HexDigest(self):
        if self.hashcheck:
            return self.md5.hexdigest()
