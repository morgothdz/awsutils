# awsutils/utils/wrappers.py
# Copyright 2013 Attila Gerendi
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

__author__ = 'Dark'

import os, io, hashlib

class SimpleWindowedFileObjectReadWrapper:

    def __init__(self, obj, start=0, end=None, mode="w+b", hashcheck = False):
        self.mode = mode
        self.obj = obj
        self.start = start

        self.hashcheck = hashcheck
        if hashcheck:
            self.md5 = hashlib.md5()
            """
            TODO: globalmd5 not working
            """
            self.globalmd5 = hashlib.md5()
            self.globalmd5clone = self.globalmd5.copy()

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
            obj.seek(self.start, io.SEEK_SET)
        if self.hashcheck:
            self.md5 = hashlib.md5()

        if end is not None:
            self.start = end

        self.size = end - start
        self.readed = 0

    def reset(self):
        if self.hashcheck:
            self.md5 = hashlib.md5()
            self.globalmd5 = self.globalmd5clone
            self.globalmd5clone = self.globalmd5.copy()
        self.readed = 0
        obj.seek(self.start, io.SEEK_SET)

    def read(self, size=99999999999999999999):
        size = min(size, self.size - self.readed)
        if size > 0:
            data = self.obj.read(size)
            l = len(data)
            if l > 0:
                if self.hashcheck:
                    self.md5.update(data)
                    self.globalmd5.update(data)
                self.readed += len(data)
            return data
        return b""

    def getMd5Digest(self):
        if self.hashcheck:
            return self.md5.digest()

    def getMd5HexDigest(self):
        if self.hashcheck:
            return self.md5.hexdigest()

    def getGlobalMd5HexDigest(self):
        if self.hashcheck:
            return self.globalmd5.hexdigest()