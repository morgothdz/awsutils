# awsutils/exceptions/aws.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import inspect, sys

def extractExceptionsFromModule2Dicitonary(modulename, baseclass):
    result = {}
    for name, obj in inspect.getmembers(sys.modules[modulename]):
        if inspect.isclass(obj) and issubclass(obj, baseclass):
            result[name] = obj
    return result

class UserInputException(Exception):
    pass

class IntegrityCheckException(Exception):
    def __init__(self, message, received = None, expected = None):
        Exception.__init__(self, message)
        self.received = received
        self.expected = expected

class AWSStatusException(Exception):
    def __init__(self, data):
        Exception.__init__(self)
        self.data = data
    def __str__(self):
        return repr(self.__dict__)

class AWSDataException(Exception):
    def __init__(self, message, data):
        Exception.__init__(self, message)
        self.message = message
        self.data = data
    def __str__(self):
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
        return "AWSPartialReception [%d]" % (self.sizeinfo,)

class AWSException(Exception):
    def __init__(self, awsresponse, httpstatus, httpreason, httpheaders):
        Exception.__init__(self, awsresponse)
        self.awsresponse = awsresponse
        self.httpstatus = httpstatus
        self.httpreason = httpreason
        self.httpheaders = httpheaders