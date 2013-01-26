# awsutils/utils/exceptions.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

def generateExceptionDictionary(modulename, exceptionprefix = 'AWSS3Exception_'):
    import inspect, sys
    result = {}
    l = len(exceptionprefix)
    for name, obj in inspect.getmembers(sys.modules[modulename]):
        if inspect.isclass(obj) and name.startswith(exceptionprefix):
            result[name[l:]] = obj
    return result