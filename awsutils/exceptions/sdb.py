# awsutils/exceptions/sdb.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.exceptions.aws import AWSException

class SDBException(AWSException):
    pass

class AccessFailure(SDBException):
    #Access to the resource  is denied
    HTTP_STATUS = 403

class AttributeDoesNotExist(SDBException):
    #Attribute does not exist
    HTTP_STATUS = 404

class AuthFailure(SDBException):
    #AWS was not able to validate the provided access credentials.
    HTTP_STATUS = 403

class AuthMissingFailure(SDBException):
    #AWS was not able to validate the provided access credentials.
    HTTP_STATUS = 403

#
class NoSuchDomain(SDBException):
    #The specified domain does not exist.
    HTTP_STATUS = 400