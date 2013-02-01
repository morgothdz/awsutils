# awsutils/exceptions/sqs.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.exceptions.aws import AWSException

class SQSException(AWSException):
    pass

class AccessDenied(SQSException):
    #Access to the resource is denied
    HTTP_STATUS = 403

class AuthFailure(SQSException):
    #A value used for authentication could not be validated, such as Signature
    HTTP_STATUS = 403

class AWS_SimpleQueueService_NonExistentQueue(SQSException):
    #The specified queue does not exist for this wsdl version
    HTTP_STATUS = 400