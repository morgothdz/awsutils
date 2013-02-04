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
    HTTP_STATUS = 401
    
class AWS_SimpleQueueService_InternalError(SQSException):
    #There is an internal problem with SQS, which you cannot resolve. Retry the request.
    # If the problem persists, contact us through the Amazon SQS Discussion Forums.
    HTTP_STATUS = 500

class AWS_SimpleQueueService_NonExistentQueue(SQSException):
    #The specified queue does not exist for this wsdl version
    HTTP_STATUS = 400

class ConflictingQueryParameter(SQSException):
    #The query parameter <parameter> is invalid. Its structure conflicts with that of another parameter.
    HTTP_STATUS = 400
    
class InternalError(SQSException):
    #There is an internal problem with SQS, which you cannot resolve. Retry the request.
    # If the problem persists, contact us through the Amazon SQS Discussion Forums.
    HTTP_STATUS = 500
    
class InvalidAccessKeyId(SQSException):
    #AWS was not able to validate the provided access credentials.
    HTTP_STATUS = 401
    
class InvalidAction(SQSException):
    #The action specified was invalid.
    HTTP_STATUS = 400
    
class InvalidAddress(SQSException):
    #The address <address> is not valid for this web service.
    HTTP_STATUS = 404
    
class InvalidHttpRequest(SQSException):
    #Invalid HTTP request. Reason: <reason>.
    HTTP_STATUS = 400
    
class InvalidParameterCombination(SQSException):
    #Two parameters were specified that cannot be used together, such as Timestamp and Expires.
    HTTP_STATUS = 400
    
class InvalidParameterValue(SQSException):
    #One or more parameters cannot be validated.
    HTTP_STATUS = 400
    
class InvalidQueryParameter(SQSException):
    #The query parameter <parameter> is invalid.
    # Please see service documentation for correct syntax.
    HTTP_STATUS = 400
    
class InvalidRequest(SQSException):
    #The service cannot handle the request. Request is invalid.
    HTTP_STATUS = 400
    
class InvalidSecurity(SQSException):
    #The provided security credentials are not valid. Reason: <reason>.
    HTTP_STATUS = 403
    
class InvalidSecurityToken(SQSException):
    #The security token used in the request is invalid. Reason: <reason>.
    HTTP_STATUS = 400
    
class MalformedVersion(SQSException):
    #Version not well formed: <version>. Must be in YYYY-MM-DD format.
    HTTP_STATUS = 400
    
class MissingClientTokenId(SQSException):
    #Request must contain AWSAccessKeyId or X.509 certificate.
    HTTP_STATUS = 403
    
class MissingCredentials(SQSException):
    #AWS was not able to authenticate the request: access credentials are missing.
    HTTP_STATUS = 401
    
class MissingParameter(SQSException):
    #A required parameter is missing.
    HTTP_STATUS = 400
    
class NoSuchVersion(SQSException):
    #An incorrect version was specified in the request.
    HTTP_STATUS = 400
    
class NotAuthorizedToUseVersion(SQSException):
    #Users who sign up to use Amazon SQS after February 1, 2008, must use API version 2008-01-01 and above;
    # not previous API versions.
    HTTP_STATUS = 401
    
class RequestExpired(SQSException):
    #The timestamp used with the signature has expired.
    HTTP_STATUS = 400
    
class RequestThrottled(SQSException):
    #Request is throttled.
    HTTP_STATUS = 403
    
class ServiceUnavailable(SQSException):
    #A required server needed by SQS is unavailable.
    # This error is often temporary; resend the request after a short wait.
    HTTP_STATUS = 503
    
class X509ParseError(SQSException):
    #Could not parse X.509 certificate.
    HTTP_STATUS = 400