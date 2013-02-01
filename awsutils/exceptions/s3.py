# awsutils/exceptions/s3.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from awsutils.exceptions.aws import AWSException

class S3Exception(AWSException):
    def __str__(self):
        return "%s[%s]: %s"%(self.awsresponse['Error']['Code'], self.httpstatus, self.awsresponse['Error']['Message'])

class AccessDenied(S3Exception):
    #Access Denied
    HTTP_STATUS = 403

class AccountProblem(S3Exception):
    #There is a problem with your AWS account that prevents the operation from completing successfully
    HTTP_STATUS = 403

class AmbiguousGrantByEmailAddress(S3Exception):
    #The e-mail address you provided is associated with more than one account
    HTTP_STATUS = 400

class BadDigest(S3Exception):
    #The Content-MD5 you specified did not match what we received
    HTTP_STATUS = 400

class BucketAlreadyExists(S3Exception):
    #The requested bucket name is not available. The bucket namespace is shared by all users of the system
    HTTP_STATUS = 409

class BucketAlreadyOwnedByYou(S3Exception):
    #Your previous request to create the named bucket succeeded and you already own it
    HTTP_STATUS = 409

class BucketNotEmpty(S3Exception):
    #The bucket you tried to delete is not empty
    HTTP_STATUS = 409

class CredentialsNotSupported(S3Exception):
    #This request does not support credentials
    HTTP_STATUS = 400

class CredentialsNotSupported(S3Exception):
    #This request does not support credentials
    HTTP_STATUS = 400

class CrossLocationLoggingProhibited(S3Exception):
    #Cross location logging not allowed. Buckets in one geographic location cannot log information to a
    #bucket in another location
    HTTP_STATUS = 403

class EntityTooSmall(S3Exception):
    #Your proposed upload is smaller than the minimum allowed object size
    HTTP_STATUS = 400

class EntityTooLarge(S3Exception):
    #Your proposed upload exceeds the maximum allowed object size
    HTTP_STATUS = 400

class ExpiredToken(S3Exception):
    #The provided token has expired
    HTTP_STATUS = 400

class IllegalVersioningConfigurationException(S3Exception):
    #Indicates that the Versioning configuration specified in the request is invalid
    HTTP_STATUS = 400

class IncompleteBody(S3Exception):
    #You did not provide the number of bytes specified by the Content-Length HTTP header
    HTTP_STATUS = 400

class IncorrectNumberOfFilesInPostRequest(S3Exception):
    #POST requires exactly one file upload per request.
    HTTP_STATUS = 400

class InlineDataTooLarge(S3Exception):
    #Inline data exceeds the maximum allowed size.
    HTTP_STATUS = 400

class InternalError(S3Exception):
    #We encountered an internal error. Please try again.
    HTTP_STATUS = 500

class InvalidAccessKeyId(S3Exception):
    #The AWS Access Key Id you provided does not exist in our records.
    HTTP_STATUS = 403

class InvalidAddressingHeader(S3Exception):
    #You must specify the Anonymous role.
    HTTP_STATUS = None

class InvalidArgument(S3Exception):
    #Invalid Argument
    HTTP_STATUS = 400

class InvalidBucketName(S3Exception):
    #The specified bucket is not valid.
    HTTP_STATUS = 400

class InvalidBucketState(S3Exception):
    #The request is not valid with the current state of the bucket.
    HTTP_STATUS = 409

class InvalidDigest(S3Exception):
    #The Content-MD5 you specified was an invalid.
    HTTP_STATUS = 400

class InvalidLocationConstraint(S3Exception):
    #The specified location constraint is not valid.
    #For more information about Regions, see How to Select a Region for Your Buckets.
    HTTP_STATUS = 400

class InvalidObjectState(S3Exception):
    #The operation is not valid for the current state of the object.
    HTTP_STATUS = 403

class InvalidPart(S3Exception):
    #One or more of the specified parts could not be found.
    #The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.
    HTTP_STATUS = 400

class InvalidPartOrder(S3Exception):
    #The list of parts was not in ascending order.Parts list must specified in order by part number.
    HTTP_STATUS = 400

class InvalidPayer(S3Exception):
    #All access to this object has been disabled.
    HTTP_STATUS = 403

class InvalidPolicyDocument(S3Exception):
    #The content of the form does not meet the conditions specified in the policy document.
    HTTP_STATUS = 400

class InvalidRange(S3Exception):
    #The requested range cannot be satisfied.
    HTTP_STATUS = 416

class InvalidRequest(S3Exception):
    #SOAP requests must be made over an HTTPS connection.
    HTTP_STATUS = 400

class InvalidSecurity(S3Exception):
    #The provided security credentials are not valid.
    HTTP_STATUS = 403

class InvalidSOAPRequest(S3Exception):
    #The SOAP request body is invalid.
    HTTP_STATUS = 400

class InvalidStorageClass(S3Exception):
    #The storage class you specified is not valid.
    HTTP_STATUS = 400

class InvalidTargetBucketForLogging(S3Exception):
    #The target bucket for logging does not exist, is not owned by you,
    # or does not have the appropriate grants for the log-delivery group.
    HTTP_STATUS = 400

class InvalidToken(S3Exception):
    #The provided token is malformed or otherwise invalid.
    HTTP_STATUS = 400

class InvalidURI(S3Exception):
    #Couldn't parse the specified URI.
    HTTP_STATUS = 400

class KeyTooLong(S3Exception):
    #Your key is too long.
    HTTP_STATUS = 400

class MalformedACLError(S3Exception):
    #The XML you provided was not well-formed or did not validate against our published schema.
    HTTP_STATUS = 400

class MalformedPOSTRequest(S3Exception):
    #The body of your POST request is not well-formed multipart/form-data.
    HTTP_STATUS = 400

class MalformedXML(S3Exception):
    #This happens when the user sends a malformed xml (xml that doesn't conform to the published xsd) for the configuration.
    # The error message is, "The XML you provided was not well-formed or did not validate against our published schema."
    HTTP_STATUS = 400

class MaxMessageLengthExceeded(S3Exception):
    #Your request was too big.
    HTTP_STATUS = 400

class MaxPostPreDataLengthExceededError(S3Exception):
    #Your POST request fields preceding the upload file were too large.
    HTTP_STATUS = 400

class MetadataTooLarge(S3Exception):
    #Your metadata headers exceed the maximum allowed metadata size.
    HTTP_STATUS = 400

class MethodNotAllowed(S3Exception):
    #The specified method is not allowed against this resource.
    HTTP_STATUS = 405

class MissingAttachment(S3Exception):
    #A SOAP attachment was expected, but none were found.
    HTTP_STATUS = None

class MissingContentLength(S3Exception):
    #You must provide the Content-Length HTTP header.
    HTTP_STATUS = 411

class MissingRequestBodyError(S3Exception):
    #This happens when the user sends an empty xml document as a request.
    # The error message is, "Request body is empty."
    HTTP_STATUS = 400

class MissingSecurityElement(S3Exception):
    #The SOAP 1.1 request is missing a security element.
    HTTP_STATUS = 400

class MissingSecurityHeader(S3Exception):
    #Your request was missing a required header.
    HTTP_STATUS = 400

class NoLoggingStatusForKey(S3Exception):
    #There is no such thing as a logging status sub-resource for a key.
    HTTP_STATUS = 400

class NoSuchBucket(S3Exception):
    #The specified bucket does not exist.
    HTTP_STATUS = 404

class NoSuchKey(S3Exception):
    #The specified key does not exist.
    HTTP_STATUS = 404

class NoSuchLifecycleConfiguration(S3Exception):
    #The lifecycle configuration does not exist.
    HTTP_STATUS = 404

class NoSuchUpload(S3Exception):
    #The specified multipart upload does not exist.
    # The upload ID might be invalid, or the multipart upload might have been aborted or completed.
    HTTP_STATUS = 404

class NoSuchVersion(S3Exception):
    #Indicates that the version ID specified in the request does not match an existing version.
    HTTP_STATUS = 404

class NotImplemented(S3Exception):
    #A header you provided implies functionality that is not implemented.
    HTTP_STATUS = 501

class NotSignedUp(S3Exception):
    #Your account is not signed up for the Amazon S3 service. You must sign up before you can use Amazon S3.
    # You can sign up at the following URL: http://aws.amazon.com/s3
    HTTP_STATUS = 403

class NotSuchBucketPolicy(S3Exception):
    #The specified bucket does not have a bucket policy.
    HTTP_STATUS = 404

class OperationAborted(S3Exception):
    #A conflicting conditional operation is currently in progress against this resource. Please try again.
    HTTP_STATUS = 409

class PermanentRedirect(S3Exception):
    #The bucket you are attempting to access must be addressed using the specified endpoint.
    # Please send all future requests to this endpoint.
    HTTP_STATUS = 301

class PreconditionFailed(S3Exception):
    #At least one of the preconditions you specified did not hold.
    HTTP_STATUS = 412

class Redirect(S3Exception):
    #Temporary redirect.
    HTTP_STATUS = 307

class RestoreAlreadyInProgress(S3Exception):
    #Object restore is already in progress.
    HTTP_STATUS = 409

class RequestIsNotMultiPartContent(S3Exception):
    #Bucket POST must be of the enclosure-type multipart/form-data.
    HTTP_STATUS = 400

class RequestTimeout(S3Exception):
    #Your socket connection to the server was not read from or written to within the timeout period.
    HTTP_STATUS = 400

class RequestTimeTooSkewed(S3Exception):
    #The difference between the request time and the server's time is too large.
    HTTP_STATUS = 403

class RequestTorrentOfBucketError(S3Exception):
    #Requesting the torrent file of a bucket is not permitted.
    HTTP_STATUS = 400

class SignatureDoesNotMatch(S3Exception):
    #The request signature we calculated does not match the signature you provided.
    # Check your AWS Secret Access Key and signing method.
    # For more information, see REST Authentication and SOAP Authentication for details.
    HTTP_STATUS = 403

class ServiceUnavailable(S3Exception):
    #Please reduce your request rate.
    HTTP_STATUS = 503

class SlowDown(S3Exception):
    #Please reduce your request rate.
    HTTP_STATUS = 503

class TemporaryRedirect(S3Exception):
    #You are being redirected to the bucket while DNS updates.
    HTTP_STATUS = 307

class TokenRefreshRequired(S3Exception):
    #The provided token must be refreshed.
    HTTP_STATUS = 400

class TooManyBuckets(S3Exception):
    #You have attempted to create more buckets than allowed.
    HTTP_STATUS = 400

class UnexpectedContent(S3Exception):
    #This request does not support content.
    HTTP_STATUS = 400

class UnresolvableGrantByEmailAddress(S3Exception):
    #The e-mail address you provided does not match any account on record.
    HTTP_STATUS = 400

class UserKeyMustBeSpecified(S3Exception):
    #The bucket POST must contain the specified field name. If it is specified, please check the order of the fields.
    HTTP_STATUS = 400