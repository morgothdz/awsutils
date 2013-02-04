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
    #AWS was not able to authenticate the request: access credentials are missing.
    HTTP_STATUS = 403

class ConditionalCheckFailed(SDBException):
    #Conditional check failed.
    # Attribute (" + name + ") value exists. OR
    # Attribute ("+ name +") value is ("+ value +") but was expected ("+ expValue +")
    HTTP_STATUS = 409
    
class ExistsAndExpectedValue(SDBException):
    #Expected.Exists=false and Expected.Value cannot be specified together
    HTTP_STATUS = 400
    
class FeatureDeprecated(SDBException):
    #The replace flag must be specified per attribute, not per item.
    HTTP_STATUS = 400
    
class IncompleteExpectedExpression(SDBException):
    #If Expected.Exists=true or unspecified, then Expected.Value has to be specified
    HTTP_STATUS = 400
    
class InternalError(SDBException):
    #Request could not be executed due to an internal service error.
    HTTP_STATUS = 500
    
class InvalidAction(SDBException):
    #The action " + actionName + " is not valid for this web service.
    HTTP_STATUS = 400
    
class InvalidHTTPAuthHeader(SDBException):
    #The HTTP authorization header is bad, use " + correctFormat".
    HTTP_STATUS = 400
    
class InvalidHttpRequest(SDBException):
    #The HTTP request is invalid. Reason: " + reason".
    HTTP_STATUS = 400
    
class InvalidLiteral(SDBException):
    #Illegal literal in the filter expression.
    HTTP_STATUS = 400
    
class InvalidNextToken(SDBException):
    #The specified next token is not valid.
    HTTP_STATUS = 400
    
class InvalidNumberPredicates(SDBException):
    #Too many predicates in the query expression.
    HTTP_STATUS = 400
    
class InvalidNumberValueTests(SDBException):
    #Too many value tests per predicate in the query expression.
    HTTP_STATUS = 400
    
class InvalidParameterCombination(SDBException):
    #The parameter " + param1 + " cannot be used with the parameter " + param2".
    HTTP_STATUS = 400
    
class InvalidParameterValue(SDBException):
    #Value (" + value + ") for parameter MaxNumberOfDomains is invalid.
    # MaxNumberOfDomains must be between 1 and 100. See "Amazon SimpleDB Error Codes"
    HTTP_STATUS = 400
    
class InvalidQueryExpression(SDBException):
    #The specified query expression syntax is not valid.
    HTTP_STATUS = 400
    
class InvalidResponseGroups(SDBException):
    #The following response groups are invalid: " + invalidRGStr.
    HTTP_STATUS = 400
    
class InvalidService(SDBException):
    #The Web Service " + serviceName + " does not exist.
    HTTP_STATUS = 400
    
class InvalidSortExpression(SDBException):
    #The sort attribute must be present in at least one of the predicates,
    # and the predicate cannot contain the is null operator.    
    HTTP_STATUS = 400
    
class InvalidURI(SDBException):
    #The URI " + requestURI + " is not valid.
    HTTP_STATUS = 400
    
class InvalidWSAddressingProperty(SDBException):
    #WS-Addressing parameter " + paramName + " has a wrong value: " + paramValue".
    HTTP_STATUS = 400
    
class InvalidWSDLVersion(SDBException):
    #Parameter (" + parameterName +") is only supported in WSDL version 2009-04-15 or beyond.
    # Please upgrade to new version
    HTTP_STATUS = 400
    
class MissingAction(SDBException):
    #No action was supplied with this request.
    HTTP_STATUS = 400
    
class MissingParameter(SDBException):
    #The request must contain the specified missing parameter.
    HTTP_STATUS = 400
    
class MissingWSAddressingProperty(SDBException):
    #WS-Addressing is missing a required parameter (" + paramName + ")".
    HTTP_STATUS = 400
    
class MultipleExistsConditions(SDBException):
    #Only one Exists condition can be specified
    HTTP_STATUS = 400
    
class MultipleExpectedNames(SDBException):
    #Only one Expected.Name can be specified
    HTTP_STATUS = 400
    
class MultipleExpectedValues(SDBException):
    #Only one Expected.Value can be specified
    HTTP_STATUS = 400
    
class MultiValuedAttribute(SDBException):
    #Attribute (" + name + ") is multi-valued.
    # Conditional check can only be performed on a single-valued attribute
    HTTP_STATUS = 409
    
class NoSuchDomain(SDBException):
    #The specified domain does not exist.
    HTTP_STATUS = 400
    
class NoSuchVersion(SDBException):
    #The requested version (" + version + ") of service " + service + " does not exist.
    HTTP_STATUS = 400
    
class NotYetImplemented(SDBException):
    #Feature " + feature + " is not yet available".
    HTTP_STATUS = 401
    
class NumberDomainsExceeded(SDBException):
    #The domain limit was exceeded.
    HTTP_STATUS = 409
    
class NumberDomainAttributesExceeded(SDBException):
    #Too many attributes in this domain.
    HTTP_STATUS = 409
    
class NumberDomainBytesExceeded(SDBException):
    #Too many bytes in this domain.
    HTTP_STATUS = 409
    
class NumberItemAttributesExceeded(SDBException):
    #Too many attributes in this item.
    HTTP_STATUS = 409
    
class NumberSubmittedAttributesExceeded(SDBException):
    #Too many attributes in a single call.
    HTTP_STATUS = 409
    
class NumberSubmittedAttributesExceeded(SDBException):
    #Too many attributes for item itemName in a single call.
    # Up to 256 attributes per call allowed.
    HTTP_STATUS = 409
    
class NumberSubmittedItemsExceeded(SDBException):
    #Too many items in a single call. Up to 25 items per call allowed.
    HTTP_STATUS = 409
    
class RequestExpired(SDBException):
    #Request has expired. " + paramType + " date is " + date".
    HTTP_STATUS = 400
    
class QueryTimeout(SDBException):
    #A timeout occurred when attempting to query domain <domain name>
    # with query expression <query expression>. BoxUsage [<box usage value>]".    
    HTTP_STATUS = 408
    
class ServiceUnavailable(SDBException):
    #Service Amazon SimpleDB is busy handling other requests, likely due to too many simultaneous requests.
    # Consider reducing the frequency of your requests, and try again. See About Response Code 503.
    HTTP_STATUS = 503
    
class TooManyRequestedAttributes(SDBException):
    #Too many attributes requested.
    HTTP_STATUS = 400
    
class UnsupportedHttpVerb(SDBException):
    #The requested HTTP verb is not supported: " + verb".
    HTTP_STATUS = 400
    
class UnsupportedNextToken(SDBException):
    #The specified next token is no longer supported. Please resubmit your query.
    HTTP_STATUS = 400
    
class URITooLong(SDBException):
    #The URI exceeded the maximum limit of "+ maxLength".
    HTTP_STATUS = 400
    