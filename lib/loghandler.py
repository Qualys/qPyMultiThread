# -*- coding=iso-8859-1 -*-
# Copyright 2021 Qualys Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import os
import socket
import sys
from logging import handlers

APICONCURRENCY = 'APIConcurrency'
AUTHERROR = 'AuthError'
AUTHERRORAFTER = 'AuthErrorAfter'
BADREQUESTSERROR = 'BadRequestsError'
CONNECTIONRESET = 'ConnectionReset'
CONNECTIONTIMEOUT = 'ConnectionTimeOut'
FILENAMEERRORED = 'FileNameErrored'
FILENAMEERROREDTIME = 'FileNameErroredTime'
GOTASSETSRESPONSE = 'GotAssetsResponse'
GOTRESPONSE = 'GotResponse'
HTTPEXCEPTION = 'HTTPException'
IOERROR = 'IOError'
IOERRORTRANSIENT = 'IOErrorTransient'
LOGCALL = 'logCall'
LOGCALLDEBUG = 'logCallDebug'
LOGPROXYCALL = 'logProxyCall'
OTHERERROR = 'OtherError'
OTHERUNKNOWNERROR = 'OtherUnknownError'
PROXYAUTHERROR = 'ProxyAuthError'
RETRY = 'Retry'
SSLERROR = 'SSLError'
TOTALDOWNLOADED = 'TotalDownloaded'
TOTALDURATION = 'TotalDuration'
TOTALHOSTS = 'TotalHosts'
TRACEUNEXPECTED = 'TraceUnexpected'
UNEXPECTEDERROR = 'UnexpectedError'
UNHANDLEDEXCEPTION = 'UnHandledException'
UNKNOWNCODE = 'UnknownCode'
UNKNOWNERROR = 'UnknownError'

DEBUG = 'debug'
ERROR = 'error'
INFO = 'info'


class logHandler(object):
    def __init__(self, name = None):
        self.qlogger = logging.getLogger(name)
        self.debug = False

    def enableDebug(self, on):
        self.debug = on

    def get_script_path(self):
        return os.path.dirname(os.path.realpath(sys.argv[0]))

    def enableLogger(self):
        basedir = self.get_script_path()
        log_dir = basedir + '/log'
        filename = log_dir + "/qPyMultiThread_%s.log" % (os.getpid())
        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
                                      "%Y-%m-%d %H:%M:%S%z")
        fh = logging.handlers.RotatingFileHandler(
                filename,
                maxBytes = 25000000, backupCount = 5)
        fh.setFormatter(formatter)

        debug_log_handler = logging.StreamHandler(sys.stdout)
        debug_log_handler.setFormatter(formatter)

        if self.debug:
            self.qlogger.setLevel(logging.DEBUG)
            fh.setLevel(logging.DEBUG)
            debug_log_handler.setLevel(logging.DEBUG)
        else:
            self.qlogger.setLevel(logging.INFO)
            fh.setLevel(logging.INFO)

        self.qlogger.addHandler(debug_log_handler)
        self.qlogger.addHandler(fh)
        return self.qlogger

    def dynamicLogger(self, msg, logLevel = 'info', *args, **kwargs):
        """
        Dynamic logger that will handle any number of variables added to a log line, and fill them in accordingly
        if it exists in the context of the log line. If the passed variable doesnt exist in the msg, it is silently
        ignored. However, if the msg contains a variable that is not sent, then a KeyError exception is thrown.

        Example:
            logline = "Calling API endpoint: {url} with params: {params}"
            dynamicLogger(logline, url = 'https://www.google.com', params = 'action=list&type=abd')
            Result:
            2018-12-31 16:27:10 -0800 [qPyMultiThread] [MainThread] INFO: Calling https://www.google.com
            with params: {'action': 'list', 'type': 'abd' }
        """
        # FIXME - iterate through the variables within a message, and if its corresponding variable is missing
        # then create it with an empty string to prevent KeyErrors
        # import inspect
        # stack = inspect.stack()
        # clsmethod = "[%s.%s]" % (str(stack[1][0].f_locals["self"].__class__.__name__), str(stack[1][0].f_code.co_name))
        # msg = clsmethod+" "+msg
        if logLevel == ERROR:
            self.qlogger.error(msg.format(*args, **kwargs))
        elif logLevel == DEBUG:
            self.qlogger.debug(msg.format(*args, **kwargs))
        else:
            self.qlogger.info(msg.format(*args, **kwargs))

    def logErrorWithContext(self, exception = None, context = None, url = None,
                            status = None, retrycount = None, duration = None, hash = None):
        """
        Prepare a log message with given pre-determined logline from messages(). If no context is given, then assume
        an Unknown Error
        :param exception: Exception object
        :param context: .messages.msg_map
        :param retrycount: Integar
        """
        m = messages()

        if context:
            msg = context
        else:
            msg = m.message(UNKNOWNCODE)

        self.dynamicLogger(msg = msg,
                           exception = exception,
                           logLevel = ERROR,
                           retrycount = retrycount,
                           status = status,
                           duration = duration,
                           url = url,
                           hash = hash)

    def checkException(self, exception = None, context = None, request = None,
                       response = None, retrycount = None, NoRetryCodes = None,
                       hash = None, logLevel = INFO):
        """Log handler for Exceptions incurred by HTTP requests

        :param exception: Exception Object
        :param url: Full Request URL
        :param retrycount: Integar
        :param retryCodes: list of integers of HTTP status codes
        :param context: .messages.msg_map
        :param request: APIRequest
        :param response:  APIResponse
        """
        m = messages()
        url = request.get_full_url()
        context = None if context is None else context
        hash = 'None' if hash is None else hash
        knowncode = None
        code = None
        retry = False
        # While making an HTTP request to qualysapi endpoints, we can hit various kinds of errors.
        # The condition of the response helps us determine how to move forward.
        # We're gonna check for a variance of:
        #   http status code
        #       - Known condition, log and retry
        #   http reason
        #       - Can contain transient errors
        #   no http attributes
        #       - Socket error
        if hasattr(exception, 'code'):
            code = exception.code
            knowncode = m.code(code)
            retry = True
            if NoRetryCodes is not None:
                if code in NoRetryCodes:
                    return False

            if not knowncode:
                context = UNKNOWNCODE if context is None else context
        else:
            retry = True
            if hasattr(exception, 'reason'):  # No code, probably a transient issue
                if isinstance(exception.reason, socket.timeout):
                    context = CONNECTIONTIMEOUT
                context = CONNECTIONTIMEOUT if 'timed out' in exception.reason else OTHERUNKNOWNERROR
            else:
                context = UNKNOWNERROR if context is None else context

        if context is not None:
            msg_map = context
        elif knowncode is not None and knowncode:
            msg_map = m.errorcodes[exception.code]
        else:
            msg_map = UNKNOWNERROR

        self.logErrorWithContext(exception = exception,
                                 context = m.message(msg_map),
                                 url = url,
                                 status = code,
                                 retrycount = retrycount,
                                 hash = hash)
        return retry

    def flush(self):
        for handler in self.qlogger.handlers:
            handler.flush()


class messages(object):
    """
    LogMessage class object for pre-defined loglines.
    """

    def __init__(self):
        self.msg_map = {
                APICONCURRENCY     : 'API Concurrently limit reached, Retry with exponential backoff. Retry count: {retrycount}',
                AUTHERROR          : '[Validate] - Authentication Error against /msp/about.php, please check credentials',
                AUTHERRORAFTER     : 'Authentication Error, but were using stored creds, so we will try again, as this is a temporary condition. Retry Count: {retrycount}',
                BADREQUESTSERROR   : 'Bad Request: {exception}',
                CONNECTIONRESET    : 'Connection was Reset by end server. Retry Count: {retrycount}',
                CONNECTIONTIMEOUT  : 'Connection Timed out to {url}, retrying. Retry count: {retrycount}, Request Hash: {hash}',
                FILENAMEERRORED    : '{dir}/vm_detections_Range-{range}_Process-{pid}_{thread}_Batch-{batch}_errored_{rand}.{extension}',
                FILENAMEERROREDTIME: '{dir}/vm_detections_startTime-{time}_Process-{pid}_{thread}_Batch-{batch}_errored_{rand}.{extension}',
                GOTASSETSRESPONSE  : 'API Response size: {size} bytes. {numids} asset ids retrieved..',
                GOTRESPONSE        : 'Request Hash: {hash}, Got {status} response from API in {duration} seconds. Receiving data...',
                HTTPEXCEPTION      : 'Request failed with HTTPException, Retrying: {url} . Error: {exception}. Retry count: {retrycount}, Request Hash: {hash}',
                IOERROR            : 'Request failed with IOError after {duration} seconds, Retrying: {url} . Error: {exception}. Trackback: {traceback}. Retry count: {retrycount}, Request Hash: {hash}',
                IOERRORTRANSIENT   : 'IOError in handling response and writing to file, {errno} : {exception}',
                OTHERERROR         : 'Unsuccessful while calling API [{code} : {reason}]. Retrying: {url} with params={params}. Retry count: {retrycount}',
                OTHERUNKNOWNERROR  : 'Unknown Error Occurred: {exception} . Retry Count: {retrycount}, Request Hash: {hash}',
                PROXYAUTHERROR     : '[Validate] - Authentication Error to proxy, please check credentials',
                RETRY              : 'Previous call failed. Exponentially backing off for {sleep} seconds before retrying. Count: {count}',
                SSLERROR           : 'Request failed with SSLError, Retrying: {url}. Error: {exception}. Retry count: {retrycount}, Request Hash: {hash}',
                TOTALDOWNLOADED    : 'Total Size downloaded: {bytes}, Total Hosts downloaded: {hosts}, Total Duration: {duration} seconds, Combined Average Speed: {speed} kb/sec.  Batch Size: {batchsize}',
                TOTALDURATION      : 'Total Duration: {duration} seconds',
                TOTALHOSTS         : 'Total Hosts downloaded: {hosts}, Batch Size: {batchsize}',
                TRACEUNEXPECTED    : 'Unknown Exception occurred. Traceback: {traceback}',
                UNEXPECTEDERROR    : 'Request failed with Unhandled Exception after {duration} seconds, Retrying: {url} . Error: {exception}. Trackback: {traceback}. Retry count: {retrycount}, Request Hash: {hash}',
                UNHANDLEDEXCEPTION : '[Validate] Got unexpected response from API: {exception}',
                UNKNOWNCODE        : 'Unknown status code returned, Retry count: {retrycount}',
                UNKNOWNERROR       : 'Unknown error while calling API. Endpoint: {url} Exception: {exception}, Retrying: {url} Retry count: {retrycount}, Request Hash: {hash}',
                LOGCALL            : 'Calling {url} with params: {params}',
                LOGCALLDEBUG       : 'Calling {url} with params: {params}',
                LOGPROXYCALL       : 'Calling {url} with params: {params} Through Proxy',
        }
        self.errorcodes = {
                400: 'BadRequestsError',
                401: 'AuthErrorAfter',
                # 403:'ForbiddenError',
                # 404:'NotFoundError',
                # 405:'MethodNotAllowedError',
                407: 'ProxyAuthError',
                409: 'APIConcurrency',
                # 413:'PayloadTooLargeError',
                # 415:'UnsupportedMediaTypeError',
                429: 'TooManyRequestsError',
                # 500:'InternalServerError',
                503: 'ConnectionReset',
        }

    def message(self, message):
        try:
            exc = self.msg_map[message]
        except KeyError:
            return False
        return exc

    def code(self, code):
        try:
            exc = self.errorcodes[code]
        except KeyError:
            return False
        return exc