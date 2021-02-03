# -*- coding=iso-8859-1 -*-
# Copyright 2019 Qualys Inc. All Rights Reserved.
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
"""
 -------------------------------------------------------------------
 qPyMultiThread.py
 -------------------------------------------------------------------
 This tool is an example of using the practices outlined in the
 Qualys v2 API User Guide under the Best Practices Section
 https://www.qualys.com/docs/qualys-api-vmpc-user-guide.pdf

  Recommendations To improve performance, Multi-threading should be used.
  Here is an outline of what the POC multi-threading script does to obtain
  the maximum throughput:
  1. Make an initial API call to the Host List API endpoint to retrieve all host IDs for the subscription that
     need to have data retrieved.
          Note: Its important to do any filtering on hosts at this point, as filtering during the detection pull
          can impact performance. Host List API Endpoint:
              https://<qualysapi url>/api/2.0/fo/asset/host/
  2. Break the total Host IDs into batches of 1,000-5,000 and send to a Queue.
  3. Launch X worker threads that will pull the batches from the Queue and launch an API call against:
  https://<qualysapi url>/ api/2.0/fo/asset/host/vm/detection/ Using Parameters:

    params = dict(
        action='list',
        show_igs=0,
        show_reopened_info=1,
        active_kernels_only=1,
        output_format='XML',
        status='Active,Re-Opened,New',
        vm_processed_after=<Date in UTC>, # Formatted as: '2019-04-05T00:00:01Z'
        truncation_limit = 0,
        ids=ids
    )

  Considerations

  Batch size
   On the backend, the host detection engine will break up the number of hosts to retrieve information on
   with a maximum size of 10,000. Using a batch size higher than this will not add any benefit to performance. In the
   same context, there are multiple places that need to pull information so there is an overhead cost regardless of the
   size being used. For that reason, using a batch size too small can start to hinder performance slightly due to the
   overhead being used on small requests. Different parameters and the amount of total data on the backend can make
   requests vary in duration, it is best to experiment with different batch size?s during peak and non-peak hours to
   determine the optimal size to use.

  Error Handling
   Robust error handling and logging is key to any automation and is recommended to implement mechanisms
   to catch exceptions and retry with exponential back off when errors are encountered. This includes all functions
   dealing with connection requests, parsing, or writing to disk. Taking care to log as much precise detail as possible
   so it will be easier to audit later should the need arise.

  Parsing
   If an error is encountered, the API will return an error code and a description of the error,
   which will look like this:

  Simple Return with error:

    <?xml version="1.0" encoding="UTF-8" ?>
    <!DOCTYPE GENERIC_RETURN SYSTEM "https://qualysapi.qualys.com/api/2.0/simple_return.dtd?>
     <SIMPLE_RETURN>
      <RESPONSE>
        <DATETIME>2018-02-14T02:51:36Z</DATETIME>
        <CODE>1234</CODE>
        <TEXT>Description of Error</TEXT>
      </RESPONSE>
    </SIMPLE_RETURN>

  Generic Return with error:

    <?xml version="1.0" encoding="UTF-8" ?>
    <!DOCTYPE GENERIC_RETURN SYSTEM "https://qualysapi.qualys.com/generic_return.dtd">
    <GENERIC_RETURN>
     <API name="index.php" username="username at="2018-02-13T06:09:27Z">
      <RETURN status="FAILED" number="999">Internal error. Please contact customer support.</RETURN>
    </GENERIC_RETURN>
    <!-- Incident signature: 123a12b12c1de4f12345678901a12a12 //-->

  A full list of Error code Responses can be found
  in the API User Guide in Appendix 1
  https://www.qualys.com/docs/qualys-api-vmpc-user-guide.pdf

  Connection Errors
   With retrieving large amounts of data sets and continuously streaming through the API for prolonged periods of time,
   comes the possibility of running into edge cases with regards to connections. Whichever method is used to make the
   outbound connection to the API endpoint, it is recommended to set a timeout to abort/retry a connection if it hasn?t
   been established in a reasonable amount of time. This is to prevent stalling out a thread, resulting in reduced
   performance. Also consider these types of connection errors, amongst others:
   -	Empty Responses
   -	Timeouts
   -	Connection Reset or Internal Error responses. Status codes: 503, 500.
   -	Connection Closed
   These can be caused by either side of the connection, so need to be caught, logged,
   and if they continue then investigated.

"""

# ---------
# Library Imports
# ---------
import copy
import ssl
import sys
import time
from optparse import IndentedHelpFormatter, OptionGroup, OptionParser, textwrap
from random import randint
from threading import Thread, current_thread
import ipaddress
from ipaddress import NetmaskValueError, AddressValueError

try:
    from hashlib import sha1 as _sha, md5 as _md5
except ImportError:
    # prior to Python 2.5, these were separate modules
    import sha
    import md5

    _sha = sha.new
    _md5 = md5.new
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# Check Python version so we can import the appropriate libraries
_ver = sys.version_info

#: Python 2.x?
is_py2 = (_ver[0] == 2)
#: Python 3.x?
is_py3 = (_ver[0] == 3)
if is_py2:
    import Queue as queue
    from urllib2 import urlopen, ProxyHandler, build_opener, install_opener, Request, HTTPSHandler, HTTPHandler
    from urllib2 import HTTPError, URLError
    from httplib import HTTPSConnection, HTTPException
    str = unicode

if is_py3:
    import queue
    from builtins import object
    from builtins import next
    from builtins import range
    from builtins import str
    from future.moves.urllib.request import urlopen, ProxyHandler, build_opener, install_opener
    from future.moves.urllib.request import Request, HTTPSHandler, HTTPHandler
    from future.moves.urllib.error import HTTPError, URLError
    from http.client import HTTPSConnection
    from http.client import HTTPException
    from builtins import input as raw_input
    str = str


# ---------
# Local Imports
# ---------
import lib
from lib.APIResponse import APIResponse, APIResponseError, XMLFileBufferedResponse
from lib.APIRequest import APIRequest
from lib.configuration import Configuration
from lib.loghandler import *
from lib import utils

logHandler = logHandler('qPyMultiThread')
init = Configuration()
init.setupFileStructure()
import requests

NORETRYCODES = [400]


class HTTPSConnectionWithKeepAlive(HTTPSConnection):
    """TCP KEEPALIVE
     In order to set tcp keepalive we need to subclass HTTPSHandler
     Here is the source code for HTTPSHandler from urllib2 github repo
     https://github.com/python/cpython/blob/2.7/Lib/urllib2.py

       class HTTPSHandler(AbstractHTTPHandler):

           def __init__(self, debuglevel=0, context=None):
               AbstractHTTPHandler.__init__(self, debuglevel)
               self._context = context

           def https_open(self, req):
               return self.do_open(httplib.HTTPSConnection, req,
                   context=self._context)

           https_request = AbstractHTTPHandler.do_request_

     As urllib2.HTTPSHandler uses httplib.HTTPSConnection we need to subclass this also.
     The connect method would create the socket.

     def connect(self):
         "Connect to a host on a given (SSL) port."

         HTTPConnection.connect(self)

         if self._tunnel_host:
             server_hostname = self._tunnel_host
         else:
             server_hostname = self.host

         self.sock = self._context.wrap_socket(self.sock,
                                               server_hostname=server_hostname)

     This is the method we would need to add the socket option for keep-alive.
     Now, one of the challenge with low level TCP settings is that
     TCP stack for each OS has a different settings.

    Usage of the new Handler:
        http_handler = HTTPSHandlerWithKeepAlive()
        opener = urllib2.build_opener(http_handler)
        urllib2.install_opener(opener)
    """

    def connect(self):
        HTTPSConnection.connect(self)
        keepalive_idle_sec = 50
        keepalive_interval_sec = 10
        keep_alive_max_fail = 25
        # Identify each OS and set the socket options
        # All possible values:
        # https://docs.python.org/2/library/sys.html#sys.platform
        if sys.platform.startswith('linux'):
            # LINUX is pretty straight forward
            # setsockopt supports all the values
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, keepalive_idle_sec)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, keepalive_interval_sec)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, keep_alive_max_fail)
        elif sys.platform.startswith('darwin'):
            # MAC OSX - is similar to linux but the only probelem is that
            # on OSX python socket module does not export TCP_KEEPIDLE,TCP_KEEPINTVL,TCP_KEEPCNT constant.
            # Taking the value for TCP_KEEPIDLE from darwin tcp.h
            # https://github.com/apple/darwin-xnu/blob/master/bsd/netinet/tcp.h
            # define TCP_KEEPALIVE           0x10    /* idle time used when SO_KEEPALIVE is enabled */
            # define TCP_KEEPINTVL       0x101   /* interval between keepalives */
            # define TCP_KEEPCNT     0x102   /* number of keepalives before close */
            # TCP_KEEPINTVL and TCP_KEEPCNT were added 5 years ago. So, older OSX would not support it.
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.sock.setsockopt(socket.IPPROTO_TCP, 0x10, keepalive_idle_sec)
            self.sock.setsockopt(socket.IPPROTO_TCP, 0x101, keepalive_interval_sec)
            self.sock.setsockopt(socket.IPPROTO_TCP, 0x102, keep_alive_max_fail)
        elif sys.platform.startswith('win'):
            # WINDOWS - To set TCP Keepalive on windows need to use sock.ioctl and more info can be found here
            # https://msdn.microsoft.com/en-us/library/dd877220%28v=vs.85%29.aspx
            # The time is in milliseconds
            self.sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, keepalive_idle_sec * 1000, keepalive_interval_sec * 1000))


class Formatter(IndentedHelpFormatter):
    def format_description(self, description):
        if not description:
            return ""
        desc_width = 150 - self.current_indent
        indent = " " * self.current_indent
        # the above is still the same
        bits = description.split('\n')
        formatted_bits = [textwrap.fill(bit,
                                        desc_width,
                                        initial_indent = indent,
                                        subsequent_indent = indent) for bit in bits]
        result = "\n".join(formatted_bits) + "\n"
        return result

    def format_option_strings(self, option):
        """Return a comma-separated list of option strings & metavariables."""
        self._short_opt_fmt = "%s"
        if option.takes_value():
            metavar = option.metavar or option.dest.upper()
            short_opts = [self._short_opt_fmt % (sopt) for sopt in option._short_opts]
            long_opts = [self._long_opt_fmt % (lopt, metavar) for lopt in option._long_opts]
        else:
            short_opts = option._short_opts
            long_opts = option._long_opts
        if self.short_first:
            opts = short_opts + long_opts
        else:
            opts = long_opts + short_opts

        return ", ".join(opts)

    def format_epilog(self, epilog):
        return "\n" + epilog


class HTTPSHandlerWithKeepAlive(HTTPSHandler):
    def https_open(self, req):
        return self.do_open(HTTPSConnectionWithKeepAlive, req)


class CongifurationException(Exception):
    pass


class APIClient(object):

    def __init__(self):
        self.timeout = 120
        self.type = "GET"
        self.auth = None
        self.url = None
        self.responseHeaders = None
        self.config = None
        self.downloadAssets = False
        self.downloadDetections = False
        self.downloadHostAssets = False
        self.id_set = None
        self.logMessages = messages()
        self.log_host_detections = False
        self.log_host_details_in_detection = False
        self.collect_advanced_host_summary = False
        self.seed_file_enabled = False
        self.log_host_summary = False
        self.completedThreads = []
        self.totalBytes = 0
        self.total_hosts = 0
        self.remaining = 0
        self.completed = 0
        self.host_logged = 0
        self.receivedBytes = 0
        self.vulns = 0
        self.detectionParameters = dict(
                action = 'list',
                show_igs = 0,
                show_reopened_info = 1,
                # active_kernels_only=1,
                output_format = 'XML',
                status = 'Active,Re-Opened,New',
                # detection_processed_after=self.config.detectionDelta,
                truncation_limit = 0
        )

    def validate(self):
        """This method is to validate configured proxy and
        Qualys credentials work before launching.
        """
        validateresponse = None
        logHandler.dynamicLogger("Validating Credentials to %s ..." % (self.config.baseURL + self.config.validateURL))

        if self.config.useProxy:
            proxytest = ProxyHandler({'https': self.config.proxyHost})
            proxyopener = build_opener(proxytest)
            try:
                install_opener(proxyopener)
            except Exception as e:
                logHandler.dynamicLogger("Failed to install proxy", logLevel = 'error')
                return e.str(e)

        request = APIRequest(
                type = 'GET',
                username = self.config.username,
                password = self.config.password)
        request.build_headers()
        validatereq = Request(
                self.config.baseURL + self.config.validateURL,
                data = None,
                headers = request.headers)
        try:
            # make request
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            https_handler = HTTPSHandler(debuglevel = 1)
            http_handler = HTTPHandler(debuglevel = 1)
            opener = build_opener(https_handler)
            opener2 = build_opener(http_handler)
            install_opener(opener)
            install_opener(opener2)

            validateresponse = urlopen(validatereq, timeout = self.timeout, context = ctx)
            APIResponse.responseHeaders = validateresponse.info()
            logHandler.dynamicLogger("Got response from Proxy, and msp/about.php endpoint...")
            return APIResponse
        except (URLError, HTTPError) as ue:
            logHandler.checkException(exception = ue,
                                      request = validatereq,
                                      retrycount = 0)
            # Failed validation of proxy or authentication
            return False
        except ssl.SSLError as e:
            context = 'SSLError'
            traceback = None
            if self.config.debug:
                import traceback
            logHandler.checkException(
                    exception = e,
                    traceback = 'TrackbackDisabled' if traceback is None else traceback.format_exc(),
                    request = validatereq,
                    response = validateresponse,
                    context = context,
                    retrycount = 0)
            return False
        except IOError as e:
            # This will occur if something happens while iterating through the chunked response being written
            # to file. Usually it means either a problem writing to disk, or a
            # transient error occurred.
            import traceback
            logHandler.dynamicLogger(
                    self.logMessages.message(IOERROR),
                    duration = 0,
                    url = validatereq.get_full_url() if validatereq is not None else None,
                    traceback = traceback.format_exc(),
                    exception = e,
                    retrycount = 0)
            return False

    def closeConn(self, conn):
        """Cleanly close a urllib2.urlopen connection
        :param conn: urllib2.urlopen
        """
        try:
            conn.close()
        except (AttributeError, NameError):
            # Connection didnt exist, safe to just pass
            pass

    def callDuration(self, startTime = None):
        """Given a start Time, return the delta between now and then
        :param startTime: time.time()
        :return: (int) seconds
        """
        if startTime is None:
            return 0
        endTime = time.time()
        return round(endTime - startTime, 2)

    def post(self, url, data = None, response = None, **kwargs):
        """Sends a POST request. Returns :class:APIResponse object.
        :param url: URL for the new :class:APIRequest object.
        :param data: (optional) Dictionary, bytes, or file-like object to send in the body of the :class:APIRequest.
        :param \*\*kwargs: Optional arguments that ``call_api`` takes.
        :return: APIResponse
        """
        return self.call_api(
                method = 'POST',
                api_route = url,
                data = data,
                response = response,
                **kwargs)

    def get(self, url = None, data = None, response = None, **kwargs):
        """Sends a POST request. Returns :class:APIResponse object.
        :param url: URL for the new :class:APIRequest object.
        :param data: (optional) Dictionary, bytes, or file-like object to send in the body of the :class:APIRequest.
        :param \*\*kwargs: Optional arguments that ``call_api`` takes.
        :return: APIResponse
        """
        return self.call_api(
                method = 'GET',
                api_route = url,
                data = data,
                response = response,
                **kwargs)

    def makeHash(self, value = None):
        """ Return a hashed string, given an input.
        :param name: String value to encode
        :return: String hash
        """
        value = str(value) + str(randint(10000, 99999))
        result = _md5(str(value).encode('utf-8'))
        hash = result.hexdigest()
        return hash

    def call_api(self, api_route = None, data = None, method = None, response = None, filename = None, **kwargs):
        """
        This method does the actual API call. Returns response or raises an Exception.
        Does not support proxy at this moment.
        :param api_route: (str) full url to make a request to
        :param data: (optional) file-like object to send in the body of the :class:APIRequest
        :param method: HTTP method to make the :class:APIRequest
        :param response: Class of the Response Handler
        :param kwargs: Optional Arguments
        :return: APIResponse
        """
        ids = None
        retrycount = 0
        hash = self.makeHash(int(time.time()))
        request = None

        # Enable http level debugging if in debug mode
        # urllib HTTP debug logging uses print, so will go to stdout
        # i.e: httplib.HTTPResponse#_read_status():
        #           if self.debuglevel > 0:
        #               print "reply:", repr(line)
        http_handler = HTTPSHandlerWithKeepAlive(debuglevel = self.config.debug)
        opener = build_opener(http_handler)
        install_opener(opener)

        # Prepare request
        prepared = APIRequest(
                type = method,
                username = self.config.username,
                password = self.config.password,
                data = data,
                api_route = api_route,
                hash = hash)
        req = prepared.build_request()

        while True:
            try:
                # Previous attempt failed, retry with exponential backoff
                if retrycount > 0:
                    logHandler.dynamicLogger(self.logMessages.message(RETRY),
                                             sleep = (30 + (2 ** retrycount)),
                                             count = retrycount)
                    time.sleep(30 + (2 ** retrycount))
                    prepared.hash = self.makeHash(int(time.time()))
                    req = prepared.build_request()

                starttime = time.time()

                # Log Request details
                if not self.config.debug and isinstance(data, dict):
                    # Create a copy, to remove the ids parameter for logging
                    datacopy = copy.deepcopy(data)
                    loglevel = DEBUG

                    if 'ids' in datacopy:
                        del datacopy['ids']
                        datacopy = '&'.join("%s=%r" % (key, val) for (key, val) in datacopy.items())
                        datacopy = datacopy.replace("'", "")
                else:
                    datacopy = data
                    if not isinstance(data, dict):
                        datacopy = "<XML Post Data>"
                    loglevel = INFO
                msg = LOGPROXYCALL if self.config.useProxy is True else LOGCALL
                logHandler.dynamicLogger(
                        self.logMessages.message(msg) + " Request Hash: %s" % prepared.hash,
                        url = api_route,
                        params = datacopy,
                        loglevel = loglevel)

                # Make Request
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                request = urlopen(req, timeout = self.timeout, context = ctx)

                # Send response to XMLFileBufferedResponse class for handling
                response.response = request

                duration = self.callDuration(startTime = starttime)

                if not response.get_response():
                    logHandler.dynamicLogger("Error during Fetching for Request Hash: %s, Cleaning up and retrying",
                                             hash, logLevel = DEBUG)
                    keep_running = True
                    self.cleanup(response.file_name)
                    retrycount += 1
                    continue

                # received, parsed, and saved response. Log its size,
                # and return.
                logHandler.dynamicLogger(
                        self.logMessages.message(GOTRESPONSE),
                        status = request.getcode(),
                        duration = duration,
                        hash = prepared.hash)
                self.receivedBytes += response.totalSize
                return response

            # Handle the different exceptions that can occur. Unless explicitly
            # told to stop, retry forever.
            # FIXME - Make retries configurable, both by max retries, and types
            # of exceptions to bail on.
            except (URLError, HTTPError, HTTPException) as ue:
                retrycount += 1
                if isinstance(ue.reason, HTTPException):
                    context = HTTPEXCEPTION
                else:
                    context = None
                # Exception handling for the various http codes/reasons returned
                shouldretry = logHandler.checkException(
                        exception = ue,
                        request = req,
                        response = response,
                        context = context,
                        retrycount = retrycount,
                        NoRetryCodes = self.config.NoRetryCodes,
                        hash = prepared.hash)
                self.renameFile(filename, filename + ".errored", hash = prepared.hash)
                if request is not None:
                    self.closeConn(request)

                if shouldretry:
                    continue
                else:
                    logHandler.dynamicLogger('Not Retrying, hit unretriable exception: %s', ue)
                    break
            except ssl.SSLError as e:
                retrycount += 1
                context = 'SSLError'
                traceback = None
                shouldretry = logHandler.checkException(exception = e,
                                                        request = req,
                                                        response = response,
                                                        context = context,
                                                        retrycount = retrycount,
                                                        NoRetryCodes = self.config.NoRetryCodes,
                                                        hash = prepared.hash)
                self.renameFile(filename, filename + ".errored", hash = prepared.hash)
                if request is not None:
                    self.closeConn(request)
                if shouldretry:
                    continue
                else:
                    break
            except IOError as e:
                # This will occur if something happens while iterating through the chunked response being written
                # to file. Usually it means either a problem writing to disk,
                # or a transient error occurred (Connection Timeout, Connection Reset)
                import traceback
                retrycount += 1
                duration = self.callDuration(startTime = starttime)
                logHandler.dynamicLogger(self.logMessages.message(IOERROR), duration = duration,
                                         url = req.get_full_url() if req is not None else None,
                                         traceback = traceback.format_exc(), exception = e, retrycount = retrycount,
                                         hash = prepared.hash)
                self.renameFile(filename, filename + ".errored", hash = prepared.hash)
                if request is not None:
                    self.closeConn(request)
                continue

            # FIXME Catchall for edge cases - include traceback to find out why it wasn't caught
            # Remove once these no longer occur, or its no longer needed.
            except Exception as e:
                import traceback
                retrycount += 1
                endtime = time.time()
                totaltime = endtime - starttime
                logHandler.dynamicLogger(
                        self.logMessages.message(UNHANDLEDEXCEPTION),
                        duration = totaltime,
                        url = req.get_full_url() if req is not None else None,
                        traceback = traceback.format_exc(),
                        exception = e,
                        retrycount = retrycount,
                        hash = prepared.hash)
                self.renameFile(filename, filename + ".errored", hash = prepared.hash)
                if request is not None:
                    self.closeConn(request)
                break

    def get_asset_ids_portal(self):
        """This method will fetch all the host ids in single API call."""
        data = '''<ServiceRequest>
                 <filters>
                <Criteria field="lastVulnScan" operator="GREATER">%s</Criteria>
                 </filters>
                 <preferences>
                 <startFromId>1</startFromId>
                 <limitResults>0</limitResults>
                 </preferences>
        </ServiceRequest>''' % self.config.detectionDelta
        api_route = '/qps/rest/2.0/ids/am/hostdetection'
        asset_ids = []
        host_ids = []
        logHandler.dynamicLogger("Fetching asset ids from portal..")
        filename = self.config.outputDir + "/assets/portalasset_ids_%s_%s.xml" % (
        os.getpid(), current_thread().getName())
        keep_running = True
        while keep_running:

            # Make an API call and send it to XMLFileBufferedResponse
            self.post(self.config.baseURL + api_route,
                      data = data,
                      response = XMLFileBufferedResponse(file_name = filename, logger = logHandler),
                      filename = filename)
            time.sleep(5)
            logHandler.dynamicLogger("Wrote API response to {filename}", filename = filename)
            logHandler.dynamicLogger("Parsing IDs..")
            tree = ET.parse(filename)
            root = tree.getroot()
            response_element = root.find('data')
            if response_element is None:
                logHandler.dynamicLogger("data tag not found")
            else:
                for id_element in response_element.findall('HostDetection'):
                    if id_element.find('id').text is not None:
                        asset_ids.append(id_element.find('id').text)
                    # if id_element.find('qwebHostId').text is not None:
                    #    host_ids.append(id_element.find('qwebHostId').text)
            keep_running = False
        return asset_ids

    def get_asset_ids(self):
        """This method will fetch all the host ids in single API call."""
        api_route = '/api/2.0/fo/asset/host/'
        params = dict(
                action = 'list',
                truncation_limit = 0,
                vm_processed_after = self.config.detectionDelta,
        )
        asset_ids = []
        if self.config.pullbyip:
            roottag = 'HOST_LIST'
            childtag = 'HOST/IP'
            params['details'] = 'Basic'
        else:
            roottag = 'ID_SET'
            childtag = 'ID'
            params['details'] = 'None'
        logHandler.dynamicLogger("Fetching asset ids..")
        filename = self.config.outputDir + "/assets/asset_ids_%s_%s.xml" % (
                os.getpid(), current_thread().getName())
        keep_running = True
        while keep_running:

            # Make an API call and send it to XMLFileBufferedResponse
            self.get(
                    self.config.baseURL + api_route,
                    data = params,
                    response = XMLFileBufferedResponse(file_name = filename, logger = logHandler),
                    filename = filename
            )
            time.sleep(5)
            logHandler.dynamicLogger("Wrote API response to {filename}", filename = filename)
            logHandler.dynamicLogger("Parsing IDs..")
            tree = ET.parse(filename)
            root = tree.getroot()
            response_element = root.find('RESPONSE')
            if response_element is None:
                logHandler.dynamicLogger("RESPONSE tag not found")
            id_set = response_element.find(roottag)
            if id_set is None:
                logHandler.dynamicLogger("%s not found" % roottag)
            else:
                for id_element in id_set.findall(childtag):
                    asset_ids.append(id_element.text)
            keep_running = False
            return asset_ids

    def vm_detection_coordinator(self, detectionQueue = None):
        """This method is the entry point of each detection thread.
        It pops out an id range entry from detection queue, and calls
        download_host_detections passing id range as argument.
        """
        keep_running = True
        ips = None
        asset_ips = []
        while keep_running:
            try:
                logHandler.dynamicLogger("Getting batch from detection_idset_queue")
                log_range = id_range = detectionQueue.get(False)
                if self.config.pullbyip:
                    for i in id_range.split(','):
                        asset_ips.append(utils.int2ip(int(i)))
                    ips = ",".join(map(str, asset_ips))
                    log_range = ips
                logHandler.dynamicLogger("Processing batch: {idrange}", idrange = log_range, logLevel = DEBUG)
                self.download_host_detections(id_range, ips)
                detectionQueue.task_done()
            except queue.Empty:
                logHandler.dynamicLogger("detection_idset_queue is empty. Exiting.")
                keep_running = False

    def portal_assethost_coordinator(self, hostAsset = None):
        """This method is the entry point of each HostAsset thread.
        It pops out an id range entry from detection queue, and calls
        download_host_detections passing id range as argument.
        """
        keep_running = True
        while keep_running:
            try:
                logHandler.dynamicLogger("Getting batch from HostAsset_idset_queue")
                id_range = hostAsset.get(False)
                logHandler.dynamicLogger("Processing batch: {idrange}", idrange = id_range, logLevel = DEBUG)
                self.download_portal_assethost(id_range)
                hostAsset.task_done()
            except queue.Empty:
                logHandler.dynamicLogger("HostAsset_idset_queue is empty. Exiting.")
                keep_running = False

    def chunk(self, ids):
        """Given a String list of Asset Ids, split them using the ',' delimiter string
        and then return in String range in the format of {First Entry} - {Last Entry}
        i.e String "1,2,3,4,5" to "1-5"
        :param ids: String comma-seperated list of Asset Ids
        :return: String Range
        """
        chunks = ids.split(',')
        if self.config.pullbyip:
            id1 = utils.int2ip(int(chunks[0]))
            id2 = utils.int2ip(int(chunks[-1]))
        else:
            id1 = chunks[0]
            id2 = chunks[-1]
        idset = "%s-%s" % (id1, id2)
        return idset

    def cleanup(self, filename):
        """Safely remove a file
        :param filename: Name of file to try to delete.
        """
        try:
            logHandler.dynamicLogger('Cleaning up filename: %s' % filename, logLevel = DEBUG)
            os.remove(filename)
            # pause to give OS time to remove the file
            time.sleep(3)
        except OSError:
            # File didnt exist, so just pass
            pass

    def renameFile(self, oldFileName, newFileName, hash = None):
        """Rename a filename from the given old name.
        :param oldFileName: (str) filename to rename from.
        :param newFileName: (str) filename to rename to.
        """
        if oldFileName is None:
            logHandler.dynamicLogger('Filename is null.. Not renaming..')
        else:
            try:
                if hash is None:
                    hash = randint(10000, 99999)
                newFileName = newFileName + ".%s" % hash
                os.rename(oldFileName, newFileName)
                if self.config.debug:
                    logHandler.dynamicLogger('Renamed old file: %s to: %s' % (oldFileName, newFileName),
                                             logLevel = DEBUG)
            except OSError:
                pass

    def determineFilename(self,
                          ids = None,
                          pid = None,
                          thread = None,
                          batch = None,
                          extension = None,
                          startTime = None):
        """When Internal Error or Malformed exceptions are returned, api_params wont always be defined
        Make sure this is caught, and revert to saving to another filename
        with a datetime instead.
        :param ids: ID Range used in filename
        :param pid: PID used in filename
        :param thread: thread used in filename
        :param batch: batch used in filename
        :param extension: extension to use in filename (defaults to xml)
        :param startTime: a time.time() representation of when the API call started
        :return: String filename
        """
        if ids is not None:
            filename = self.logMessages.message(FILENAMEERRORED).format(
                    dir = self.config.tempDirectory,
                    range = ids,
                    pid = pid,
                    thread = thread,
                    batch = batch,
                    rand = randint(10000, 99999),
                    extension = extension)
        else:
            filename = self.logMessages.message(FILENAMEERROREDTIME).format(
                    dir = self.config.tempDirectory,
                    time = startTime.strftime('%Y-%m-%d-%M-%S'),
                    pid = pid,
                    thread = thread,
                    batch = batch,
                    rand = randint(10000, 99999),
                    extension = extension)
        return filename

    def verifyResponse(self, parseresponse,
                       filename = None,
                       orig_filename = None
                       ):
        """An HTTP response from Qualys API endpoints can be returned as a 200 Okay, and still have errors.
        Client Errors, and sometimes transient errors will be returned in the RESPONSE tag of the XML with
        the following info Tags:
            CODE    : This contains a special code representing the specific error that occurred.
                      A full list of error codes is located in the API User Guide.
            Message : This will contain a description of the error code, to give you more
                      information as to what happened.
        This method will parse the response and check for those errors, and rename and move the XML file
        to the tmp directory to allow referencing later.

        :param parseresponse: parsed response from the _parse() method which will contain a dict of errors
        :param orig_filename: original filename
        :return: True if their are no errors, False otherwise.
        """

        if "Internal Error" in parseresponse or "Internal error" in parseresponse:
            logHandler.dynamicLogger(
                    "API Internal Error, Leaving File {filename}, and Retrying",
                    filename = filename, logLevel = DEBUG)
            self.renameFile(oldFileName = orig_filename, newFileName = filename)
            return False
        elif "not well-formed" in parseresponse:
            logHandler.dynamicLogger(
                    "Malformed XML detected in {filename}, Retrying",
                    filename = filename, logLevel = DEBUG)
            self.renameFile(oldFileName = orig_filename, newFileName = filename)
            return False
        else:
            return True

    def getETA(self):
        """This method averages the durations for all completed threads,
        and estimates an ETA to complete
        """
        allThreads = 0

        for i in self.completedThreads:
            allThreads += i

        avgTime = round((allThreads / len(self.completedThreads)), 0)
        remainingBatches = round(self.remaining / self.config.chunkSize, 0)
        eta = round(((remainingBatches * avgTime) / self.config.numDetectionThreads) / 60, 2)
        if self.config.debug:
            logHandler.dynamicLogger(
                    "completedThreads durations: {completed}, Total for all Threads: {allThreads}, "
                    "avgTime: {avgTime}, remainingBatches: {remainingBatches}, eta: {eta} minutes, "
                    "totalRemaining hosts: {remainingHosts}, "
                    "totalCompleted hosts: {completedHosts}",
                    completed = self.completedThreads,
                    allThreads = allThreads,
                    avgTime = avgTime,
                    remainingBatches = remainingBatches,
                    eta = eta,
                    remainingHosts = self.remaining,
                    completedHosts = self.completed,
                    logLevel = DEBUG)
        return eta

    def download_host_detections(self, ids, asset_ips = None):
        """This method will invoke call_api method for asset/host/vm/detection/ API."""
        api_route = '/api/2.0/fo/asset/host/vm/detection/'
        params = self.detectionParameters
        params['vm_processed_after'] = self.config.detectionDelta
        if asset_ips is not None:
            params['ips'] = asset_ips
        else:
            params['ids'] = ids

        batch = 1
        keep_running = True
        file_extension = 'xml'

        # Convert the list of ids into a Range to use in the filename.
        # i.e 123456-999999 instead of 123456,123457,123458...
        idset = self.chunk(ids)

        logHandler.dynamicLogger("Downloading VM detections for batch: {idset}", idset = idset)

        if params['output_format'] != 'XML':
            file_extension = 'csv'
            params['truncation_limit'] = 0
        while keep_running:
            filename = self.config.outputDir + "/vm_detections/vm_detections_Range-%s_Process-%s_%s_Batch-%d.%s" % (
                    idset, os.getpid(), current_thread().getName(), batch, file_extension)

            startTime = time.time()

            # Make Request
            response = self.get(
                    url = self.config.baseURL + api_route,
                    data = params,
                    response = XMLFileBufferedResponse(file_name = filename, logger = logHandler),
                    filename = filename)
            duration = self.callDuration(startTime = startTime)
            self.completedThreads.append(duration)

            size = 0
            if response.totalSize is not None:
                size = response.totalSize
                self.totalBytes = self.totalBytes + response.totalSize
            logHandler.dynamicLogger(
                    "Wrote API response with {size} bytes, "
                    "avg speed: {speed} KB/sec, "
                    "Combined speed: {combinedSpeed} KB/sec "
                    "API Duration: {duration}, to {filename}, "
                    "Hosts remaining: {remaining}, "
                    "ETA until completion: {ETA} minutes, "
                    "Total Data received: {totalBytesreceived} MB, "
                    "Projected Total size: {projectedTotal} MB",
                    size = size,
                    speed = round(response.totalSize / duration / 1000, 2),
                    combinedSpeed = round((response.totalSize / duration / 1000) * self.config.numDetectionThreads, 2),
                    duration = duration,
                    remaining = self.remaining,
                    ETA = self.getETA(),
                    totalBytesreceived = round((self.totalBytes / 1024 / 1024), 2),
                    projectedTotal = round(
                        ((size * (self.remaining / self.config.chunkSize)) + self.totalBytes) / 1024 / 1024, 2),
                    filename = filename)

            # API Endpoint returned data, parse it for error codes, and
            # completeness. Retry if needed.
            parsed = self._parse(filename,
                                 format = params['output_format'])
            vfileName = self.determineFilename(ids = idset,
                                               pid = os.getpid(),
                                               thread = current_thread().getName(),
                                               batch = batch,
                                               extension = file_extension)
            verified = self.verifyResponse(parseresponse = parsed,
                                           orig_filename = filename,
                                           filename = vfileName)

            if not verified:
                keep_running = True
                self.cleanup(filename)
                continue

            # Check if the result was truncated, and another batch needs to be retrieved
            # Note we can only get here after successfully receiving, parsing, and saving the result
            # of an API call into a file. A Missing Response XML Tag should never occur in a
            # VM Host Detection API call.
            if params['output_format'] == 'XML':
                if not os.path.isfile(filename):
                    continue
                tree = ET.parse(filename)
                root = tree.getroot()
                response_element = root.find('RESPONSE')
                if response_element is None:
                    logHandler.dynamicLogger(
                            "RESPONSE tag not found in {filename}. Please check the file.",
                            filename = filename,
                            logLevel = 'error')
                    keep_running = False
                warning_element = response_element.find('WARNING')
                if warning_element is None:
                    keep_running = False
                else:
                    next_page_url = warning_element.find('URL').text
                    params = utils.get_params_from_url(url = next_page_url)
                    batch += 1

    def download_portal_assethost(self, ids):
        """This method will invoke call_api method for asset/host/vm/detection/ API."""
        api_route = '/qps/rest/2.0/search/am/hostdetection'
        params = """<ServiceRequest>
                    <filters>
                    <Criteria field="id" operator="IN">%s</Criteria>
                            <Criteria field="detection.found"
                            operator="EQUALS">true</Criteria>
                            <Criteria field="detection.ignored"
                            operator="EQUALS">false</Criteria>
                            <Criteria field="detection.disabled"
                            operator="EQUALS">false</Criteria>
                            <Criteria field="detection.showresults"
                            operator="EQUALS">false</Criteria>
                            <Criteria field="detection.typeDetected"
                            operator="IN">4,2</Criteria>
                            <Criteria field="detection.nonRunningKernal"
                            operator="EQUALS">false</Criteria>
                     </filters>
                     <preferences>
                     <startFromId>1</startFromId>
                     <limitResults>%s</limitResults>
                     </preferences>
        </ServiceRequest>""" % (ids, self.config.chunkSize)
        batch = 1
        keep_running = True
        file_extension = 'xml'

        # Convert the list of ids into a Range to use in the filename.
        # i.e 123456-999999 instead of 123456,123457,123458...
        idset = self.chunk(ids)

        logHandler.dynamicLogger("Downloading Portal data for ids {idset}", idset = idset)

        while keep_running:
            filename = self.config.outputDir + "/portal/Portal_hostasset_Range-%s_Process-%s_%s_Batch-%d.%s" % (
                    idset, os.getpid(), current_thread().getName(), batch, file_extension)

            startTime = time.time()

            # Make Request
            response = self.post(
                    url = self.config.baseURL + api_route,
                    data = params,
                    response = XMLFileBufferedResponse(file_name = filename, logger = logHandler),
                    filename = filename)
            duration = self.callDuration(startTime = startTime)
            self.completedThreads.append(duration)
            size = 0
            if response.totalSize is not None:
                size = response.totalSize
            logHandler.dynamicLogger(
                    "Wrote API response with {size} bytes, "
                    "avg speed: {speed} kb/sec, "
                    "API Duration: {duration}, to {filename}, "
                    "Hosts remaining: {remaining}, "
                    "ETA until completion: {ETA} minutes",
                    size = size,
                    speed = round(response.totalSize / duration / 1000, 2),
                    duration = duration,
                    remaining = self.remaining,
                    ETA = self.getETA(),
                    filename = filename)

            # API Endpoint returned data, parse it for error codes, and
            # completeness. Retry if needed.
            parsed = self._parse(filename)
            vfileName = self.determineFilename(ids = idset,
                                               pid = os.getpid(),
                                               thread = current_thread().getName(),
                                               batch = batch,
                                               extension = file_extension)
            verified = self.verifyResponse(parseresponse = parsed,
                                           orig_filename = filename,
                                           filename = vfileName)
            if not verified:
                keep_running = True
                self.cleanup(filename)
                continue
            else:
                keep_running = False

    def _parse(self, file_name, format = None):
        """Parse the resulting file gathered from the Qualys API Endpoint.
        Since error codes for incorrect parameters, and possibly transient errors can be returned,
        Need to double check the final XML file for error codes, and completeness.
        If there is a transient error, then send back for a retry, otherwise raise an Exception.
        :param file_name: XML file to parse
        :return: dict or an Exception message if applicable.
        """
        logHandler.dynamicLogger("Parsing detection file %s" % file_name)
        total = 0
        logged = 0
        response = {'error': False}
        load_next_batch = False
        next_url = None

        try:
            context = iter(ET.iterparse(file_name, events = ('end',)))
            _, root = next(context)
            for event, elem in context:
                # Handle client errors
                if elem.tag == "RESPONSE":
                    code = elem.find('CODE')
                    error = elem.find('TEXT')
                    if code is not None:
                        logHandler.dynamicLogger(
                                "API Client ERROR. Code={errorcode}, Message={errorText}",
                                errorcode = code.text,
                                errorText = error.text)
                        elem.clear()
                        # We dont want to retry these client errors, so raise
                        # an Exception
                        raise APIResponseError("API Client Error. Code=%s, Message=%s" %
                                               (code.text, error.text))
                    elem.clear()
                # Internal Errors resulting in a 999 response will be DOCTYPE
                # GENERIC_RETURN
                elif elem.tag == "GENERIC_RETURN":
                    code = elem.find('RETURN')
                    if code is not None:
                        if "Internal error" in code.text:
                            elem.clear()
                            raise ET.ParseError("API Error - Found Internal Error. Clean up and Retry")
                    elem.clear()
                elif elem.tag == 'HOST':
                    total += 1
                    if self._process_root_element(elem):
                        logged += 1
                    elem.clear()
                elif elem.tag == "WARNING":
                    load_next_batch = True
                    next_url = elem.find('URL')
                    elem.clear()
                root.clear()
        except ET.ParseError as e:
            logHandler.dynamicLogger(
                    "Failed to parse API Output for endpoint {endpoint}. Message: {message}",
                    endpoint = self.config.baseURL,
                    message = str(e),
                    logLevel = ERROR)
            try:
                self.renameFile(oldFileName = file_name, newFileName = file_name + ".errored")
            except Exception as err:
                logHandler.dynamicLogger(
                        "Could not rename errored xml response filename. Reason: {message}",
                        message = str(err),
                        logLevel = ERROR)
            return str(e)

        logHandler.dynamicLogger("Parsed %d Host entries. Logged=%d" % (total, logged))
        self.remaining -= self.config.chunkSize
        self.completed += self.config.chunkSize
        return response

    def _process_root_element(self, elem):
        """Method used to parse and gather statistics on data retrieved.
        :param elem: XML element from iter(ET.iterparse())
        :return: True or False depending on successful parsing
        """

        host_fields_to_log = [
                'ID', 'IP', 'TRACKING_METHOD', 'DNS',
                'NETBIOS', 'OS', 'LAST_SCAN_DATETIME', 'TAGS']
        detection_fields_to_log = [
                'QID', 'TYPE', 'PORT', 'PROTOCOL', 'SSL', 'STATUS',
                'LAST_UPDATE_DATETIME', 'LAST_FOUND_DATETIME', 'FIRST_FOUND_DATETIME', 'LAST_TEST_DATETIME']
        fields_to_encode = ['OS', 'DNS', 'NETBIOS']

        if elem.tag == "HOST":
            plugin_output = []
            host_summary = []
            vulns_by_type = {
                    'POTENTIAL': 0,
                    'CONFIRMED': 0
            }
            vulns_by_status = {
                    'ACTIVE'   : 0,
                    'NEW'      : 0,
                    'FIXED'    : 0,
                    'RE-OPENED': 0
            }
            vulns_by_severity = {}
            other_stats = {}
            host_vuln_count = 0

            host_id = None
            for sub_ele in list(elem):
                name = sub_ele.tag
                if name == "ID":
                    host_id = sub_ele.text
                    name = "HOST_ID"
                    host_summary.append("HOST_ID=" + host_id)
                if name in host_fields_to_log:
                    if name == "TAGS":
                        host_tags = []
                        tag_elements = sub_ele.findall('./TAG/NAME')
                        for tag_element in list(tag_elements):
                            host_tags.append(tag_element.text)
                        val = ",".join(host_tags)
                    else:
                        val = sub_ele.text
                    if name in fields_to_encode:
                        val = val.encode('utf-8')
                    host_summary.append("%s=\"%s\"" % (name, val))

            if not host_id:
                logHandler.dynamicLogger("Unable to find host_id", logLevel = 'error')
                return False

            host_line = ", ".join(host_summary)
            dl = elem.find('DETECTION_LIST')
            if dl is not None:
                for detection in list(dl):
                    vuln_summary = []
                    qid_node = detection.find('QID')
                    if qid_node is not None:
                        host_vuln_count += 1
                        qid = int(qid_node.text)
                        type = detection.find('TYPE').text.upper()

                        status_element = detection.find('STATUS')
                        if status_element is not None:
                            status = detection.find('STATUS').text.upper()
                        else:
                            status = "-"

                        severity = detection.find('SEVERITY')
                        if severity is not None:
                            severity = severity.text
                        # else:
                        #    severity = self.get_qid_severity(qid)

                        if severity:
                            severity_key = 'SEVERITY_%s' % severity
                            vuln_summary.append('SEVERITY=%s' % severity)

                            vulns_by_severity[severity_key] = vulns_by_severity.get(
                                    severity_key, 0) + 1
                            if self.collect_advanced_host_summary:
                                # Break down, count of vulns by each severity
                                # and each status, type
                                type_severity_key = '%s_%s' % (
                                        type, severity_key)
                                status_severity_key = '%s_%s' % (
                                        status, severity_key)
                                other_stats[type_severity_key] = other_stats.get(
                                        type_severity_key, 0) + 1
                                other_stats[status_severity_key] = other_stats.get(
                                        status_severity_key, 0) + 1

                        for sub_ele in list(detection):
                            name = sub_ele.tag
                            val = sub_ele.text.upper()

                            if name == 'TYPE':
                                vulns_by_type[val] = vulns_by_type.get(
                                        val, 0) + 1

                            if name == 'STATUS':
                                vulns_by_status[val] = vulns_by_status.get(
                                        val, 0) + 1

                            if name in detection_fields_to_log:
                                vuln_summary.append("%s=\"%s\"" % (name, val))

                        if self.log_host_detections:
                            host_id_line = "HOSTVULN: "

                            if not self.log_host_details_in_detection:
                                host_id_line = "HOSTVULN: HOST_ID=%s," % host_id
                            else:
                                host_id_line = "HOSTVULN: %s," % host_line

                            if self.seed_file_enabled:
                                logHandler.dynamicLogger(
                                        "%s %s" %
                                        (host_id_line, ", ".join(vuln_summary)))

            if self.log_host_summary:

                host_summary = [
                        "HOSTSUMMARY: %s" %
                        host_line,
                        self.get_log_line_from_dict(vulns_by_severity),
                        self.get_log_line_from_dict(vulns_by_type),
                        self.get_log_line_from_dict(vulns_by_status)]

                if self.collect_advanced_host_summary:
                    host_summary.append(
                            self.get_log_line_from_dict(other_stats))
                if plugin_output:
                    host_summary.append(", ".join(plugin_output))

                host_summary.append("TOTAL_VULNS=%s" % host_vuln_count)
                if self.seed_file_enabled:
                    logHandler.dynamicLogger(", ".join(host_summary))

            self.host_logged += 1

            return True

    @staticmethod
    def get_log_line_from_dict(dict_obj):
        return ', '.join("%s=%r" % (key, val)
                         for (key, val) in dict_obj.items())

    def assets_coordinator(self, assets_idset_queue):
        """This method is entry point of each asset download thread.
        It pops out an id range entry from assets queue,
        and calls download_assets method passing id range as argument.
        :param assets_idset_queue: Queue object
        """
        keep_running = True
        while keep_running:
            try:
                logHandler.dynamicLogger("Getting id set from assets_idset_queue")
                id_range = assets_idset_queue.get(False)
                logHandler.dynamicLogger("Processing id set: {idrange}", idrange = id_range)
                self.download_assets(id_range)
                assets_idset_queue.task_done()
            except queue.Empty:
                logHandler.dynamicLogger("assets_idset_queue is empty. Exiting.")
                keep_running = False

    def download_assets(self, ids):
        """This method will invoke call_api method for asset/host API."""
        api_route = '/api/2.0/fo/asset/host/'
        params = dict(
                action = 'list',
                echo_request = 1,
                details = 'All/AGs',
                ids = ids,
                truncation_limit = 5000
        )
        batch = 1
        logHandler.dynamicLogger("Downloading assets data..")
        keep_running = True
        while keep_running:
            filename = self.config.outputDir + "/assets/assets_Range-%s_Proc-%s_%s_Batch-%d.xml" % (
                    ids, os.getpid(), current_thread().getName(), batch)
            response = self.get(
                    self.config.baseURL + api_route,
                    params,
                    response = XMLFileBufferedResponse(file_name = filename, logger = logHandler))
            logHandler.dynamicLogger(
                    "Wrote API response to {filename}",
                    filename = filename)
            logHandler.dynamicLogger("Parsing response XML...")
            tree = ET.parse(filename)
            root = tree.getroot()
            response_element = root.find('RESPONSE')
            if response_element is None:
                logHandler.dynamicLogger(
                        "RESPONSE tag not found in {filename}. Please check the file.",
                        filename = filename)
                keep_running = False
            warning_element = response_element.find('WARNING')
            if warning_element is None:
                logHandler.dynamicLogger("End of pagination for ids {ids}", ids = ids)
                keep_running = False
            else:
                next_page_url = warning_element.find('URL').text
                params = utils.get_params_from_url(url = next_page_url)
                batch += 1

    def checkLimits(self):
        """Check the current subscription API limits
        to ensure we dont start too many threads
        """
        limits = {
                'x-concurrency-limit-limit'  : APIResponse.responseHeaders.get('X-Concurrency-Limit-Limit', "0"),
                'x-ratelimit-remaining'      : APIResponse.responseHeaders.get('X-RateLimit-Remaining', "0"),
                'x-concurrency-limit-running': APIResponse.responseHeaders.get('X-Concurrency-Limit-Running', "0"),
                'x-ratelimit-window-sec'     : APIResponse.responseHeaders.get('X-RateLimit-Window-Sec', "0")
        }

        if int(limits.get('x-concurrency-limit-limit')) < self.config.numDetectionThreads:
            logHandler.dynamicLogger(
                    'Configured number of threads: {numthreads} is higher than subscription limit of '
                    'x-concurrency-limit-limit: {limit}, Please verify subscription limits and reduce '
                    'configured threads to use.',
                    numthreads = self.config.numDetectionThreads,
                    limit = int(limits.get('x-concurrency-limit-limit')))
            exit()
        else:
            logHandler.dynamicLogger(
                    'Configured number of threads: {numthreads}, '
                    'x-concurrency-limit-limit: {limit}, ',
                    numthreads = self.config.numDetectionThreads,
                    limit = int(limits.get('x-concurrency-limit-limit'))
            )

    def startThreads(self,
                     target = None,
                     assetids = None,
                     ThreadName = None,
                     Queue = None,
                     threadcount = 0):
        """
        :param target: is the callable object to be invoked by the Thread.run() method.
                       Defaults to None, meaning nothing is called.
        :param assetids: utils.IDset split into chunks for Enqueuing
        :param ThreadName: is the thread name. By default, a unique name is constructed in
                            the form "Thread-N" where N is a small decimal number.
        :param Queue: Queue object.
        :param threadcount: Total number of threads to start.
        :return: Thread.__repr__ - list of Threads and their status
        """
        workers = []
        batchcnt = len(assetids)
        if batchcnt < threadcount:
            threadcount = batchcnt
            self.config.numDetectionThreads = batchcnt

        logHandler.dynamicLogger("Starting {numthreads} threads for {ThreadType} download. Total batches: {cnt} ...",
                                 numthreads = threadcount,
                                 ThreadType = ThreadName,
                                 cnt = batchcnt
                                 )

        for id_chunk in assetids:
            id_range = id_chunk.tostring()
            queue.Queue.put(Queue, id_range)

        for i in range(0, threadcount):
            thread = Thread(target = target, name = ThreadName + '-' + str(i), args = (Queue,))
            thread.setDaemon(True)
            thread.start()
            workers.append(thread)
            logHandler.dynamicLogger("Started {ThreadName} thread # {threadnum}", ThreadName = ThreadName,
                                     threadnum = i)
        return workers

    def execute(self):
        """Main method of this code."""
        startTime = time.time()
        self.parse_options()

        assetWorkers = []
        detectionWorkers = []
        hostAssetWorkers = []
        if self.downloadHostAssets:
            self.id_set = self.get_asset_ids_portal()
        else:
            self.id_set = self.get_asset_ids()
        logHandler.dynamicLogger(self.logMessages.message(GOTASSETSRESPONSE),
                                 numids = len(self.id_set),
                                 size = self.receivedBytes)

        self.total_hosts = self.remaining = len(self.id_set)
        ids = utils.IDSet()

        if self.config.pullbyip:
            for i in self.id_set:
                try:
                    # skip invalid IPv4 addresses
                    ipaddress.IPv4Address(str(i))
                    ipid = utils.ip2int(i)
                    ids.addString(str(ipid))
                except (AddressValueError, NetmaskValueError):
                    continue
        else:
            for i in self.id_set:
                ids.addString(i)
        chunks = ids.split(self.config.chunkSize)

        if self.downloadAssets:
            assetWorkers = self.startThreads(
                    threadcount = self.config.numAssetThreads,
                    assetids = chunks,
                    target = self.assets_coordinator,
                    ThreadName = 'assetsThread',
                    Queue = queue.Queue())

        if self.downloadDetections:
            detectionWorkers = self.startThreads(
                    threadcount = self.config.numDetectionThreads,
                    assetids = chunks,
                    target = self.vm_detection_coordinator,
                    ThreadName = 'detectionThread',
                    Queue = queue.Queue())

        if self.downloadHostAssets:
            hostAssetWorkers = self.startThreads(
                    threadcount = self.config.numHostAssetThreads,
                    assetids = chunks,
                    target = self.portal_assethost_coordinator,
                    ThreadName = 'hostAssetThread',
                    Queue = queue.Queue())

        workers = assetWorkers + detectionWorkers + hostAssetWorkers

        # Block until all threads have completed
        for worker in workers:
            worker.join()

        duration = self.callDuration(startTime = startTime)
        logHandler.dynamicLogger(self.logMessages.message(TOTALDOWNLOADED),
                                 duration = duration,
                                 bytes = self.receivedBytes,
                                 speed = round(self.receivedBytes / duration / 1000, 2),
                                 hosts = self.host_logged,
                                 batchsize = self.config.chunkSize)

    def parse_options(self):
        """This method parses all options given in command line."""
        username = None
        password = None
        fmt = Formatter()
        description = "Multithreading for Qualys Host Detection v2 API"
        epilog = "Examples:\n" \
                 "Using a batch size of 500 hosts, with 5 threads in parallel," \
                 " for all hosts processed since 2019-04-01:\n\n" \
                 "localhost:HostDetection testuser$ python qPyMultiThread.py -u quays_nw93 -c 500 -d 5 -v 2019-04-01\n" \
                 " QG Password:\n" \
                 " 2019-04-05 20:16:07-0700 INFO: [qPyMultiThread] Validating Credentials to " \
                 "https://qualysapi.qualys.com/msp/about.php ...\n" \
                 " 2019-04-05 20:16:13-0700 INFO: [qPyMultiThread] Got response from Proxy, " \
                 "and msp/about.php endpoint...\n" \
                 " 2019-04-05 20:16:13-0700 INFO: [qPyMultiThread] Validation successful, " \
                 "proceeding. verifying limits\n" \
                 " \n\n"
        option = OptionParser(formatter = fmt, description = description, epilog = epilog)
        parser = OptionGroup(option, title = "Connection Options")

        parser.add_option("-s",
                          dest = "baseURL",
                          default = "https://qualysapi.qualys.com",
                          help = "Qualys API Server. Defaults to US Platform 1")
        parser.add_option("-u",
                          dest = "username",
                          help = "Qualys API Username")
        parser.add_option("-p",
                          dest = "password",
                          help = "Qualys API Password")
        parser.add_option("-P",
                          dest = "useProxy",
                          default = False,
                          help = "Enable/Disable using Proxy")
        parser.add_option("-x",
                          dest = "proxyHost",
                          default = 'None',
                          help = "Proxy to use e.g http://localhost:3128")
        conf = OptionGroup(option, title = "Configuration Options")
        conf.add_option("-a",
                        dest = "numAssetThreads",
                        default = 0,
                        help = "Number of threads to fetch host assets")
        conf.add_option("-d",
                        dest = "numDetectionThreads",
                        default = 0,
                        help = "Number of threads to fetch host detections")
        conf.add_option("-z",
                        dest = "numHostAssetThreads",
                        default = 0,
                        help = "Number of threads to fetch Portal HostAssets")
        conf.add_option("-v",
                        dest = "detectionDelta",
                        default = "2000-01-01",
                        help = "Only pull Data scanned after this date.")
        conf.add_option("-c",
                        dest = "chunkSize",
                        default = 1000,
                        help = "Batch size of Host ID chunks")
        conf.add_option("-i",
                        dest = "pullbyip",
                        default = False,
                        action = 'store_false',
                        help = "Enable pulling data by batches of IPs instead of host ids")
        conf.add_option("-D",
                        dest = "debug",
                        default = False,
                        action = 'store_false',
                        help = "Enable Debug Logging")
        option.add_option_group(parser)
        option.add_option_group(conf)
        (options, values) = option.parse_args()

        if len(sys.argv[1:]) == 0:
            option.print_help()
            exit(1)

        username = options.username
        password = options.password

        if username is None or username == '':
            username = raw_input("QG Username: ")

        if password is None or password == '':
            import getpass
            password = getpass.getpass("QG Password:")

        self.config = Configuration(
                username = username,
                password = password,
                baseURL = options.baseURL,
                useProxy = options.useProxy,
                proxyHost = options.proxyHost,
                validateURL = '/msp/about.php',
                numAssetThreads = int(options.numAssetThreads),
                numDetectionThreads = int(options.numDetectionThreads),
                numHostAssetThreads = int(options.numHostAssetThreads),
                chunkSize = int(options.chunkSize),
                detectionDelta = options.detectionDelta,
                debug = options.debug,
                NoRetryCodes = NORETRYCODES,
                key = None,
                pullbyip = options.pullbyip,
                default_settings = None)

        if self.config.debug:
            logHandler.enableDebug(True)
        logHandler.enableLogger()
        logHandler.dynamicLogger('Debug logging enabled', logLevel = DEBUG)

        if self.config.useProxy is True:
            if self.config.proxyHost == 'None':
                raise CongifurationException("Please set the Proxy Host with -P or --proxy when using -x")
            try:
                tryproxy = self.validate()
                if tryproxy:
                    logHandler.dynamicLogger("Proxy Check successful, proceeding.")
                else:
                    logHandler.dynamicLogger("Proxy Check failed, exiting")
                    exit()
            except Exception as e:
                raise Exception("Proxy Check Failed: %s" % e)

        if self.config.numAssetThreads >= 1:
            self.downloadAssets = True
        if self.config.numDetectionThreads >= 1:
            self.downloadDetections = True
        if self.config.numHostAssetThreads >= 1:
            self.downloadHostAssets = True

        if not self.downloadAssets and not self.downloadDetections and not self.downloadHostAssets:
            raise CongifurationException(
                    "Please set at least one of -a / -d / -h option You haven't set any of them with valid value")
        try:
            v = self.validate()
            if v:
                logHandler.dynamicLogger("Validation successful, proceeding. verifying limits")
                self.checkLimits()
            else:
                exit()
        except Exception as e:
            import traceback
            logHandler.dynamicLogger(
                    self.logMessages.message(TRACEUNEXPECTED),
                    'error',
                    traceback = traceback.format_exc())
            raise Exception("Validation Failed: %s" % e)


if __name__ == "__main__":
    qapi = APIClient()
    qapi.execute()
