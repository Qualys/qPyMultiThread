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
import copy
import sys

_ver = sys.version_info

#: Python 2.x?
is_py2 = (_ver[0] == 2)

if is_py2:
    quit()

from urllib.parse import urlencode
from urllib.request import Request

from .auth import _basic_auth_str
from .__version__ import __version__


class APIRequest(object):

    def __init__(self,
                 type = None,
                 api_route = None,
                 data = None,
                 username = None,
                 password = None,
                 hash = 123):
        self.username = username if username is not None else None
        self.password = password if password is not None else None
        self.type = type if type is not None else 'GET'
        self.timeout = 3600
        self.hostcount = 0
        self.data = data
        self.api_route = api_route
        self.parameters = ''
        self.hash = hash if hash is not None else None
        self.headers = self.default_headers()

    def default_user_agent(self, name = "qPyMultiThread"):
        """
        Return a string representing the default user agent.

        :return: str
        """
        return '%s/%s Request Hash %s HostCount %s Parameters %s' % (name,
                                                                     __version__,
                                                                     self.hash,
                                                                     self.hostcount,
                                                                     self.parameters)

    def default_headers(self):
        """
        Return a default set of headers used in all Requests

        :return: dict of default HTTP headers
        """

        return {
                'User-Agent'      : self.default_user_agent(),
                'X-Requested-With': self.default_user_agent(),
                'Connection'      : 'keep-alive'
        }

    def build_headers(self):
        """
           Builds the HTTP headers required by the API client.
           Adds auth, and other headers if necessary.
        """

        self.headers = self.default_headers()
        self.headers['Authorization'] = _basic_auth_str(self.username, self.password)

        if self.type == "Portal":
            self.headers['Content-Type'] = 'text/xml'

        return self.headers

    def build_request(self):
        """
        Build the urllib2 request object with complete url, parameters and header

        :param api_route: Full URL to make a Request.
        :param data: file-like object to send in the body of the Request
        :return: :class:urllib2 Request object
        """
        data = '' if self.data is None else self.data
        # For a request that includes URL parameters, urlencode them
        # otherwise, leave file-like data alone
        if isinstance(data, dict):
            # Create a copy, to remove the ids parameter and post the rest in
            # the X-Requested-With header
            paramscopy = copy.deepcopy(data)

            data = urlencode(data).encode('utf-8')

            if 'ids' in paramscopy:
                self.hostcount = paramscopy['ids'].count(',') + 1
                del paramscopy['ids']
                paramscopy = '&'.join("%s=%r" % (key, val) for (key, val) in paramscopy.items())
                self.parameters = paramscopy.replace("'", "")
        else:
            self.type = 'Portal'
        self.build_headers()
        return Request(self.api_route,
                       data = data,
                       headers = self.headers
                       )
