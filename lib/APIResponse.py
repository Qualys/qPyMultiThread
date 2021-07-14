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
import abc
import sys
# Check Python version so we can import the appropriate libraries
_ver = sys.version_info

#: Python 2.x?
is_py2 = (_ver[0] == 2)
#: Python 3.x?
is_py3 = (_ver[0] == 3)
if is_py2:
    from httplib import IncompleteRead
if is_py3:
    from http.client import IncompleteRead


class APIResponse():
    __metaclass__ = abc.ABCMeta
    _response = None
    _responseHeaders = None
    _totalSize = None

    @property
    def response(self):
        return self._response

    @property
    def totalSize(self):
        return self._totalSize

    @property
    def responseHeaders(self):
        return self._responseHeaders

    @response.setter
    def response(self, value):
        self._response = value

    @totalSize.setter
    def totalSize(self, value):
        self._totalSize = value

    @responseHeaders.setter
    def responseHeaders(self, value):
        self._responseHeaders = value

    def get_response(self):
        return self._handle_and_return_response()

    @abc.abstractmethod
    def _handle_and_return_response(self):
        pass


class APIResponseError(Exception):
    pass


class XMLFileBufferedResponse(APIResponse):

    def __init__(self,
                 file_name = None,
                 read_chunk_size = 1,
                 logger = None):
        self._file_name = file_name
        self._responseHeaders = None
        self.totalSize = None
        self.read_chunk_size = read_chunk_size
        self.logger = logger

    @property
    def file_name(self):
        return self._file_name

    def build_response(self):
        """Iterates over the response data. This avoids reading the content
        at once into memory for large responses.  The chunk size is the
        number of bytes it should read into memory on each iteration. The last
        Chunk will contain < chunk_size and trigger the EOF for a response.
        """
        while True:
            try:
                if self.totalSize > 3000 and self.read_chunk_size == 1:
                    self.logger.dynamicLogger('First 3KB written, increasing chunk size.', logLevel = 'debug')
                    self.read_chunk_size = 4 * 1024
                chunk = self.response.read(self.read_chunk_size)
            except IncompleteRead as e:
                chunk = e.partial
            if not chunk:
                break
            with open(self.file_name, 'ab') as fp:
                self.totalSize += len(chunk)
                fp.write(chunk)

    def _handle_and_return_response(self):
        self.totalSize = 0
        self.build_response()
        return True

