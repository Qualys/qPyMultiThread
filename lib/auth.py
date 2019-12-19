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
Ported from the requests package.

This module contains the authentication handlers.
"""
import base64
import sys

_ver = sys.version_info

#: Python 2.x?
is_py2 = (_ver[0] == 2)

#: Python 3.x?
is_py3 = (_ver[0] == 3)

builtin_str = str


class HTTPBasicAuth(object):
    """Attaches HTTP Basic Authentication to the given Request object."""

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __eq__(self, other):
        return all([
                self.username == getattr(other, 'username', None),
                self.password == getattr(other, 'password', None)
        ])

    def __ne__(self, other):
        return not self == other

    def __call__(self, r):
        r.headers['Authorization'] = _basic_auth_str(self.username, self.password)
        return r


def _basic_auth_str(username, password):
    """Returns a Basic Auth string."""
    username = str(username)
    password = str(password)

    if isinstance(username, str):
        username = username.encode('latin1')

    if isinstance(password, str):
        password = password.encode('latin1')

    authstr = 'Basic ' + to_native_string(
            base64.b64encode(b':'.join(((username, password))).strip()
                             ))
    return authstr


def to_native_string(string, encoding = 'ascii'):
    """
    Given a string object, regardless of type, returns a representation of
    that string in the native string type, encoding and decoding where
    necessary. This assumes ASCII unless told otherwise.
    """
    if isinstance(string, builtin_str):
        out = string
    else:
        if is_py2:
            out = string.encode(encoding)
        else:
            out = string.decode(encoding)

    return out
