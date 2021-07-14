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
import sys

_ver = sys.version_info

#: Python 2.x?
is_py2 = (_ver[0] == 2)

#: Python 3.x?
is_py3 = (_ver[0] == 3)

if is_py2:
    from urlparse import urlparse, parse_qsl
if is_py3:
    from urllib.parse import urlparse, parse_qsl

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

MAX_IPV4_ADDRESS = 4294967295


class InvalidIPException(Exception):
    """
    Thrown by IPContainer whenever a bad IP/range is encountered
    """

    def __init__(self, ips, message):
        self.ips = ips
        # self.message = message
        self.msg = message

    def __str__(self):
        return "Invalid IP(s) --> '%s' : %s" % (self.ips, self.msg)


def ip2int(ipstr):
    """
    Convert IPv4 address into its packed integer representation
    """
    parts = [int(x) for x in ipstr.split(".")]
    if len(parts) != 4:
        raise InvalidIPException(ipstr, "Could not convert to integer representation")
    # end if

    int_ip = 0
    for part in parts:
        int_ip = (int_ip << 8) | part

    return int_ip


def int2ip(ip_int):
    """
    Convert an IP in packed integer representation into a string
    """
    if ip_int < 0 or ip_int > MAX_IPV4_ADDRESS:
        raise InvalidIPException(str(ip_int), "Not an unsigned 32-bit integer")
    mask = 0xff
    parts = []
    # loop four times
    for i in range(4):
        octet = ip_int & mask
        parts.append(str(octet))
        ip_int = ip_int >> 8
    # end while
    parts.reverse()
    return ".".join(parts)


# end ip2int


def get_params_from_url(url):
    """
    This method returns dictionary of URL parameters.
    """
    return dict(parse_qsl(urlparse(url).query))


def chunk_id_set(self, id_set, num_threads):
    """
    This method chunks given id set into sub-id-sets of given size
    :type id_set: Sized
    """
    for i in range(0, len(id_set), num_threads):
        yield id_set[i:i + num_threads]


class IDSet(object):
    """Simple implementation of IDSet; makes a lot of assumptions"""

    def __init__(self):
        self.items = {}

    def addString(self, id_or_range):
        if id_or_range.count("-", 1) == 1:
            (left, right) = id_or_range.split("-", 1)
            self.items[int(left)] = int(right)
        else:
            self.items[int(id_or_range)] = int(id_or_range)
            # self.items[ip2int(id_or_range)] = ip2int(id_or_range)

    def addRange(self, left, right):
        self.items[left] = right

    def count(self):
        id_count = 0
        for k in sorted(self.items):
            id_count = id_count + ((self.items[k] - k) + 1)

        return id_count

    def iterRanges(self):

        def theRanges():
            for k in sorted(self.items):
                yield k, self.items[k]

        return theRanges()

    def split(self, max_size_of_list):
        """Splits the current IDSet into multiple IDSets based on the max_size_of_list
        :param max_size_of_list:
        :return: IDSet Object
        """
        return IDSet.__split(list(self.iterRanges()), max_size_of_list)

    def tostring(self):
        out = []
        for k, v in self.iterRanges():
            if k == v:
                out.append(k)
            else:
                out.append("%d-%d" % (k, v))
        return ",".join(map(str, out))

    # end tostring

    @staticmethod
    def __split(ranges, max_size_of_list):

        lists = []
        counter = 0
        cur_list = IDSet()
        for k, v in ranges:
            t_size = v - k + 1
            if counter + t_size < max_size_of_list:
                counter += t_size
                cur_list.addRange(k, v)
            elif counter + t_size == max_size_of_list:
                cur_list.addRange(k, v)
                lists.append(cur_list)
                cur_list = IDSet()
                counter = 0
            else:
                room_left = max_size_of_list - counter
                cur_list.addRange(k, k + room_left - 1)
                lists.append(cur_list)

                # at this point, our remaining range is k+room_left to v, and that could be multiple ranges as well
                lists = lists + IDSet.__split([(k + room_left, v)], max_size_of_list)
                # get the last list
                cur_list_count = lists[-1].count()
                if cur_list_count < max_size_of_list:
                    cur_list = lists.pop()
                    counter = cur_list_count
                else:
                    cur_list = IDSet()
                    counter = 0

        if cur_list.count() > 0:
            lists.append(cur_list)

        return lists
