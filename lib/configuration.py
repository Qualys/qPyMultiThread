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
import os
import sys
import time


class Configuration(object):
    def __init__(self,
                 username = None,
                 password = None,
                 baseURL = None,
                 useProxy = None,
                 proxyHost = None,
                 validateURL = '/msp/about.php',
                 numAssetThreads = None,
                 numDetectionThreads = None,
                 numHostAssetThreads = None,
                 chunkSize = None,
                 detectionDelta = None,
                 outputDir = './output',
                 tempDirectory = './tmp',
                 logDir = './log',
                 debug = False,
                 NoRetryCodes = [],
                 key = None,
                 pullbyip = 0,
                 default_settings = None):
        self.load_from_file = False
        self.username = username
        self.password = password
        self.baseURL = baseURL
        self.tempDirectory = tempDirectory
        self.outputDir = outputDir
        self.logDir = logDir
        self.useProxy = useProxy
        self.proxyHost = proxyHost
        self.validateURL = validateURL
        self.numAssetThreads = numAssetThreads
        self.numDetectionThreads = numDetectionThreads
        self.numHostAssetThreads = numHostAssetThreads
        self.detectionDelta = detectionDelta
        self.chunkSize = chunkSize
        self.debug = debug
        self.key = key
        self.pullbyip = pullbyip
        self.NoRetryCodes = NoRetryCodes
        self.basedir = self.get_script_path()

        self.setupFileStructure()

        # Verify directory structure is set up already
        self.check_tree(self.outputDir)
        self.check_tree(self.tempDirectory)
        self.check_tree(self.logDir)
        self.check_tree(self.outputDir + '/assets')
        self.check_tree(self.outputDir + '/vm_detections')
        self.check_tree(self.outputDir + '/portal')
        time.sleep(1)

    def get_script_path(self):
        return os.path.dirname(os.path.realpath(sys.argv[0]))

    def setupFileStructure(self):
        filebase = ['./output', './output/assets', './output/vm_detections', './log']
        for dir in filebase:
            if not os.path.exists(dir):
                print("Directory %s doesn't exist, creating.." % dir)
                try:
                    os.makedirs(dir)
                except OSError:
                    print("Error while creating output directory.")
                    raise

    def check_tree(self, filepath):
        '''
        This method writes given response into given file.
        If complete path to file does not exist, it will create it.
        '''
        filename = filepath + "/filename.log"
        # qlogger.debug("Checking: %s Returned: %s" % (filepath, os.path.exists(os.path.dirname(filename))))
        if not os.path.exists(os.path.dirname(filename)):
            try:
                print("Making Directory: %s" % os.path.dirname(filename))
                os.makedirs(os.path.dirname(filename))
            except OSError:
                print("Error while creating director for: %s" % filepath)
                raise