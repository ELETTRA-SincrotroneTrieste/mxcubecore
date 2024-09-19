# encoding: utf-8
#
#  Project: MXCuBE
#  https://github.com/mxcube
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

__copyright__ = """ Copyright © 2020 by the MXCuBE collaboration """
__credits__ = ["ELETTRA"]
__license__ = "LGPLv3+"
__category__ = "General"


import os
import time
import logging
import subprocess

import gevent

from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore import hwo_header_log


class XRD1OfflineProcessing(HardwareObject):

    def __init__(self, name):
        HardwareObject.__init__(self, name)

    @hwo_header_log
    def init(self):
        pass

    @hwo_header_log
    def execute_fastdp(self, params_dict):
        pass

    @hwo_header_log
    def execute_autoproc(self, params_dict):
        pass

    @hwo_header_log
    def execute_adxv(self, params_dict):
        pass

    @hwo_header_log
    def create_autoproc_input(self, params_dict):
        pass