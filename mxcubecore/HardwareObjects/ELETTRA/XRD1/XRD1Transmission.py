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

import time

import PyTango
import gevent

from mxcubecore.HardwareObjects.abstract.AbstractTransmission \
    import AbstractTransmission
from mxcubecore import hwo_header_log


class XRD1Transmission(AbstractTransmission):

    def __init__(self, name):

        super(XRD1Transmission, self).__init__(name)
        self.ch_attenuation = None
        self.tolerance = None
        self.timeout = None

    @hwo_header_log
    def init(self):

        super(XRD1Transmission, self).init()
        self.ch_attenuation = self.get_channel_object('attenuation', optional=False)

        # SIGNALS CONNECTIONS
        self.connect(self.ch_attenuation, 'update', self.on_update)

        self.update_state(self.STATES.READY)

    @hwo_header_log
    def on_update(self, tango_val):

        if tango_val is None:
            self.update_state()
        else:
            self.update_value(self.get_transmission(tango_val))

    @hwo_header_log
    def get_value(self):

        try:
            value = self.get_transmission(self.ch_attenuation.get_value())
            self.log.info(f"Read the value of the \"{self.username}\" "
                          f"(it's \"{value}{self.unit}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read \"{self.username}\" from the " \
                      f"attribute \"{self.ch_attenuation.attribute_name}\" of the " \
                      f"tango device \"{self.ch_attenuation.device_name}\" " \
                      f"(transmission = 100. - attenuation)"
            self.log.exception(err_msg)
            value = None
        return value

    @hwo_header_log
    def _set_value(self, value):

        attenuation = 100. - value
        self.update_state(self.STATES.BUSY)
        try:
            self.ch_attenuation.set_value(attenuation)  # It's synchronous
            self.log.info(f"Value of the \"{self.username}\" set to "
                          f"\"{value}{self.unit}\"")

        except PyTango.DevFailed:
            err_msg = f"Failed to change the value of the \"{self.username}\" setting" \
                      f" the attribute \"{self.ch_attenuation.attribute_name}\" of" \
                      f" the tango device \"{self.ch_attenuation.device_name}\" to" \
                      f" \"{attenuation}\" (attenuation = 100. - transmission)"
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        finally:
            self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_transmission(self, attenuation):
        # Min function avoids transmission=0 which prevent to show no trasmission value
        # (i.e. "- %") in frontend
        return 100. - min(attenuation, 99.999999999)
