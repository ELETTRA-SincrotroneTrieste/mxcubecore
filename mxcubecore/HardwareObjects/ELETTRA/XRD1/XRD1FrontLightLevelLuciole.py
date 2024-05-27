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

import PyTango
import gevent

from mxcubecore.HardwareObjects.abstract.AbstractMotor import AbstractMotor
from mxcubecore import  hwo_header_log


class XRD1FrontLightLevelLuciole(AbstractMotor):

    unit = "%"

    def __init__(self, name):
        super(XRD1FrontLightLevelLuciole, self).__init__(name)
        self.ch_light_level_ch1 = None
        self.ch_light_level_ch2 = None

    @hwo_header_log
    def init(self):

        super(XRD1FrontLightLevelLuciole, self).init()
        self.ch_light_level_ch1 = self.get_channel_object("light_level_percentage_ch1",
                                                          optional=False)
        self.ch_light_level_ch2 = self.get_channel_object("light_level_percentage_ch2",
                                                          optional=False)

        # SIGNALS CONNECTIONS
        self.connect(self.ch_light_level_ch1, "update", lambda _: self.update_value())
        self.connect(self.ch_light_level_ch2, "update", lambda _: self.update_value())

        self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_value(self):

        try:
            value = int((self.ch_light_level_ch1.get_value() +
                         self.ch_light_level_ch2.get_value()) / 2)
            self.log.info(f"Read the level of the \"{self.username}\" "
                          f"(it's \"{value}{self.unit}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the level of the \"{self.username}\" from the " \
                      f"attributes \"{self.ch_light_level_ch1.attribute_name}/" \
                      f"{self.ch_light_level_ch2.attribute_name}\" of the" \
                      f" tango device \"{self.ch_light_level_ch1.device_name}\" "
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value

    @hwo_header_log
    def _set_value(self, value):

        value = int(value)
        self.update_state(self.STATES.BUSY)
        try:
            self.ch_light_level_ch1.set_value(value)
            self.ch_light_level_ch2.set_value(value)
            gevent.sleep(0.5)
            self.log.info(f"Level of the \"{self.username}\" set to "
                          f"\"{value}{self.unit}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the level of the \"{self.username}\" setting" \
                      f" the attributes \"{self.ch_light_level_ch1.attribute_name}/" \
                      f"{self.ch_light_level_ch2.attribute_name}\" of the " \
                      f"tango device \"{self.ch_light_level_ch1.device_name}\" to" \
                      f" \"{value}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        finally:
            self.update_state(self.STATES.READY)
