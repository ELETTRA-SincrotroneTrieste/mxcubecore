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
from mxcubecore import hwo_header_log


class XRD1BackLightLevel(AbstractMotor):

    unit = "%"

    def __init__(self, name):
        super(XRD1BackLightLevel, self).__init__(name)
        self.ch_light_level = None

    @hwo_header_log
    def init(self):

        super(XRD1BackLightLevel, self).init()
        self.ch_light_level = self.get_channel_object("light_level_percentage",
                                                      optional=False)

        # SIGNALS CONNECTIONS
        self.connect(self.ch_light_level, "update", self.update_value)

        self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_value(self):

        try:
            value = int(self.ch_light_level.get_value())
            self.log.info(f"Read the level of the \"{self.username}\""
                          f" (it's \"{value}{self.unit}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the level of the \"{self.username}\" from the" \
                      f" attribute \"{self.ch_light_level.attribute_name}\" of the" \
                      f" tango device \"{self.ch_light_level.device_name}\""
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value

    @hwo_header_log
    def _set_value(self, value):

        value = int(value)
        self.update_state(self.STATES.BUSY)
        try:
            self.ch_light_level.set_value(value)
            gevent.sleep(0.5)
            self.log.info(f"Level of the \"{self.username}\" set to"
                          f" \"{value}{self.unit}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the level of the \"{self.username}\" setting" \
                      f" the attribute \"{self.ch_light_level.attribute_name}\"" \
                      f" of the tango device \"{self.ch_light_level.device_name}\"" \
                      f" to \"{value}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        finally:
            self.update_state(self.STATES.READY)
