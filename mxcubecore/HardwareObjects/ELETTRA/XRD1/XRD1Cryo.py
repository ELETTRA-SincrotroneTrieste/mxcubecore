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
from mxcubecore.HardwareObjects.abstract.AbstractActuator import AbstractActuator
from mxcubecore import hwo_header_log


class XRD1Cryo(AbstractActuator):

    unit = "K"

    def __init__(self, name):

        super(XRD1Cryo, self).__init__(name)
        self.ch_gas_temperature = None

    @hwo_header_log
    def init(self):

        super(XRD1Cryo, self).init()
        self.ch_gas_temperature = self.get_channel_object("gas_temperature",
                                                          optional=False)

        # SIGNALS CONNECTIONS
        self.connect(self.ch_gas_temperature, "update", self.update_value)

        self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_value(self):

        try:
            value = self.ch_gas_temperature.get_value()
            self.log.info(f"Read the gas temperature of the \"{self.username}\" "
                          f"(it's \"{value} {self.unit}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the gas temperature of the \"{self.username}\"" \
                      f" from the attribute " \
                      f"\"{self.ch_gas_temperature.attribute_name}\" of the tango " \
                      f"device \"{self.ch_gas_temperature.device_name}\" "
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value
