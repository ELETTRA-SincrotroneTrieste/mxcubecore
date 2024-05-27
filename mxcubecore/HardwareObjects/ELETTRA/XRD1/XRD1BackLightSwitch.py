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

from mxcubecore.HardwareObjects.abstract.AbstractNState import AbstractNState
from mxcubecore import hwo_header_log


class XRD1BackLightSwitch(AbstractNState):

    # TODO Evaluate whether coupling/synchronizing the Tango state with the HWO state

    def __init__(self, name):

        super(XRD1BackLightSwitch, self).__init__(name)
        self.ch_light_state = None

    @hwo_header_log
    def init(self):

        super(XRD1BackLightSwitch, self).init()
        self.ch_light_state = self.get_channel_object("light_state", optional=False)

        # SIGNALS CONNECTIONS
        self.connect(self.ch_light_state,
                     "update",
                     lambda tango_val: self.update_value(self.value_to_enum(tango_val)))

        self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_value(self):

        try:
            value = self.value_to_enum(self.ch_light_state.get_value())
            self.log.info(f"Read the status of \"{self.username}\""
                          f" (it's \"{value.name}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the status of the \"{self.username}\" from " \
                      f"the attribute \"{self.ch_light_state.attribute_name}\" of " \
                      f"the tango device \"{self.ch_light_state.device_name}\" "
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value

    @hwo_header_log
    def _set_value(self, value):

        self.update_state(self.STATES.BUSY)
        tango_value = value.value
        try:
            self.ch_light_state.set_value(tango_value)
            gevent.sleep(1)
            self.log.info(f"State of the \"{self.username}\" changed to"
                          f" \"{value.name}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the state of the \"{self.username}\" setting" \
                      f" the attribute \"{self.ch_light_state.attribute_name}\" of " \
                      f"the tango device \"{self.ch_light_state.device_name}\" to " \
                      f"\"{tango_value}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        finally:
            self.update_state(self.STATES.READY)
