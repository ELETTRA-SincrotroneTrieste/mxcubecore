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
import gevent
import PyTango

from mxcubecore.HardwareObjects.abstract.AbstractShutter import AbstractShutter
from mxcubecore import  hwo_header_log


class XRD1SafetyShutter(AbstractShutter):

    def __init__(self, *args, **kwargs):

        super(XRD1SafetyShutter, self).__init__(*args, **kwargs)
        self.ch_shutter_state = None
        self.cmd_start_method = None
        self.timeout = None  # [s]

    @hwo_header_log
    def init(self):

        super(XRD1SafetyShutter, self).init()
        self.ch_shutter_state = self.get_channel_object("ss_shutter_state",
                                                        optional=False)
        self.cmd_start_method = self.get_command_object("start_method")
        self.timeout = self.get_property("timeout")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_shutter_state, "update",
                     lambda tango_val: self.update_value(self.value_to_enum(tango_val)))

        self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_value(self):

        try:
            value = self.value_to_enum(self.ch_shutter_state.get_value())
            self.log.info(f"Read the status of the \"{self.username}\" "
                          f"(it's \"{value.name}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the status of the \"{self.username}\" from " \
                      f"the attribute \"{self.ch_shutter_state.attribute_name}\" of " \
                      f"the tango device \"{self.ch_shutter_state.device_name}\" "
            self.log.exception(err_msg)
            value = self.value_to_enum(None)
        return value

    @hwo_header_log
    def _set_value(self, value):

        tango_value = value.value
        self.update_state(self.STATES.BUSY)
        try:
            if self.get_value() != value:
                if value == self.VALUES.OPEN:
                    command = "Open_Last_BL_Valves"
                else:
                    command = "Close_Last_BL_Valves"
                self.cmd_start_method(command)
                time.sleep(0.2)
                with gevent.Timeout(
                    self.timeout, TimeoutError(f"Timed out. State of the "
                                               f"\"{self.username}\" didn't change "
                                               f"after {self.timeout} sec")
                ):
                    while self.get_value() != value:
                        gevent.sleep(self.ch_shutter_state.polling / 1000)
                self.log.info(f"State of the \"{self.username}\" changed to"
                              f" \"{value.name}\"")
            else:
                self.log.info(f"State of the \"{self.username}\" is already"
                              f" \"{value.name}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the state of the \"{self.username}\" " \
                      f"setting the command \"{self.cmd_start_method.command}\" of " \
                      f"the tango device \"{self.cmd_start_method.device_name}\" to" \
                      f" \"{tango_value}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        except TimeoutError as e:
            self.user_log.error(str(e))
            raise RuntimeError(str(e))
        finally:
            self.update_state(self.STATES.READY)
        return value
