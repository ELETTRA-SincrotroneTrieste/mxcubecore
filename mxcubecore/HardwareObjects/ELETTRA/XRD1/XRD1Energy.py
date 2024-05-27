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

from mxcubecore.BaseHardwareObjects import HardwareObjectState
from mxcubecore.HardwareObjects.abstract.AbstractEnergy import AbstractEnergy
from mxcubecore import hwo_header_log


class XRD1Energy(AbstractEnergy):

    map_to_mxcube_state = {
        PyTango.DevState.OFF: HardwareObjectState.READY,
        PyTango.DevState.RUNNING: HardwareObjectState.BUSY,
        PyTango.DevState.ON: HardwareObjectState.BUSY,
        PyTango.DevState.MOVING: HardwareObjectState.BUSY,
        PyTango.DevState.INIT: HardwareObjectState.OFF,
        PyTango.DevState.DISABLE: HardwareObjectState.OFF,
        PyTango.DevState.FAULT: HardwareObjectState.FAULT,
        PyTango.DevState.UNKNOWN: HardwareObjectState.UNKNOWN
    }

    def __init__(self, name):

        super(XRD1Energy, self).__init__(name)
        self.ch_mono_energy = None
        self.ch_mono_state = None
        self.ch_target_energy = None
        self.cmd_start_method = None
        self.cmd_abort_method = None
        self.timeout = None

    @hwo_header_log
    def init(self):

        super(XRD1Energy, self).init()
        self.ch_mono_energy = self.get_channel_object("mono_energy", optional=False)
        self.ch_mono_state = self.get_channel_object("mono_state", optional=False)
        self.ch_target_energy = self.get_channel_object("target_energy", optional=False)
        self.cmd_start_method = self.get_command_object("start_method")
        self.cmd_abort_method = self.get_command_object("abort")
        self.timeout = self.get_property("timeout")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_mono_energy, "update", self.update_value)
        self.connect(self.ch_mono_state, "update", lambda tango_stat: self.update_state(
            self.map_to_mxcube_state.get(tango_stat, self.STATES.UNKNOWN)))

    @hwo_header_log
    def get_value(self):

        try:
            value = self.ch_mono_energy.get_value()
            self.log.info(f"Read the value of the \"{self.username}\" "
                          f"(it's \"{value}{self.unit}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read \"{self.username}\" from the " \
                      f"attribute \"{self.ch_mono_energy.attribute_name}\" of the " \
                      f"tango device \"{self.ch_mono_energy.device_name}\""
            self.log.exception(err_msg)
            raise ValueError(err_msg)

        return value

    @hwo_header_log
    def _set_value(self, value):
        try:
            if self.get_state() == HardwareObjectState.BUSY:
                raise RuntimeError("Monochromator is BUSY")
            self.ch_target_energy.set_value(value)
            self.cmd_start_method("change_energy")
            gevent.sleep(0.2)
            with gevent.Timeout(
                self.timeout, TimeoutError(f"Timed out. The \"{self.username}\" "
                                           f"has not reached the target value after"
                                           f" {self.timeout} sec")
            ):
                while self.ch_mono_state.get_value() != PyTango.DevState.OFF:
                    self.log.debug(f"Waiting \"{self.username}\" to reach the target"
                                   f" value {value}")
                    gevent.sleep(self.ch_mono_state.polling / 1000)
            self.log.info(f"The \"{self.username}\" reached the target value"
                          f" \"{value}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to reach the target value of the \"{self.username}\" " \
                      f"setting the attribute " \
                      f"\"{self.ch_target_energy.attribute_name}\" of the tango " \
                      f"device \"{self.ch_target_energy.device_name}\" to \"{value}\"" \
                      f" and calling the command \"{self.cmd_start_method.command}\"" \
                      f" of the same tango device with argument \"change_energy\""
            raise RuntimeError(err_msg)
        except TimeoutError as e:
            self.user_log.error(str(e))
            raise RuntimeError(str(e))
        except RuntimeError as e:
            self.log.warning(str(e))
            self.user_log.warning(str(e))
            raise e
        return value

    @hwo_header_log
    def abort(self):

        try:
            self.cmd_abort_method()
            self.log.info(f"Abort command sent to the \"{self.username}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to abort changing the value of the \"{self.username}\"" \
                      f" calling the command \"{self.cmd_abort_method.command}\" of " \
                      f"the tango device \"{self.cmd_abort_method.device_name}\""
            self.log.exception(err_msg)
            raise RuntimeError(err_msg)
