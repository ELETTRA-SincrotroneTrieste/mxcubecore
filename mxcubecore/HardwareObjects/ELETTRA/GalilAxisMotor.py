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


class GalilAxisMotor(AbstractMotor):

    map_to_mxcube_state = {
        PyTango.DevState.ON: AbstractMotor.STATES.READY,
        PyTango.DevState.STANDBY: AbstractMotor.STATES.READY,
        PyTango.DevState.MOVING: AbstractMotor.STATES.BUSY,
        PyTango.DevState.FAULT: AbstractMotor.STATES.FAULT,
        PyTango.DevState.OFF: AbstractMotor.STATES.OFF,
        PyTango.DevState.UNKNOWN: AbstractMotor.STATES.UNKNOWN
    }

    def __init__(self, name):
        AbstractMotor.__init__(self, name)
        self.ch_position = None
        self.ch_state = None
        self.cmd_set_position = None
        self.ch_accuracy = None
        self.cmd_stop = None
        self.ch_velocity = None
        self.timeout = None  # [s]

    @hwo_header_log
    def init(self):

        super(GalilAxisMotor, self).init()
        self.ch_position = self.get_channel_object("axis_position", optional=False)
        self.ch_state = self.get_channel_object("axis_state", optional=False)
        self.ch_velocity = self.get_channel_object("axis_velocity", optional=True)
        self.ch_accuracy = self.get_channel_object("axis_accuracy", optional=True)
        self.cmd_stop = self.get_command_object("stop_axis")
        self.timeout = self.get_property("timeout")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_position, "update", self.update_value)
        self.connect(self.ch_state, "update",
                     lambda state: self.update_state(
                         self.map_to_mxcube_state.get(state, self.STATES.UNKNOWN)))
        if self.ch_velocity:
            self.connect(self.ch_velocity, "update", self.set_velocity)

        self._tolerance = self.ch_accuracy.get_value()

    @hwo_header_log
    def get_value(self):

        try:
            value = self.ch_position.get_value()
            self.log.info(f"Read the position of the axis \"{self.username}\""
                          f" (it's \"{value})\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the position of the axis \"{self.username}\"" \
                      f" from the attribute \"{self.ch_position.attribute_name}\" of" \
                      f" the tango device \"{self.ch_position.device_name}\""
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value

    @hwo_header_log
    def get_state(self):

        try:
            tango_state = self.ch_state.get_value()
            state = self.map_to_mxcube_state.get(tango_state, self.STATES.UNKNOWN)
            self.log.info(f"Read the state of the axis \"{self.username}\" "
                          f"(it's \"{state.name}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the state of the axis \"{self.username}\" from" \
                      f" the attribute \"{self.ch_state.attribute_name}\" of the " \
                      f"tango device \"{self.ch_state.device_name}\" "
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return state

    @hwo_header_log
    def _set_value(self, value):

        try:
            self.ch_position.set_value(value)
            gevent.sleep(0.5)
            with gevent.Timeout(self.timeout,
                                TimeoutError(f"Timed out. The axis \"{self.username}\""
                                             f" has not reached the target position "
                                             f"after {self.timeout} sec")):
                while self.get_state() != self.STATES.READY:
                    self.log.debug(f"Waiting \"{self.username}\" to reach the target"
                                   f" position {value}")
                    gevent.sleep(self.ch_state.polling / 1000)
            self.log.info(f"The axis \"{self.username}\" reached the target"
                          f" position \"{value}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the position of the axis \"{self.username}\"" \
                      f" setting the attribute \"{self.ch_position.attribute_name}\"" \
                      f" of the tango device \"{self.ch_position.device_name}\" to" \
                      f" \"{value}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        except TimeoutError as e:
            self.user_log.error(str(e))
            raise RuntimeError(str(e))

    @hwo_header_log
    def abort(self):

        try:
            self.cmd_stop()
            self.log.info(f"Abort command sent to the axis \"{self.username}\"")
            # ensure that the state is updated at least once after the polling time
            # in case we miss the state update
            gevent.sleep(self.ch_state.polling / 1000)
        except PyTango.DevFailed:
            err_msg = f"Failed to abort positioning of the axis \"{self.username}\"" \
                      f" calling the command \"{self.cmd_stop.command}\" of the tango" \
                      f" device \"{self.cmd_stop.device_name}\""
            self.log.exception(err_msg)
            raise RuntimeError(err_msg)

    @hwo_header_log
    def get_motor_mnemonic(self):

        return f"{self.username} ({self.name()})"
