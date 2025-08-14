# encoding: utf-8
#
#  Project name: MXCuBE
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
#  You should have received a copy of the GNU General Lesser Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

__copyright__ = """ Copyright © 2020 by the MXCuBE collaboration """
__credits__ = ["ELETTRA"]
__license__ = "LGPLv3+"
__category__ = "General"

import sys
from threading import Thread

import PyTango
import gevent

from mxcubecore.BaseHardwareObjects import HardwareObjectState
from mxcubecore.HardwareObjects.abstract.AbstractMotor import AbstractMotor
from mxcubecore import trace_call_log


class MD2TangoMotor(AbstractMotor):
    """Motor Motor implementation"""

    map_to_mxcube_state = {
        "INIT": AbstractMotor.STATES.UNKNOWN,
        "ON": AbstractMotor.STATES.READY,
        "READY": AbstractMotor.STATES.READY,
        "STANDBY": AbstractMotor.STATES.READY,
        "RUNNING": AbstractMotor.STATES.BUSY,
        "MOVING": AbstractMotor.STATES.BUSY,
        "FAULT": AbstractMotor.STATES.FAULT,
        "ALARM": AbstractMotor.STATES.WARNING,
        "UNKNOWN": AbstractMotor.STATES.UNKNOWN,
    }

    def __init__(self, name):
        super().__init__(name)
        self.motor_name = None
        self.poll_interval = None
        self.ch_wait_moves = None
        self.cmd_get_motor_state = None
        self.cmd_get_motor_limits = None
        self.cmd_get_motor_position = None
        self.cmd_set_motor_position = None
        self.cmd_abort = None
        self.old_value = None
        self.old_state = None

    @trace_call_log
    def init(self):
        """Initialise the motor"""
        super().init()
        self.motor_name = self.getProperty("motor_name")
        self.poll_interval = self.getProperty("poll_interval")
        self.ch_wait_moves = self.get_channel_object("wait_moves")
        self.cmd_get_motor_state = self.get_command_object("get_motor_state")
        self.cmd_get_motor_limits = self.get_command_object("get_motor_limits")
        self.cmd_get_motor_position = self.get_command_object("get_motor_position")
        self.cmd_set_motor_position = self.get_command_object("set_motor_position")
        self.cmd_abort = self.get_command_object("abort")

        thread = Thread(target=self.poll)
        thread.daemon = True
        thread.start()

        self.ch_wait_moves.set_value(False)
        self.update_limits()
        self.update_state()

    @trace_call_log
    def poll(self):
        self.log.info(f"Starting polling the position and state of {self.username}")
        while True:
            gevent.sleep(float(self.poll_interval) / 1000)
            try:
                self.update_value()
            except (ValueError, PyTango.DevFailed):
                self.log.exception(
                    "Error occurred during the polling of the motor position"
                )
            except Exception:
                self.log.exception(
                    "Unexpected error occurred during the polling of the motor position"
                )
            try:
                self.update_state()
            except (ValueError, PyTango.DevFailed):
                self.log.exception(
                    "Error occurred during the polling of the motor state"
                )
            except Exception:
                self.log.exception(
                    "Unexpected error occurred during the polling of the motor state"
                )

    @trace_call_log
    def get_state(self) -> AbstractMotor.STATES:
        state = self.cmd_get_motor_state(self.motor_name)
        state = self.map_to_mxcube_state.get(state, self.STATES.UNKNOWN)
        self.log.debug(
            f'Read the state of the motor "{self.username}" (value: "{state.name}")'
        )
        return state

    @trace_call_log
    def get_value(self) -> float:
        value = self.cmd_get_motor_position(self.motor_name)
        self.log.debug(
            f'Read the position of the motor "{self.username}" (value: "{value})"'
        )
        return value

    @trace_call_log
    def _set_value(self, value: float):
        self.cmd_set_motor_position(value)

    @trace_call_log
    def __get_limits(self) -> [float, float]:
        try:
            _low, _high = self.cmd_get_motor_limits(self.motor_name)
            self.log.debug(
                f'Read the limits of the motor "{self.username}" '
                f'(the low limit is "{_low}" and the high limit is "{_high})" '
            )
            if _low == float("-inf"):
                _low = -sys.float_info.max

            if _high == float("inf"):
                _high = sys.float_info.max

            return _low, _high
        except PyTango.DevFailed:
            return self._nominal_limits

    @trace_call_log
    def get_limits(self) -> [float, float]:
        self._nominal_limits = self.__get_limits()
        return self._nominal_limits

    @trace_call_log
    def abort(self):
        self.cmd_abort()
