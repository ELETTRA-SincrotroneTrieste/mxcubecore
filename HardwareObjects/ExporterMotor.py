# encoding: utf-8
#
#  Project: MXCuBE
#  https://github.com/mxcube.
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
#  along with MXCuBE.  If not, see <http://www.gnu.org/licenses/>.
"""
Example xml file:
<device class="ExporterMotor">
  <username>phiy</username>
  <exporter_address>wid30bmd2s:9001</exporter_address>
  <motor_name>AlignmentY</motor_name>
  <tolerance>1e-2</tolerance>
</device>
"""

import sys
from gevent import Timeout, sleep
from HardwareRepository.HardwareObjects.abstract.AbstractMotor import AbstractMotor
from HardwareRepository.Command.Exporter import Exporter
from HardwareRepository.Command.exporter.ExporterStates import ExporterStates

__copyright__ = """ Copyright © 2019 by the MXCuBE collaboration """
__license__ = "LGPLv3+"


class ExporterMotor(AbstractMotor):
    """Motor using the Exporter protocol, based on AbstractMotor"""

    def __init__(self, name):
        AbstractMotor.__init__(self, name)
        self.username = None
        self._motor_pos_suffix = None
        self._motor_state_suffix = None
        self._exporter = None
        self.motor_position = None
        self.motor_state = None

    def init(self):
        """Initialise the motor"""
        AbstractMotor.init(self)

        self._motor_pos_suffix = self.getProperty("position_suffix", "Position")
        self._motor_state_suffix = self.getProperty("state_suffix", "State")

        _exporter_address = self.getProperty("exporter_address")
        _host, _port = _exporter_address.split(":")
        self._exporter = Exporter(_host, int(_port))

        self.motor_position = self.add_channel(
            {
                "type": "exporter",
                "exporter_address": _exporter_address,
                "name": "position",
            },
            self.motor_name + self._motor_pos_suffix,
        )
        if self.motor_position:
            self.get_value()
            self.motor_position.connectSignal("update", self.update_value)

        self.motor_state = self.add_channel(
            {
                "type": "exporter",
                "exporter_address": _exporter_address,
                "name": "motor_state",
            },
            self.motor_name + self._motor_state_suffix,
        )

        if self.motor_state:
            self.motor_state.connectSignal("update", self._update_state)

        self.update_state()

    def get_state(self):
        """Get the motor state.
        Returns:
            (enum 'HardwareObjectState'): Motor state.
        """
        try:
            _state = self.motor_state.get_value().upper()
            self.specific_state = _state
            return ExporterStates.__members__[_state].value
        except (KeyError, AttributeError):
            return self.STATES.UNKNOWN

    def _update_state(self, state):
        try:
            state = state.upper()
            state = ExporterStates.__members__[state].value
        except (AttributeError, KeyError):
            state = self.STATES.UNKNOWN
        return self.update_state(state)

    def _get_hwstate(self):
        """Get the hardware state, reported by the MD2 application.
        Returns:
            (string): The state.
        """
        try:
            return self._exporter.read_property("HardwareState")
        except BaseException:
            return "Ready"

    def _get_swstate(self):
        """Get the software state, reported by the MD2 application.
        Returns:
            (string): The state.
        """
        return self._exporter.read_property("State")

    def _ready(self):
        """Get the "Ready" state - software and hardware.
        Returns:
            (bool): True if both "Ready", False otherwise.
        """
        if self._get_swstate() == "Ready" and self._get_hwstate() == "Ready":
            return True
        return False

    def _wait_ready(self, timeout=3):
        """Wait for the state to be "Ready".
        Args:
            timeout (float): waiting time [s].
        Raises:
            RuntimeError: Execution timeout.
        """
        with Timeout(timeout, RuntimeError("Execution timeout")):
            while not self._ready():
                sleep(0.01)

    def wait_move(self, timeout=20):
        """Wait until the end of move ended, using the application state.
        Args:
            timeout(float): Timeout [s]. Default value is 20 s
        """
        self._wait_ready(timeout)

    def wait_motor_move(self, timeout=20):
        """Wait until the end of move ended using the motor state.
        Args:
            timeout(float): Timeout [s]. Default value is 20 s
        Raises:
            RuntimeError: Execution timeout.
        """
        with Timeout(timeout, RuntimeError("Execution timeout")):
            while self.get_state() != self.STATES.READY:
                sleep(0.01)

    def get_value(self):
        """Get the motor position.
        Returns:
            (float): Motor position.
        """
        self._nominal_value = self.motor_position.get_value()
        return self._nominal_value

    def __get_limits(self, cmd):
        """Returns motor low and high limits.
        Args:
            cmd (str): command name
        Returns:
            (tuple): two floats tuple (low limit, high limit).
        """
        try:
            _low, _high = self._exporter.execute(cmd, self.motor_name)
            # inf is a problematic value, convert to sys.float_info.max
            if _low == float("-inf"):
                _low = -sys.float_info.max

            if _high == float("inf"):
                _high = sys.float_info.max

            return _low, _high
        except ValueError:
            return -1e4, 1e4

    def get_limits(self):
        """Returns motor low and high limits.
        Args:
            cmd (str): command name
        Returns:
            (tuple): two floats tuple (low limit, high limit).
        """
        self._nominal_limits = self.__get_limits("getMotorLimits")
        return self._nominal_limits

    def get_dynamic_limits(self):
        """Returns motor low and high dynamic limits.
        Returns:
            (tuple): two floats tuple (low limit, high limit).
        """
        return self.__get_limits("getMotorDynamicLimits")

    def _set_value(self, value):
        """Move motor to absolute value.
        Args:
            value (float): target value
        """
        self.motor_position.set_value(value)

    def abort(self):
        """Stop the motor movement immediately."""
        if self.get_state() != self.STATES.UNKNOWN:
            self._exporter.execute("abort")

    def home(self, timeout=None):
        """Homing procedure.
        Args:
            timeout (float): optional - timeout [s].
        """
        self._exporter.execute("startHomingMotor", self.motor_name)
        self.wait_ready(timeout)

    def get_max_speed(self):
        """Get the motor maximum speed.
        Returns:
            (float): the maximim speed [unit/s].
        """
        return self._exporter.execute("getMotorMaxSpeed", self.motor_name)

    def name(self):
        """Get the motor name. Should be removed when GUI ready"""
        return self.motor_name
