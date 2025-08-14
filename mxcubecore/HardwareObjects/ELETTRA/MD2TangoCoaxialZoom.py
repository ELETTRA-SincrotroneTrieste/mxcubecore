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

import enum
from threading import Thread

import PyTango
import gevent

from mxcubecore.BaseHardwareObjects import HardwareObjectState
from mxcubecore.HardwareObjects.abstract.AbstractMotor import AbstractMotor
from mxcubecore import trace_call_log


class MD2TangoCoaxialZoom(AbstractMotor):

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
        self.ch_zoom_value = None
        self.cmd_get_motor_state = None
        self.cmd_abort = None
        self.old_state = None

    @trace_call_log
    def init(self):
        """Initialise the motor"""
        super().init()
        self.motor_name = self.getProperty("motor_name")
        self.poll_interval = self.getProperty("poll_interval")
        self.ch_zoom_value = self.get_channel_object("zoom_value")
        self.cmd_get_motor_state = self.get_command_object("get_motor_state")
        self.cmd_abort = self.get_command_object("abort")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_zoom_value, 'update',
                     lambda tg_value: self.update_value(self.value_to_enum(tg_value)))

        thread = Thread(target=self.poll)
        thread.daemon = True
        thread.start()

        self.update_state()

    @trace_call_log
    def poll(self):
        self.log.info(f"Starting polling the state of {self.username}")
        while True:
            gevent.sleep(float(self.poll_interval) / 1000)
            try:
                self.update_state()
            except (ValueError, PyTango.DevFailed):
                self.log.exception(
                    'Error occurred during the polling of the zoom state'
                )
            except Exception:
                self.log.exception(
                    'Unexpected error occurred during the polling of the zoom state'
                )

    @trace_call_log
    def get_state(self) -> AbstractMotor.STATES:
        tango_state = self.cmd_get_motor_state(self.motor_name)
        state = self.map_to_mxcube_state.get(tango_state, self.STATES.UNKNOWN)
        self.log.debug(
            f'Read the state of "{self.username}" (value: "{state.name}")'
        )
        return state

    @trace_call_log
    def get_value(self) -> int:
        value = self.value_to_enum(self.ch_zoom_value.get_value())
        self.log.debug(
            f'Read the level of the "{self.username}" (value: "{value.nmae})"'
        )
        return value

    @trace_call_log
    def _set_value(self, value: enum.Enum):
        self.ch_zoom_value.set_value(value.value)

    @trace_call_log
    def abort(self):
        self.cmd_abort()
