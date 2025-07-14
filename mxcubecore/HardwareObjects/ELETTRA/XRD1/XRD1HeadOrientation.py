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
from mxcubecore import  trace_call_log
from mxcubecore import HardwareRepository as HWR


class XRD1HeadOrientation(AbstractNState):

    # Dict which maps tango state to mxcube state
    state_map = {
        PyTango.DevState.ON: AbstractNState.STATES.READY,
        PyTango.DevState.STANDBY: AbstractNState.STATES.READY,
        PyTango.DevState.MOVING: AbstractNState.STATES.BUSY,
        PyTango.DevState.RUNNING: AbstractNState.STATES.BUSY,
        PyTango.DevState.FAULT: AbstractNState.STATES.FAULT,
        PyTango.DevState.OFF: AbstractNState.STATES.OFF,
        PyTango.DevState.UNKNOWN: AbstractNState.STATES.UNKNOWN
    }

    def __init__(self, name):

        super(XRD1HeadOrientation, self).__init__(name)
        self.chn_get_sample_orientation = None
        self.chn_state = None
        self.cmd_set_sample_bottom = None
        self.cmd_set_sample_left = None
        self.cmd_set_sample_top = None
        self.cmd_abort_sample_orientation = None
        self.timeout = None

    @trace_call_log
    def init(self):

        super(XRD1HeadOrientation, self).init()
        self.timeout = self.get_property("timeout")
        self.chn_get_sample_orientation = self.get_channel_object(
            "get_sample_orientation")
        self.chn_state = self.get_channel_object("state")
        self.cmd_set_sample_bottom = self.get_command_object("set_sample_bottom")
        self.cmd_set_sample_left = self.get_command_object("set_sample_left")
        self.cmd_set_sample_top = self.get_command_object("set_sample_top")
        self.cmd_abort_sample_orientation = self.get_command_object(
            "abort_sample_orientation")

        # SIGNALS CONNECTIONS
        self.connect(self.chn_get_sample_orientation, 'update',
                     lambda tango_val: self.update_value(self.value_to_enum(tango_val)))
        self.connect(self.chn_state, 'update',
                     lambda state: self.update_state(self.state_map.get(state)))

        self.update_value()
        self.update_state()

    @trace_call_log
    def set_sample_triclinic(self):
        HWR.beamline.diffractometer.kappa.set_value(-30.00)

    @trace_call_log
    def get_value(self):

        try:
            value = self.value_to_enum(self.chn_get_sample_orientation.get_value())
            if HWR.beamline.diffractometer \
                and value == self.VALUES.UNKNOWN \
                and HWR.beamline.diffractometer.kappa.is_in_postion(-30.00):
                value = self.VALUES.Triclinic
            self.log.info(f"Read the \"{self.username}\" (it's \"{value.name})\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the \"{self.username}\" from the attribute " \
                      f"\"{self.chn_get_sample_orientation.attribute_name}\" of the" \
                      f" tango device \"{self.chn_get_sample_orientation.device_name}\""
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value

    @hwo_header_log
    def _set_value(self, value):

        # TODO Evaluate whether implement others safety check for the head orientation
        # Safety checks
        if value == self.VALUES.Triclinic and self.get_value() != self.VALUES.Left:
            err_msg = f"Operation not permitted. \"TRICLINIC\" position can be reached ONLY from \"LEFT\" " \
                      f"position (current position: \"{self.get_value().name}\")"
            self.user_log.warning(err_msg)
            raise Exception(err_msg)

        if value == self.VALUES.Bottom:
            # TODO Do safety check before uncomment following line
            # cmd_set_ = self.cmd_set_sample_bottom
            raise NotImplemented
        elif value == self.VALUES.Top:
            # TODO Do safety check before uncomment following line
            # cmd_set_ = self.cmd_set_sample_top
            raise NotImplemented
        elif value == self.VALUES.Left:
            cmd_set_ = self.cmd_set_sample_left
        elif value == self.VALUES.Triclinic:
            cmd_set_ = self.set_sample_triclinic
        else:
            raise Exception(f"Position {value.name} not found")

        try:
            cmd_set_()
            with gevent.Timeout(
                self.timeout, TimeoutError(f"Timed out. The \"{self.username}\" "
                                           f"has not reached the target position "
                                           f"({value.name}) after {self.timeout}"
                                           f" sec")
            ):
                while self.get_value() != value:
                    self.log.debug(f"Waiting \"{self.username}\" to reach the target"
                                   f" position {value.name}")
                    gevent.sleep(self.chn_get_sample_orientation.polling / 1000)
            self.log.info(f"The \"{self.username}\" changed to \"{value.name}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the position of the \"{self.username}\" to" \
                      f" \"{value.name}\" calling the command \"{cmd_set_.command}\"" \
                      f" of the tango device \"{cmd_set_.device_name}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        except TimeoutError as e:
            self.user_log.error(str(e))
            raise RuntimeError(str(e))
        finally:
            self.update_state()

    @hwo_header_log
    def get_state(self):

        try:
            tango_state = self.chn_state.get_value()
            state = self.state_map.get(tango_state, self.STATES.UNKNOWN)
            self.log.info(f"Read the state of the \"{self.username}\" "
                          f"(it's \"{state.name}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the state of the \"{self.username}\" " \
                      f"from the attribute " \
                      f"\"{self.chn_state.attribute_name}\" of the tango device" \
                      f" \"{self.chn_state.device_name}\""
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return state

    @hwo_header_log
    def abort(self):

        try:
            self.cmd_abort_sample_orientation()
            self.log.info(f"Abort command sent to the \"{self.username}\"")
            # ensure that the state is updated at least once after the polling time
            # in case we miss the state update
            gevent.sleep(self.chn_state.polling / 1000)
            self.update_state()
        except PyTango.DevFailed:
            err_msg = f"Failed to abort positioning of the \"{self.username}\" " \
                      f"calling the command " \
                      f"\"{self.cmd_abort_sample_orientation.command}\" of the tango " \
                      f"device \"{self.cmd_abort_sample_orientation.device_name}\""
            self.log.exception(err_msg)
            raise RuntimeError(err_msg)
