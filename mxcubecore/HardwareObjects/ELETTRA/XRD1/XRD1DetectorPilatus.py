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
import PyTango

from mxcubecore.HardwareObjects.abstract.AbstractDetector import AbstractDetector
from mxcubecore.BaseHardwareObjects import HardwareObjectState
from mxcubecore import hwo_header_log


class XRD1DetectorPilatus(AbstractDetector):

    # !!! NOTE !!! This HWO is only used for reading the status of the detector

    map_to_mxcube_state = {
        PyTango.DevState.ON: HardwareObjectState.READY,
        PyTango.DevState.RUNNING: HardwareObjectState.BUSY,
        PyTango.DevState.INIT: HardwareObjectState.OFF,
        PyTango.DevState.FAULT: HardwareObjectState.FAULT,
        PyTango.DevState.DISABLE: HardwareObjectState.OFF,
        PyTango.DevState.UNKNOWN: HardwareObjectState.UNKNOWN
    }

    def __init__(self, name):

        super(XRD1DetectorPilatus, self).__init__(name)
        self.ch_state = None
        self.ch_energy_threshold = None
        self.ch_last_image_taken = None
        self.cmd_start_acq = None
        self.cmd_stop_acq = None
        self.cmd_reset = None

    @hwo_header_log
    def init(self):

        super(XRD1DetectorPilatus, self).init()

        self._exposure_time_limits = eval(self.get_property("exposure_time_limits",
                                                            "[0.04, 60000]"))
        self.ch_state = self.get_channel_object("state", optional=False)
        self.cmd_start_acq = self.get_command_object("start_acq")
        self.cmd_stop_acq = self.get_command_object("stop_acq")
        self.cmd_reset = self.get_command_object("restart")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_state, "update", lambda state: self.update_state(
            self.map_to_mxcube_state.get(state, self.STATES.UNKNOWN)))

        self.update_state()

    @hwo_header_log
    def get_state(self):

        try:
            tango_state = self.ch_state.get_value()
            state = self.map_to_mxcube_state.get(tango_state, self.STATES.UNKNOWN)
            self.log.info(f"Read the status of the \"{self.username}\" "
                          f"(it's \"{state.name}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the status of the \"{self.username}\" from " \
                      f"the attribute \"{self.ch_state.attribute_name}\" of the " \
                      f"tango device \"{self.ch_state.device_name}\" "
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return state

    @hwo_header_log
    def has_shutterless(self):

        return True

    @hwo_header_log
    def prepare_acquisition(self, *args, **kwargs):

        # TODO evaluate whether make some Pilatus configuration
        return

    @hwo_header_log
    def start_acquisition(self):

        try:
            self.cmd_start_acq()
            self.log.info(f"Acquisition of the \"{self.username}\" started")
        except PyTango.DevFailed:
            err_msg = f"Failed to start the acquisition of the \"{self.username}\"" \
                      f" calling the command {self.cmd_start_acq.command}\" of the" \
                      f" tango device \"{self.cmd_start_acq.device_name}\""
            self.log.exception(err_msg)
            raise RuntimeError(err_msg)
        finally:
            time.sleep(1)
            self.update_state()

    @hwo_header_log
    def stop_acquisition(self):

        try:
            self.cmd_stop_acq()
            self.log.info(f"Acquisition of the \"{self.username}\" stopped")
        except PyTango.DevFailed:
            err_msg = f"Failed to stop the acquisition of the \"{self.username}\" " \
                      f"calling the command {self.cmd_start_acq.command}\" of the " \
                      f"tango device \"{self.cmd_start_acq.device_name}\""
            self.log.exception(err_msg)
            raise RuntimeError(err_msg)
        finally:
            time.sleep(1)
            self.update_state()

    @hwo_header_log
    def restart(self) -> None:

        try:
            self.cmd_reset()
            self.log.info(f"The \"{self.username}\" has been reset")
        except PyTango.DevFailed:
            err_msg = f"Failed to reset the \"{self.username}\" calling the command" \
                      f" {self.cmd_start_acq.command}\"" \
                      f" of the tango device \"{self.cmd_start_acq.device_name}\""
            self.log.exception(err_msg)
            raise RuntimeError(err_msg)
        finally:
            time.sleep(1)
            self.update_state()
