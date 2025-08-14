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

import logging
import time

import gevent

from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.HardwareObjects.ELETTRA.XRD2 import XRD2SampleChanger
from mxcubecore.HardwareObjects.abstract.AbstractSampleChanger import SampleChangerState
from mxcubecore.TaskUtils import task
from mxcubecore import trace_call_log


class XRD2SampleChangerMaint(HardwareObject):
    __TYPE__ = "ELETTRA_XRD2_SC"
    NO_OF_LIDS = 1

    def __init__(self, name):
        super().__init__(name)

        self.sample_changer = None

    def init(self):
        self.sample_changer = self.get_object_by_role("sample_changer")

        # SIGNALS CONNECTIONS
        self.connect(self.sample_changer, "scInfoChanged", self._update_global_state)

    # TODO spostare da un'altra parte
    # def _doRestartMX3(self):
    #     """
    #     Kill and restart mxcube-server
    #
    #     :returns: None
    #     :rtype: None
    #     """
    #     os.setsid()
    #     Popen('{}/../../../restart_mxcube3.sh'.format(os.path.dirname(os.path.abspath(__file__))), shell=True, stdout=PIPE, stderr=PIPE)
    #     raise RuntimeError("*** MXCuBE has been killed & restarted - Please Refresh the browser page! ***")

    @trace_call_log
    def _do_reset(self):
        """
        Reset sample changer
        """
        self.sample_changer.reset()

    def _do_trash2S(self):
        """
        Trash (DROP!) samples opening (both) gripper(s) in EXChange point

        """
        self.sample_changer._do_trash2S()

    @trace_call_log
    def _do_defrost(self):
        """
        Defrost sample changer

        """
        self.sample_changer._do_defrost()

    @trace_call_log
    def _do_park(self):
        """
        Park sample changer

        """
        self.sample_changer._do_park()

    @trace_call_log
    def _do_unpark(self):
        """
        Unpark sample changer

        """
        self.sample_changer._do_unpark()

    @trace_call_log
    def _do_change_gripper(self):
        """
        ChangeGripper on  sample changer

        """
        raise NotImplemented
        self.sample_changer.change_gripper()

    @trace_call_log
    def _update_global_state(self):
        state_dict, cmd_state, message = self.get_global_state()
        # self.log.debug("State_dict: %s, cmd_state: %s, message: %s", str(state_dict), str(cmd_state), message)
        self.emit("globalStateChanged", (state_dict, cmd_state, message))

    @trace_call_log
    def get_global_state(self):
        sc_state = self.sample_changer.get_state()
        cmd_state = {
            "defrost": sc_state == SampleChangerState.Ready,
            "park": sc_state in [SampleChangerState.Ready, SampleChangerState.Exchange],
            "unpark": sc_state == SampleChangerState.Parked,
            "trash2S": sc_state == SampleChangerState.Alarm,
            "reset": sc_state == SampleChangerState.Alarm,
        }
        state_dict = self.sample_changer.sc_info
        message = ""
        print(f"AAAAAAAA cmd_state: {cmd_state}")
        print(f"AAAAAAAA state_dict: {state_dict}")

        return state_dict, cmd_state, message

    def get_cmd_info(self):
        """return information about existing commands for this object
        the information is organized as a list
        with each element contains
        [ cmd_name,  display_name, category ]
        """
        """ [cmd_id, cmd_display_name, nb_args, cmd_category, description ] """
        cmd_list = [
            [
                "Actions",
                [
                    ["defrost", "Defrost", "Actions"],
                    ["park", "Park Robot", "Actions"],
                    ["unpark", "Unpark Robot", "Actions"],
                ],
            ],
            [
                "EMERGENCY Actions - !!! ASK BL STAFF BEFORE CLICKING !!!",
                [
                    ["trash2S", "Trash Stucked Samples", "Actions"],
                    ["reset", "Reset", "Actions"],
                ],
            ],
        ]
        return cmd_list

    def send_command(self, cmdname, args=None):
        if cmdname in ["defrost"]:
            self._do_defrost()

        if cmdname in ["park"]:
            self._do_park()

        if cmdname in ["unpark"]:
            self._do_unpark()

        if cmdname in ["trash2S"]:
            self._do_trash2S()

        if cmdname in ["reset"]:
            self._do_reset()
            # TODO
            # self.sample_changer.wait_states([SampleChangerState.Ready], 30)

        self._update_global_state()
        return True
