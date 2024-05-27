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


from mxcubecore.HardwareObjects.abstract.AbstractMachineInfo import AbstractMachineInfo
from mxcubecore import hwo_header_log


TOPUP_STATUS = {1: 'OFF', 2: 'ON', 4: 'WARMUP', 8: 'RUNNING', 16: 'FAULT'}
USER_BEAM = {0: 'User Beam OFF', 1: 'User Beam ON'}


class MachineInfoElettra(AbstractMachineInfo):

    def __init__(self, *args):

        AbstractMachineInfo.__init__(self, *args)
        self.ch_ring_current = None
        self.ch_operator_message = None
        self.ch_filling_mode = None
        self.ch_lifetime = None

    @hwo_header_log
    def init(self):

        super(MachineInfoElettra, self).init()
        self.ch_ring_current = self.get_channel_object('current')
        self.ch_operator_message = self.get_channel_object('operator_msg')
        self.ch_filling_mode = self.get_channel_object('filling_mode')
        self.ch_lifetime = self.get_channel_object('lifetime')

        # SIGNALS CONNECTIONS
        self.connect(self.ch_ring_current, "update", self._update_machine_info)
        self.connect(self.ch_operator_message, "update", self._update_machine_info)
        self.connect(self.ch_filling_mode, "update", self._update_machine_info)
        self.connect(self.ch_lifetime, "update", self._update_machine_info)

    @hwo_header_log
    def _update_machine_info(self, value, sender):
        self._mach_info_dict[sender.name()] = value
        self.emit("valueChanged")

    @hwo_header_log
    def get_current(self):
        current = self._mach_info_dict['current']
        return current if current is not None else "Unknown"

    @hwo_header_log
    def get_message(self):
        operator_message = USER_BEAM.get(self._mach_info_dict['operator_msg'],
                                         "Unknown")
        lifetime = self._mach_info_dict['lifetime']
        message = f"{operator_message} (Lifetime: {lifetime: .2f} hours)"
        return message

    @hwo_header_log
    def get_fill_mode(self):
        filling_mode = TOPUP_STATUS.get(self._mach_info_dict['filling_mode'], "Unknown")
        return filling_mode
