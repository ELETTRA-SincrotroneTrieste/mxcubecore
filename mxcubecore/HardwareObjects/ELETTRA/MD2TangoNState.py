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

from mxcubecore.HardwareObjects.abstract.AbstractNState import AbstractNState
from mxcubecore import trace_call_log


class MD2TangoNState(AbstractNState):
    def __init__(self, name):
        super().__init__(name)
        self.ch_state = None

    @trace_call_log
    def init(self):
        super().init()
        self.ch_state = self.get_channel_object("state")

        # SIGNALS CONNECTIONS
        self.connect(
            self.ch_state,
            "update",
            lambda tg_value: self.update_value(self.value_to_enum(tg_value)),
        )

        self.update_state(self.STATES.READY)

    @trace_call_log
    def get_value(self):
        value = self.value_to_enum(self.ch_state.get_value())
        self.log.debug(
            f'Read the state of "{self.username}" (value: {self.value_to_enum(value)})'
        )
        return value
