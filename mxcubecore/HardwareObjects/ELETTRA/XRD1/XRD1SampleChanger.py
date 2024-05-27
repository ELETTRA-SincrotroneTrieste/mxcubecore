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

from mxcubecore.HardwareObjects.abstract import AbstractSampleChanger


class XRD1SampleChanger(AbstractSampleChanger.SampleChanger):

    __TYPE__ = "Manual"

    def __init__(self, *args, **kwargs):

        super(XRD1SampleChanger, self).__init__(self.__TYPE__, False, *args, **kwargs)

    def init(self):

        super(XRD1SampleChanger, self).init()

        self._set_state(AbstractSampleChanger.SampleChangerState.Disabled)

    def load_sample(self, holder_length, sample_location=None, wait=False):
        self.load(sample_location, wait)

    def _do_abort(self):
        return

    def _do_change_mode(self, mode):
        return

    def _do_update_info(self):
        return

    def _do_select(self, component):
        return

    def _do_scan(self, component, recursive):
        return

    def _do_load(self, sample=None):
        return

    def _do_unload(self, sample_slot=None):
        return

    def _do_reset(self):
        return
