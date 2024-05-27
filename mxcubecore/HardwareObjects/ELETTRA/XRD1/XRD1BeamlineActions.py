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

import enum
import logging
import time

from mxcubecore.HardwareObjects.BeamlineActions import BeamlineActions
from mxcubecore.BaseHardwareObjects import HardwareObjectState
from mxcubecore import HardwareRepository as HWR


class EnumArg(str, enum.Enum):

    centring = "Centring"
    data_collection = "DataCollection"
    beam_location = "BeamLocation"
    transfer = "Transfer"
    unknown = "Unknown"


class GoToWellKnownPos:
    def __call__(self, *args, **kw):

        if HWR.beamline.diffractometer.get_state() != HardwareObjectState.OFF:
            HWR.beamline.diffractometer.abort_centring_operation()
        time.sleep(1)

        if HWR.beamline.diffractometer.is_in_well_known_pos():
            logging.getLogger("user_level_log").info(
                "Phi is already in a \"Well Known Position\"")
        else:
            HWR.beamline.diffractometer.go_to_well_known_pos()


class SampleOnTop:
    def __call__(self, *args, **kw):

        HWR.beamline.diffractometer.head_orientation.set_value(
            HWR.beamline.diffractometer.head_orientation.VALUES.Top)


class SampleOnBottom:
    def __call__(self, *args, **kw):

        HWR.beamline.diffractometer.head_orientation.set_value(
            HWR.beamline.diffractometer.head_orientation.VALUES.Bottom)


class SampleOnLeft:
    def __call__(self, *args, **kw):

        HWR.beamline.diffractometer.head_orientation.set_value(
            HWR.beamline.diffractometer.head_orientation.VALUES.Left)


class XRD1BeamlineActions(BeamlineActions):

    def __init__(self, *args):
        super().__init__(*args)
