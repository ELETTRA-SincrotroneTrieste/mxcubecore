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

from mxcubecore.HardwareObjects.BeamlineActions import BeamlineActions, ControllerCommand
from mxcubecore.BaseHardwareObjects import HardwareObjectState
from mxcubecore import HardwareRepository as HWR
import gevent


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
        if HWR.beamline.diffractometer.head_orientation.get_value() == \
            HWR.beamline.diffractometer.head_orientation.VALUES.Top:
            logging.getLogger("user_level_log").info(
                "Sample is already in TOP position")
        else:
            HWR.beamline.diffractometer.head_orientation.set_value(
                HWR.beamline.diffractometer.head_orientation.VALUES.Top)


class SampleOnBottom:
    def __call__(self, *args, **kw):
        if HWR.beamline.diffractometer.head_orientation.get_value() == \
            HWR.beamline.diffractometer.head_orientation.VALUES.Bottom:
            logging.getLogger("user_level_log").info(
                "Sample is already in BOTTOM position")
        else:
            HWR.beamline.diffractometer.head_orientation.set_value(
                HWR.beamline.diffractometer.head_orientation.VALUES.Bottom)


class SampleOnLeft:
    def __call__(self, *args, **kw):

        if HWR.beamline.diffractometer.head_orientation.get_value() == \
            HWR.beamline.diffractometer.head_orientation.VALUES.Left:
            logging.getLogger("user_level_log").info(
                "Sample is already in LEFT position")
        else:
            HWR.beamline.diffractometer.head_orientation.set_value(
                HWR.beamline.diffractometer.head_orientation.VALUES.Left)


class SampleTriclinic:
    def __call__(self, *args, **kw):

        if HWR.beamline.diffractometer.head_orientation.get_value() == \
            HWR.beamline.diffractometer.head_orientation.VALUES.Triclinic:
            logging.getLogger("user_level_log").info(
                "Sample is already in TRICLINIC position")
        else:
            HWR.beamline.diffractometer.head_orientation.set_value(
                HWR.beamline.diffractometer.head_orientation.VALUES.Triclinic)


def _cmd_done(obj, cmd_execution):
    """Handle the command execution.

    This overrides ControllerCommand._cmd_done(...) because it emitted both 'commandReplyArrived' and 'commandReady'
    after executing a command. As a result, the message 'Command <command name> done' was appearing twice in the UI log.

    Args:
        (obj): Command execution greenlet.
    """

    try:
        res = cmd_execution.get()
        res = res if res else ""
    except Exception:
        logging.getLogger("HWR").exception("%s: execution failed", str(obj.name()))
        obj.emit("commandFailed", (str(obj.name()),))
    else:
        if isinstance(res, gevent.GreenletExit):
            # command aborted
            obj.emit("commandFailed", (str(obj.name()),))
        else:
            obj.emit("commandReplyArrived", (str(obj.name()), res))


class XRD1BeamlineActions(BeamlineActions):

    def __init__(self, *args):
        super().__init__(*args)

        # A horrible patch (don't do again and ask mxcube community), see the method's docstring for clarifications
        ControllerCommand._cmd_done = _cmd_done
