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

from mxcubecore.HardwareObjects.abstract.AbstractBeam import AbstractBeam
from mxcubecore import hwo_header_log


class XRD1Beam(AbstractBeam):
    def __init__(self, name):

        super(XRD1Beam, self).__init__(name)

    @hwo_header_log
    def init(self):

        super(XRD1Beam, self).init()
        self._aperture = self.get_object_by_role("aperture")

        # SIGNALS CONNECTIONS
        self.connect(self._aperture, 'valueChanged', self.aperture_diameter_update)

        self.aperture_diameter_update(self._aperture.get_value())

    @hwo_header_log
    def aperture_diameter_update(self, size):

        if size == self._aperture.VALUES.UNKNOWN:
            width = 0
            height = 0
            label = 0
        else:
            width = float(size.name) / 1000  # [mm]
            height = float(size.name) / 1000  # [mm]
            label = size.name

        self._beam_size_dict["aperture"] = [width, height]

        # ! Note ! The slot "beam_changed" in ../routes/signals.py does not use "label"
        self._beam_info_dict["label"] = label
        self.evaluate_beam_info()
        self.re_emit_values()

    @hwo_header_log
    def get_value(self):

        return list(self.get_beam_info_dict().values())

    @hwo_header_log
    def get_available_size(self):

        return {
            "type": "enum",
            "values": [value.name for value in self._aperture.VALUES
                       if value.name != 'UNKNOWN']
        }

    @hwo_header_log
    def set_value(self, value):

        self._aperture.set_value(self._aperture.VALUES[value])

    @hwo_header_log
    def set_beam_position_on_screen(self, beam_x_y):

        self._beam_position_on_screen = tuple(beam_x_y)
        self.emit("beamPosChanged", (self._beam_position_on_screen,))
