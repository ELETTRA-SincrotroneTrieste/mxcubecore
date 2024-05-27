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

from mxcubecore.HardwareObjects.abstract.AbstractFlux import AbstractFlux
from mxcubecore import hwo_header_log


class XRD1Flux(AbstractFlux):

    unit = "ph/s"

    def __init__(self, name):

        super(XRD1Flux, self).__init__(name)
        self.ch_beam_flux = None

    @hwo_header_log
    def init(self):

        super(XRD1Flux, self).init()
        self.ch_beam_flux = self.get_channel_object("beam_flux", optional=False)

        # SIGNALS CONNECTIONS
        self.connect(self.ch_beam_flux, "update", self.update_value)

    @hwo_header_log
    def get_value(self):

        try:
            value = self.ch_beam_flux.get_value()
            self.log.info(f"Read the value of the \"{self.username}\" "
                          f"(it's \"{value} {self.unit}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the level of the \"{self.username}\" from the" \
                      f" attribute \"{self.ch_beam_flux.attribute_name}\" of the tango" \
                      f" device \"{self.ch_beam_flux.device_name}\" "
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value

    @hwo_header_log
    def _set_value(self, value):

        raise NotImplementedError(f"Flux can not be set to {value}")
