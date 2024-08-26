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

from mxcubecore.HardwareObjects.TangoLimaMpegVideo import TangoLimaMpegVideo


class XRD1CameraBZoom(TangoLimaMpegVideo):
    def __init__(self, name):
        super(XRD1CameraBZoom, self).__init__(name)

    def init(self):
        super().init()

    def get_available_stream_sizes(self):
        try:
            w, h = self.get_width(), self.get_height()
            video_sizes = [(w, h), (int(w * 0.8), int(h * 0.8)), (int(w * 0.6), int(h * 0.6))]
        except (ValueError, AttributeError):
            video_sizes = []

        return video_sizes

    def restart_streaming(self, size):
        self.stop_streaming()
        self.start_streaming(self._format, tuple(map(lambda x: int(x), size)), "8000")
