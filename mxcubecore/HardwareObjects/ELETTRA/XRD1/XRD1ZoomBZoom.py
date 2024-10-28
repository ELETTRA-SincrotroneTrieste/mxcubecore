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

from enum import Enum
import PyTango

from mxcubecore.HardwareObjects.abstract.AbstractNState import AbstractNState
from mxcubecore import hwo_header_log


class XRD1ZoomBZoom(AbstractNState):

    def __init__(self, name):

        AbstractNState.__init__(self, name)
        self.ch_camera_zoom_level = None
        self.ch_camera_num_zoom_levels = None
        self.ch_scale_um_per_pixel = None

    @hwo_header_log
    def init(self):

        self.ch_camera_num_zoom_levels = self.get_channel_object("num_zoom_levels",
                                                                 optional=False)
        super(XRD1ZoomBZoom, self).init()
        self.ch_camera_zoom_level = self.get_channel_object("video_zoom",
                                                            optional=False)
        self.ch_scale_um_per_pixel = self.get_channel_object("scale_um_per_pixel",
                                                             optional=False)

        # SIGNALS CONNECTIONS
        self.connect(self.ch_camera_zoom_level, 'update',
                     lambda tango_val: self.update_value(self.value_to_enum(tango_val)))

        self.update_limits((0, len(self.VALUES)))
        self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_value(self):

        try:
            value = self.value_to_enum(self.ch_camera_zoom_level.get_value())
            self.log.info(f"Read the zoom level of \"{self.username}\" "
                          f"(it's \"{value.name}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the zoom level of the \"{self.username}\" from" \
                      f" the attribute \"{self.ch_camera_zoom_level.attribute_name}\" "\
                      f"of the tango device \"{self.ch_camera_zoom_level.device_name}\""
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return value

    @hwo_header_log
    def _set_value(self, value):

        self.update_state(self.STATES.BUSY)
        tango_value = value.value

        try:
            self.ch_camera_zoom_level.set_value(tango_value)
            self.log.info(f"Zoom level of the \"{self.username}\" changed to"
                          f" \"{value.name}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the zoom level of the \"{self.username}\"" \
                      f" setting the attribute " \
                      f"\"{self.ch_camera_zoom_level.attribute_name}\" of the tango" \
                      f" device \"{self.ch_camera_zoom_level.device_name}\" to" \
                      f" \"{tango_value}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        finally:
            self.update_state(self.STATES.READY)

    @hwo_header_log
    def initialise_values(self):

        try:
            num_of_zoom_lvls = self.ch_camera_num_zoom_levels.get_value()
            values = {"LEVEL%s" % str(idx): idx + 1 for idx in range(num_of_zoom_lvls)}
            self.VALUES = Enum("ValueEnum", dict(values, **{
                item.name: item.value for item in self.VALUES}))
            self.log.info(f"Range of levels of the \"{self.username}\""
                          f" initialized: {self.VALUES}")

        except PyTango.DevFailed:
            err_msg = f"Failed to read the number of zoom levels of the " \
                      f"\"{self.username}\" from the attribute " \
                      f"\"{self.ch_camera_zoom_level.attribute_name}\" of the " \
                      f"tango device \"{self.ch_camera_zoom_level.device_name}\" "
            self.log.exception(err_msg)
            self.VALUES = Enum("ValueEnum", dict({"LEVEL0": None}, **{
                item.name: item.value for item in self.VALUES}))

    @hwo_header_log
    def get_um_per_pixel(self):
        return self.ch_scale_um_per_pixel.get_value()

    @hwo_header_log
    def update_limits(self, limits=None):
        super(AbstractNState, self).update_limits(limits)
