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
import gevent

from mxcubecore.HardwareObjects.abstract.AbstractNState import AbstractNState
from mxcubecore import hwo_header_log


class XRD1Aperture(AbstractNState):

    def __init__(self, name):
        super(XRD1Aperture, self).__init__(name)
        self.ch_diameters_size_list = None
        self.ch_get_diameter_size = None
        self.cmd_set_diameter_size = None
        self.full_beam_in_um = None
        self.timeout = None

    @hwo_header_log
    def init(self):

        self.ch_diameters_size_list = self.get_channel_object("diameters_size_list",
                                                              optional=False)
        self.full_beam_in_um = str(self.get_property("full_beam"))
        super(XRD1Aperture, self).init()
        self.ch_get_diameter_size = self.get_channel_object("get_diameter_size",
                                                            optional=False)
        self.cmd_set_diameter_size = self.get_command_object("set_diameter_size")
        self.timeout = self.get_property("timeout")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_get_diameter_size, 'update',
                     lambda tango_val: self.update_value(self.value_to_enum(tango_val)))

        self.update_state(self.STATES.READY)

    @hwo_header_log
    def get_value(self):

        try:
            value = self.value_to_enum(self.ch_get_diameter_size.get_value())
            self.log.info(f"Read the diameter size of the \"{self.username}\""
                          f" (it's \"{value.name})\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the diameter size of the \"{self.username}\"" \
                      f" from the attribute " \
                      f"\"{self.ch_get_diameter_size.attribute_name}\" of the " \
                      f"tango device \"{self.ch_get_diameter_size.device_name}\" "
            self.log.exception(err_msg)
            value = self.VALUES.UNKNOWN
        return value

    @hwo_header_log
    def _set_value(self, value):

        tango_value = value.value
        self.update_state(self.STATES.BUSY)
        try:
            self.cmd_set_diameter_size(tango_value)
            gevent.sleep(1)
            with gevent.Timeout(self.timeout,
                                TimeoutError(f"Timed out. The diameter size of the"
                                             f"\"{self.username}\" has not reached the "
                                             f"target value after {self.timeout} sec")):
                while self.ch_get_diameter_size.get_value() != tango_value:
                    self.log.debug(f"Waiting \"{self.username}\" to reach the target"
                                   f" value {value.name}")
                    gevent.sleep(self.ch_get_diameter_size.polling / 1000)

            self.log.info(f"Diameter size of the \"{self.username}\" changed to"
                          f" \"{value.name}\"")
        except PyTango.DevFailed:
            err_msg = f"Failed to change the size of the diameters of the " \
                      f"\"{self.username}\" to \"{tango_value}\" calling the command" \
                      f" \"{self.cmd_set_diameter_size.command}\" of the tango device" \
                      f" \"{self.cmd_set_diameter_size.device_name}\""
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)
        except TimeoutError as e:
            self.user_log.error(str(e))
            raise RuntimeError(str(e))
        finally:
            self.update_state(self.STATES.READY)

    @hwo_header_log
    def initialise_values(self):

        try:
            diameters_size = self.ch_diameters_size_list.get_value()
            values = {diameter_size.split(' ')[0]: diameter_size
                      for diameter_size in diameters_size
                      if diameter_size != "Full beam"}
            values[self.full_beam_in_um] = "Full beam"
            values = dict(reversed(sorted(values.items(),
                                          key=lambda item: int(item[0]))))
            self.VALUES = Enum("ValueEnum", dict(values, **{item.name: item.value
                                                            for item in self.VALUES}))
            self.log.info(f"List of diameters size of the \"{self.username}\""
                          f" initialized: {self.VALUES}")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the list of diameters size of the" \
                      f" \"{self.username}\" from the " \
                      f"attribute \"{self.ch_diameters_size_list.attribute_name}\" of" \
                      f" the tango device \"{self.ch_diameters_size_list.device_name}\""
            self.log.exception(err_msg)
