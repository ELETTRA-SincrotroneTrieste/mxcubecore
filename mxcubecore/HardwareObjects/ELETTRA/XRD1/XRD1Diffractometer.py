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
import time
import PyTango
import gevent
from gevent.event import AsyncResult

from mxcubecore.HardwareObjects.GenericDiffractometer import GenericDiffractometer
from mxcubecore import HardwareRepository as HWR
from mxcubecore import hwo_header_log


class TwoClickCentringState(int, enum.Enum):
    IDLE = 0  # user has to click on Start!
    USER_SELECTING = 1  # at beginning; user has to do first click
    MOVING_H = 2  # after the user has selected the first point and has clicked on
    # "Next Step" =  will move H
    MOVING_V = 3  # after the we moved H axis =  will move V axis
    ROTATING_PHI = 4  # after the we moved V axis =  we'are rotating phi
    WAITING_PHI = 5  # after the we rotated phi =  wait for phi stop
    WAITING_PICTURE_AXES = 6  # after the we rotated phi =  wait for new picture axes
    DONE = 7  # phi back in original position =  we're done!
    ERROR = 8  # we got an error
    USER_ABORTED = 9  # we got an error


class XRD1Diffractometer(GenericDiffractometer):

    map_to_mxcube_state = {
        PyTango.DevState.ON: GenericDiffractometer.STATES.READY,
        PyTango.DevState.FAULT: GenericDiffractometer.STATES.FAULT,
        PyTango.DevState.OFF: GenericDiffractometer.STATES.OFF,
        PyTango.DevState.UNKNOWN: GenericDiffractometer.STATES.UNKNOWN
    }

    CENTRING_METHOD_MANUAL = "Manual 2-click"
    MANUAL3CLICK_MODE = "Manual 2-click"

    def __init__(self, *args):

        super(XRD1Diffractometer, self).__init__(*args)
        self.ch_centering_selected_x_mm = None
        self.ch_centering_selected_y_mm = None
        self.ch_centering_state = None
        self.ch_centering_phase = None
        self.ch_beam_center_x = None
        self.ch_beam_center_y = None
        self.ch_state = None
        self.cmd_centring_start_method = None
        self.cmd_centring_clean_all = None
        self.cmd_centring_abort = None
        self.timeout_centring_operation = 60  # [sec]
        self.beam_center_x = None
        self.beam_center_y = None
        self.head_orientation = None
        self.mount_mode = None
        self.last_centred_position = [333,222]  # [x, y]
        self.well_known_phi_positions = None

    @hwo_header_log
    def init(self) -> bool:

        super(XRD1Diffractometer, self).init()

        self.centring_methods = {
            XRD1Diffractometer.CENTRING_METHOD_MANUAL: self.start_manual_centring
        }

        # CENTRING_MOTORS_NAME are configured in the xml
        self.ch_centering_selected_x_mm = self.get_channel_object(
            "centering_selected_x_mm", optional=False)
        self.ch_centering_selected_y_mm = self.get_channel_object(
            "centering_selected_y_mm", optional=False)
        self.ch_centering_state = self.get_channel_object(
            "centering_state", optional=False)
        self.ch_centering_phase = self.get_channel_object(
            "centering_phase", optional=False)
        self.ch_beam_center_x = self.get_channel_object(
            "beam_center_x", optional=False)
        self.ch_beam_center_y = self.get_channel_object(
            "beam_center_y", optional=False)
        self.ch_state = self.get_channel_object("state", optional=False)
        self.cmd_centring_start_method = self.get_command_object(
            "centring_start_method")
        self.cmd_centring_abort = self.get_command_object("centring_abort")
        self.head_orientation = self.get_object_by_role("headorientation")
        self.mount_mode = self.get_property("sample_mount_mode", "manual")
        self.well_known_phi_positions = eval(self.get_property("well_known_phi_pos"))

        # SIGNALS CONNECTIONS
        self.connect(self.kappa_phi, "valueChanged", self.update_phi)
        self.connect(self.alignment_z, "valueChanged", self.update_phiz)
        self.connect(self.kappa, "valueChanged", self.update_kappa)
        self.connect(self.omega, "valueChanged", self.update_omega)
        self.connect(self.centring_x, "valueChanged", self.update_sampx)
        self.connect(self.centring_y, "valueChanged", self.update_sampy)
        self.connect(self.zoom, "valueChanged", self.update_zoom)
        self.connect(self.ch_beam_center_x, "update", self.update_beam_center_x)
        self.connect(self.ch_beam_center_y, "update", self.update_beam_center_y)

        self.update_phi()
        self.update_phiz()
        self.update_kappa()
        self.update_omega()
        self.update_sampx()
        self.update_sampy()
        self.update_zoom()

        self.set_phase(GenericDiffractometer.PHASE_UNKNOWN)

    @property
    def omega(self):
        """omega motor object

        Returns:
            AbstractActuator
        """
        return self.motor_hwobj_dict.get("omega")

    @property
    def kappa_phi(self):
        """kappa_phi motor object

        Returns:
            AbstractActuator
        """
        return self.motor_hwobj_dict.get("phi")

    @hwo_header_log
    def get_centring_state(self):
        tango_value = self.ch_centering_state.get_value()
        return TwoClickCentringState(tango_value)

    @hwo_header_log
    def update_beam_center_x(self, x=None):
        if x is None:
            x = self.ch_beam_center_x.get_value()
        self.beam_center_x = x

    @hwo_header_log
    def update_beam_center_y(self, y=None):
        if y is None:
            y = self.ch_beam_center_y.get_value()
        self.beam_center_y = y

    @hwo_header_log
    def convert_pixels_to_mm(self, x, y):

        # read um per pixel takes into account the zoom level (at least it should )
        mm_per_pixel = self.zoom.get_um_per_pixel() / 1000.

        stream_width, stream_height, image_scale = \
            HWR.beamline.sample_view.camera.get_stream_size()
        image_height = float(HWR.beamline.sample_view.camera.get_height())
        image_width = float(HWR.beamline.sample_view.camera.get_width())

        scale_ratio_x = stream_width / image_width
        scale_ratio_y = stream_height / image_height

        mm_per_pixel_x_scaled = mm_per_pixel / scale_ratio_x
        mm_per_pixel_y_scaled = mm_per_pixel / scale_ratio_y

        x_mm = float(x - (stream_width / 2.0)) * mm_per_pixel_x_scaled
        y_mm = float((stream_height / 2.0) - y) * mm_per_pixel_y_scaled

        return x_mm, y_mm

    @hwo_header_log
    def manual_centring(self):

        # TODO evaluate whether handle phase changing using executer state
        self.set_phase(GenericDiffractometer.PHASE_CENTRING)
        try:
            x, y = None, None
            # Abort if previous 2-clicks centring procedure if it's still running
            # (executer)
            if self.get_state() != self.STATES.OFF:
                self.abort_centring_operation()

            # Start a new 2-clicks centring procedure (executer)
            self.cmd_centring_start_method("twoClickcentring_fsm")
            self.log.debug(f"Start {self.CENTRING_METHOD_MANUAL} centring procedure of"
                           f" \"{self.username}\" (executer)")

            for click in range(HWR.beamline.click_centring_num_clicks):
                self.user_clicked_event = AsyncResult()
                x, y = self.user_clicked_event.get()
                x_mm, y_mm = self.convert_pixels_to_mm(x, y)
                self.log.debug(f"Cursor position:"
                               f"\n- In pixel [x: {x} px, y: {y} px] (top-left origin)"
                               f"\n- In mm [x: {x_mm} mm, y: {y_mm} mm]")

                # Wait the executer to turn in USER_SELECTING state
                centring_state = self.get_centring_state()
                while centring_state != TwoClickCentringState.USER_SELECTING:
                    if centring_state in [TwoClickCentringState.ERROR,
                                          TwoClickCentringState.USER_ABORTED]:
                        err_msg = f"{self.CENTRING_METHOD_MANUAL} centring procedure " \
                                  f"failed. Centring state of the tango device " \
                                  f"{self.ch_centering_state.device_name} is " \
                                  f"\"{centring_state.name}\""
                        self.log.error(err_msg)
                        self.user_log.error(err_msg)
                        raise Exception(err_msg)
                    gevent.sleep(self.ch_centering_state.polling / 1000)
                    centring_state = self.get_centring_state()

                # Set up a new step of the 2 clicks centring procedure (executer)
                self.ch_centering_selected_x_mm.set_value(x_mm)
                self.ch_centering_selected_y_mm.set_value(y_mm)
                # Trigger the new step of the 2 clicks centring procedure (executer)
                self.ch_centering_state.set_value(TwoClickCentringState.MOVING_H.value)
                self.log.debug(f"Triggered next step of {self.CENTRING_METHOD_MANUAL}"
                               f" centring procedure of \"{self.username}\"")

                centring_state = self.get_centring_state()
                while centring_state in [TwoClickCentringState.MOVING_H,
                                         TwoClickCentringState.MOVING_V,
                                         TwoClickCentringState.ROTATING_PHI,
                                         TwoClickCentringState.WAITING_PHI,
                                         TwoClickCentringState.WAITING_PICTURE_AXES]:
                    gevent.sleep(self.ch_centering_state.polling / 1000)
                    centring_state = self.get_centring_state()

                if centring_state in [TwoClickCentringState.ERROR,
                                      TwoClickCentringState.USER_ABORTED]:
                    err_msg = f"{self.CENTRING_METHOD_MANUAL} centring procedure " \
                              f"failed. Centring state of the tango device " \
                              f"{self.ch_centering_state.device_name} is " \
                              f"\"{centring_state.name}\""
                    self.log.error(err_msg)
                    self.user_log.error(err_msg)
                    raise Exception(err_msg)

            self.last_centred_position = [x, y]
            centred_pos_dir = self.current_motor_positions.copy()
            self.log.info(f"Centred motors position:\n {centred_pos_dir}")
            return centred_pos_dir
        except:
            # Handle user manual centering canceling
            if self.get_state() != self.STATES.OFF:
                self.abort_centring_operation()
            raise
        finally:
            self.set_phase(GenericDiffractometer.PHASE_UNKNOWN)

    @hwo_header_log
    def abort_centring_operation(self):
        self.cmd_centring_abort()
        self.log.debug(f"Aborting centring operation of the \"{self.username}\" ...")

        # Wait the executer to turn in OFF state
        with gevent.Timeout(self.timeout_centring_operation, RuntimeError(
            f"Timed out. Fail to abort the previous {self.CENTRING_METHOD_MANUAL}"
            f" centering procedure calling the command "
            f"\"{self.cmd_centring_abort.command}\" of the tango"
            f" device \"{self.cmd_centring_abort.device_name}\"")
        ):
            while self.get_state() != self.STATES.OFF:
                gevent.sleep(self.ch_state.polling / 1000)
        # After the executer goes OFF it's needed to wait few seconds more for the
        # centring procedure to exit
        time.sleep(1)
        self.log.warning(f"Centring operation of the \"{self.username}\" aborted")

    @hwo_header_log
    def move_to_motors_positions(self, motors_positions, wait=False):

        # The executer is in charge of moving the motors
        pass

    @hwo_header_log
    def get_positions(self):

        return self.current_motor_positions.copy()

    @hwo_header_log
    def get_state(self):

        try:
            tango_state = self.ch_state.get_value()
            state = self.map_to_mxcube_state.get(tango_state, self.STATES.UNKNOWN)
            self.log.info(f"Read the state of the \"{self.username}\" "
                          f"(it's \"{state.name}\")")
        except PyTango.DevFailed:
            err_msg = f"Failed to read the state of the \"{self.username}\" from" \
                      f" the attribute \"{self.ch_state.attribute_name}\" of the " \
                      f"tango device \"{self.ch_state.device_name}\" "
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return state

    @hwo_header_log
    def is_waiting_user_input(self):
        """
        Detects if device is moving
        """

        return self.get_centring_state() == TwoClickCentringState.USER_SELECTING

    @hwo_header_log
    def is_in_fault(self):
        """
        Detects if device is moving
        """

        return self.get_centring_state() in [TwoClickCentringState.ERROR,
                                             TwoClickCentringState.USER_ABORTED]

    @hwo_header_log
    def is_moving(self):
        """
        Detects if device is moving
        """

        return self.get_centring_state() in [TwoClickCentringState.MOVING_H,
                                             TwoClickCentringState.MOVING_V,
                                             TwoClickCentringState.ROTATING_PHI,
                                             TwoClickCentringState.WAITING_PHI,
                                             TwoClickCentringState.WAITING_PICTURE_AXES]

    @hwo_header_log
    def is_ready(self):
        """
        Detects if device is ready for centring procedure
        """

        motors_states = [
            self.kappa.get_state(),
            self.kappa_phi.get_state(),
            self.alignment_z.get_state(),
            self.centring_x.get_state(),
            self.centring_y.get_state()
        ]
        if not all(state == self.STATES.READY for state in motors_states):
            ready = False
            self.user_log.warning("Diffractometer is moving...")
        elif self.head_orientation.get_value() != self.head_orientation.VALUES.Left:
            ready = False
            self.user_log.warning("Sample is not in LEFT position. Use "
                                  "\"Beamline Actions\" to put it in LEFT position")
        elif not self.is_in_well_known_pos():
            ready = False
            self.user_log.warning("Phi motor is not in a \"Well Known Position\". Use "
                                  "\"Beamline Actions\" to put Phi to a \"Well Known "
                                  "Position\"")
        else:
            ready = True
        return ready

    @hwo_header_log
    def go_to_well_known_pos(self):

        # Make phi position orthogonal to the camera
        curr_pos = self.kappa_phi.get_value()
        min_diff = 360
        for position, pos_name in self.well_known_phi_positions.items():
            diff = abs(curr_pos % 360 - position)
            if diff <= min_diff:
                min_diff = diff
                go_to_pos = position
                go_to_name_pos = pos_name
        self.cmd_centring_start_method(go_to_name_pos)
        self.log.debug(f"Current position: {curr_pos} -> "
                       f"closest \"Well Known Position\" : "
                       f"{go_to_pos}")

    @hwo_header_log
    def is_in_well_known_pos(self):
        in_pos = False
        curr_pos = self.kappa_phi.get_value()
        diffs = [abs(curr_pos - pos) for pos in self.well_known_phi_positions.keys()]
        for diff in diffs:
            if diff < 0.0001:
                in_pos = True
        return in_pos

    @hwo_header_log
    def automatic_centring(self):

        raise NotImplemented

    @hwo_header_log
    def motor_positions_to_screen(self, centred_positions_dict):

        return self.last_centred_position[0], self.last_centred_position[1]

        # print "#### motor_positions_to_screen", centred_positions_dict
        # current_motor_positions
        xc_pix = self.tangoproxy.BeamPositionHorizontal
        yc_pix = self.tangoproxy.BeamPositionVertical
        # print "#### - Beam pos", xc_pix,yc_pix
        try:
            pixels_per_mm_x, pixels_per_mm_y = self.get_pixels_per_mm()
            actual_positions = self.get_positions()
            xc_mm = actual_positions['sampx']
            yc_mm = actual_positions['sampy']
            x_mm = centred_positions_dict['phiy']
            y_mm = centred_positions_dict['phiz']
            # RB: Check the sign in the formula, depends on the motor direction
            x_pix = xc_pix + pixels_per_mm_x * (x_mm - xc_mm) * -1.
            y_pix = yc_pix + pixels_per_mm_y * (y_mm - yc_mm)
            # print "#### - Calculated pos", x_pix,y_pix
        except:
            x_pix = xc_pix
            y_pix = yc_pix
        return round(x_pix), round(y_pix)


    @hwo_header_log
    def update_phiz(self, pos=None):
        if not pos:
            pos = self.alignment_z.get_value()
        self.current_motor_positions["phiz"] = pos

    @hwo_header_log
    def update_sampx(self, pos=None):
        if not pos:
            pos = self.centring_x.get_value()
        self.current_motor_positions["sampx"] = pos

    @hwo_header_log
    def update_sampy(self, pos=None):
        if not pos:
            pos = self.centring_y.get_value()
        self.current_motor_positions["sampy"] = pos

    @hwo_header_log
    def update_zoom(self, pos=None):
        if not pos:
            pos = self.zoom.get_value().value
        self.current_motor_positions["zoom"] = pos
        self.pixels_per_mm_x = int(1 / (self.zoom.get_um_per_pixel() / 1000.))
        self.pixels_per_mm_y = int(1 / (self.zoom.get_um_per_pixel() / 1000.))

    @hwo_header_log
    def update_phi(self, pos=None):
        if not pos:
            pos = self.kappa_phi.get_value()
        self.current_motor_positions["phi"] = pos

    @hwo_header_log
    def update_kappa(self, pos=None):
        if not pos:
            pos = self.kappa.get_value()
        self.current_motor_positions["kappa"] = pos

    @hwo_header_log
    def update_omega(self, pos=None):
        if not pos:
            pos = self.omega.get_value()
        self.current_motor_positions["omega"] = pos

    @hwo_header_log
    def re_emit_values(self):

        self.emit("zoomMotorPredefinedPositionChanged", None, None)

    @hwo_header_log
    def move_omega_relative(self, relative_angle):

        self.kappa_phi.set_value_relative(relative_angle, 5)

    @hwo_header_log
    def set_phase(self, phase, timeout=None):

        self.current_phase = str(phase)
        self.emit("PhaseChanged", (self.current_phase,))

    @hwo_header_log
    def get_point_from_line(self, point_one, point_two, index, images_num):

        return point_one.as_dict()
