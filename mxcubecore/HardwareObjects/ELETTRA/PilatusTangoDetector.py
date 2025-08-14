# encoding: utf-8
#
#  Project name: MXCuBE
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
#  You should have received a copy of the GNU General Lesser Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

__copyright__ = """ Copyright © 2020 by the MXCuBE collaboration """
__credits__ = ["ELETTRA"]
__license__ = "LGPLv3+"
__category__ = "General"

import enum
import time
import traceback

import PyTango
import gevent

from mxcubecore.BaseHardwareObjects import HardwareObjectState
from mxcubecore.HardwareObjects.abstract.AbstractDetector import AbstractDetector
from mxcubecore import trace_call_log


class PilatusTangoDetector(AbstractDetector):
    map_to_mxcube_state = {
        PyTango.DevState.INIT: AbstractDetector.STATES.BUSY,
        PyTango.DevState.ON: AbstractDetector.STATES.READY,
        PyTango.DevState.RUNNING: AbstractDetector.STATES.BUSY,
        PyTango.DevState.FAULT: AbstractDetector.STATES.FAULT,
        PyTango.DevState.ALARM: AbstractDetector.STATES.WARNING,
        PyTango.DevState.UNKNOWN: AbstractDetector.STATES.UNKNOWN,
    }

    def __init__(self, name):
        """
        Descript. :
        """
        AbstractDetector.__init__(self, name)

        self.distance_motor_hwobj = None
        self.detector_distance = None
        self.detect_type = None
        self.has_shut_less = None
        self.ch_state = None
        self.ch_status = None
        self.ch_threshold = None
        self.ch_last_image_taken = None
        self.ch_file_prefix = None
        self.ch_file_dir = None
        self.ch_num_frames = None
        self.ch_num_exposure_per_frame = None
        self.ch_trigger_mode = None
        self.ch_exposure_time = None
        self.ch_file_start_num = None
        self.ch_exposure_period = None
        self.ch_mx_settings = None
        self.cmd_init = None
        self.cmd_start_acq = None
        self.cmd_stop_acq = None
        self.cmd_reset = None

    def init(self):
        """
        Descript. :
        """
        AbstractDetector.init(self)

        self._roi_modes_list = eval(self.get_property("roi_mode_list", "[]"))

        # min total, max total, rate")
        self._exposure_time_limits = eval(
            self.get_property("exposure_time_limits", "[0.084, 3600, 0.084]")
        )

        self.detect_type = self.get_property("type")
        self.has_shut_less = self.get_property("hasShutterless")

        # Tango channels and commands
        self.ch_state = self.get_channel_object("state", optional=False)
        self.ch_status = self.get_channel_object("status", optional=False)
        self.ch_threshold = self.get_channel_object("threshold", optional=False)
        self.ch_last_image_taken = self.get_channel_object(
            "last_image_taken", optional=False
        )
        self.ch_file_prefix = self.get_channel_object("file_prefix", optional=False)
        self.ch_file_dir = self.get_channel_object("file_dir", optional=False)
        self.ch_num_frames = self.get_channel_object("num_frames", optional=False)
        self.ch_num_exposure_per_frame = self.get_channel_object(
            "num_exposure_per_frame", optional=False
        )
        self.ch_trigger_mode = self.get_channel_object("trigger_mode", optional=False)
        self.ch_exposure_time = self.get_channel_object("exposure_time", optional=False)
        self.ch_exposure_period = self.get_channel_object(
            "exposure_period", optional=False
        )
        self.ch_file_start_num = self.get_channel_object(
            "file_start_num", optional=False
        )
        self.ch_mx_settings = self.get_channel_object("mx_settings", optional=False)

        self.cmd_init = self.get_command_object("init")
        self.cmd_start_acq = self.get_command_object("start_acq")
        self.cmd_stop_acq = self.get_command_object("stop_acq")
        self.cmd_reset = self.get_command_object("reset")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_state, "update", self._update_state)
        # self.connect(self.ch_status, "update", self.update_status)

        self.update_state()

    @trace_call_log
    def _update_state(self, tango_state=None):
        if tango_state is None:
            try:
                state = self.get_state()
            except PyTango.DevFailed:
                state = self.STATES.UNKNOWN
        else:
            state = self.map_to_mxcube_state.get(tango_state, self.STATES.UNKNOWN)

        # Send UI message in case of warnings/errors
        if state in [self.STATES.WARNING, self.STATES.FAULT]:
            try:
                status = self.ch_status.get_value()
                self.user_log.warning("Pilatus status: %s", status)
            except PyTango.DevFailed:
                self.log.exception("Error occurred getting Pilatus status")

        self.update_state(state)

    @trace_call_log
    def get_state(self):
        try:
            tango_state = self.ch_state.get_value()
            state = self.map_to_mxcube_state.get(tango_state, self.STATES.UNKNOWN)
            self.log.info(f'Read the state of the Pilatus (it\'s "{state.name}")')
        except PyTango.DevFailed:
            err_msg = (
                f"Failed to read the state of the Pilatus"
                f' from the attribute "{self.ch_state.attribute_name}"'
                f' of the tango device "{self.ch_state.device_name}" '
            )
            self.log.exception(err_msg)
            raise ValueError(err_msg)
        return state

    @trace_call_log
    def has_shutterless(self):
        return self.has_shut_less

    @trace_call_log
    def get_last_frame_taken(self):
        """
        Description : Returns the file path of the last frame taken or None in case
                      of error
        rtype: string or None
        """
        try:
            last_frame = self.ch_last_image_taken.get_value()
        except PyTango.DevFailed:
            self.log.exception("Tango error occurred retrieving last frame taken")
            last_frame = None
        return last_frame

    @trace_call_log
    def wait_and_init(self, retry=-1):
        try:
            self.log.info("Pilatus please_init()")
            self.cmd_init()
        except PyTango.DevFailed as e:
            if e.args[0].desc == "Pilatus reply: 1 ERR access denied":
                raise RuntimeError(
                    "*** Detector CAMSERVER not responding! Access Denied - Ask BL staff to restart it ***"
                )
            elif e.args[0].desc == "Pilatus wrong reply":
                if 0 <= retry <= 90:
                    self.log.info(
                        "- Pilatus wait_and_init ...wrong reply since %d s" % int(retry)
                    )
                    gevent.sleep(5)
                    self.wait_and_init(retry=retry + 5)
                else:
                    raise RuntimeError(
                        "*** Detector fixThreshold Tango Error: %s ***"
                        % traceback.format_exc()
                    )
        except:
            self.log.exception("Error occurred during the Pilatus initialization")

    @trace_call_log
    def fix_threshold(self, energy, fix, retry=-1):
        """
        Description : set P6M threshold to 70% energy (eV!) if needed or asked
        """
        try:
            thresholdP6M = self.ch_threshold.get_value()
            self.log.info("Pilatus Current Threshold is %d eV" % thresholdP6M)
            if fix and not (0.6 * energy <= thresholdP6M <= 0.8 * energy):
                thresholdP6M = round(0.70 * energy, 0)
                self.log.info("Pilatus Fixing Threshold (%d eV)" % thresholdP6M)
                self.ch_threshold.set_value(thresholdP6M)
            else:
                self.log.info("Pilatus NOT changing Threshold (%d eV)" % thresholdP6M)
            return thresholdP6M, True
        except PyTango.DevFailed as e:
            if e.args[0].desc == "Pilatus reply: 1 ERR access denied":
                raise RuntimeError(
                    "*** Detector CAMSERVER not responding! Access Denied - Ask BL staff to restart it ***"
                )
            elif e.args[0].desc == "Pilatus wrong reply":
                if 0 <= retry <= 90:
                    self.log.info(
                        "- Pilatus fixThreshold ...wrong reply since %d s" % int(retry)
                    )
                    gevent.sleep(5)
                    self.fix_threshold(energy, fix, retry=retry + 5)
                elif self.ch_state.get_value() == "INIT":
                    self.log.info(
                        "...PilatusXM is in a fake INIT? trying to recover with a new Init... hope not to end up in a loop"
                    )
                    self.wait_and_init(retry=-1)
                    gevent.sleep(30)
                    self.fix_threshold(energy, fix, retry=1)
                else:
                    raise RuntimeError(
                        "*** Detector fixThreshold Tango Error: %s ***"
                        % traceback.format_exc()
                    )
            elif (e.args[0].desc == "Threshold has not been set") and (
                "ALARM" in self.ch_status.get_value()
            ):
                self.log.info(
                    "...PilatusXM is in ALARM - trying to recover with an Init... hope not to end up in a loop"
                )
                self.wait_and_init(retry=-1)
                gevent.sleep(30)
                self.fix_threshold(energy, fix, retry=1)
            elif e.args[0].desc == "Threshold has not been set":
                self.log.info("Pilatus Threshold not set - doing it now!")
                thresholdP6M = round(0.70 * energy, 0)
                self.ch_threshold.set_value(thresholdP6M)
                return thresholdP6M, False
            else:
                raise RuntimeError(
                    "*** Detector fixThreshold Tango Error: %s ***"
                    % traceback.format_exc()
                )

    @trace_call_log
    def please_init(self, energy):
        thresholdP6M, thresholdState = self.fix_threshold(
            energy=1000 * energy, fix=False, retry=1
        )
        self.user_log.warning("Setting Detector Threshold (%d eV)" % thresholdP6M)
        self.wait_and_init(retry=1)
        if thresholdState:
            raise RuntimeError(
                "*** Detector Initialization! - Current threshold is %d eV ***"
                % thresholdP6M
            )
        else:
            raise RuntimeError(
                "*** Detector Initialization! - Threshold set NOW to %d eV ***"
                % thresholdP6M
            )

    @trace_call_log
    def generate_header(self, start_angle, delta_angle):
        header_info = []

        # TODO valutare se ha senso mettere tutto sotto try e ripetere un tot di volte o eventualmente o
        #  magari si possono ripetere solo le chiamate agli strumenti
        try:
            header_info.append(
                "Wavelength %.4f" % self.bl_control.energy.getCurrentWavelength()
            )
        except:
            self.log.exception(
                "Error occurred generating the header for the Pilatus images"
            )
        try:
            header_info.append(
                "Detector_distance %.5f"
                % (self.bl_control.detector_distance.getPosition() * 0.001)
            )
        except:
            self.log.exception(
                "Error occurred generating the header for the Pilatus"
                f" ({self.detect_type}) images"
            )
        try:
            # TODO doveremmo usare il beam position reale ?
            # beampos = self.bl_control.beam_info.get_beam_position()
            # header_info.append("Beam_xy %.3f %.3f" % (beampos[0],beampos[1]))
            header_info.append("Beam_xy %.3f %.3f" % (1257, 1331))
        except:
            self.log.exception(
                "Error occurred generating the header for the Pilatus images"
            )

        try:
            header_info.append("Start_angle %.3f" % start_angle)
            header_info.append("Angle_increment %.3f" % delta_angle)
            header_info.append("Detector_2theta 0.000")
            header_info.append("Flux Temperature:_%.1f_K" % self.get_cryo_temperature())
        except:
            self.log.exception(
                f'Error occurred generating the header for the "Pilatus images'
            )

        try:
            mot_positions = self.bl_control.diffractometer.get_positions()
            header_info.append("Kappa %.3f" % mot_positions["kappa"])
            header_info.append("Phi %.3f" % mot_positions["kappa_phi"])
            header_info.append("Phi_increment 0.000")
            header_info.append("Omega %.3f" % start_angle)
            header_info.append("Omega_increment %.3f" % delta_angle)
            header_info.append("Oscillation_axis OMEGA")
        except:
            self.log.exception(
                f'Error occurred generating the header for the "Pilatus images'
            )

        return header_info

    @trace_call_log
    def write_header(self, header_list):
        header_info = ""
        for item in header_list:
            header_info += item
            if "Oscillation_axis" not in item:
                header_info += " "
        self.ch_mx_settings.set_value(header_info)

    @trace_call_log
    def change_file_dir_force(self, file_dir):
        # There are problems with the Pilatus FileDir setup, add more tries
        attempts = 3
        for i in range(attempts):
            gevent.sleep(0.6)
            try:
                self.ch_file_dir.set_value(file_dir)
                written_file_dir = self.ch_file_dir.get_value()
                if file_dir == written_file_dir:
                    break
                self.log.warning(
                    f"Pilatus {self.ch_file_dir.attribute_name}"
                    f" anomalous behaviour: "
                    f"[read: {written_file_dir}, "
                    f"written: {file_dir}] "
                )
            except PyTango.DevFailed:
                self.log.exception(
                    f'Error occurred setting the attribute "{self.ch_file_dir.attribute_name}"'
                    f'of the tango device "{self.ch_file_dir.device_name}") to {file_dir}'
                )
                continue

    @trace_call_log
    def ensure_detector_is_ready(self):
        if self.get_state() == self.STATES.FAULT:
            self.reset(raise_exc=False)
        if self.get_state() == self.STATES.BUSY:
            self.stop_acquisition(raise_exc=False)
        if self.get_state() != self.STATES.READY:
            self.reset()

    @trace_call_log
    def prepare_acquisition(
        self,
        data_collect_parameters,
        start_angle,
        delta_angle,
        prefix_in=None,
        mesh_scan=False,
    ):
        self.ensure_detector_is_ready()
        try:
            total_exp_time = data_collect_parameters["oscillation_sequence"][0][
                "exposure_time"
            ]
            num_frames = data_collect_parameters["oscillation_sequence"][0][
                "number_of_images"
            ]
            if prefix_in is None:
                prefix = data_collect_parameters["fileinfo"]["prefix"]
            else:
                prefix = prefix_in
            file_start_num = data_collect_parameters["oscillation_sequence"][0][
                "start_image_number"
            ]
            run_number = data_collect_parameters["fileinfo"]["run_number"]
            image_path = "%s/%d" % (
                data_collect_parameters["fileinfo"]["directory"],
                run_number,
            )
            file_prefix = prefix + "_" + str(run_number)
            file_prefix = file_prefix.replace(":", "-")
            image_path = image_path.replace(":", "-")

            self.ensure_detector_is_ready()  # TODO è veramente necessario ?????

            self.ch_file_prefix.set_value(file_prefix)
            image_path = image_path.replace("//", "/").replace(
                "/net/xrd2-pilatus/", "/ramdisk/"
            )
            self.change_file_dir_force(image_path)
            self.ch_num_frames.set_value(num_frames)
            gevent.sleep(0.1)
            # Force to 1 the number of exposures per frame
            if self.ch_num_exposure_per_frame.get_value() != 1:
                self.ch_num_exposure_per_frame.set_value(1)
            single_exp_time = (float)(total_exp_time) / num_frames
            if single_exp_time < 0.08333:
                msg = f"Too short exposure time (i.e. {single_exp_time} sec) for this Pilatus"
                self.log.error(msg)
                raise RuntimeError(msg)

            # External Trigger Mode
            if mesh_scan:
                # Multi trigger mode
                self.ch_trigger_mode.set_value(3)
            else:
                # Single external trigger mode
                self.ch_trigger_mode.set_value(2)
            gevent.sleep(0.1)

            # Problem when doing fine slicing -- make the exposure period shorther by 0.0006
            # also exposuretime (0.003 readout - 0.006)
            if mesh_scan:
                self.ch_exposure_time.set_value(
                    single_exp_time - 0.014
                )  # 0.0136 # da manuale: 0.013
            else:
                self.ch_exposure_time.set_value(
                    single_exp_time - 0.0036
                )  # da manuale: 0.034
            gevent.sleep(0.1)
            self.ch_exposure_period.set_value(single_exp_time - 0.0006)
            gevent.sleep(0.1)
            self.ch_file_start_num.set_value(file_start_num)
            gevent.sleep(0.1)

            header_info = self.generate_header(start_angle, delta_angle)
            self.write_header(header_info)
        except Exception as e:
            self.log.exception(f"Error occurred preparing Pilatus for the acquisition")
            raise e

    @trace_call_log
    def start_acquisition(self, no_exc=False):
        """
        Starts the acquisition
        """
        try:
            self.cmd_start_acq()
            timeout = 2 * self.ch_state.polling / 1000
            with gevent.Timeout(
                timeout,
                TimeoutError(
                    f'Timed out. The Pilatus state has not changed into "BUSY"'
                    f' after {timeout} sec from the "start" command'
                ),
            ):
                self.log.debug(
                    "Waiting the Pilatus to change the state into"
                    ' "BUSY" after "start" command'
                )
                while (
                    self._state != self.STATES.BUSY
                ):  # TODO se non funziona cambia in self.get_state()
                    gevent.sleep(self.ch_state.polling / 1000)
            self.log.info(f"The Pilatus has been started successfully")
        except PyTango.DevFailed:
            err_msg = (
                f"Failed to start acquisition of the Pilatus"
                f' calling the command "{self.cmd_stop_acq.command}"'
                f' of the tango device "{self.cmd_stop_acq.device_name}"'
            )
            self.log.exception(err_msg)
            raise RuntimeError(err_msg)

    @trace_call_log
    def stop_acquisition(self, raise_exc=True):
        """
        Stops the acquisition
        """
        try:
            self.cmd_stop_acq()
            timeout = 2 * self.ch_state.polling / 1000
            with gevent.Timeout(
                timeout,
                TimeoutError(
                    f'Timed out. The Pilatus state has not changed into "READY"'
                    f" after {timeout} sec from the stop command"
                ),
            ):
                self.log.debug(
                    f"Waiting the Pilatus to change the state into"
                    f' "READY" after stop command'
                )
                while (
                    self._state != self.STATES.READY
                ):  # TODO se non funziona cambia in self.get_state()
                    gevent.sleep(self.ch_state.polling / 1000)
            self.log.info(f"The Pilatus has been stopped successfully")
        except PyTango.DevFailed:
            err_msg = (
                f"Failed to stop the acquisition of the Pilatus"
                f' calling the command "{self.cmd_stop_acq.command}"'
                f' of the tango device "{self.cmd_stop_acq.device_name}"'
            )
            self.log.exception(err_msg)
            if raise_exc:
                raise RuntimeError(err_msg)

    @trace_call_log
    def reset(self, raise_exc=True):
        """
        Resets the detector
        """
        try:
            self.cmd_reset()
            timeout = 2 * self.ch_state.polling / 1000
            with gevent.Timeout(
                timeout,
                TimeoutError(
                    f'Timed out. The Pilatus state has not changed into "READY"'
                    f" after {timeout} sec from the reset command"
                ),
            ):
                self.log.debug(
                    f"Waiting the Pilatus to change the state into"
                    f' "READY" after reset command'
                )
                while (
                    self._state != self.STATES.READY
                ):  # TODO se non funziona cambia in self.get_state()
                    gevent.sleep(self.ch_state.polling / 1000)
            self.log.info(f"The Pilatus has been reset successfully")
        except PyTango.DevFailed:
            err_msg = (
                f"Failed to reset the Pilatus"
                f' calling the command "{self.cmd_stop_acq.command}"'
                f' of the tango device "{self.cmd_stop_acq.device_name}"'
            )
            self.log.exception(err_msg)
            if not raise_exc:
                raise RuntimeError(err_msg)

    def restart(self) -> None:
        raise NotImplementedError
