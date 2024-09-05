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


import json
import time
import os
import gevent
import PyTango

from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.HardwareObjects.abstract.AbstractMultiCollect import \
    AbstractMultiCollect
from mxcubecore import HardwareRepository as HWR
from mxcubecore.TaskUtils import task
from mxcubecore.model.queue_model_objects import Sample, Crystal
from mxcubecore import hwo_header_log


class XRD1MultiCollect(AbstractMultiCollect, HardwareObject):

    def __init__(self, name):

        AbstractMultiCollect.__init__(self)
        HardwareObject.__init__(self, name)

        self._centring_status = None
        self.ready_event = None
        self.actual_frame_num = 0
        self.ch_acq_mode = None
        self.ch_start_phi = None
        self.ch_delta_phi = None
        self.ch_exposure_time = None
        self.ch_total_frames = None
        self.ch_start_frame = None
        self.ch_file_root = None
        self.ch_run_number = None
        self.ch_file_cbf_selected = None
        self.ch_sub_dir = None
        self.ch_forced_investigation = None
        self.ch_detector_distance_mm = None
        self.ch_state = None
        self.cmd_start = None
        self.cmd_clean_all = None
        self.cmd_abort = None
        self.collection_id = None

    @hwo_header_log
    def init(self):

        self.setControlObjects(
            diffractometer=HWR.beamline.diffractometer,
            sample_changer=HWR.beamline.sample_changer,
            lims=HWR.beamline.lims,
            safety_shutter=HWR.beamline.safety_shutter,
            machine_current=HWR.beamline.machine_info,
            cryo_stream=HWR.beamline.cryo,
            energy=HWR.beamline.energy,
            resolution=HWR.beamline.resolution,
            detector_distance=HWR.beamline.detector.distance,
            transmission=HWR.beamline.transmission,
            undulators=HWR.beamline.undulators,
            flux=HWR.beamline.flux,
            detector=HWR.beamline.detector,
            beam_info=HWR.beamline.beam,
        )

        self.ch_acq_mode = self.get_channel_object("acq_mode", optional=False)
        self.ch_start_phi = self.get_channel_object("start_phi", optional=False)
        self.ch_delta_phi = self.get_channel_object("delta_phi", optional=False)
        self.ch_exposure_time = self.get_channel_object("exposure_time",
                                                        optional=False)
        self.ch_total_frames = self.get_channel_object("total_frames", optional=False)
        self.ch_start_frame = self.get_channel_object("start_frame", optional=False)
        self.ch_file_root = self.get_channel_object("file_root", optional=False)
        self.ch_run_number = self.get_channel_object("run_number", optional=False, )
        self.ch_file_cbf_selected = self.get_channel_object("file_cbf_selected",
                                                            optional=False)
        self.ch_sub_dir = self.get_channel_object("sub_dir", optional=False)
        self.ch_forced_investigation = self.get_channel_object("forced_investigation",
                                                               optional=False)
        self.ch_detector_distance_mm = self.get_channel_object("detector_distance_mm",
                                                               optional=False)
        self.ch_state = self.get_channel_object("state", optional=False)
        self.cmd_start = self.get_command_object("start")
        self.cmd_abort = self.get_command_object("abort")

        self.emit("collectConnected", (True,))
        self.emit("collectReady", (True,))

    @hwo_header_log
    def do_collect(self, owner, data_collect_parameters):

        data_collect_parameters["collection_start_time"] = \
            time.strftime("%Y-%m-%d %H:%M:%S")

        # Reset collection id on each data collect
        self.collection_id = None

        self.prepare_collection(data_collect_parameters)

        # Tango xrd1/dac/collect (executer)
        self.setup_collect_tango_executer(data_collect_parameters)

        # Handle manual sample
        if data_collect_parameters['sample_reference']['blSampleId'] == -1:
            # Check if there is a sample with the same name and acronym in
            # the ISPyB database, if it doesn't exist a new one will be created
            # In any case the "data_collect_parameters" will be updated with the
            # sample_id
            self.populate_dc_params_with_sample_info(data_collect_parameters)

        self.populate_dc_params_with_beamline_info(data_collect_parameters)

        self.populate_dc_params_with_osc_info(data_collect_parameters)

        self.populate_dc_params_with_centring_info(data_collect_parameters)

        print(f"current lims sample: {self.current_lims_sample}")

        sample_id, sample_location, sample_code = \
            self.get_sample_info_from_parameters(data_collect_parameters)

        try:
            self.log.info("Storing data collection metadata in to LIMS")
            data_collect_parameters["blSampleId"] = sample_id
            self.collection_id, detector_id = HWR.beamline.lims.store_data_collection(
                data_collect_parameters, wait=True)
            data_collect_parameters["collection_id"] = self.collection_id
            self.user_log.info("Data collection metadata stored in LIMS")
        except Exception:
            self.log.exception("Failed to store data collection metadata in to LIMS")
            self.user_log.exception(
                "Failed to store data collection metadata in to LIMS")

        self.cmd_start()
        t_start = time.time()
        exp_time = float(data_collect_parameters['oscillation_sequence'][0]['exposure_time'])
        num_imgs = float(data_collect_parameters['oscillation_sequence'][0]['number_of_images'])
        total_acq_time = exp_time * num_imgs
        offset = 10
        with gevent.Timeout(total_acq_time + offset,
                            TimeoutError(f"Timed out. The datacollection "
                                         f"\"{self.username}\" took too much time to "
                                         f"end (more than the total exposure: "
                                         f"{total_acq_time} sec)")):
            while True:
                try:
                    if self.ch_state.get_value() in [PyTango.DevState.OFF,
                                                     PyTango.DevState.FAULT]:
                        break
                except PyTango.DevFailed.timeout:
                    pass

                elapsed_time = min(time.time() - t_start, total_acq_time)
                num = int(num_imgs * (elapsed_time/total_acq_time))
                self.emit('collectImageTaken', num)
                gevent.sleep(self.ch_state.polling / 1000)


        print(f"QUEUE --- id: {sample_id}")

        data_collect_parameters["collection_end_time"] = time.strftime(
            "%Y-%m-%d %H:%M:%S")

    @task
    @hwo_header_log
    def take_crystal_snapshots(self, number_of_snapshots):

        self.bl_control.diffractometer.take_snapshots(number_of_snapshots, wait=True)

    @task
    @hwo_header_log
    def data_collection_hook(self, data_collect_parameters):
        pass

    @hwo_header_log
    def setup_collect_tango_executer(self, data_collect_parameters):

        # VUO dataset info
        investigation = HWR.beamline.session.get_investigation()
        experiment = os.path.relpath(data_collect_parameters['fileinfo']['directory'],
                                     HWR.beamline.session.get_base_image_directory()).split("/")[0]
        dataset = data_collect_parameters['fileinfo']['run_number']
        file_basename = data_collect_parameters['fileinfo']['prefix']

        self.log.info("Acq information:\n"
                      f"Investigation: {investigation}\n"
                      f"experiment: {experiment}\n"
                      f"dataset: {dataset}\n"
                      f"file_basename: {file_basename}"
                      )

        self.log.debug(f"Configuring device tango (executer) "
                       f"{self.ch_start_phi.device_name} for the acquisition of the "
                       f"dataset {investigation}/{experiment}/{dataset}")

        self.ch_acq_mode.set_value(2)  # Default ACQ_CONTINUOUS
        self.ch_start_phi.set_value(
            data_collect_parameters['oscillation_sequence'][0]['start'])
        self.ch_delta_phi.set_value(
            data_collect_parameters['oscillation_sequence'][0]['range'])
        self.ch_exposure_time.set_value(data_collect_parameters['oscillation_sequence']
                                        [0]['exposure_time'])
        self.ch_total_frames.set_value(data_collect_parameters['oscillation_sequence']
                                       [0]['number_of_images'])
        self.ch_start_frame.set_value(data_collect_parameters['oscillation_sequence']
                                      [0]['start_image_number'])
        self.ch_forced_investigation.set_value(investigation)
        self.ch_sub_dir.set_value(experiment)
        self.ch_run_number.set_value(dataset)
        self.ch_file_root.set_value(file_basename)
        self.ch_file_cbf_selected.set_value(True)  # Default cbf (instead of tif)
        self.ch_detector_distance_mm.set_value(self.bl_control.detector_distance.get_value())

        self.log.info(f"Device tango (executer) {self.ch_start_phi.device_name}"
                      f" configured for the acquisition of the dataset "
                      f"{investigation}/{experiment}/{dataset}")

    @task
    @hwo_header_log
    def data_collection_cleanup(self):
        pass

    @hwo_header_log
    def populate_dc_params_with_centring_info(self, data_collect_parameters):

        self.user_log.info("Getting centring status")
        centring_status = self.bl_control.diffractometer.get_centring_status()

        print(f"centring_info: {centring_status}")

        # Save sample centring positions
        motors_to_move_before_collect = data_collect_parameters.setdefault("motors", {})

        for motor, pos in centring_status.get("motors", {}).items():
            if motor in motors_to_move_before_collect:
                continue
            motors_to_move_before_collect[motor] = pos

        print(f"motors_to_move_before_collect: {motors_to_move_before_collect}")

        current_diffract_pos = self.bl_control.diffractometer.get_positions()

        print(f"current_diffract_pos: {current_diffract_pos}")
        positions_str = ""
        for motor, pos in motors_to_move_before_collect.items():
            if pos is not None and motor is not None:
                positions_str += f"{motor}={pos} "

        data_collect_parameters["actualCenteringPosition"] = positions_str.strip()

        try:
            data_collect_parameters["centeringMethod"] = centring_status["method"]
        except Exception:
            data_collect_parameters["centeringMethod"] = None

        print(type(data_collect_parameters))

        print(f"data_collect_parameters DOPO: {data_collect_parameters}")

        # TODO evaluate whether retrieve data collection from DB

    @hwo_header_log
    def populate_dc_params_with_osc_info(self, data_collect_parameters):

        data_collect_parameters['oscillation_sequence'][0]['end'] = \
            data_collect_parameters['oscillation_sequence'][0]['start'] + \
            data_collect_parameters['oscillation_sequence'][0]['range']
        data_collect_parameters['rotation_axis'] = 'Phi'

    @hwo_header_log
    def populate_dc_params_with_beamline_info(self, data_collect_parameters):

        self.user_log.info("Getting machine and beamline "
                           "status information")

        data_collect_parameters["synchrotronMode"] = self.get_machine_fill_mode()
        data_collect_parameters["flux"] = self.get_flux()
        data_collect_parameters["wavelength"] = self.get_wavelength()
        data_collect_parameters["detectorDistance"] = self.get_detector_distance()
        data_collect_parameters["resolution"] = self.get_resolution()
        data_collect_parameters["transmission"] = self.get_transmission()
        beam_centre_x, beam_centre_y = self.get_beam_centre()
        data_collect_parameters["xBeam"] = beam_centre_x
        data_collect_parameters["yBeam"] = beam_centre_y
        data_collect_parameters["fileinfo"]["suffix"] = self.get_file_suffix()
        beam_size_x, beam_size_y = self.get_beam_size()
        data_collect_parameters["beamSizeAtSampleX"] = beam_size_x
        data_collect_parameters["beamSizeAtSampleY"] = beam_size_y
        data_collect_parameters["beamShape"] = self.get_beam_shape()

    @hwo_header_log
    def populate_dc_params_with_sample_info(self, data_collect_parameters):

        # TODO move this part to the queue system if possible

        queue_sample: Sample = HWR.beamline.queue_manager.get_current_entry() \
            .get_data_model().get_sample_node()

        '''
        print("+++++++++++++++ queue_sample")
        print(queue_sample.__dict__)
        print(queue_sample)
        print(queue_sample.crystals)
        '''

        sample_name = queue_sample.get_name()
        crystal: Crystal = queue_sample.crystals[0]
        sample_location = 0

        proposal_number = HWR.beamline.session.proposal_number
        proposal_code = HWR.beamline.session.proposal_code
        proposal_info_dict = HWR.beamline.lims.get_proposal(proposal_code,
                                                            int(proposal_number))
        proposal_id = proposal_info_dict['Proposal']['proposalId']

        # Check if the sample already exists for this proposal
        # (otherwise create a new one)
        db_samples = HWR.beamline.lims.get_session_samples(
            data_collect_parameters["sessionId"], sample_name=sample_name,
            acronym=crystal.protein_acronym)

        # TODO move this part ISPyBAPIClient
        if not db_samples:
            # Check if the protein already exists for this proposal
            # (otherwise create a new one)
            db_protein = HWR.beamline.lims.get_protein(proposal_id,
                                                       crystal.protein_acronym)
            if db_protein is None:
                protein_dict = {'acronym': crystal.protein_acronym,
                                'proposalId': proposal_id}
                db_protein = HWR.beamline.lims._insert_protein(protein_dict)
                self.log.info(f"New crystal [acronym:{crystal.protein_acronym}]"
                              f" created")
            crystal_dict = {'proteinId': db_protein.proteinId}
            db_crystal = HWR.beamline.lims._insert_crystal(crystal_dict)
            sample_dict = {'name': sample_name, 'location': sample_location,
                           'crystalId': db_crystal.crystalId}
            db_sample = HWR.beamline.lims._insert_sample(sample_dict)
            self.log.info(f"Sample [name:{sample_name} - "
                          f"acronym:{crystal.protein_acronym}] created")
        else:
            db_sample = db_samples[0]
            self.log.debug(f"Sample [name:{sample_name} - "
                           f"acronym:{crystal.protein_acronym}] already exists")
        data_collect_parameters['sample_reference']['blSampleId'] = \
            db_sample.blSampleId

        '''
        queue_sample.lims_id = db_sample.blSampleId
        queue_sample: Sample = HWR.beamline.queue_manager.get_current_entry()\
            .get_data_model().get_sample_node()
        print(f"queue_sample.lims_id: {queue_sample.lims_id}")
        '''

    @hwo_header_log
    def set_detector_filenames(
        self, frame_number, start, filename, jpeg_full_path, jpeg_thumbnail_full_path
    ):
        return

    @hwo_header_log
    def prepare_collection(self, data_collect_parameters):

        transmission = data_collect_parameters.get("transmission")
        if transmission is not None:
            curr_transmission = float(self.bl_control.transmission.get_value())
            if curr_transmission != transmission:
                self.bl_control.transmission.set_value(float(transmission))

        resolution = data_collect_parameters.get("resolution")['upper']
        if resolution is not None:
            curr_resolution = float(self.bl_control.resolution.get_value())
            if curr_resolution != resolution:
                self.bl_control.resolution.set_value(float(resolution))

        kappa = data_collect_parameters.get("kappa")
        if kappa is not None:
            curr_kappa = float(self.bl_control.kappa.get_value())
            if curr_kappa != kappa:
                self.bl_control.kappa.set_value(float(kappa))

    @hwo_header_log
    def prepare_oscillation(self, start, osc_range, exptime, npass):
        pass

    @hwo_header_log
    def do_oscillation(self, start, end, exptime, npass):
        pass

    @hwo_header_log
    def start_acquisition(self, exptime, npass, first_frame):
        pass

    @hwo_header_log
    def write_image(self, last_frame):

        self.actual_frame_num += 1
        return

    @hwo_header_log
    def last_image_saved(self):

        return self.actual_frame_num

    @hwo_header_log()
    def stop_acquisition(self):

        self.cmd_abort()

    @task
    @hwo_header_log
    def write_input_files(self, collection_id):

        return

    @hwo_header_log
    def get_transmission(self):
        transmission = None
        if self.bl_control.transmission is not None:
            transmission = self.bl_control.transmission.get_value()
        return transmission

    @hwo_header_log
    def get_resolution(self):
        resolution = None
        if self.bl_control.resolution is not None:
            resolution = self.bl_control.resolution.get_value()
        return resolution

    @hwo_header_log
    def get_detector_distance(self):

        distance = None
        if self.bl_control.detector_distance is not None:
            distance = self.bl_control.detector_distance.get_value()
        return distance

    @hwo_header_log
    def get_wavelength(self):

        wavelength = None
        if self.bl_control.energy is not None:
            wavelength = self.bl_control.energy.get_wavelength()
        return wavelength

    @hwo_header_log
    def get_file_suffix(self):

        file_suffix = None
        if self.bl_control.detector is not None:
            file_suffix = self.bl_control.detector.get_property("fileSuffix")
        return file_suffix

    @hwo_header_log
    def get_beam_size(self):

        beam_size = None, None
        if self.bl_control.beam_info is not None:
            beam_size = self.bl_control.beam_info.get_beam_size()
        return beam_size

    @hwo_header_log
    def get_beam_shape(self):

        beam_shape = None
        if self.bl_control.beam_info is not None:
            beam_shape = self.bl_control.beam_info.get_beam_shape()
        return beam_shape

    @hwo_header_log
    def get_machine_current(self):

        current = 0
        if self.bl_control.machine_current is not None:
            current = self.bl_control.machine_current.get_current()
        return current

    @hwo_header_log
    def get_machine_message(self):

        machine_message = ""
        if self.bl_control.machine_current is not None:
            machine_message = self.bl_control.machine_current.get_message()
        return machine_message

    @hwo_header_log
    def get_flux(self):

        flux = None
        if self.bl_control.flux is not None:
            flux = self.bl_control.flux.get_value()
        return flux

    @hwo_header_log
    def get_machine_fill_mode(self):

        fill_mode = ""
        if self.bl_control.machine_current is not None:
            fill_mode = self.bl_control.machine_current.get_fill_mode()
        return fill_mode

    @hwo_header_log
    def get_cryo_temperature(self):

        temperature = None
        if self.bl_control.cryo_stream is not None:
            temperature = self.bl_control.cryo_stream.get_value()
        return temperature

    @hwo_header_log
    def get_current_energy(self):

        energy = None
        if self.bl_control.cryo_stream is not None:
            energy = self.bl_control.cryo_stream.get_value()
        return energy

    @hwo_header_log
    def get_beam_centre(self):

        x, y = None, None
        if self.bl_control.diffractometer is not None:
            x = self.bl_control.diffractometer.beam_center_x
            y = self.bl_control.diffractometer.beam_center_y
        return x, y

    @hwo_header_log
    def diffractometer(self):

        return self.bl_control.diffractometer

    @hwo_header_log
    def sanity_check(self, collect_params):
        # TODO Evaluate whether implement this check
        pass

    @hwo_header_log
    def reset_detector(self):
        pass

    @hwo_header_log
    def prepare_input_files(self, files_directory, prefix, run_number,
                            process_directory):
        pass

    @hwo_header_log
    def store_image_in_lims(self, frame, first_frame, last_frame):

        return True

    @hwo_header_log
    def get_oscillation(self, oscillation_id):

        return self.oscillations_history[oscillation_id - 1]

    @hwo_header_log
    def set_helical(self, helical_on):
        pass

    @hwo_header_log
    def set_helical_pos(self, helical_oscil_pos):
        pass

    @hwo_header_log
    def get_archive_directory(self, directory):
        pass

    @task
    @hwo_header_log
    def generate_image_jpeg(self, filename, jpeg_path, jpeg_thumbnail_path):
        pass

    @hwo_header_log
    def move_motors(self, motor_position_dict):
        for motor, value in motor_position_dict:
            self.bl_control.diffractometer.motor_hwobj_dict[motor].set_value(value)

    @hwo_header_log
    def open_safety_shutter(self):
        pass

    @hwo_header_log
    def close_safety_shutter(self):
        pass

    @hwo_header_log
    def prepare_intensity_monitors(self):
        pass

    @hwo_header_log
    def prepare_acquisition(self, take_dark, start, osc_range, exptime, npass,
                            number_of_images, comment):
        pass

    @hwo_header_log
    def get_undulators_gaps(self):
        pass

    @hwo_header_log
    def get_resolution_at_corner(self):
        pass

    @hwo_header_log
    def get_slit_gaps(self):
        pass

    @hwo_header_log
    def set_fast_characterisation(self, value: bool):
        pass

    @hwo_header_log
    def close_fast_shutter(self):
        pass
