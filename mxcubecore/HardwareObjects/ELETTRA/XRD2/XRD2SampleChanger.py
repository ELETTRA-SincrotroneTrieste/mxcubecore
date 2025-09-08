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

import PyTango
import gevent
from gevent.lock import Semaphore

from mxcubecore.HardwareObjects.abstract.AbstractSampleChanger import *
from mxcubecore.HardwareObjects.abstract.sample_changer import Container
from mxcubecore.HardwareObjects.abstract.sample_changer.Sample import Sample
from mxcubecore import HardwareRepository as HWR
from mxcubecore import trace_call_log
from mxcubeweb.routes import signals
from mxcubeweb.routes.signals import loaded_sample_changed


class StaubliStates(enum.Enum):

    UNKNOWN = SampleChangerState.Unknown
    IDLE = SampleChangerState.Ready
    ERROR = SampleChangerState.Fault
    UNMOUNTING = SampleChangerState.Unloading
    ENDING_UNMOUNT = SampleChangerState.Unloading
    MOUNTING = SampleChangerState.Loading
    ENDING_MOUNT = SampleChangerState.Loading
    SWAPPING = SampleChangerState.Loading
    ENDING_SWAP = SampleChangerState.Loading
    DEFROST = SampleChangerState.Disabled
    COOLDOWN = SampleChangerState.Disabled
    UNPARKING = SampleChangerState.Moving
    PARKING = SampleChangerState.Moving
    PARKED = SampleChangerState.Disabled
    GRIPPER_EXCHANGE = SampleChangerState.Disabled
    EXCHANGING = SampleChangerState.Moving
    EXCHANGE = SampleChangerState.Disabled
    SAFEPOINT = SampleChangerState.Moving

    @classmethod
    def from_staubli_status(cls, status: str):
        status = status.strip()
        if "IDLE" == status:
            state = cls.IDLE
        elif "ERROR" in status:
            state = cls.ERROR
        elif "UNMOUNTING" == status:
            state = cls.UNMOUNTING
        elif "ENDING: UNMOUNT" == status:
            state = cls.ENDING_UNMOUNT
        elif "MOUNTING" == status:
            state = cls.MOUNTING
        elif "ENDING: MOUNT" == status:
            state = cls.ENDING_MOUNT
        elif "SWAPPING" == status:
            state = cls.SWAPPING
        elif "ENDING: SWAP" == status:
            state = cls.ENDING_SWAP
        elif "DEFROST" == status:
            state = cls.DEFROST
        elif "COOLDOWN" == status:
            state = cls.COOLDOWN
        elif "UNPARKING" == status:
            state = cls.UNPARKING
        elif "PARKING" == status:
            state = cls.PARKING
        elif "PARKED" == status:
            state = cls.PARKED
        elif "GRIPPER EXCHANGE" == status:
            state = cls.GRIPPER_EXCHANGE
        elif "EXCHANGING" == status:
            state = cls.EXCHANGING
        elif "EXCHANGE" == status:
            state = cls.EXCHANGE
        elif "SAFEPOINT" == status:
            state = cls.SAFEPOINT
        else:
            state = cls.UNKNOWN

        return state


@enum.unique
class SpeState(enum.Enum):
    Single = 1
    Double = 2

    @classmethod
    def get(cls, value):
        try:
            return cls(value)
        except ValueError:
            return cls(2)


@enum.unique
class GripperType(enum.Enum):
    Single = 1
    Double = 2

    @classmethod
    def get(cls, value):
        try:
            return cls(value)
        except ValueError:
            return cls(2)


@enum.unique
class PinStatus(enum.Enum):
    NotDetected = "0"
    Detected = "1"
    Unknown = "2"

    @classmethod
    def get(cls, value):
        try:
            return cls(str(value))
        except ValueError:
            return cls("2")


class XRD2SampleChanger(SampleChanger):
    # TODO prova ad implementare usando l'abstract:
    # usa i "do_something"
    #

    __TYPE__ = "SAMPLE CHANGER"
    NO_OF_BASKETS = 12
    NO_OF_SAMPLES_IN_BASKET = 16
    DEFROST_DURATION = 360  # s
    # PARKING_DURATION = 300  # s
    # UNPARKING_DURATION = 120  # s

    def __init__(self, name):
        super().__init__(self.__TYPE__, False, name)

        self.no_of_baskets = None
        self.no_of_samples_in_basket = None
        self.signal_wait_task = None
        self.state_change_lock = Semaphore()
        self.ena_hw_state_sync = True
        self.pin_detection_mismatch = False
        self.pin_detection_mismatched = False
        self.diffractometer = None
        self.lims = None
        self.last_pin_status_readtime = 0
        self.last_pin_status_value = PinStatus.NotDetected
        self.message = ""
        self.ch_basket_selected = None
        self.ch_sample_selected = None
        self.ch_basket_2_swap = None
        self.ch_sample_2_swap = None
        self.ch_sc_status = None
        self.ch_pin_status = None
        self.ch_gripper_type = None
        self.ch_script_failed = None
        self.ch_step_by_step_log = None
        self.ch_state = None
        self.ch_status = None
        self.cmd_start_method = None
        self.sc_info = {
            "basket_selected": None,
            "sample_selected": None,
            "hist_selected": None,
            "basket_2_swap": None,
            "sample_2_swap": None,
            "hist_2_swap": None,
            "pin_status": None,
            "state": None,
        }

    def init(self):
        self.no_of_baskets = self.get_property(
            "no_of_baskets", XRD2SampleChanger.NO_OF_BASKETS
        )
        self.no_of_samples_in_basket = self.get_property(
            "no_of_samples_in_basket", XRD2SampleChanger.NO_OF_SAMPLES_IN_BASKET
        )

        self.lims = self.get_object_by_role("lims")
        self.diffractometer = self.get_object_by_role("diffractometer")

        self.ch_basket_selected = self.get_channel_object("basket_selected")
        self.ch_sample_selected = self.get_channel_object("sample_selected")
        self.ch_basket_2_swap = self.get_channel_object("basket_2_swap")
        self.ch_sample_2_swap = self.get_channel_object("sample_2_swap")
        self.ch_sc_status = self.get_channel_object("sc_status")
        self.ch_pin_status = self.get_channel_object("pin_status")
        self.ch_gripper_type = self.get_channel_object("gripper_type")
        self.ch_script_failed = self.get_channel_object("script_failed")
        self.ch_step_by_step_log = self.get_channel_object("step_by_step_log")
        self.ch_state = self.get_channel_object("state")
        self.ch_status = self.get_channel_object("status")
        self.cmd_start_method = self.get_command_object("start_method")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_basket_selected, "update", self.update_sc_info)
        self.connect(self.ch_sample_selected, "update", self.update_sc_info)
        self.connect(self.ch_basket_2_swap, "update", self.update_sc_info)
        self.connect(self.ch_sample_2_swap, "update", self.update_sc_info)
        self.connect(self.ch_sc_status, "update", self.update_sc_info)
        self.connect(self.ch_pin_status, "update", self.update_sc_info)

        self._init_sc_contents()

        SampleChanger.init(self)

        self._update_state()

    @trace_call_log
    def _update_state(self, hw_status: str = None):

        if hw_status is None:
            hw_status = self._get_hw_status(timeout=3)

        self._hw_state = StaubliStates.from_staubli_status(hw_status)

        self.log.debug(f"Sample changer current state: {self._hw_state.name}")

        with self.state_change_lock:
            # Synchronize the sample changer HWO state with the Staubli (hw) state (only if sync enabled)
            if self.ena_hw_state_sync:
                self._set_state(state=self._hw_state.value)
                if self._hw_state == StaubliStates.ERROR:
                    self.message = hw_status.split("ERROR:")[1]
                else:
                    self.message = ""

    @property
    def _selected_basket(self):
        return self.ch_basket_selected.get_value()

    @_selected_basket.setter
    def _selected_basket(self, value: int | str):
        value = int(value)
        self.ch_basket_selected.set_value(value)

    @property
    def _selected_sample(self):
        return self.ch_sample_selected.get_value()

    @_selected_sample.setter
    def _selected_sample(self, value: int | str):
        value = int(value)
        self.ch_sample_selected.set_value(value)

    @property
    def _basket_2_swap(self):
        return self.ch_basket_2_swap.get_value()

    @_basket_2_swap.setter
    def _basket_2_swap(self, value: int | str):
        value = int(value)
        self.ch_basket_2_swap.set_value(value)

    @property
    def _sample_2_swap(self):
        return self.ch_sample_2_swap.get_value()

    @_sample_2_swap.setter
    def _sample_2_swap(self, value: int | str):
        value = int(value)
        self.ch_sample_2_swap.set_value(value)

    @trace_call_log
    def update_sc_info(self, value, sender):
        self.sc_info[sender.name()] = value

        if sender.name() == "sc_status":
            self._update_state(hw_status=value)

        self.log.info("Sample changer current info: %s", str(self.sc_info))
        self.emit("scInfoChanged")

    def _wait_method_execution(self, timeout: int = 300, wait_only_if: list = None):
        with Timeout(
            timeout,
            Exception(
                'Timeout waiting for sample changer task finish (executer "OFF")'
            ),
        ):
            sleep(3)
            while True:
                try:
                    if self.ch_state.get_value() == PyTango.DevState.OFF:
                        task_failed = self.ch_script_failed.get_value()
                        task_log = self.ch_step_by_step_log.get_value()
                        break
                    if (
                        wait_only_if
                        and not self.get_hw_state() in wait_only_if
                    ):
                        break
                except PyTango.DevFailed as e:
                    self.log.exception(
                        'Tango error occurred waiting for samplechanger "OFF"'
                    )
                    if "Not able to acquire serialization" in e.args[0].desc:
                        pass
                    elif "TRANSIENT_CallTimedout" in e.args[0].desc:
                        pass
                    else:
                        raise e
                sleep(1)
            return task_failed, task_log

    @trace_call_log
    def force_loaded_sample(self, sample_location: str):
        try:
            if sample_location.strip() in [':','']:
                self._selected_basket = -1
                self._selected_sample = -1
                self._reset_loaded_sample()
            else:
                basket, sample = sample_location.split(":")
                self._selected_basket = basket
                self._selected_sample = sample
                sample_obj = self.get_component_by_address(
                    Container.Pin.get_sample_address(
                        self._selected_basket, self._selected_sample
                    )
                )
                self._set_loaded_sample(sample_obj)
        except:
            self.log.exception("Error occurred forcing loaded sample")

    @trace_call_log
    def defrost(self):
        if not self.is_state_in([StaubliStates.IDLE]):
            err_msg = 'Defrost aborted! The sample changer hw state must be "IDLE" (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)
            return
        self._set_state(SampleChangerState.Moving)
        self.cmd_start_method("defrost")
        self.log.info('"defrost" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
        sleep(1)
        self._wait_method_execution(wait_only_if=[StaubliStates.DEFROST])

    @trace_call_log
    def unpark(self):
        if not self.is_state_in([StaubliStates.PARKED]):
            err_msg = 'Unpark aborted! The sample changer hw state must be "PARKED" (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)
            return
        self._set_state(SampleChangerState.Moving)
        self.cmd_start_method("unpark")
        self.log.info('"unpark" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
        sleep(1)
        self._wait_method_execution()

    @trace_call_log
    def park(self):
        if not self.is_state_in([StaubliStates.IDLE, StaubliStates.EXCHANGE]):
            err_msg = 'Park aborted! The sample changer hw state must be "IDLE" or "EXCHANGE"  (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)
            return
        self._set_state(SampleChangerState.Moving)
        self.cmd_start_method("park")
        self.log.info('"park" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
        sleep(1)
        self._wait_method_execution()

    @trace_call_log
    def trash2S(self):

        """
        These are the steps of the reset sequence:
         - SET(FORCE_IDLE)
         - go to SAF
         - wait
         - go to EXC
        - SET(GRIPPER_OPEN)
         - PAR
         - UNP
        """

        if not self.is_state_in([StaubliStates.ERROR]):
            err_msg = 'Trash aborted! The sample changer hw state must be "ERROR" (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)
            return

        self.ena_hw_state_sync = False
        try:
            self._set_state(SampleChangerState.Disabled)
            startTimestampRA = time.time()
            recovery_time = 60
            recovery_time = 10  # todo ripristina 60
            self.cmd_start_method("force_idle")
            self.log.info('"force_idle" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
            self._wait_method_execution()
            self.cmd_start_method("safe_point")
            self.log.info('"safe_point" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
            self._wait_method_execution()
            sleep(5)
            self.cmd_start_method("exchange_point")
            self.log.info('"exchange_point" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
            self._wait_method_execution()
            while recovery_time > 0:
                self.user_log.error("!!! Gripper will drop pins in {} s !!!".format(recovery_time))
                sleep(5)
                recovery_time -= 5
            self.cmd_start_method("write_samplechanger_gripper_open")
            self.log.info('"write_samplechanger_gripper_open" execution request sent to the tango device "%s"',self.cmd_start_method.device_name)
            self._wait_method_execution()
            self.park()
            self.user_log.info("!!! Gripper will be ready soon... !!!")
            self._wait_method_execution()
            self.unpark()
            self._wait_method_execution()

            # TODO implementa store_robo_action
            # self.lims.set_robot_action(dewarLocation=self.device.Basket_selected,
            #         containerLocation=self.device.Sample_selected,
            #         actionType='DISPOSE',
            #         status='SUCCESS',
            #         message='Trashing [last SWAP from {}:{} to {}:{}]'.format(self.device.Basket_to_swap, self.device.Sample_to_swap, self.device.Basket_selected, self.device.Sample_selected),
            #         startTimestamp=startTimestampRA,
            #         endTimestamp=time.time())
        finally:
            self.ena_hw_state_sync = True
            self._update_state()

    @trace_call_log
    def get_loaded_sample(self):
        if (time.time() - self.last_pin_status_readtime) > 1:
            # Check pin_status at 1Hz
            self.last_pin_status_readtime = time.time()
            self.last_pin_status_value = self.get_pin_state()

        self.handle_state_on_mismatch()

        if self.last_pin_status_value == PinStatus.NotDetected:
            return None

        loaded_sample = super().get_loaded_sample()

        if self.last_pin_status_value == PinStatus.Detected and loaded_sample is None:
            return None

        print("AAAAAAAAA self._selected_basket, self._selected_sample")
        print(self._selected_basket, self._selected_sample)
        print("AAAA loaded_sample", loaded_sample)

        return loaded_sample


    @trace_call_log
    def chained_load(self, sample_to_unload, sample_to_load):
        """
        Chain the unload of a sample with a load.

        Args:
            sample_to_unload (tuple): sample address on the form
                                      (component1, ... ,component_N-1, component_N)
            sample_to_load (tuple): sample address on the form
                                      (component1, ... ,component_N-1, component_N)
            (Object): Value returned by _execute_task either a Task or result of the
                      operation
        """

        # If single grip, unload sample before load
        sc_gripper = self.get_gripper_type()
        if sc_gripper == GripperType.Single and self.get_loaded_sample() is not None:
            self.unload(sample_to_unload, True)
            self.wait_ready(timeout=10)
        return self.load(sample_to_load)

    # @trace_call_log
    # def is_mounted_sample(self, sample):
    #     if isinstance(sample, tuple):
    #         sample = "%s:%s" % sample
    #
    #     return sample == self.get_loaded_sample()

    @trace_call_log
    def load(self, sample=None, wait=True):
        """
        Load a sample.

        Args:
            sample (tuple): sample address on the form
                            (component1, ... ,component_N-1, component_N)
            wait (boolean): True to wait for load to complete False otherwise

        Returns
            (Object): Value returned by _execute_task either a Task or result of the
                      operation
        """
        self.assert_ready_for_load()
        sample = self._resolve_component(sample)

        print("Sample resolved:")
        print(sample)

        # Do a chained load in this case
        if self.has_loaded_sample():
            # Do a chained load in this case
            if (sample is None) or (sample == self.get_loaded_sample()):
                raise RuntimeError(
                    "The sample "
                    + str(self.get_loaded_sample().get_address())
                    + " is already loaded"
                )
            return self.chained_load(self.get_loaded_sample(), sample)
        self._do_load(sample)

    @trace_call_log
    def unload(self, sample=None, wait=True):
        """
        Unload sample to location sample_slot, unloads to the same slot as it
        was loaded from if None is passed

        Args:
            sample (tuple): sample address on the form
                               (component1, ... ,component_N-1, component_N)
            wait: If True wait for unload to finish otherwise return immediately

        Returns:
            (Object): Value returned by _execute_task either a Task or result of the
                      operation
        """
        self.assert_ready_for_unload()
        sample = self._resolve_component(sample)
        # In case we have manually mounted we can command an unmount
        if not self.has_loaded_sample():
            raise Exception("No sample is loaded")
        self._do_unload(sample)

    @trace_call_log
    def reset(self, wait=True):
        """
        Reset the sample changer.

        wait: If True wait for reset to finish otherwise return immediately

        Returns:
            (Object): Value returned by _execute_task either a Task or result of the
                      operation
        """

        if not self.is_state_in([StaubliStates.ERROR]):
            err_msg = "Parking aborted! Sample changer is not IDLE or EXCHANGE state (Current hw state: %s)"
            self.user_log.error(err_msg, self._hw_state.name)
            return

        self.ena_hw_state_sync = False
        try:
            self._set_state(SampleChangerState.Disabled)
            self.cmd_start_method("write_samplechanger_gripper_open")
            self.log.info('"write_samplechanger_gripper_open" execution request sent to the tango device: %s',
                          self.cmd_start_method.device_name)
            self._wait_method_execution()
            self.cmd_start_method("force_idle")
            self.log.info('"force_idle" execution request sent to the tango device: %s', self.cmd_start_method.device_name)
            sleep(1)
            self._wait_method_execution()
            if self.ch_script_failed.get_value():
                self.log.info(
                    "--> SampleChangerElettra_XRD2 _doReset (re)setting samplechanger self.ch_script_failed.get_value() to FALSE"
                )
                self.ch_script_failed.set_value(False)
        finally:
            self.ena_hw_state_sync = True
            self._update_state()

    @trace_call_log
    def _do_load(self, sample, wait=False):
        start_timestamp_RA = time.time()
        action_Type_RA = "LOAD"
        status_RA = "skip"
        message_RA = ""

        # Wait defrost or cooldown finishing
        if self._hw_state in [StaubliStates.DEFROST, StaubliStates.COOLDOWN]:
            self._wait_defrost_and_cooldown(operation="sample loading")

        # Abort if single pin and a pin is detected
        sc_gripper = self.get_gripper_type()
        if sc_gripper == GripperType.Single and self.is_pin_detected():
            err_msg = "Sample loading aborted! Pin is expected not to be present but it's detected instead."
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

        # if self.diffractometer is not None:
        #     # Abort already pending Crystal centring procedures
        #     if self.diffractometer.get_current_centring_method() is not None:
        #         self.diffractometer.cancelCentringMethod(reject=True)
        #     self.diffractometer.wait_device_ready(30)
        #     if not self.diffractometer.is_ready():
        #         logmsg = "DIFFRACTOMETER NOT READY"
        #         err_msg = "SampleChangerElettra_XRD2.load ABORTED: %s" % logmsg
        #         self.user_log.error(err_msg)
        #         raise RuntimeError("SampleChangerElettra_XRD2.load ABORTED: %s" % logmsg)

        if isinstance(sample, tuple):
            basket_no, sample_no = sample
        else:
            basket_no, sample_no = sample.split(":")

        status_RA = "ERROR"

        dewarLocationRA = int(basket_no)
        containerLocationRA = int(sample_no)
        try:
            self._selected_sample = sample_no
            self._selected_basket = basket_no
            sample_to_unload: Sample = self.get_loaded_sample() # It will be None if single gripper
            sample_to_load = self.get_component_by_address(
                Container.Pin.get_sample_address(int(basket_no), int(sample_no))
            )
            if sample_to_unload is None:
                samp_to_unload_basket_no, samp_to_unload_sample_no = 0, 1
            else:
                samp_to_unload_basket_no, samp_to_unload_sample_no = sample_to_unload.get_coords()
            if sc_gripper == GripperType.Double:
                self._basket_2_swap = samp_to_unload_basket_no
                self._sample_2_swap = samp_to_unload_sample_no
                self.cmd_start_method("swap")
                self.log.info('"swapping" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
            else:
                self.cmd_start_method("mount")
                self.log.info('"mount"  execution request sent to the tango device "%s"', self.cmd_start_method.device_name)

            sleep(1)
            self._wait_method_execution()

            sc_action_failed = self.ch_script_failed.get_value()
            if sc_action_failed:
                # Check on pin detection and sample changer status done in executer
                err_msg = "Mounting Script Failed!"
                step_by_step = self.ch_step_by_step_log.get_value()
                self.log.error(f"Sample loading failed! %s - StepByStepLog: %s)" % (err_msg, step_by_step))
                self._reset_loaded_sample()
                self._selected_sample = -1
                self._selected_basket = -1
            else:
                self._set_loaded_sample(sample_to_load)
                self.log.info(f"Sample loaded successfully! Sample %s has been mounted", str(sample_to_load.get_coords()))
                status_RA = "SUCCESS"

        except Exception:
            self.log.exception("Error occurred during load method execution")
            self._reset_loaded_sample()

        finally:
            try:
                if status_RA is not "skip":
                    # TODO implementa store_robo_action
                    # self.lims.set_robot_action(dewarLocation=dewarLocationRA,
                    #                            containerLocation=containerLocationRA, actionType=action_Type_RA,
                    #                            status=status_RA, message=err_msg,
                    #                            startTimestamp=start_timestamp_RA, endTimestamp=time.time())
                    # ND20241225: ISPyB don't store strategies yet - using tmp file, better removed changing sample...
                    import os

                    best_strategy = "/tmp/best.xml"
                    if os.path.isfile(best_strategy):
                        os.remove(best_strategy)
            except:
                pass

        return self.get_loaded_sample()

    @trace_call_log
    def _do_unload(self, sample_location=None, wait=None):
        startTimestampRA = time.time()
        actionTypeRA = "UNLOAD"
        status_RA = "skip"
        messageRA = ""

        # Wait defrost or cooldown finishing
        if self._hw_state in [StaubliStates.DEFROST, StaubliStates.COOLDOWN]:
            self._wait_defrost_and_cooldown(operation="sample unloading")

        # if self.diffractometer is not None:
        #     # Abort already pending Crystal centring procedures
        #     if self.diffractometer.get_current_centring_method() is not None:
        #         self.diffractometer.cancelCentringMethod(reject=True)
        #     self.diffractometer.wait_device_ready(30)
        #     if not self.diffractometer.is_ready():
        #         logmsg = "DIFFRACTOMETER NOT READY"
        #         err_msg = "SampleChangerElettra_XRD2.unload ABORTED: %s" % logmsg
        #         self.user_log.error(err_msg)
        #         raise RuntimeError(err_msg)

        if isinstance(sample_location, tuple):
            basket_no, sample_no = sample_location
        else:
            basket_no, sample_no = sample_location.split(":")

        status_RA = "ERROR"
        dewarLocationRA = int(basket_no)
        containerLocationRA = int(sample_no)
        try:
            self.cmd_start_method("unmount")
            self.log.info('"unmount" execution request sent to the tango device "%s"', self.cmd_start_method.device_name)
            sleep(1)
            self._wait_method_execution()
            sc_action_failed = self.ch_script_failed.get_value()
            if sc_action_failed:
                err_msg = "Unmount Script Failed!"
                step_by_step = self.ch_step_by_step_log.get_value()
                self.log.error("Sample unloading failed! %s - StepByStepLog: %s)", (err_msg, step_by_step))
            else:
                status_RA = "SUCCESS"
                self.log.debug('Sample "%s" unloaded successfully!', sample_location)
        except Exception:
            self.log.exception("Unexpected error occurred during `_do_unload` method execution")
        finally:
            self._reset_loaded_sample()
            try:
                if status_RA is not "skip":
                    # TODO implementa store_robo_action
                    # self.lims.set_robot_action(dewarLocation=dewarLocationRA,
                    #                                  containerLocation=containerLocationRA, actionType=actionTypeRA,
                    #                                  status=status_RA, message=messageRA,
                    #                                  startTimestamp=startTimestampRA, endTimestamp=time.time())
                    # ND20241225: ISPyB don't store strategies yet - using tmp file, better removed changing sample...
                    import os

                    best_strategy = "/tmp/best.xml"
                    if os.path.isfile(best_strategy):
                        os.remove(best_strategy)
            except:
                pass

    @trace_call_log
    def _wait_defrost_and_cooldown(self, operation: str = "operation"):
        try:
            self.user_log.warning(f"Sample changer is DEFROSTING/COOLDOWN, the {operation} will continue after it, please wait")
            self.wait_hw_states([StaubliStates.IDLE], timeout=self.DEFROST_DURATION)
            self.user_log.warning(f"DEFROSTING/COOLDOWN finished, sample changer will go on with the {operation} now!")
        except TimeoutError:
            raise RuntimeError(
                f"Timeout error occurred waiting sample changer defrosting in {self.operation}"
            )

    def _do_abort(self):
        raise NotImplemented

    def _do_change_mode(self, mode):
        raise NotImplemented

    def _do_update_info(self):
        return

    def _do_select(self, component):
        raise NotImplemented

    @trace_call_log
    def _do_scan(self, component, recursive):
        # print("SampleChangerElettra_XRD2", '_doScan')
        # # Dummy operation
        # sleep(1)
        # self.default_sample_prefix = "s_%d_" % int(time.time())
        # self._init_sc_contents()
        # self._set_state(SampleChangerState.Ready)
        return

    @trace_call_log
    def _do_reset(self):
        """
        Recover from real ERROR from StaubliTCP
        """
        self.cmd_start_method("write_samplechanger_gripper_open")
        self.log.info('"write_samplechanger_gripper_open" command sent to the tango device %s',self.cmd_start_method.device_name)
        self._wait_method_execution()
        self.cmd_start_method("force_idle")
        self.log.info('"force_idle" command sent to the tango device %s', self.cmd_start_method.device_name)
        sleep(1)
        self._wait_method_execution()
        if self.ch_script_failed.get_value():
            self.log.info(
                "--> SampleChangerElettra_XRD2 _doReset (re)setting samplechanger self.ch_script_failed.get_value() to FALSE"
            )
            self.ch_script_failed.set_value(False)

    # @trace_call_log
    # def _init_sc_contents(self):
    #     """
    #     Initializes the sample changer content with default values.
    #
    #     :returns: None
    #     :rtype: None
    #     """
    #
    #     print("AAAAAAAAAAAAAAAAAAAAAAAAA")
    #
    #     self._clear_components()
    #     self.content = self.lims.get_samples()
    #
    #     print(self.content)
    #
    #     # sc_mw_content.csv
    #     if len(self.content):
    #         self.containers = []
    #         sample_list=[]
    #         # First round create only Baskets with the right dimension
    #         baskets_read = {}
    #         baskets_prio = {}
    #         for line in self.content:
    #             containerCode = line['ContainerCode']
    #             containerPriority = int(line['ContainerPriority'])
    #             if containerCode not in baskets_read.keys():
    #                 baskets_read[containerCode] = 0
    #                 baskets_prio[containerCode] = containerPriority
    #                 self.containers.append(containerCode)
    #             baskets_read[containerCode] += 1
    #         for bCode in self.containers:
    #             #num_of_samples = baskets_read[bCode]
    #             num_of_samples = 10
    #             basket = Container.Basket(self, baskets_prio[bCode], samples_num=num_of_samples)
    #             self._add_component(basket)
    #             datamatrix = bCode
    #             scanned = False
    #             present = True
    #             basket._set_info(present, datamatrix, scanned)
    #         # Second round create Samples
    #         for line in self.content:
    #             containerCode = line['containerCode']
    #             sampleName = line['sampleName']
    #             samplePosition = int(line['sampleLocation'])
    #             sampleBarcode = line['code']
    #             blsampleId = line['blsampleId']
    #             proteinAcronym = line['proteinAcronym']
    #             diffractionPlanId = line["diffractionPlan"]["diffractionPlanId"]
    #             crystalId = line["crystalId"]
    #             basket_index = baskets_prio[containerCode] #self.containers.index(containerCode)
    #             sample_list.append(("", basket_index, samplePosition, 1, Container.Pin.STD_HOLDERLENGTH))
    #             for basket in self.get_basket_list():
    #                 if basket_index == basket._number:
    #                     slot = Container.Pin(basket , basket._number, samplePosition)
    #                     basket._addComponent(slot)
    #
    #             sample = self.get_component_by_address(Container.Pin.get_sample_address(basket_index, samplePosition))
    #             scanned = loaded = has_been_loaded = False
    #             present = True
    #             sample._set_info(present, sampleName, scanned)
    #             sample._set_loaded(loaded, has_been_loaded)
    #             sample._set_holder_length(Container.Pin.STD_HOLDERLENGTH)
    #             sample._set_property("blsampleId",blsampleId)
    #             sample._set_property("diffractionPlanId",diffractionPlanId)
    #             sample._set_property("crystalId",crystalId)
    #             sample._set_property("proteinAcronym",proteinAcronym)
    #             # TODO non dovrebe servire
    #             # dc_num = self.lims.get_sample_dc_number(blsampleId)
    #             # sample._set_property("datacollectionsNum",dc_num)
    #
    #     if self.is_pin_detected():
    #         self._set_state(SampleChangerState.Loaded)
    #         self._selected_basket = self.ch_basket_selected.get_value()
    #         self._selected_sample = self.ch_sample_selected.get_value()
    #         self.log.info("SampleChangerElettra_XRD2 sample already mounted %d %d" % (self._selected_basket, self._selected_sample))
    #         sample = self.get_loaded_sample()
    #         print()
    #
    #         if sample is not None:
    #             loaded = has_been_loaded = True
    #             sample._set_loaded(loaded, has_been_loaded)
    #             self._trigger_loaded_sample_changed_event(self.get_loaded_sample())
    #     else:
    #         self._set_state(SampleChangerState.Ready)

    @trace_call_log
    def _init_sc_contents(self):
        """
        Initializes the sample changer content with default values.

        :returns: None
        :rtype: None
        """

        for i in range(self.no_of_baskets):
            basket = Container.Basket(
                self, i + 1, samples_num=self.no_of_samples_in_basket
            )
            self._add_component(basket)

        for basket_index in range(self.no_of_baskets):
            basket = self.get_components()[basket_index]
            datamatrix = None
            present = True
            scanned = False
            basket._set_info(present, datamatrix, scanned)

        sample_list = []
        for basket_index in range(self.no_of_baskets):
            for sample_index in range(self.no_of_samples_in_basket):
                sample_list.append(
                    (
                        "",
                        basket_index + 1,
                        sample_index + 1,
                        1,
                        Container.Pin.STD_HOLDERLENGTH,
                    )
                )
        for spl in sample_list:
            address = Container.Pin.get_sample_address(spl[1], spl[2])
            sample = self.get_component_by_address(address)
            datamatrix = "matr%d_%d" % (spl[1], spl[2])
            present = scanned = loaded = has_been_loaded = False
            sample._set_info(present, datamatrix, scanned)
            sample._set_loaded(loaded, has_been_loaded)
            sample._set_holder_length(spl[4])

        if self.is_pin_detected():
            self._set_state(SampleChangerState.Loaded)
            self._selected_basket = self.ch_basket_selected.get_value()
            self._selected_sample = self.ch_sample_selected.get_value()
            self.log.info(
                "SampleChangerElettra_XRD2 sample already mounted %d %d"
                % (self._selected_basket, self._selected_sample)
            )
            sample = self.get_loaded_sample()
            if sample is not None:
                loaded = has_been_loaded = True
                sample._set_loaded(loaded, has_been_loaded)
                self._trigger_loaded_sample_changed_event(self.get_loaded_sample())
        else:
            self._set_state(SampleChangerState.Ready)

    @trace_call_log
    def _get_hw_status(self, timeout: float = None) -> str:
        try:
            with Timeout(
                timeout,
                f'Timed out. Failed to read the SC status '
                f'from the attribute "{self.ch_sc_status.attribute_name}" '
                f'of the tango device "{self.ch_sc_status.device_name}"'
            ):
                while True:
                    try:
                        return self.ch_sc_status.get_value()
                    except PyTango.DevFailed as e:
                        self.log.exception('Error occurred retrieving SC status')
                        if "Not able to acquire serialization" in e.args[0].desc:
                            pass
                        elif "TRANSIENT_CallTimedout" in e.args[0].desc:
                            pass
                        else:
                            raise e
                    sleep(self.ch_sc_status.polling / 1000)
        except (PyTango.DevFailed, TimeoutError):
            self.log.exception('Error occurred retrieving SC status. Set to "UNKNOWN"')
            return 'UNKNOWN'

    @trace_call_log()
    def get_message(self):
        return self.message

    @trace_call_log
    def get_gripper_type(self):
        return GripperType.get(self.ch_gripper_type.get_value())

    @trace_call_log
    def get_pin_state(self):
        return PinStatus.get(self.ch_pin_status.get_value())

    @trace_call_log
    def is_pin_detected(self):
        return self.get_pin_state() == PinStatus.Detected

    @trace_call_log
    def handle_state_on_mismatch(self):
        """Validate consistency between hardware pin detection and internal variable"""
        if self.last_pin_status_value == PinStatus.Detected and super().get_loaded_sample():
            if not self.pin_detection_mismatched:
                self.pin_detection_mismatched = True
                err_msg = (
                    f"Pin detected by hardware but sample address is not known"
                    f"(basket={self._selected_basket}, sample={self._selected_sample})"
                )
                self.log.error(err_msg)
                self.user_log.error(
                    "%s. Please assign manually the location of the mounted sample using the beamline actions",
                    err_msg
                )
                with self.state_change_lock:
                    self.ena_hw_state_sync = False
                    self.message = err_msg
                    self._set_state(SampleChangerState.Fault)
                raise RuntimeError(err_msg)
        else:
            if self.pin_detection_mismatched:
                self.pin_detection_mismatched = False
                self.ena_hw_state_sync = True
                self._update_state()

    @trace_call_log
    def is_state_in(self, hw_states: list[StaubliStates]):
        self._update_state()
        for hw_state in hw_states:
            if self.state == hw_state.value and self._hw_state == hw_state:
                return True
        else:
            return False

    @trace_call_log
    def get_hw_state(self):
        return self._hw_state

    def wait_hw_states(self, states_list: list, timeout=None):
        with Timeout(
            timeout, RuntimeError(f"Timeout waiting one of these states: {states_list}")
        ):
            while not self.get_hw_state() in states_list:
                sleep(0.5)

    @trace_call_log
    def assert_ready_for_load(self):
        err_msg = ""
        if not self.is_state_in([StaubliStates.IDLE, StaubliStates.DEFROST, StaubliStates.COOLDOWN]):
            err_msg = (
                f'Sample loading aborted! The sample changer hw state must be "IDLE", "DEFROST" or "COOLDOWN" '
                f' (Current hw state: {self._hw_state.name}).'
                f' Use the sample changer actions to manage it (click on the equipment tab).'
            )
        if self.get_pin_state() == PinStatus.Detected and self.get_gripper_type() == GripperType.Single:
            err_msg = 'Sample loading aborted! A sample is already mounted'
        if err_msg:
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

    @trace_call_log
    def assert_ready_for_unload(self):
        err_msg = ""
        if not self.is_state_in([StaubliStates.IDLE, StaubliStates.DEFROST, StaubliStates.COOLDOWN]):
            err_msg = (
                f'Sample unloading aborted! The sample changer hw state must be "IDLE", "DEFROST" or "COOLDOWN" '
                f' (Current hw state: {self._hw_state.name}).'
                f' Use the sample changer actions to manage it (click on the equipment tab).'
            )
        if self.get_pin_state() == PinStatus.Detected and self.get_gripper_type() == GripperType.Single:
            err_msg = 'Sample unloading aborted! A sample is already mounted'
        if err_msg:
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

    def is_powered(self):
        return True
