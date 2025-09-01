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

from mxcubecore.HardwareObjects.abstract.AbstractSampleChanger import *
from mxcubecore.HardwareObjects.abstract.sample_changer import Container
from mxcubecore import HardwareRepository as HWR
from mxcubecore import trace_call_log


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
    DEFROST = SampleChangerState.Moving
    COOLDOWN = SampleChangerState.Moving
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
        self._ena_hw_state_sync = True
        self.diffractometer = None
        self.lims = None
        self.last_pin_status_readtime = 0
        self.last_pin_status_value = "0"
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
        if self._ena_hw_state_sync:
            self._set_state(state=self._hw_state.value)

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

    def force_loaded_sample(self, sample_location: str):
        try:
            basket, sample = sample_location.split(":")
            self._selected_basket = basket
            self._selected_sample = sample
            self._ena_hw_state_sync = True
            self._update_state()
        except:
            self.log.exception("Error occurred forcing loaded sample")

    def defrost(self, wait: bool = True):
        self._update_state()
        if self._hw_state == StaubliStates.IDLE:
            return self._execute_task(SampleChangerState.Moving, wait, self._do_defrost)
        else:
            err_msg = 'Defrost aborted! Sample changer must be in "IDLE" state (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)

    def unpark(self, wait: bool = True):
        self._update_state()
        if self._hw_state == StaubliStates.PARKED:
            return self._execute_task(SampleChangerState.Moving, wait, self._do_unpark)
        else:
            err_msg = 'Unparking aborted! Sample changer must be in "PARKED" state (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)

    def park(self, wait: bool = True):
        self._update_state()
        if self._hw_state in [StaubliStates.IDLE, StaubliStates.EXCHANGE]:
            return self._execute_task(SampleChangerState.Moving, wait, self._do_park)
        else:
            err_msg = 'Parking aborted! Sample changer must be in "IDLE" or "EXCHANGE" state (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)

    def trash2S(self, wait: bool = True):
        self._update_state()
        if self._hw_state == StaubliStates.ERROR:
            self._ena_hw_state_sync = False
            return self._execute_task(SampleChangerState.Moving, wait, self._do_trash2S)
        else:
            err_msg = 'Trashing aborted! Sample changer is not in "ERROR" state (Current hw state: %s)'
            self.user_log.error(err_msg, self._hw_state.name)

    @trace_call_log
    def assert_not_charging(self):
        # !!! Since we are using the AbstractSampleChanger implementation of load an unload method this method will be
        # called to check the feasibility of the operation. The of the method name it is obviously misleading since we
        # actually check the readiness for the mounting and unmount operations
        # TODO propose to the mx3 community to replace `assert_not_charging` with somthing like these more general
        #  `assert_ready_for_load` and `assert_ready_for_unload` methods (should be abstract)

        self._update_state()
        if not(self._hw_state in [StaubliStates.DEFROST, StaubliStates.COOLDOWN]
               or (self._hw_state == StaubliStates.IDLE and not self.is_executing_task())):
            err_msg = (
                f'Sample can not be unloaded or loaded! Sample changer must be in "IDLE", "DEFROST" or "COOLDOWN" '
                f'state (Current hw state: {self._hw_state.name}).'
                f" Use the sample changer actions to manage it (click on the equipment tab)."
            )
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

    @trace_call_log
    def get_loaded_sample(self):
        if (time.time() - self.last_pin_status_readtime) > 1:
            # Check pin_status at 1Hz
            self.last_pin_status_readtime = time.time()
            self.last_pin_status_value = self.get_pin_state()
        if not self.is_pin_detected():
            return None
        if -1 in [self._selected_basket, self._selected_sample]:
            err_msg = (
                f"Pin detected but the sample address can not be retrieved from the executer "
                f"(basketSelected = {self._selected_basket} - sampleSelected={self._selected_sample})"
            )
            if self._ena_hw_state_sync:
                self._ena_hw_state_sync = False
                self._set_state(SampleChangerState.Fault)
                self.user_log.error("%s. Please assign manually the location of the mounted sample", err_msg)
                self.log.error("%s. Please assign manually the location of the mounted sample", err_msg)
            # raise RuntimeError(err_msg)
        else:
            print("AAAAAAAAA self._selected_basket, self._selected_sample")
            print(self._selected_basket, self._selected_sample)

            return self.get_component_by_address(
                Container.Pin.get_sample_address(
                    self._selected_basket, self._selected_sample
                )
            )

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
    def reset(self, wait=True):
        """
        Reset the sample changer.

        wait: If True wait for reset to finish otherwise return immediately

        Returns:
            (Object): Value returned by _execute_task either a Task or result of the
                      operation
        """

        self._update_state()
        if self._hw_state == StaubliStates.ERROR:
            self._ena_hw_state_sync = False
            return self._execute_task(SampleChangerState.Resetting, wait, self._do_reset)
        else:
            err_msg = "Parking aborted! Sample changer is not IDLE or EXCHANGE state (Current hw state: %s)"
            self.user_log.error(err_msg, self._hw_state.name)


    @trace_call_log
    def _do_load(self, sample_location, wait=False):
        start_timestamp_RA = time.time()
        action_Type_RA = "LOAD"
        status_RA = "skip"
        message_RA = ""

        # Wait defrost or cooldown finishing
        if self._hw_state in [StaubliStates.DEFROST, StaubliStates.COOLDOWN]:
            self._wait_defrost_and_cooldown(operation="sample loading")

        # Abort if single pin is detected
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

        if isinstance(sample_location, tuple):
            basket, sample = sample_location
        else:
            basket, sample = sample_location.split(":")

        status_RA = "ERROR"

        dewarLocationRA = int(basket)
        containerLocationRA = int(sample)
        try:
            if self.get_loaded_sample() is None:
                old_basket_selected, old_sample_selected = 0, 1
            else:
                old_basket_selected, old_sample_selected = (
                    self._selected_basket,
                    self._selected_sample,
                )
            self._selected_sample = sample
            self._selected_basket = basket

            if sc_gripper == GripperType.Double:
                self._basket_2_swap = old_basket_selected
                self._sample_2_swap = old_sample_selected
                self.cmd_start_method("swap")
                self.log.info('"swapping" command sent to the tango device %s', self.cmd_start_method.device_name)
            else:
                self.cmd_start_method("mount")
                self.log.info('"mount" command sent to the tango device %s', self.cmd_start_method.device_name)

            sleep(1)
            self._wait_method_execution()

            sc_action_failed = self.ch_script_failed.get_value()
            sample_obj = self.get_component_by_address(
                Container.Pin.get_sample_address(int(basket), int(sample))
            )

            if sc_gripper == GripperType.Double:
                unloaded_sample_obj = self.get_component_by_address(
                    Container.Pin.get_sample_address(
                        int(old_basket_selected), int(old_sample_selected)
                    )
                )
                if unloaded_sample_obj:
                    loaded = has_been_loaded = False
                    unloaded_sample_obj._set_loaded(loaded, has_been_loaded)

            if sc_action_failed:
                # Check on pin detection and sample changer status done in executer
                err_msg = "Mounting Script Failed!"
                step_by_step = self.ch_step_by_step_log.get_value()
                self.log.error(
                    f"Sample loading failed! {err_msg} - StepByStepLog: {step_by_step})"
                )
                loaded = has_been_loaded = False
                sample_obj._set_loaded(loaded, has_been_loaded)
                self._selected_sample = -1
                self._selected_basket = -1
            else:
                err_msg = ""
                loaded = has_been_loaded = True
                sample_obj._set_loaded(loaded, has_been_loaded)
                self.log.info(f"Sample loaded successfully! Sample %s has been mounted", str(sample_obj.get_coords()))
                status_RA = "SUCCESS"

        except Exception:
            self.log.exception("Error occurred during load method execution")
            self._selected_basket = -1
            self._selected_sample = -1

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
        statusRA = "skip"
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
            basket, sample = sample_location
        else:
            basket, sample = sample_location.split(":")

        statusRA = "ERROR"
        dewarLocationRA = int(basket)
        containerLocationRA = int(sample)
        try:
            self.cmd_start_method("unmount")
            self.log.info('"unmount" command sent to the tango device %s', self.cmd_start_method.device_name)
            sleep(1)
            self._wait_method_execution()
            sc_action_failed = self.ch_script_failed.get_value()
            if sc_action_failed:
                err_msg = "Unmount Script Failed!"
                step_by_step = self.ch_step_by_step_log.get_value()
                self.log.error("Sample unloading failed! %s - StepByStepLog: %s)", (err_msg, step_by_step))
            else:
                sample_obj = self.get_component_by_address(
                    Container.Pin.get_sample_address(
                        self._selected_basket, self._selected_sample
                    )
                )
                loaded = has_been_loaded = False
                sample_obj._set_loaded(loaded, has_been_loaded)
                self._selected_basket = -1
                self._selected_sample = -1

                statusRA = "SUCCESS"
                self.log.debug("Sample unloaded successfully! Sample %s has been mounted", str(sample_obj.get_coords()))

        except Exception:
            self.log.exception("Unexpected error occurred during load method execution")
        finally:
            try:
                if statusRA is not "skip":
                    # TODO implementa store_robo_action
                    # self.lims.set_robot_action(dewarLocation=dewarLocationRA,
                    #                                  containerLocation=containerLocationRA, actionType=actionTypeRA,
                    #                                  status=statusRA, message=messageRA,
                    #                                  startTimestamp=startTimestampRA, endTimestamp=time.time())
                    # ND20241225: ISPyB don't store strategies yet - using tmp file, better removed changing sample...
                    import os

                    best_strategy = "/tmp/best.xml"
                    if os.path.isfile(best_strategy):
                        os.remove(best_strategy)
            except:
                pass
        self._trigger_loaded_sample_changed_event(self.get_loaded_sample())
        return

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

    @trace_call_log
    def _do_defrost(self):
        if self.get_state() == SampleChangerState.Ready:
            self.cmd_start_method("defrost")
            self.log.info('"defrost" command sent to the tango device %s', self.cmd_start_method.device_name)
            sleep(1)
            self._wait_method_execution(wait_only_if=[StaubliStates.DEFROST])
        else:
            err_msg = "Defrost aborted! Sample changer is not Ready (Current state: %s)"
            self.user_log.error(err_msg, SampleChangerState.tostring(self.get_state()))

    def _do_unpark(self):
        self.cmd_start_method("unpark")
        self.log.info('"unpark" command sent to the tango device %s', self.cmd_start_method.device_name)
        sleep(1)
        self._wait_method_execution()

    @trace_call_log
    def _do_park(self):
        self.cmd_start_method("park")
        self.log.info('"park" command sent to the tango device %s', self.cmd_start_method.device_name)
        sleep(1)
        self._wait_method_execution()

    def _do_trash2S(self):
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

        startTimestampRA = time.time()
        recovery_time = 60
        recovery_time = 10  # todo ripristina 60
        self.cmd_start_method("force_idle")
        self.log.info('"force_idle" command sent to the tango device %s', self.cmd_start_method.device_name)
        self._wait_method_execution()
        self.cmd_start_method("safe_point")
        self.log.info('"safe_point" command sent to the tango device %s', self.cmd_start_method.device_name)
        self._wait_method_execution()
        sleep(5)
        self.cmd_start_method("exchange_point")
        self.log.info('"exchange_point" command sent to the tango device %s', self.cmd_start_method.device_name)
        self._wait_method_execution()
        while recovery_time > 0:
            self.user_log.error(
                "!!! Gripper will drop pins in {} s !!!".format(recovery_time)
            )
            sleep(5)
            recovery_time -= 5
        self.cmd_start_method("write_samplechanger_gripper_open")
        self.log.info(
            '"write_samplechanger_gripper_open" command sent to the tango device %s',
            self.cmd_start_method.device_name
        )
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
        return

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
    def get_hw_state(self):
        return self._hw_state

    def _on_task_ended(self, task):
        """What to do when task ended normally"""
        try:
            msg = f"Task ended. Return value: {task.get()}"
            self.log.debug(msg)
        except Exception as err:
            msg = f"Error while executing sample changer task: {err}"
            self.log.error(msg)
        finally:
            self._ena_hw_state_sync = True
            self._update_state()

    def wait_hw_states(self, states_list: list, timeout=None):
        with Timeout(
            timeout, RuntimeError(f"Timeout waiting one of these states: {states_list}")
        ):
            while not self.get_hw_state() in states_list:
                sleep(0.5)

    def is_powered(self):
        return True
