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


class SampleChangerNotAvailable(Exception):
    def __init__(self, msg="Sample changer not available for mounting operation"):
        super(SampleChangerNotAvailable, self).__init__(msg)
        self.user_msg = "Sample changer not available for mounting operation"

    def get_user_msg(self):
        return self.user_msg


class DefrostException(SampleChangerNotAvailable):
    def __init__(self, msg="Sample changer in defrosting or cooling"):
        super(DefrostException, self).__init__(msg)
        self.user_msg = "*** Robot is DEFROSTING - Please wait... ***"


class ParkException(SampleChangerNotAvailable):
    def __init__(self, msg="Sample changer parked or parking"):
        super(ParkException, self).__init__(msg)
        self.user_msg = "Robot is PARKED - Please use Unpark command first"


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
        self.diffractometer = None
        self.lims = None
        self.last_pin_status_readtime = 0
        self.last_pin_status_value = "0"
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
    def _update_state(self, sc_status=None):
        if sc_status is None:
            sc_status = self.ch_sc_status.get_value()

        sc_status = sc_status.strip()
        if "IDLE" == sc_status:
            state = SampleChangerState.Ready
        elif "ERROR" in sc_status:
            state = SampleChangerState.Alarm
        elif "DEFROST" == sc_status:
            state = SampleChangerState.Defrost
        elif "COOLDOWN" == sc_status:
            state = SampleChangerState.Cooldown
        elif "PARKING" == sc_status:
            state = SampleChangerState.Parking
        elif "PARKED" == sc_status:
            state = SampleChangerState.Parked
        elif "UNPARKING" == sc_status:
            state = SampleChangerState.Unparking
        elif "UNMOUNTING" == sc_status or "ENDING: UNMOUNT" == sc_status:
            state = SampleChangerState.Unloading
        elif "MOUNTING" == sc_status or "ENDING: MOUNT" == sc_status:
            state = SampleChangerState.Loading
        elif "SWAPPING" == sc_status or "ENDING: SWAP" == sc_status:
            state = SampleChangerState.Swapping
        elif "EXCHANGE" == sc_status:
            state = SampleChangerState.Exchange
        elif "EXCHANGING" == sc_status:
            state = SampleChangerState.Exchanging
        elif "GRIPPER EXCHANGE" == sc_status:
            state = SampleChangerState.GripperExchange
        elif "SAFEPOINT" == sc_status:
            state = SampleChangerState.Safepoint
        else:
            state = SampleChangerState.Unknown

        self.log.info(
            f"Sample changer current state: {SampleChangerState.tostring(state)}"
        )
        self._set_state(state=state)

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
            self._update_state(sc_status=value)

        self.log.info(f"Sample changer current info: {self.sc_info}")
        self.emit("scInfoChanged")

    def _wait_task(self, timeout: int = 300, waiting_state_list: list = None):
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
                        waiting_state_list
                        and not self.get_state() in waiting_state_list
                    ):
                        break
                except PyTango.DevFailed as e:
                    self.log.exception(
                        'Tango error occurred waiting for samplechanger "OFF"'
                    )
                    if "Not able to acquire serialization" in e.args[0].desc:
                        continue
                    elif "TRANSIENT_CallTimedout" in e.args[0].desc:
                        continue
                    else:
                        raise e
                sleep(1)
            return task_failed, task_log

    def force_loaded_sample(self, sample_location: str):
        try:
            basket, sample = sample_location.split(":")
            self._selected_basket = basket
            self._selected_sample = sample
            self.set_state(SampleChangerState.Ready)
        except:
            self.log.exception("Error occurred forcing loaded sample")

    def load_sample(self, holder_length, sample_location=None, wait=False):
        if isinstance(sample_location, tuple):
            element = "%d:%02d" % sample_location
        self.load(element, wait)

    @trace_call_log
    def load(self, sample_location, wait=False):
        start_timestamp_RA = time.time()
        action_Type_RA = "LOAD"
        status_RA = "skip"
        message_RA = ""

        self._update_state()
        # Check sample changer readiness
        if self.get_state() not in [
            SampleChangerState.Ready,
            SampleChangerState.Defrost,
        ]:
            err_msg = (
                f'Sample can not be loaded! Sample changer is not "Ready". Use the sample changer '
                f" actions to manage it, clicking on the equipment tab."
            )
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

        # If defrosting wait finishing
        if self.get_state() == SampleChangerState.Defrost:
            self.wait_defrost(operation="sample loading")

        # If single grip unload sample before load
        sc_gripper = self.get_gripper_type()
        if sc_gripper == GripperType.Single and self.get_loaded_sample() is not None:
            self.unload(self.get_loaded_sample().get_coords(), True)

        self._update_state()
        # Abort if not ready
        if not self.is_ready():
            err_msg = (
                f'Sample loading aborted! Sample changer is expected to be "Ready" but it\'s'
                f' "{SampleChangerState.tostring(self.state)}" instead'
            )
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

        # Abort if single pin is detected
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

        # Let's be sure that MD is in Transfer mode just before sending the swap/mount command
        # this is handled by procedure on xrd2-control-01:/runtime/etc/executer/scripts/xrd2_samplechanger.py
        # ...just for extra safety
        #

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
                self.log.info('"swapping" command sent to the samplechanger executer')
            else:
                self.cmd_start_method("mount")
                self.log.info('"mount" command sent to the samplechanger executer')

            sleep(1)
            self._wait_task()

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
                self.log.info(
                    f"Sample loaded successfully! Sample {sample_obj.get_coords()} has been mounted"
                )
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
    def unload(self, sample_location=None, wait=None):
        startTimestampRA = time.time()
        actionTypeRA = "UNLOAD"
        statusRA = "skip"
        messageRA = ""

        self._update_state()

        self._update_state()
        # Check sample changer readiness
        if self.get_state() not in [
            SampleChangerState.Ready,
            SampleChangerState.Defrost,
        ]:
            err_msg = (
                f'Sample can not be unloaded! Sample changer is not "Ready". Use the sample changer '
                f" actions to manage it, clicking on the equipment tab."
            )
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

        # If defrosting wait finishing
        if self.get_state() == SampleChangerState.Defrost:
            self.wait_defrost(operation="sample unloading")

        self._update_state()
        # Abort if not ready
        if not self.is_ready():
            err_msg = (
                f'Sample unloading aborted! Sample changer is expected to be "Ready" but it\'s'
                f' "{SampleChangerState.tostring(self.state)}" instead'
            )
            self.user_log.error(err_msg)
            raise RuntimeError(err_msg)

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

        # Let's be sure that MD is in Transfer mode just before sending the swap/mount command
        # this is handled by procedure on xrd2-control-01:/runtime/etc/executer/scripts/xrd2_samplechanger.py
        # ...just for extra safety
        #
        statusRA = "ERROR"
        dewarLocationRA = int(basket)
        containerLocationRA = int(sample)
        """
        RB : next 2 lines should be unuseful
        self.device.Sample_selected = int(sample)
        self.ch_basket_selected.set_value(int(basket)
        """
        try:
            self.cmd_start_method("unmount")
            self.log.info('"unmount" command sent to the samplechanger executer')
            sleep(1)
            self._wait_task()
            sc_action_failed = self.ch_script_failed.get_value()
            if sc_action_failed:
                err_msg = "Unmount Script Failed!"
                step_by_step = self.ch_step_by_step_log.get_value()
                self.log.error(
                    f"Sample unloading failed! {err_msg} - StepByStepLog: {step_by_step})"
                )
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
                self.log.debug(
                    f"Sample unloaded successfully! Sample {sample_obj.get_coords()} has been mounted"
                )

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
        # self._trigger_loaded_sample_changed_event(self.get_loaded_sample())
        return

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
            self._set_state(SampleChangerState.Alarm)
            # self.user_log.error(f"{err_msg}. Please assign manually the location of the mounted sample")
            self.log.error(
                f"{err_msg}. Please assign manually the location of the mounted sample"
            )
            # raise RuntimeError(err_msg)
        else:
            print("AAAAAAAAA self._selected_basket, self._selected_sample")
            print(self._selected_basket, self._selected_sample)

            return self.get_component_by_address(
                Container.Pin.get_sample_address(
                    self._selected_basket, self._selected_sample
                )
            )

    @trace_call_log
    def is_mounted_sample(self, sample):
        if isinstance(sample, tuple):
            sample = "%s:%s" % sample

        return sample == self.get_loaded_sample()

    @trace_call_log
    def _do_park(self):
        # TODO valutare
        # self.set_state(SampleChangerState.Moving)

        if self.get_state() in [SampleChangerState.Ready, SampleChangerState.Exchange]:
            self.cmd_start_method("park")
            self.log.info('"park" command sent to the samplechanger executer')
            sleep(1)
            self._wait_task()
        else:
            err_msg = "Parking aborted! Sample changer is not Ready or Exchange (Current state: %s)"
            self.user_log.error(err_msg, SampleChangerState.tostring(self.get_state()))

    def _do_unpark(self):
        # TODO valutare
        # self.set_state(SampleChangerState.Moving)

        if self.get_state() is SampleChangerState.Parked:
            self.cmd_start_method("unpark")
            self.log.info('"unpark" command sent to the samplechanger executer')

            sleep(1)
            self._wait_task()
        else:
            err_msg = (
                "Unparking aborted! Sample changer is not Parked (Current state: %s)"
            )
            self.user_log.error(err_msg, SampleChangerState.tostring(self.get_state()))

    def _do_trash2S(self):
        # TODO valutare
        # self.set_state(SampleChangerState.Moving)
        if self.get_state() != SampleChangerState.Alarm:
            err_msg = "Trashing aborted! Is not in ERROR state"
            self.user_log.error(err_msg)
            return

        # SET(FORCE_IDLE), go to SAF, wait, go to EXC, SET(GRIPPER_OPEN) to drop (hopefully...), PAR, UNP
        # ...and finally Reset to update SC Tango variables (based on pin detection)
        startTimestampRA = time.time()
        recovery_time = 60
        recovery_time = 10  # todo ripristina 60
        # self._set_state(SampleChangerState.Trashing)
        self.cmd_start_method("force_idle")
        self._wait_task()
        self.cmd_start_method("safe_point")
        self._wait_task()
        sleep(5)
        self.cmd_start_method("exchange_point")
        self._wait_task()
        while recovery_time > 0:
            self.user_log.error(
                "!!! Gripper will drop pins in {} s !!!".format(recovery_time)
            )
            sleep(5)
            recovery_time -= 5
        self.cmd_start_method("write_samplechanger_gripper_open")
        self._wait_task()
        # self._set_state(SampleChangerState.Moving)
        self._do_park()
        self.user_log.info("!!! Gripper will be ready soon... !!!")
        self._wait_task()
        self._do_unpark()
        self._wait_task()
        self.reset()
        # self._set_state(SampleChangerState.Ready)

        # TODO implementa store_robo_action
        # self.lims.set_robot_action(dewarLocation=self.device.Basket_selected,
        #         containerLocation=self.device.Sample_selected,
        #         actionType='DISPOSE',
        #         status='SUCCESS',
        #         message='Trashing [last SWAP from {}:{} to {}:{}]'.format(self.device.Basket_to_swap, self.device.Sample_to_swap, self.device.Basket_selected, self.device.Sample_selected),
        #         startTimestamp=startTimestampRA,
        #         endTimestamp=time.time())
        return

    @trace_call_log
    def _do_defrost(self):
        # TODO valutare
        # self.set_state(SampleChangerState.Moving)
        if self.get_state() == SampleChangerState.Ready:
            self.cmd_start_method("defrost")
            self.log.info('"Defrost" command sent to the samplechanger executer')
            sleep(1)
            self._wait_task(waiting_state_list=[SampleChangerState.Defrost])
        else:
            err_msg = "Defrost aborted! Sample changer is not Ready (Current state: %s)"
            self.user_log.error(err_msg, SampleChangerState.tostring(self.get_state()))

    def _do_abort(self):
        raise NotImplemented

    def _do_change_mode(self, mode):
        raise NotImplemented

    def _do_update_info(self):
        return

    def _do_select(self, component):
        raise NotImplemented

    def _do_load(self, sample=None):
        raise NotImplemented

    def _do_unload(self, sample_slot=None):
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
    def reset(self):
        """
        Recover from real ERROR from StaubliTCP
        """
        # TODO valuta
        # self.set_state(SampleChangerState.Resetting)
        self.cmd_start_method("write_samplechanger_gripper_open")
        self._wait_task()
        self.cmd_start_method("force_idle")
        sleep(1)
        self._wait_task()
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
    def _get_status(self, timeout=60):
        with Timeout(timeout, "Timeout waiting for sc status"):
            while True:
                try:
                    return self.ch_sc_status.get_value()
                except PyTango.DevFailed as e:
                    if "Not able to acquire serialization" in e.args[0].desc:
                        continue
                    elif "TRANSIENT_CallTimedout" in e.args[0].desc:
                        continue
                    else:
                        raise e

    @trace_call_log
    def get_gripper_type(self):
        return GripperType.get(self.ch_gripper_type.get_value())

    @trace_call_log
    def get_pin_state(self):
        return PinStatus.get(self.ch_pin_status.get_value())

    @trace_call_log
    def is_pin_detected(self):
        return self.get_pin_state() == PinStatus.Detected

    def wait_states(self, states_list: list, timeout=None):
        """Wait for current sample changer operation to finish.
        Blocks for timeout seconds or forever if timeout is None
        Args:
            timeout (int): timeout [s].
        Raises:
            (Exception): If operation lasts longer than the timeout.
        """

        with Timeout(
            timeout, RuntimeError(f"Timeout waiting one of these states: {states_list}")
        ):
            while not self.get_state() in states_list:
                sleep(0.5)

    @trace_call_log
    def wait_defrost(self, operation: str = "operation"):
        try:
            self.user_log.warning(
                f"Sample changer is DEFROSTING, the {operation} will continue after it, please wait"
            )
            self.wait_states(
                [SampleChangerState.Cooldown, SampleChangerState.Ready],
                timeout=self.DEFROST_DURATION,
            )
            self.user_log.warning(
                f"DEFROSTING finished, sample changer will go on with the {operation} now!"
            )
        except TimeoutError:
            raise RuntimeError(
                f"Timeout error occurred waiting sample changer defrosting in {self.operation}"
            )

    def is_powered(self):
        return True
