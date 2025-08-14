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
import gevent
import PyTango
from mxcubecore.HardwareObjects.abstract.AbstractActuator import AbstractActuator
from mxcubecore import trace_call_log

# TODO controlla gli enum

@enum.unique
class CryoRunMode(enum.Enum):
    StartUp: 0  # Initialising
    StartUpFail: 1  # Initialisation failed
    StartUpOK: 2  # Ready
    Run: 3  # Running
    SetUp: 4  # Set up mode
    ShutdownOK: 5  # Shut down without error
    ShutdownFail: 6  # Shut down with error


@enum.unique
class CryoPhase(enum.Enum):
    Stop: -99  #fake phase for comparison
    Restart: -1
    Ramp: 0  # Temperature changed at a controlled rate to final value
    Cool: 1  # Temperature changed as fast as possible to final value
    Plat: 2  # Temperature held for a defined period of time at the specified value
    Hold: 3  # Temperature held indefinitely at the specified value
    End: 4  # Device is shutdown in a controlled fashion
    Purge: 5
    DeletePhs: 6
    LoadPrgrm: 7
    SavePrgrm: 8
    PurgeSoak: 9
    Ramp_Wait: 10  # During a Ramp, device is waiting for temperature to 'catch up'
                   # with set point


@enum.unique
class CryoAlarm(enum.Enum):
    NoAlarm: 0  # No errors or warnings
    StopPressdRestartPls: 1  # Stop pressed
    StopCmdRestartPls: 2  # Stop command
    EndDoneRestartPls: 3  # End complete
    PurgeDone_RestartPls: 4  # Purge complete
    TWarn: 5  # Temp warning
    HighBackP: 6  # Pressure warning
    PoorVacuum: 7  # Check vacuum
    StartUpFail: 8  # Self-check fail
    LowFlow: 9  # Flow rate fail
    BigTempErr: 10  # Temp control error
    TReadError: 11  # Gas type error
    SensorFail: 12  # Temp reading error
    DegradationPwrSpply: 13  # Suct temp error
    HeatSinkOvrHt: 14  # ??? Sensor fail
    PowerOvrHt: 15  # ??? Brownout
    PowerLss: 16  # ??? Sink overheat


class Cryostream1000STangoCryo(AbstractActuator):

    unit = "K"

    map_to_mxcube_state = {
        CryoRunMode.StartUp.value: AbstractActuator.STATES.BUSY,
        CryoRunMode.StartUpFail.value: AbstractActuator.STATES.FAULT,
        CryoRunMode.StartUpOK.value: AbstractActuator.STATES.READY,
        CryoRunMode.Run.value: AbstractActuator.STATES.BUSY,
        CryoRunMode.SetUp.value: AbstractActuator.STATES.WARNING,
        CryoRunMode.ShutdownOK.value: AbstractActuator.STATES.OFF,
        CryoRunMode.ShutdownFail.value: AbstractActuator.STATES.FAULT,
        }

    def __init__(self, name):

        super().__init__(name)
        self.ch_gas_temp = None
        self.ch_target_temp = None
        self.ch_pressure = None
        self.ch_alarm_code = None
        self.ch_phase_id = None
        self.ch_run_mode = None
        self.cmd_ramp = None
        self.cmd_cool = None
        self.cmd_purge = None
        self.cmd_hold = None
        self.cmd_end = None
        self.cmd_stop = None
        self.cmd_restart = None
        self.cmd_anneal = None
        self.cmd_anneal_time = None
        self.default_rate = 360  # Max speed: 360 K/h

    @trace_call_log
    def init(self):

        super().init()
        self.ch_gas_temp = self.get_channel_object("gas_temp")
        self.ch_target_temp = self.get_channel_object("target_temp")
        self.ch_pressure = self.get_channel_object("pressure")
        self.ch_alarm_code = self.get_channel_object("alarm_code")
        self.ch_phase_id = self.get_channel_object("phase_id")
        self.ch_run_mode = self.get_channel_object("run_mode")
        self.cmd_ramp = self.get_command_object("ramp")
        self.cmd_cool = self.get_command_object("cool")
        self.cmd_purge = self.get_command_object("purge")
        self.cmd_hold = self.get_command_object("hold")
        self.cmd_end = self.get_command_object("end")
        self.cmd_stop = self.get_command_object("stop")
        self.cmd_restart = self.get_command_object("restart")
        self.cmd_anneal = self.get_command_object("anneal")
        self.cmd_anneal_time = self.get_command_object("anneal_time")

        # SIGNALS CONNECTIONS
        self.connect(self.ch_gas_temp, "update", self.update_value)
        self.connect(
            self.ch_run_mode,
            "update",
            lambda tg_value: self.update_state(
                self.map_to_mxcube_state.get(
                    tg_value, AbstractActuator.STATES.UNKNOWN
                )
            )
        )

        self.update_state(self.STATES.READY)

    # TODO valutare se portare anche gli altri segnali

    @trace_call_log
    def get_value(self) -> float:
        value = self.ch_gas_temp.get_value()
        self.log.debug(f"Read the gas temperature of the \"{self.username}\" "
                       f"(value: \"{value} {self.unit}\")")
        return value

    @trace_call_log
    def _set_value(self, value: float):
        if self.ch_run_mode.get_value() in [CryoRunMode.ShutdownFail.value,
                                            CryoRunMode.ShutdownOK.value]:
            self.restart()
        if self.ch_phase_id.get_value() == CryoPhase.RAMP.value:
            self.hold()
        while self.ch_phase_id.get_value() != CryoPhase.RAMP.value:
            self.cmd_ramp([self.default_rate, value])
            self.user_log.warning(
                f"{self.username}: Ramping to {value} {self.unit}"
            )
            gevent.sleep(self.ch_gas_temp.polling / 1000)

    @trace_call_log
    def cool(self, value: float):
        if self.ch_phase_id.get_value() == CryoPhase.Cool.value:
            self.hold()
        if value < self.get_value():
            while self.ch_phase_id.get_value() != CryoPhase.Cool.value:
                self.user_log.warning(
                    f"{self.username}: Cooling to {value} {self.unit}"
                )
                self.cmd_cool(value)
                gevent.sleep(self.ch_gas_temp.polling / 1000)
        else:
            self.set_value(value)

    @trace_call_log
    def restart(self):
        run_mode = self.ch_run_mode.get_value()
        if run_mode == CryoRunMode.ShutdownOK.value:
            with gevent.Timeout(60, TimeoutError(
                f'"{self.username}: RESTART FAILED. System not restarted after '
                f'{self.timeout} sec'
            )):
                while self.ch_run_mode.get_value() == CryoRunMode.ShutdownOK.value:
                    self.cmd_restart()
                    self.log.info(
                        f"{self.username}: WAITING for the restart process to begin"
                    )
                    gevent.sleep(3 * (self.ch_run_mode.polling / 1000))
                while True:
                    run_mode = self.ch_run_mode.get_value()
                    self.log.info(
                        f"{self.username}: WAITING for the restart to complete (current"
                        f" run mode: {CryoRunMode(run_mode).name})")
                    if run_mode == CryoRunMode.StartUpOK.value:
                        self.log.warning(f'{self.username}: RESTART SUCCESSFUL!')
                        break
                    if run_mode == CryoRunMode.StartUpFail.value:
                        raise RuntimeError(
                            f"{self.username}: RESTART FAILED. Controller needs a"
                            f" manual restart "
                            f"(current run mode: {CryoRunMode(run_mode).name})"
                        )
                    gevent.sleep(3 * (self.ch_run_mode.polling / 1000))
        elif run_mode in [CryoRunMode.StartUp.value, CryoRunMode.ShutdownFail.value]:
            raise RuntimeError(
                f"{self.username}: RESTART NOT POSSIBLE. Please check controller State!"
                f"  (current run mode: {CryoRunMode(run_mode).name})"
            )
        else:
            self.user_log.warning(f"{self.username}: SYSTEM STARTED!")

    @trace_call_log
    def anneal(self, seconds):
        try:
            if self.ch_run_mode.get_value() != CryoRunMode.Run.value:
                raise Exception
            seconds = float(seconds)
            if seconds > 60 or seconds < 0.1:
                seconds = 3.0
            self.cmd_anneal_time(seconds)
            self.cmd_anneal()
            self.user_log.warning(
                f"{self.username}: ANNEALING sample for {seconds} sec ..."
            )
            gevent.sleep(seconds)
            self.user_log.warning(f"{self.username}: ANNEALING done")

        except PyTango.DevFailed:
            self.user_log.error(
                f"{self.username}: ANNEALING FAILED. "
                f"Please check controller State! "
                f"(current run mode: {CryoRunMode(self.ch_run_mode.get_value()).name})"
            )
            raise

    @trace_call_log
    def end(self):
        self.user_log.warning(f"{self.username}: Switching off stream...")
        while self.ch_phase_id.get_value() != CryoPhase.End.value:
            self.cmd_end(self.default_rate)
            gevent.sleep(1)

    @trace_call_log
    def hold(self):
        run_mode = self.ch_run_mode.get_value()
        if run_mode != CryoRunMode.Run.value:
            raise RuntimeError(
                f"{self.username}: HOLD NOT POSSIBLE. CryoJet is not running "
                f"(current run mode: {CryoRunMode(run_mode).name})"
            )
        else:
            # TODO ??? perche error e perchè ripetere il comando ?
            self.user_log.error(f'{self.username}: HOLDING current temperature!')
            while self.ch_phase_id.get_value() != CryoPhase.Hold.value:
                self.cmd_hold()
                gevent.sleep(self.ch_run_mode.polling / 1000)

    @trace_call_log
    def stop(self):
        self.hold()

    @trace_call_log
    def abort(self):
        self.user_log.warning(f"{self.username}: STOPPING system ...")
        while self.ch_run_mode.get_value() not in [CryoRunMode.ShutdownOK.value,
                                                   CryoRunMode.ShutdownFail.value]:
            # TODO perchè ripetere il comando ?
            self.cmd_stop()
            gevent.sleep(self.ch_run_mode.polling / 1000)

    @trace_call_log
    def purge(self):
        self.user_log.warning(f"{self.username}: PURGING system ...")
        while self.ch_phase_id.get_value() != CryoPhase.Purge:
            # TODO perchè ripetere il comando ?
            self.cmd_purge()
            gevent.sleep(self.ch_phase_id.polling / 1000)