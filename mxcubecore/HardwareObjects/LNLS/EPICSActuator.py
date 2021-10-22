"""
Superclass for EPICS actuators.

Should be put as the first superclass,
e.g. class EPICSMotor(EPICSActuator, AbstractMotor):

Example of xml file:

<device class="LNLS.EPICSActuator">
    <channel type="epics" name="epicsActuator_val">MNC:B:LUCIOLE01:LIGHT_CH1</channel>
    <channel type="epics" name="epicsActuator_rbv" polling="500">MNC:B:LUCIOLE01:LIGHT_CH1</channel>
    <username>BackLight</username>
    <motor_name>BackLight</motor_name>
    <default_limits>(0, 8000)</default_limits>
</device>
"""

import time
import random
import gevent
from mxcubecore.HardwareObjects.abstract import AbstractActuator


class EPICSActuator(AbstractActuator.AbstractActuator):
    """EPCIS actuator class"""

    ACTUATOR_VAL  = 'epicsActuator_val' # target
    ACTUATOR_RBV  = 'epicsActuator_rbv' # readback

    def __init__(self, name):
        super(EPICSActuator, self).__init__(name)
        self.__move_task = None
        self._nominal_limits = (-1E4, 1E4)

    def init(self):
        """ Initialization method """
        super(EPICSActuator, self).init()
        self.update_state(self.STATES.READY)

    def _move(self, value):
        """ Value change routine.
        Args:
            value : target actuator value

        Returns:
            final actuator value (may differ from target value)
        """
        self.update_state(self.STATES.BUSY)
        time.sleep(0.3)
        current_value = self.get_value()
        #self.update_value(current_value)
        self.update_state(self.STATES.READY)
        return current_value

    def get_value(self):
        """Override AbstractActuator method."""
        return self.get_channel_value(self.ACTUATOR_RBV)

    def set_value(self, value, timeout=0):
        """ Override AbstractActuator method."""
        if self.read_only:
            raise ValueError("Attempt to set value for read-only Actuator")
        if self.validate_value(value):
            if timeout or timeout is None:
                with gevent.Timeout(
                    timeout, RuntimeError("Motor %s timed out" % self.username)
                ):
                    self._set_value(value)
                    new_value = self._move(value)
            else:
                self._set_value(value)
                self.__move_task = gevent.spawn(self._move, value)
        else:
            raise ValueError("Invalid value %s; limits are %s"
                             % (value, self.get_limits())
                             )

    def abort(self):
        """Imediately halt movement. By default self.stop = self.abort"""
        if self.__move_task is not None:
            self.__move_task.kill()
        self.update_state(self.STATES.READY)
        
    def _set_value(self, value):
        """ Override AbstractActuator method."""
        self.set_channel_value(self.ACTUATOR_VAL, value)
