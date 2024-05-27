from mxcubecore.BaseHardwareObjects import HardwareObject

import os
import time
import gevent
import pprint
import logging
import requests
import binascii

# import threading
from mxcubecore.HardwareObjects.SecureXMLRpcRequestHandler import (
    SecureXMLRpcRequestHandler,
)
from mxcubecore import HardwareRepository as HWR

try:
    from httplib import HTTPConnection
except Exception:
    # Python3
    pass


class State(object):
    """
    Class for mimic the PyTango state object
    """

    def __init__(self, parent):
        self._value = "ON"
        self._parent = parent

    def get_value(self):
        return self._value

    def set_value(self, new_value):
        self._value = new_value
        self._parent.state_changed(new_value)

    def del_value(self):
        pass

    value = property(get_value, set_value, del_value, "Property for value")


class XRD1Workflow(HardwareObject):

    def __init__(self, name):

        HardwareObject.__init__(self, name)
        self._state = State(self)
        self.command_failed = False
        self.gevent_event = None
        self.chn_file_to_process = None

    def _init(self):
        pass

    def init(self):

        self.gevent_event = gevent.event.Event()
        self.chn_file_to_process = self.get_channel_object("file_to_process",
                                                           optional=False)
        self._state.value = "ON"

    def getState(self):
        return self._state

    def setState(self, new_state):
        self._state = new_state

    def delState(self):
        pass

    state = property(getState, setState, delState, "Property for state")

    def command_failure(self):
        return self.command_failed

    def set_command_failed(self, *args):
        self.log.error("Workflow '%s' Tango command failed!" % args[1])
        self.command_failed = True

    def state_changed(self, new_value):
        new_value = str(new_value)
        self.log.debug(
            "%s: state changed to %r", str(self.name()), new_value
        )
        self.emit("stateChanged", (new_value,))

    def workflow_end(self):
        """
        The workflow has finished, sets the state to 'ON'
        """
        # If necessary unblock dialog
        if not self.gevent_event.is_set():
            self.gevent_event.set()
        self.state.value = "ON"

    def open_dialog(self, dict_dialog):
        # If necessary unblock dialog
        if not self.gevent_event.is_set():
            self.gevent_event.set()
        self.params_dict = dict()
        if "reviewData" in dict_dialog and "inputMap" in dict_dialog:
            review_data = dict_dialog["reviewData"]
            for dict_entry in dict_dialog["inputMap"]:
                if "value" in dict_entry:
                    value = dict_entry["value"]
                else:
                    value = dict_entry["defaultValue"]
                self.params_dict[dict_entry["variableName"]] = str(value)
            self.emit("parametersNeeded", (review_data,))
            self.state.value = "OPEN"
            self.gevent_event.clear()
            while not self.gevent_event.is_set():
                self.gevent_event.wait()
                time.sleep(0.1)
        return self.params_dict

    def get_values_map(self):
        return self.params_dict

    def set_values_map(self, params):
        self.params_dict = params
        self.gevent_event.set()

    def get_available_workflows(self):
        workflow_list = list()
        no_wf = len(self["workflow"])
        for wf_i in range(no_wf):
            wf = self["workflow"][wf_i]
            dict_workflow = dict()
            dict_workflow["name"] = str(wf.title)
            dict_workflow["path"] = str(wf.path)
            try:
                req = [r.strip() for r in wf.get_property("requires").split(",")]
                dict_workflow["requires"] = req
            except (AttributeError, TypeError):
                dict_workflow["requires"] = []
            dict_workflow["doc"] = ""
            workflow_list.append(dict_workflow)
        return workflow_list

    def abort(self):

        self.log.info("Aborting current workflow")
        # If necessary unblock dialog
        if not self.gevent_event.is_set():
            self.gevent_event.set()
        self.command_failed = False
        self.state.value = "ON"

    def start(self, list_arguments):
        # If necessary unblock dialog
        if not self.gevent_event.is_set():
            self.gevent_event.set()
        self.state.value = "RUNNING"

        self.dict_parameters = {}
        index = 0
        if len(list_arguments) == 0:
            self.error_stream("ERROR! No input arguments!")
            return
        elif len(list_arguments) % 2 != 0:
            self.error_stream("ERROR! Odd number of input arguments!")
            return
        while index < len(list_arguments):
            self.dict_parameters[list_arguments[index]] = list_arguments[index + 1]
            index += 2
        logging.info("Input arguments:")
        logging.info(pprint.pformat(self.dict_parameters))

        if "modelpath" in self.dict_parameters:
            modelpath = self.dict_parameters["modelpath"]
            if "." in modelpath:
                modelpath = modelpath.split(".")[0]
            self.workflow_name = os.path.basename(modelpath)
        else:
            self.error_stream("ERROR! No modelpath in input arguments!")
            return

        time0 = time.time()
        self.start_workflow()
        time1 = time.time()
        logging.info("Time to start workflow: {0}".format(time1 - time0))

    def start_workflow(self):
        logging.info("Starting workflow {0}".format(self.workflow_name))

        self.dict_parameters["initiator"] = HWR.beamline.session.endstation_name
        self.dict_parameters["sessionId"] = HWR.beamline.session.session_id
        self.dict_parameters["externalRef"] = HWR.beamline.session.get_proposal()

        # TODO calling DataAnalysis here

