"""
TangoDiffractometerMD2 (MD2)
"""
import os
import time
import logging
import math
import PyTango
import traceback

import gevent

from mxcubecore import trace_call_log
try:
  import lucid3 as lucid
except ImportError:
  logging.warning("Could not find autocentring library, automatic centring is disabled")
from mxcubecore.HardwareObjects.GenericDiffractometer import GenericDiffractometer
from mxcubecore import HardwareRepository as HWR

# to test the flat face

#import elettra_ispyb_utils as ispybutils
#from mxcube3.routes import signals, qutils

# ND: numero di step in cui dividere la progressBar nella GUI
progressStep = 20
# ND: ritardo in exptime per readout ...?
detDelay = 0.0



class MD2TangoDiffractometer(GenericDiffractometer):

    map_to_mxcube_sw_state = {
        PyTango.DevState.ON: GenericDiffractometer.STATES.READY,
        PyTango.DevState.STANDBY: GenericDiffractometer.STATES.READY,
        PyTango.DevState.MOVING: GenericDiffractometer.STATES.BUSY,
        PyTango.DevState.RUNNING: GenericDiffractometer.STATES.BUSY,
        PyTango.DevState.UNKNOWN: GenericDiffractometer.STATES.UNKNOWN,
        PyTango.DevState.OFF: GenericDiffractometer.STATES.OFF,
        PyTango.DevState.FAULT: GenericDiffractometer.STATES.FAULT,
        PyTango.DevState.ALARM: GenericDiffractometer.STATES.WARNING,
    }
    map_to_mxcube_hw_state = {
        "Fault": GenericDiffractometer.STATES.FAULT,
        "Ready": GenericDiffractometer.STATES.READY,
        "Running": GenericDiffractometer.STATES.BUSY,
        "Moving": GenericDiffractometer.STATES.BUSY,
        "Busy": GenericDiffractometer.STATES.BUSY,
        "Unknown": GenericDiffractometer.STATES.BUSY,
        "Offline": GenericDiffractometer.STATES.OFF,
    }

    # TODO Note:
    # Elimina tutto il codice commentato alla fine del porting
    # - ho commentato zoom, perchè mi aspetto non serva, il multicollect

    MOTOR_TO_EXPORTER_NAME = {"focus":"AlignmentX", "kappa":"Kappa",
                                  "kappa_phi":"Phi", "phi": "Omega",
                                  "phiy":"AlignmentY", "phiz":"AlignmentZ",
                                  "sampx":"CentringX", "sampy":"CentringY",
                                  "zoom":"Zoom"}

    CENTRING_METHOD_MANUAL = "Manual 3-click"

    def __init__(self, name):
        super().__init__(name)
        self.head_type = None
        self.aperture = None
        # self.zoom = None
        self.front_light = None
        self.back_light = None
        self.back_light_switch = None
        self.front_light_switch = None
        self.phix_focus = None
        self.centring_clicks = None
        self.ch_phase = None
        self.ch_hw_state = None
        self._hw_state = None
        self._sw_state = None


    def init(self):
        super().init()

        self.aperture = self.get_object_by_role('aperture')
        self.front_light = self.get_object_by_role('frontlight')
        self.back_light = self.get_object_by_role('backlight')
        self.back_light_switch = self.get_object_by_role('backlightswitch')
        self.front_light_switch = self.get_object_by_role('frontlightswitch')
        self.phix_focus = float(self.get_property("phix_focus"))
        # self.zoom = self.motor_hwobj_dict.get("zoom")
        self.ch_phase = self.channel_dict["CurrentPhase"]
        self.ch_hw_state = self.channel_dict["HardwareState"]
        self.ch_sw_state = self.channel_dict["State"]

        self.centring_clicks = HWR.beamline.click_centring_num_clicks

        # SIGNALS CONNECTIONS
        # self.connect(self.zoom, "valueChanged", lambda _: self.update_zoom_calibration())
        self.connect(self.alignment_y, "update", self.update_phiy)
        self.connect(self.alignment_z, "update", self.update_phiz)
        self.connect(self.omega, "update", self.update_phi)
        self.connect(self.kappa_phi, "update", self.update_kappa_phi)
        self.connect(self.kappa, "update", self.update_kappa)
        self.connect(self.centring_x, "update", self.update_sampx)
        self.connect(self.centring_y, "update", self.update_sampy)
        self.connect(self.ch_hw_state, "update", self.hw_state_changed)

        # to make it comaptible
        self.camera = self.camera_hwobj

        self.update_phix()
        self.update_phiy()
        self.update_phiz()
        self.update_phi()
        self.update_kappa_phi()
        self.update_kappa()
        self.update_sampx()
        self.update_sampy()
        self.update_zoom()

    @trace_call_log
    def get_status(self):
        return self.channel_dict['Status'].get_value()

    @trace_call_log
    def get_current_phase(self):
        return self.channel_dict["CurrentPhase"].get_value()

    @trace_call_log
    def current_phase_changed(self, value=None):
        if value is None:
            value = self.get_current_phase()
        self.current_phase = value
        self.emit("phaseChanged", value)

    @trace_call_log
    def get_hw_state(self):
        hw_state = self.map_to_mxcube_hw_state.get(
            self.ch_hw_state.get_value(), GenericDiffractometer.STATES.UNKNOWN)
        return hw_state

    @trace_call_log
    def hw_state_changed(self, state):
        self._hw_state = self.map_to_mxcube_hw_state.get(
            state, GenericDiffractometer.STATES.UNKNOWN)
        if self._hw_state == self._sw_state:
            self.update_state(self._hw_state)

    @trace_call_log
    def get_sw_state(self):
        sw_state = self.map_to_mxcube_sw_state.get(
            self.ch_sw_state.get_value(), GenericDiffractometer.STATES.UNKNOWN)
        return sw_state

    @trace_call_log
    def state_changed(self, state):
        self._sw_state = self.map_to_mxcube_sw_state.get(
            state, GenericDiffractometer.STATES.UNKNOWN)
        if self._hw_state == self._sw_state:
            self.update_state(self._sw_state)

    @trace_call_log
    def update_phix(self, pos=None):
        if not pos:
            pos = self.phix.get_value()
        self.current_motor_positions["phix"] = pos

    @trace_call_log
    def update_phiy(self, pos=None):
        if not pos:
            pos = self.phiy.get_value()
        self.current_motor_positions["phiy"] = pos

    @trace_call_log
    def update_phiz(self, pos=None):
        if not pos:
            pos = self.phiz.get_value()
        self.current_motor_positions["phiz"] = pos

    @trace_call_log
    def update_sampx(self, pos=None):
        if not pos:
            pos = self.sampx.get_value()
        self.current_motor_positions["sampx"] = pos

    @trace_call_log
    def update_sampy(self, pos=None):
        if not pos:
            pos = self.sampy.get_value()
        self.current_motor_positions["sampy"] = pos

    # @trace_call_log
    # def update_zoom(self, pos=None):
    #     if not pos:
    #         pos = self.zoom.get_value()
    #     if pos == self.zoom.VALUES.UNKNOWN:
    #         pos = None
    #     else:
    #         pos = pos.value
    #     self.current_motor_positions["zoom"] = pos

    @trace_call_log
    def update_phi(self, pos=None):
        if not pos:
            pos = self.phi.get_value()
        self.current_motor_positions["phi"] = pos

    @trace_call_log
    def update_kappa_phi(self, pos=None):
        if not pos:
            pos = self.kappa_phi.get_value()
        self.current_motor_positions["kappa_phi"] = pos

    @trace_call_log
    def update_kappa(self, pos=None):
        if not pos:
            pos = self.kappa.get_value()
        self.current_motor_positions["kappa"] = pos

    @trace_call_log
    def motor_positions_to_screen(self, centred_positions_dict):
        xc_pix = self.channel_dict["BeamPositionHorizontal"].get_value()
        yc_pix = self.channel_dict["BeamPositionVertical"].get_value()
        pixels_per_mm_x, pixels_per_mm_y = self.get_pixels_per_mm()
        actual_positions = self.get_positions()
        xc_mm = actual_positions['phiy']
        yc_mm = actual_positions['phiz']
        x_mm = centred_positions_dict['phiy']
        y_mm = centred_positions_dict['phiz']
        x_pix = xc_pix + pixels_per_mm_x * (x_mm - xc_mm) * -1.
        y_pix = yc_pix + pixels_per_mm_y * (y_mm - yc_mm)

        return round(x_pix), round(y_pix)

        '''

        ???

        try:
            c = centred_positions_dict

            if self.head_type == GenericDiffractometer.HEAD_TYPE_MINIKAPPA:
                kappa = self.motor_hwobj_dict["kappa"]
                phi = self.motor_hwobj_dict["kappa_phi"]

    #        if (c['kappa'], c['kappa_phi']) != (kappa, phi) \
    #         and self.minikappa_correction_hwobj is not None:
    #            c['sampx'], c['sampy'], c['phiy'] = self.minikappa_correction_hwobj.shift(
    #            c['kappa'], c['kappa_phi'], [c['sampx'], c['sampy'], c['phiy']], kappa, phi)
            xy = self.centring_hwobj.centringToScreen(c)
            # x = (xy['X'] + c['beam_x']) * self.pixels_per_mm_x + \
            x = xy['X'] * self.pixels_per_mm_x + \
                self.zoom_centre['x']
            # y = (xy['Y'] + c['beam_y']) * self.pixels_per_mm_y + \
            y = xy['Y'] * self.pixels_per_mm_y + \
                self.zoom_centre['y']
            return x, y
        except:
            self.log.exception("TangoDiffractometerMD2 got an exception: %s" % traceback.format_exc())
        '''


    def osc_scan_4d(self, start, end, exptime, nframes, motors_pos, wait=False):
        global progressStep, detDelay
        if self.in_plate_mode():
            scan_speed = math.fabs(end-start) / exptime
            # todo, JN, get scan_speed limit
            """
            low_lim, hi_lim = map(float, self.scanLimits(scan_speed))
            if start < low_lim:
                raise ValueError("Scan start below the allowed value %f" % low_lim)
            elif end > hi_lim:
                raise ValueError("Scan end abobe the allowed value %f" % hi_lim)
            """

        self.channel_dict["ScanNumberOfFrames"].set_value(nframes)
        if self.get_current_phase() != "DataCollection":
            self.wait_device_ready(200)
            self.set_phase("DataCollection", wait=True)

        self.wait_device_ready(10)
        scan_params = []
        scan_params.append("%0.3f"% start)
        scan_params.append("%0.3f"% (end-start))
        scan_params.append("%0.3f"% exptime)
        scan_params.append("%0.3f" % motors_pos['1']['phiy'])
        scan_params.append("%0.3f" % motors_pos['1']['phiz'])
        scan_params.append("%0.3f" % motors_pos['1']['sampx'])
        scan_params.append("%0.3f" % motors_pos['1']['sampy'])
        scan_params.append("%0.3f" % motors_pos['2']['phiy'])
        scan_params.append("%0.3f" % motors_pos['2']['phiz'])
        scan_params.append("%0.3f" % motors_pos['2']['sampx'])
        scan_params.append("%0.3f" % motors_pos['2']['sampy'])
        task_id = self.command_dict["startScan4DEx"](scan_params)
        self.log.info(
            f"MD2 diffractometer: 4D scan started with this param: {scan_params}."
        )
        if wait:
            for i in range(0, progressStep):
                signals.collect_image_taken(float(nframes)*i/progressStep)
                time.sleep((exptime+detDelay*nframes)/progressStep)
            self.wait_device_ready(3600, task_id) #timeout of 1 hour
            self.log.info("MD2 diffractometer: 4D scan finished.")


    def osc_scan(self, start, end, exptime, nframes, wait=False):
        global progressStep, detDelay
        if self.in_plate_mode():
            scan_speed = math.fabs(end-start) / exptime
            # todo, JN, get scan_speed limit
            """
            low_lim, hi_lim = map(float, self.scanLimits(scan_speed))
            if start < low_lim:
                raise ValueError("Scan start below the allowed value %f" % low_lim)
            elif end > hi_lim:
                raise ValueError("Scan end abobe the allowed value %f" % hi_lim)
            """

        self.tangoproxy.ScanNumberOfFrames = nframes
        if str(self.get_current_phase()) != "DataCollection":
            self.wait_device_ready(200)
            self.set_phase("DataCollection",wait=True)
        numberPasses = 1
        self.wait_device_ready(10)
        task_id = self.tangoproxy.startScanEx(['1', str(start),str((end-start)),str(exptime),str(numberPasses)])
        self.log.info("MD2 diffractometer: oscillation scan started.")
        if wait:
            for i in range(0, progressStep):
                signals.collect_image_taken(float(nframes)*i/progressStep)
                time.sleep((exptime+detDelay*nframes)/progressStep)
            self.wait_device_ready(3600, task_id) #timeout of 1 hour
            self.log.info("MD2 diffractometer: oscillation scan finished.")

    def mesh_scan(self, start, end, exptime, grid_dict, wait=False, already_centered=False):
        global progressStep, detDelay
        print "THE GRID:\t", grid_dict
        mesh_cols = grid_dict['num_cols']
        mesh_rows = grid_dict['num_rows']
	#
	# 23/02/2022 RB: in a theoretical world the mesh width is calculated like this:
        md2_mesh_width = (mesh_cols-1) * (grid_dict['cell_h_space'] + grid_dict['cell_width']) * 0.001
        md2_mesh_height = ((mesh_rows -1)* (grid_dict['cell_v_space'] + grid_dict['cell_height']) * 0.001) + 0.001
        if not already_centered:
            real_mesh_width = mesh_cols * (grid_dict['cell_h_space'] + grid_dict['cell_width']) * 0.001
            real_mesh_height = mesh_rows * (grid_dict['cell_v_space'] + grid_dict['cell_height']) * 0.001
            mesh_delta_x = -1 * (grid_dict['x1'] + real_mesh_width/2.)
            mesh_delta_y = grid_dict['y1'] + real_mesh_height/2.
            #mesh_original_x = grid_dict['motor_positions']['phiy']
            #mesh_original_y = grid_dict['motor_positions']['phiz']
            actual_positions = self.get_positions()
            mesh_original_x = actual_positions['phiy']
            mesh_original_y = actual_positions['phiz']
            # Adjust the sample position
            logging.getLogger('HWR').info("MESH SCAN: Moving (phiy,phiz) by (%g,%g)" % (mesh_delta_x,mesh_delta_y))
            logging.getLogger('HWR').info("MESH SCAN: Moving (phiy,phiz) to (%g,%g)" % (mesh_original_x + mesh_delta_x,mesh_original_y + mesh_delta_y))
            self.phiy.syncMove(mesh_original_x + mesh_delta_x)
            time.sleep(1)
            self.wait_device_ready(30)
            #
            #  AH here too change to center with centeringTableVertical instead
            #
            #self.phiz.syncMove(mesh_original_y + mesh_delta_y)
            CTVP = ispybutils.getCentringTableVerticalPosition()
            print("starting CentringTableVerticalPosition value {}".format(CTVP))
            ispybutils.setCentringTableVerticalPosition(CTVP +( mesh_delta_y) )

            print('               ****       ')
            print('               ****       ')
            print("^^^^^^^^^^^^^^^  value of the CTVP after rasterRecentering the mesh to the beam {}".format( ispybutils.getCentringTableVerticalPosition()))
            time.sleep(1)
            self.wait_device_ready(30)
            print('               ****       ')
            print('               ****       ')
        print("before it starts the mesh scan ", self.get_positions())
        self.tangoproxy.saveCentringPositions()

        if str(self.get_current_phase()) != "DataCollection":
            self.wait_device_ready(200)
            self.set_phase("DataCollection",wait=True)
        self.wait_device_ready(10)

        self.tangoproxy.ScanStartAngle = start
        self.tangoproxy.ScanExposureTime = exptime / mesh_rows
        self.tangoproxy.ScanNumberOfFrames = (mesh_cols * mesh_rows)
# AH March 1 divide instead of multiply by cols
        #self.tangoproxy.ScanRange = (end-start) *  mesh_cols
        self.tangoproxy.ScanRange = (end-start)
        #self.tangoproxy.ScanSpeed = ?
        logging.getLogger('HWR').info("MESH SCAN params: (%g,%g,%d,%d)" % (md2_mesh_width,md2_mesh_height,mesh_rows,mesh_cols))
        task_id = self.tangoproxy.startRasterScan([str(md2_mesh_width),str(md2_mesh_height),str(mesh_rows),str(mesh_cols),"1","0","0"])
        self.log.info("MD2 diffractometer: raster scan started.")
        if wait:
            for i in range(0, progressStep):
                time.sleep(1.0*(exptime+detDelay*nframes)/progressStep)
                signals.collect_image_taken(float(mesh_cols)*mesh_rows*i/progressStep)
            self.wait_device_ready(3600, task_id) #timeout of 1 hour
            self.log.info("MD2 diffractometer: raster scan finished.")


    def set_phase(self, phase, wait=False, timeout=60, wait_for_readiness=60):
        if self.get_current_phase() == phase:
            self.log.debug(f'{self.username}: already in phase "{phase}".' )
            return
        self.wait_device_ready(200)
        if self.is_ready():
            self.log.info("MD2 diffractometer: setting phase %s." % str(phase))
            task_id = self.tangoproxy.startSetPhase(phase)
            if wait:
                self.wait_device_ready(timeout, task_id)
            self.log.info("MD2 diffractometer: phase %s set." % str(phase))
        else:
            self.log.exception("MD2 diffractometer not ready, unable to set phase.")


    def move_motors(self, motor_positions, timeout=15):
        """
        Moves diffractometer motors to the requested positions

        :param motors_dict: dictionary with motor names or hwobj
                            and target values.
        :type motors_dict: dict
        """
        print "TangoDiffractometerMD2.move_motors",motor_positions
        for motor in motor_positions.keys():
            position = motor_positions[motor]
            if type(motor) in (str, unicode):
                motor_role = motor
                motor = self.motor_hwobj_dict[motor_role]
                del motor_positions[motor_role]
                if motor is None:
                    continue
                motor_positions[motor] = position
            try:
                motor.move(position)
            except:
                self.log.error("MD2 diffractometer: unable to move motor %s to pos %g." % (motor,position))
        self.wait_device_ready(timeout)


    def move_sync_motors(self, motors_dict, wait=True, timeout=30, mk_exclude=['kappa', 'kappa_phi', 'phi']):
        argin = ""
        self.log.debug("ND: in move_sync_motors, wait: %s, motors: %s, time: %s " %(wait, motors_dict, time.time()))
        for motor in motors_dict.keys():
            if motor not in mk_exclude:
                position = motors_dict[motor]
                if position is None:
                    continue
                name = self.MOTOR_TO_EXPORTER_NAME[motor]
                argin += "%s=%0.3f," % (name, position)
        if not argin:
            return
        self.wait_device_ready(2000)
        # ND: Is exporter needed in xml?!
        #self.command_dict["startSimultaneousMoveMotors"](argin)
        print('--> ND: executing md2.startSimultaneousMoveMotorsEx(\"{}\")'.format(argin))
        task_id = self.tangoproxy.startSimultaneousMoveMotors(argin)
        if wait:
            self.wait_device_ready(timeout, task_id)

    @trace_call_log
    def wait_device_ready(self, timeout=10, task_id = None):
        """ Waits when diffractometer status is ready:

        :param timeout: timeout in second
        """
        with gevent.Timeout(timeout, Exception(f"Timeout waiting for the {self.username} to be ready")):
            while not self.is_ready(task_id):
                gevent.sleep(0.5)

    wait_ready = wait_device_ready # Called by the component "beamline"

    def is_ready(self, task_id = None):
        """
        Detects if device is ready
        """
        try:
            #dfrmct_state =  self.channel_dict["State"].getValue()
            if task_id is not None:
                task_busy = self.command_dict["isTaskRunning"](task_id)
                if task_busy:
                    return False
            hw_state = self.ch_hw_state
            self.log.info('MD2 NNNNNNN Detects if device is ready:  "- %s -"' % str(dfrmct_state))
        except:
            self.log.exception("Error occurred retrieving the MD")
            return False
        return (dfrmct_state in ["READY"])

    def moveToBeam(self, x, y):
        try:
            xpos = self.tangoproxy.BeamPositionHorizontal
            ypos = self.tangoproxy.BeamPositionVertical
            self.beam_position = (xpos,ypos)
#AH
            beam_xc = self.beam_position[0]
            beam_yc = self.beam_position[1]
            pixels_per_mm_x, pixels_per_mm_y = self.get_pixels_per_mm()

           #self.centring_phiz.moveRelative((y-beam_yc)/float(self.pixelsPerMmZ))
            #self.centring_phiy.moveRelative(-1*(x-beam_yc)/float(self.pixelsPerMmY))
            self.phiz.moveRelative((y-beam_yc)/float(pixels_per_mm_y))
            self.phiy.moveRelative(-1*(x-beam_xc)/float(pixels_per_mm_x))

	except:
            self.log.exception("MD2: could not center to beam, aborting")

    def acceptMyCentring(self, method=CENTRING_METHOD_MANUAL, timeout=30):
        try:
            self.wait_device_ready(timeout)
            self.tangoproxy.saveCentringPositions()
            self.acceptCentring()
            self.accept_centring()

            self.shape_history_hwobj = self.get_object_by_role("shape_history")
            points = self.shape_history_hwobj.get_points()
            maxP = -1
            for point in points:
                if int(point.id[1:]) > maxP:
                    maxP = int(point.id[1:])
            return {"response": {'shape': "P{}".format(maxP)}, "status_code": 200}
        except:
            traceback.print_exc()
            return {"response": {}, "status_code": 409}

    def removeCurrentPoint(self):
        self.emit('centringRemoveCP')

    def acceptCentring(self, method=CENTRING_METHOD_MANUAL):
        self.current_state = DiffractometerState.tostring(\
            DiffractometerState.Moving)
        try:
            if not "motors" in self.centring_status:
                self.setMotorsPositions()
        finally:
            self.current_state = DiffractometerState.tostring(\
                DiffractometerState.Ready)
        self.accept_centring()
        self.emit('centringSuccessful', (method, self.centring_status))

    def setMotorsPositions(self):
        self.centring_status["motors"] = {}
        for motor in self.MOTOR_TO_EXPORTER_NAME.keys():
            self.centring_status["motors"][motor] = self.tangoproxy.getMotorPosition(self.MOTOR_TO_EXPORTER_NAME[motor])
            if math.isnan(self.centring_status["motors"][motor]):
                self.centring_status["motors"][motor] = 0

    def rejectCentring(self):
        self.reject_centring()

    def set_backlightON(self, setPwr=False):
        if self.tangoproxy.backlightison is False:
            self.back_light_switch.actuatorIn()
            if setPwr:
                self.tangoproxy.backlightlevel = 50

    def manual_centring(self):
        """
        """
        # ND: saving clicks to handle motor movements manually
        # clkNDict = [{'width': None, 'height': None},{'x':None,'y':None,'pxMm':None,'phi':None},]
        clkNDict = [{},]

        print("are we set for manual_centring")
        self.emit_progress_message("Doing 3-click centring")
        self.wait_device_ready(100)
        try:
            self.set_phase("Centring",wait=True)
            # Wait for ready
            t0 = time.time()
            while not self.is_ready():
                if ( time.time() - t0 ) > 10:
                    self.log.exception("MD2: not IDLE")
                    return None
                time.sleep(1)

# If backlight goes in timeout or is off let's check
            if os.path.isfile(os.path.expanduser('~/MX3DEBUG')) == False:
                self.set_backlightON(True)
# FronLight COMMENTED - Let user deal with FrontLight...
#            if self.tangoproxy.frontlightison is False:
#                self.front_light_switch.actuatorIn()
#                self.tangoproxy.frontlightlevel = 22
            # ND: bypassing MD2 function to move motors manually:
            # routes get the clicks in a 3clkNDict + get3clkNDict() rotate omega + calc3clkNDict() calc center
            # self.tangoproxy.startManualSampleCentring()
            for click in range(self.centring_clicks):
                if os.path.isfile(os.path.expanduser('~/MX3DEBUG')) == False:
                    self.set_backlightON(False)
                self.user_clicked_event = gevent.event.AsyncResult()
                x, y, pxMm, phi, width, height = self.user_clicked_event.get()
                self.current_state = DiffractometerState.tostring(\
                    DiffractometerState.Moving)

                try:
                    clkNDict.append({'x':x, 'y':y, 'pxMm':pxMm, 'phi':phi})
                    if  click == (self.centring_clicks - 1):
                        clkNDict[0]={'width':width, 'height':height}
                        self.calc3clkNDict(clkNDict)
                    else:
                        self.get3clkNDict(click+1, clkNDict)
                    #self.tangoproxy.setCentringClick([str(x),str(y)])
                    self.emit_progress_message("Waiting motion to complete")
                    while self.tangoproxy.getMotorState("Omega").upper() in ["RUNNING","MOVING"] or self.is_busy():
                        time.sleep(0.05)
                finally:
                    self.current_state = DiffractometerState.tostring(\
                        DiffractometerState.Ready)
            self.wait_device_ready(2000)
            self.tangoproxy.saveCentringPositions()
            centredPos= { "sampx": self.tangoproxy.getMotorPosition("CentringX"),
                          "sampy": self.tangoproxy.getMotorPosition("CentringY"),
                          "phiy": self.tangoproxy.getMotorPosition("AlignmentY")}
            curr_time = time.strftime("%Y-%m-%d %H:%M:%S")
            self.centring_status["endTime"] = curr_time
            self.centring_status["motors"] = {}
            self.centring_status["method"] = self.CENTRING_METHOD_MANUAL
            self.centring_status["valid"] = False
            method = self.CENTRING_METHOD_MANUAL
            self.acceptCentring(method)
        except Exception as e:
            self.cancelCentringMethod(True)
            traceback.print_exc()
            raise e
        return centredPos

    def is_busy(self):
        """
        Detects if device is Busy
        """
        return self.tangoproxy.hardwarestate == 'Busy'

    def is_moving(self):
        """
        Detects if device is Moving
        """
        return self.current_state == DiffractometerState.tostring(\
                    DiffractometerState.Moving) or self.is_busy()

    def please_cryoUD(self):
        """
        Description. : ND WIIIIIIIIIIIIIIP 20240502
        """
        try:
            logging.getLogger('HWR').info("MiniDiff please_cryoUD()")
            if not self.tangoproxy.cryoisback:
                self.tangoproxy.Abort()
        except:
            logging.getLogger('HWR').info("MiniDiff please_cryoUD exception: %s" % traceback.format_exc())

    def please_abort(self):
        """
        Description. :
        """
        try:
            logging.getLogger('HWR').info("MiniDiff please_abort()")
            self.tangoproxy.Abort()
            qutils.qutils_stop()
        except:
            logging.getLogger('HWR').info("MiniDiff please_abort exception: %s" % traceback.format_exc())

    def get3clkNDict(self, CLICK_COUNT, clkNDict):
        # Just turn omega please and check that omega was increased  ...not decreased by 90 deg!!!
        retryPhi = 3
        startPhi = float(clkNDict[CLICK_COUNT]['phi'])
        endPhi = (startPhi + 90) % 360
        print('NNNNNNNNNNNNNNNNNNNNNNNNN GETclkNDict: {} {} {}'.format(CLICK_COUNT, startPhi, clkNDict))
        while retryPhi:
            self.tangoproxy.omegaposition = endPhi
            self.wait_device_ready(2000)
            retryPhi -= 1
            if self.tangoproxy.omegaposition == endPhi:
                break

    def calc3clkNDict(self, clkNDict):
        omegaTol = 10
        # Sanity check sui valori di Omega rispetto al click1
        chk31clk = abs(int(round(float(clkNDict[3]['phi']) - float(clkNDict[1]['phi']))))
        chk21clk = abs(int(round(float(clkNDict[2]['phi']) - float(clkNDict[1]['phi']))))
        if chk21clk == 270:
            chk21clk = chk21clk - 180
        if len(clkNDict) != (self.centring_clicks + 1) or\
           (chk31clk >= (180 + omegaTol)) or (chk31clk <= (180 - omegaTol)) or\
           (chk21clk >= (90 + omegaTol)) or (chk21clk <= (90 - omegaTol)):
            print('NNNN clkNDict PROBLEM!: chk31 {}, chk21 {}, length {}'.format(chk31clk, chk21clk, len(clkNDict)))
            self.user_log.error("3-Click centring ABORTED! Repeat it, please (Omega mismatch)")
            return

        # I'm implicitly assuming that the beam is in the middle of the camera !!!
        print('NNNNNNNNNNNNNNNNNNNNNNNNN clkNDict NNNNNNNNNNNNNNNNNNNNNNNNN')
        print('NNNNNNN --> Input: {}'.format(clkNDict))
        md2_motdict = {}
        for item in self.tangoproxy.motorpositions:
            key, value = item.split("=")
            md2_motdict[key] = value
        beamXY = {'x': int(clkNDict[0]['width'])/2, 'y': int(clkNDict[0]['height'])/2}
        print('NNNNNNN --> Starting motor values: {}'.format(md2_motdict))

        # Go to default focus value - hardcoded in md2: fixed by bzoom focal depth
        # and fixed for SC mount as default - l'ho definito in minidiff-md2.xml
        set_motdict = {"focus": self.phix_focus}

        # Let's adjust sample "length" based on average x1+2+3 --> phiY
        deltaX = ((float(clkNDict[3]['x']) - beamXY['x'])/float(clkNDict[3]['pxMm']) +
                 (float(clkNDict[2]['x']) - beamXY['x'])/float(clkNDict[2]['pxMm']) +
                 (float(clkNDict[1]['x']) - beamXY['x'])/float(clkNDict[1]['pxMm']))/3
        set_motdict["phiy"] = float(md2_motdict['AlignmentY']) - deltaX

        # Click y1 & y3 define rotation axis position relative to screen center
        # deltaCVP is distance, from CENTER - along height,  midway between y1 and y3 - i.e. offset
        # to correct phiZ to rotate on the beam (assuming it correspond to image center)
        # deltaCVP then redefined to "y3 - ymean"
        # y2 ("+ 90 deg") is necessary to correct depth offset = deltaCFS: move on deltaCVP, assuming no wobbling on spindle
        deltaCVP = ((float(clkNDict[3]['y']) - beamXY['y'])/float(clkNDict[3]['pxMm']) +
                   (float(clkNDict[1]['y']) - beamXY['y'])/float(clkNDict[1]['pxMm']))/2
        deltaCFS = (float(clkNDict[2]['y']) - beamXY['y'])/float(clkNDict[2]['pxMm']) - deltaCVP
        set_motdict["phiz"] = float(md2_motdict['AlignmentZ']) + deltaCVP
        deltaCVP = (float(clkNDict[3]['y']) - beamXY['y'])/float(clkNDict[3]['pxMm']) - deltaCVP

        vector = math.sqrt(pow(deltaCVP,2) + pow(deltaCFS,2))
        omega = float(clkNDict[1]['phi'])*math.pi/180
        alpha = math.atan2(deltaCVP, deltaCFS)
        eta = math.pi/2 - omega - alpha

        set_motdict["sampx"] = float(md2_motdict['CentringX']) - vector*math.sin(eta)
        set_motdict["sampy"] = float(md2_motdict['CentringY']) - vector*math.cos(eta)

        # Set motors positions finally
        print('NNNNNNN --> Final motor values:  {}'.format(set_motdict))
        print('NNNNNNNNNNNNNNNNNNNNNNNNN clkNDict: om {}, eta {}, al {}, dCVP {}, dCFS {}'.format(omega, eta, alpha, deltaCVP, deltaCFS))
        self.move_sync_motors(set_motdict)


    def automatic_centring(self):
        """
        """
        self.emit_progress_message("Doing automatic centring kkkkk")
        self.log.info("MD2: changing phase")
        self.wait_device_ready(100)
        self.set_phase("Centring",wait=True)
        self.front_light_switch.actuatorIn()
        self.log.info("MD2: automatic centring started")
        '''
        # Autocentring MD2 options
        # CRYSTAL_CENTRING, LOOP_CENTRING_ONLY, NEEDLE_CENTRING_ONLY
        self.tangoproxy.startAutoSampleCentring("LOOP_CENTRING_ONLY")
        self.wait_device_ready(100)
        #self.tangoproxy.saveCentringPositions()

        centredPos= { "sampx": self.tangoproxy.getMotorPosition("CentringX"),
                      "sampy": self.tangoproxy.getMotorPosition("CentringY"),
                      "phiy": self.tangoproxy.getMotorPosition("AlignmentY")}
        '''
        print('automatic_centring called')

        centredPos = self.automatic_centring_lucid_with_md2()
        if centredPos is None:
                self.log.exception("Unable to find loop, aborting")
                self.tangoproxy.Abort()
                self.cancelCentringMethod(True)
                self.user_log.info("Sorry can't find a clear loop, Your Turn to try to center!")
                raise RuntimeError("Could not centre sample automatically.")

# ND20230713 - Commenting this: go to "<phiz_beam_pos>-0.074</phiz_beam_pos>"
# ND           from "ELETTRA/HardwareObjectsMockup.xml/minidiff-md2.xml" ...why should we fix it?!?
# ND    try:
# ND        # Move Z to the beam
# ND        self.phiz.syncMove(self.phiz_beam_pos)
# ND    except:
# ND        self.log.exception("Unable to move Z motor to the beam")
# ND20230713 - End

        self.user_log.info("Done with automatic centring, Your Turn!")
        curr_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.centring_status["endTime"] = curr_time
        self.centring_status["motors"] = {}
        self.centring_status["method"] = self.CENTRING_METHOD_AUTO
        self.centring_status["valid"] = True
        method = self.CENTRING_METHOD_AUTO
        self.acceptCentring(method)

        self.emit('centring Done', (method, self.centring_status))
        return centredPos


    def automatic_centring_lucid_with_md2(self):
        """
        Use the lucid point to mimic a 3click
        """
        print("automatic_centring_lucid")
        # Wait for Omega still
        while self.tangoproxy.getMotorState("Omega").upper() in ["RUNNING","MOVING"]:
            time.sleep(0.05)


# ND20230713: Tweaking Z "phiz":"AlignmentZ" --> -0.25
#             and Samp-Y "sampy":"CentringY" --> 1.0
#             to bring the pin in the BZoom view
        try:
            # Move Z
            self.phiz.syncMove(-0.25)
        except:
            self.log.exception("Unable to move Z motor")
        try:
            # Move Samp-Y
            self.sampy.syncMove(1.0)
        except:
            self.log.exception("Unable to move Samp-Y motor")
# ND20230713 - End

        # Start looking for a loop
        checks = 0
        max_checks = 10
        while (checks < max_checks):
            # Grab a crystal snapshot
            image = self.get_sample_image()
            snapshot_filename = "crystal_snapshot.jpg"
            with open(snapshot_filename, 'w') as file:
                file.write(image)
            (info, x, y), a = lucid.find_loop(snapshot_filename,IterationClosing=6)
            checks += 1
            if (x < 0 or y < 0):
                # Wait for ready
                t0 = time.time()
                while not self.phiy.isReady():
                    if ( time.time() - t0 ) > 10:
                        self.log.exception("PHIY: not IDLE")
                        return None
                    time.sleep(0.1)
                self.phiy.syncMoveRelative(0.1)
                time.sleep(1)
            else:
                # Loop found
                break
        # Do not even start centring if no loop found
        if (x < 0 or y < 0):
            self.user_log.error("Did not find the loop... It's your Turn!")
            return None
        self.log.info("Loop found, let's start the automatic centring")
        x_ratio, y_ratio = self.get_pixels_per_mm()
        x_rel_step = ((self.camera.getWidth() / 2) - x) / x_ratio
        y_rel_step = ((self.camera.getHeight() / 2) - y) / y_ratio
        # Wait for ready
        t0 = time.time()
        while not self.phiy.isReady():
            if ( time.time() - t0 ) > 10:
                self.log.exception("PHIY: not IDLE")
                return None
            time.sleep(0.1)
        self.phiy.syncMoveRelative(x_rel_step)
        '''
        # Vertical correction disabled...
        time.sleep(0.5)
        self.phiz.syncMoveRelative(- y_rel_step)
        '''
        # Wait for ready
        t0 = time.time()
        while not self.is_ready():
            if ( time.time() - t0 ) > 10:
                self.log.exception("MD2: not IDLE")
                return None
            time.sleep(1)
        self.tangoproxy.startManualSampleCentring()
        time.sleep(1)
        # Simulate 3-click centring
        for click in range(3):
            # Grab a crystal snapshot
            image = self.get_sample_image()
            snapshot_filename = "crystal_snapshot.jpg"
            with open(snapshot_filename, 'w') as file:
                file.write(image)
            (info, x, y) , a = lucid.find_loop(snapshot_filename,IterationClosing=6)
            if (x < 0 or y < 0):
                return None
            self.tangoproxy.setCentringClick([str(x),str(y)])
            self.emit_progress_message("Waiting motion to complete")
            while self.tangoproxy.getMotorState("Omega").upper() not in ["RUNNING","MOVING"]:
                time.sleep(0.5)
            while self.tangoproxy.getMotorState("Omega").upper() in ["RUNNING","MOVING"]:
                time.sleep(0.5)

        self.tangoproxy.saveCentringPositions()

        centredPos= { "sampx": self.tangoproxy.getMotorPosition("CentringX"),
                      "sampy": self.tangoproxy.getMotorPosition("CentringY"),
                      "phiy": self.tangoproxy.getMotorPosition("AlignmentY")}
        # now find the flat face
        #self.flatface()

# ND20230713: skip to speed up mounting, 2 be moved to BL-Actions...
# ND     ispybutils.FlatFace2()
# ND     print("flatface done, it is always rotated by 180 degrees (AH ????)")
# ND20230713 - End

        return centredPos

    def flatface(self, number=20, angle=7):
        '''
        *** This takes twice as much time as just calling it from
        *** elettra.ispyb_utils, it is also more reliable

        using lucid3 to reorient the loop to the flat face looking at the beam.
        we find the min area first and then rotate 90 degrees
        we are not saving the images but we could
        the defaults parameter will cover 105 degrees so we should find the min area.

        :param number: int, default 15, number of image taken
        :param angle:  int, default :7 angle increment from one image to the other

        '''
        min_angle = self.phi.getPosition()
        start_angle = min_angle
        snapshot_filename = "crystal_snapshot.jpg"
        area = []
        for i in xrange (1, number+1):
            image = self.get_sample_image()
            with open(snapshot_filename, 'w') as file:
                file.write(image)
            results = lucid.find_loop(snapshot_filename,IterationClosing=6)
            area.append(results[1])
            #self.phi.syncMove(self.phi.getPosition() + int(angle))
            self.tangoproxy.omegaposition = self.tangoproxy.omegaposition + angle
            while self.tangoproxy.getMotorState("Omega").upper() in ["RUNNING","MOVING"]:
                time.sleep(0.1) # it takes about 0.3 sec for each move
                print('ZZzz')

        print(area)
        print (area.index(min(area)))
        delta = area.index(min(area)) *angle

        min_angle = start_angle + delta
        print(min_angle)
        #self.phi.syncMove(start_angle)
        #self.tangoproxy.omegaposition = start_angle
        #while self.tangoproxy.getMotorState("Omega") in [PyTango.DevState.RUNNING,PyTango.DevState.MOVING]:
        #    time.sleep(0.1) # it takes about 0.3 sec for each move
        #print('back to zero?', self.phi.getPosition())
        self.tangoproxy.omegaposition = (min_angle + 90.0)
        while self.tangoproxy.getMotorState("Omega").upper() in ["RUNNING","MOVING"]:
            time.sleep(0.1) # it takes about 0.3 sec for each movetime.sleep(1.0)
        print('centring Done, Omega = {}'.format(self.tangoproxy.omegaposition ))

    def cancelCentringMethod(self, reject=False):
        self.cancel_centring_method(reject)

    def cancel_centring_method(self, reject=False):
        """
        """
        self.log.exception("MD2: centring procedure aborted")
        self.emit_centring_failed()
        self.emit_progress_message("Unable to center loop..done")
        self.current_centring_method = None
        self.tangoproxy.Abort()
        if reject:
            self.rejectCentring()

    def get_centred_point_from_coord(self, x, y, return_by_names=None):
        """
        Descript. :
        """

        DX = (x - self.beam_position[0]) / self.pixels_per_mm_x
        DY = (y - self.beam_position[1]) / self.pixels_per_mm_y

        pos = { self.phiy: self.phiy.getPosition() - DX,
               self.phiz: self.phiz.getPosition() + DY}
        if return_by_names:
            pos = self.convert_from_obj_to_name(pos)
        print "=======>>>> Converted screen coords to motor pos ",pos
        return pos

    @trace_call_log
    def get_sample_image(self):
        image = self.tangoproxy.GetImageJPG(False)
        return image
