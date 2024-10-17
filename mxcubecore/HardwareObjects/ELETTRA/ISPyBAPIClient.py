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

import time
import typing
from urllib.parse import urljoin

from ispyb.sqlalchemy import DataCollection, DataCollectionGroup, Position, \
    MotorPosition, GridInfo, Proposal, BLSession, Person, SessionHasPerson, BLSample,\
    Protein, Crystal

ISPyB_sp_v3 = True
if ISPyB_sp_v3:
    import ispyb
    import ispyb.sqlalchemy
    from ispyb.connector.mysqlsp.main import ISPyBMySQLSPConnector
    from ispyb.sp.mxacquisition import MXAcquisition
else:
    import sys
    sys.path.append("/home/alessandro/devel/projects/mxcube/mxcube_local/ispyb-api")
    import ispyb

import mysql.connector
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy.orm
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker, joinedload
from sqlalchemy.orm import Session as DBSession
from sqlalchemy.sql.expression import func

from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore import HardwareRepository as HWR
import mxcubecore.model.lims_model as model
from mxcubecore import hwo_header_log

# TODO replace this import with the module installation!
'''
import sys
sys.path.append('/home/alessandro/devel/projects/mxcube/mxcube_local/ispyb-api')
'''


class ISPyBAPIClient(HardwareObject):
    """
    Web-service client for ISPyB.
    """

    def __init__(self, name):
        HardwareObject.__init__(self, name)
        self.__translations = {}
        self.__disabled = False
        self.loginType = None
        self.base_result_url = None
        self.beamline_name = None
        self.lims_rest = None
        self.ispyb_config = None
        self.ispyb_api_conn = None
        self.SqlAlchemySession = None

    @hwo_header_log
    def init(self):
        """
        Init method declared by HardwareObject.
        """

        self.lims_rest = self.get_object_by_role("lims_rest")
        self.loginType = self.get_property("loginType", "proposal")
        self.beamline_name = HWR.beamline.session.beamline_name
        self.ispyb_config = self.get_property("ispyb_config")
        try:
            self.base_result_url = self.get_property("base_result_url").strip()
        except AttributeError:
            self.log.warning("Base result url not set in the configuration file")

        self.SqlAlchemySession = sessionmaker(create_engine(ispyb.sqlalchemy.url(
            self.ispyb_config), connect_args={"use_pure": True}))
        try:
            self.ispyb_api_conn: ISPyBMySQLSPConnector = ispyb.open(self.ispyb_config)
            self.log.info(f"ISPyB configuration loaded: {self.ispyb_config}")
        except mysql.connector.errors.Error:
            self.log.exception(f"Failed to connect to ISPyB DB")
            return
        except ispyb.ISPyBException:
            self.log.exception("Failed to interface with ISPyB DB")
            return
        except Exception:
            self.log.exception(f"Unexpected error occurred")
            return

    @hwo_header_log
    def get_db_session(self):
        db_sess: DBSession = self.SqlAlchemySession()
        try:
            yield db_sess
        except SQLAlchemyError as e:
            self.log.exception("Error occurred querying ISPyB (rollback)")
            db_sess.rollback()
            raise e
        finally:
            db_sess.close()

    @hwo_header_log
    def get_login_type(self):

        self.loginType = self.get_property("loginType") or "proposal"
        return self.loginType

    @hwo_header_log
    def login(self, login_id, psd, ldap_connection=None, create_session=None):

        raise NotImplementedError

    @hwo_header_log
    def get_todays_session(self, prop_info, create_session=False):

        sessions = prop_info.get("Session", [])
        sessions = [] if sessions == [{}] else sessions

        # Check if there are sessions in the proposal
        today_sessions = None

        # Check for today's session
        for session in sessions:
            beamline = session["beamlineName"]
            start_date = "%s 00:00:00" % session["startDate"].split()[0]
            end_date = "%s 23:59:59" % session["endDate"].split()[0]
            try:
                start_struct = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                self.log.exception("Fail to create struct_time object from "
                                   "startDate")
            else:
                try:
                    end_struct = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    self.log.exception(
                        "Fail to create struct_time object from endDate")
                else:
                    start_time = time.mktime(start_struct)
                    end_time = time.mktime(end_struct)
                    current_time = time.time()
                    # Check beamline name
                    if beamline == self.beamline_name:
                        # Check date
                        if start_time <= current_time <= end_time:
                            today_sessions = session
                            break
        new_session_flag = False

        if today_sessions is None:
            self.log.debug("No session found for today")
            today_sessions = {}
            # todays_session = [prop_info["Session"][0]]

        else:  # todays_session found
            session_id = today_sessions["sessionId"]
            self.log.debug(f"Getting local contact for session with ID: "
                           f"{session_id}")
            localcontact = self.get_session_local_contact(session_id)

        is_inhouse = HWR.beamline.session.is_inhouse(
            prop_info["Proposal"]["code"], prop_info["Proposal"]["number"]
        )
        return {
            "session": today_sessions,
            "new_session_flag": new_session_flag,
            "is_inhouse": is_inhouse,
        }

    @hwo_header_log
    def echo(self):
        """ISPyBAPIClient for the echo method."""

        answer = False

        if not self.ispyb_api_conn:
            err_msg = f"Error in lims echo: Could not connect to ISPyB DB using " \
                      f"ispyb-api (config file: \"{self.ispyb_config}\")"
            self.log.warning(err_msg)
            raise Exception(err_msg)
        try:
            self.ispyb_api_conn.conn.ping()
            return True
        except mysql.connector.errors.Error:
            self.log.exception(f"Failed to ping to ISPyB DB")
        except Exception:
            self.log.exception(f"Unexpected error occurred, during ping")
        return answer

    @hwo_header_log
    def extract_protein_from_mx_collection(self, mx_collection: dict) -> dict:

        protein = {}
        if mx_collection.get('sample_reference'):
            protein['acronym'] = mx_collection['sample_reference']['protein_acronym']
        return protein

    @hwo_header_log
    def extract_sample_from_mx_collection(self, mx_collection: dict) -> dict:

        sample = {}
        if mx_collection.get('sample_reference'):
            sample['name'] = mx_collection['sample_reference']['sample_name']
            sample['location'] = mx_collection['sample_reference']['sample_location']
        return sample

    @hwo_header_log
    def extract_grid_info(self, grid_dict: dict, mx_collection: dict) -> dict:
        """

        :param grid_dict:
        :type grid_dict: dict
        :return: A dynamic dict whose keys correspond to the fields names of the ISPyB
                 table "gridinfo"
        :rtype: dict
        """

        grid_info = {}
        if mx_collection.get('collection_grp_id'):
            grid_info['dataCollectionGroupId'] = mx_collection['collection_grp_id']
        if grid_dict.get('cell_width'):
            # This is actually step size not full length
            grid_info['dx_mm'] = grid_dict['cell_width'] * 0.001
            # On XRD2 this alternative was commented => grid_dict['dx_mm'] / grid_dict['steps_x']
        if grid_dict.get('cell_height'):
            grid_info['dy_mm'] = grid_dict['cell_height'] * 0.001  # Same above
        if grid_dict.get('steps_x'):
            grid_info['steps_x'] = grid_dict['steps_x']  # Number of step in the raster
        if grid_dict.get('steps_y'):
            grid_info['steps_y'] = grid_dict['steps_y']
        if grid_dict.get('motor_positions'):
            grid_info['meshAngle'] = grid_dict['motor_positions']['phi']
        if grid_dict.get('pixels_per_mm'):
            grid_info['pixelsPerMicronX'] = 1000 / grid_dict['pixels_per_mm'][0]
        if grid_dict.get('pixels_per_mm'):
            grid_info['pixelsPerMicronY'] = 1000 / grid_dict['pixels_per_mm'][1]
        if grid_dict.get('screen_coord'):
            grid_info['snapshot_offsetXPixel'] = grid_dict['screen_coord'][0]
        if grid_dict.get('screen_coord'):
            grid_info['snapshot_offsetYPixel'] = grid_dict['screen_coord'][1]
        grid_info['orientation'] = 'horizontal'
        grid_info['snaked'] = True  # hardcoded for now

        return grid_info

    @hwo_header_log
    def extract_position_from_mx_collection(self, mx_collection: dict) -> dict:
        """
        Extract a dictionary which represents the ISPyB position record. The length of
         the dictionary depends on the number of data saved in the mx_collection

        :param mx_collection: The data collection parameters
        :type mx_collection: dict

        :returns: A dynamic dict whose keys correspond to the fields names of the ISPyB
                  table "position"
        :rtype: dict
        """
        position = {}
        if mx_collection.get('pos_id'):
            position['positionId'] = mx_collection['pos_id']
        if mx_collection.get('motors'):
            if mx_collection['motors'].get('phiy') is not None:
                position['posX'] = mx_collection['motors']['phiy']
            if mx_collection['motors'].get('phiz') is not None:
                position['posY'] = mx_collection['motors']['phiz']
            if mx_collection['motors'].get('focus') is not None:
                position['posZ'] = mx_collection['motors']['focus']
            if mx_collection['motors'].get('zoom') is not None:
                position['scale'] = mx_collection['motors']['zoom']
        return position

    @hwo_header_log
    def extract_motor_position_from_mx_collection(self, mx_collection: dict) -> dict:
        """
        Extract a dictionary which represents the ISPyB motor position record. The
        length of the dictionary depends on the number of data saved in the
        mx_collection

        :param mx_collection: The data collection parameters.
        :type mx_collection: dict

        :returns: A dynamic dict whose keys correspond to the fields names of the ISPyB
                  table "motorposition"
        :rtype: dict
        """
        motor_position = {}
        if mx_collection.get('motor_pos_id'):
            motor_position['motorPositionId'] = mx_collection['motor_pos_id']
        if mx_collection.get('oscillation_sequence'):
            motor_position['omega'] = mx_collection['oscillation_sequence'][0]['start']
        if mx_collection.get('motors'):
            # !!! Note !!! phy is focus on XRD2
            if mx_collection['motors'].get('phiy') is not None:
                motor_position['phiX'] = mx_collection['motors']['phiy']
            if mx_collection['motors'].get('phiy') is not None:
                motor_position['phiY'] = mx_collection['motors']['phiy']
            if mx_collection['motors'].get('phiz') is not None:
                motor_position['phiZ'] = mx_collection['motors']['phiz']
            if mx_collection['motors'].get('sampx') is not None:
                motor_position['sampX'] = mx_collection['motors']['sampx']
            if mx_collection['motors'].get('sampy') is not None:
                motor_position['sampY'] = mx_collection['motors']['sampy']
            if mx_collection['motors'].get('omega') is not None:
                motor_position['omega'] = mx_collection['motors']['omega']
            if mx_collection['motors'].get('kappa') is not None:
                motor_position['kappa'] = mx_collection['motors']['kappa']
            if mx_collection['motors'].get('chi') is not None:
                motor_position['chi'] = mx_collection['motors']['chi']
            if mx_collection['motors'].get('phi') is not None:
                motor_position['phi'] = mx_collection['motors']['phi']
            # !!! Note !!! gridIndexY and gridIndexZ set to 0 in XRD2

        return motor_position

    @hwo_header_log
    def extract_data_collection_group_from_mx_collection(self, mx_collection: dict):
        """
        Extract a dictionary which represents the ISPyB data collection group. The
        length of the dictionary depends on the number of data saved in the
        mx_collection

        :param mx_collection: The data collection parameters.
        :type mx_collection: dict

        :returns: A dynamic dict whose keys correspond to the fields names of the ISPyB
         table "datacollectiongroup"
        :rtype: dict
        """

        data_collection_group = {}
        if mx_collection.get('collection_grp_id'):
            data_collection_group['dataCollectionGroupId'] = \
                mx_collection['collection_grp_id']
        if mx_collection.get('sessionId'):
            data_collection_group['sessionId'] = mx_collection['sessionId']
        if mx_collection.get('sessionId'):
            data_collection_group['experimentType'] = mx_collection['experiment_type']
        return data_collection_group

    @hwo_header_log
    def extract_data_collection_from_mx_collection(self, mx_collection: dict) -> dict:
        """
        Extract a dictionary which represents the ISPyB data collection. The length of
         the dictionary depends on the number of data saved in the mx_collection

        :param mx_collection: The data collection parameters.
        :type mx_collection: dict

        :returns: A dynamic dict whose keys correspond to the fields names of the ISPyB
         table "datacollection"
        :rtype: dict
        """
        data_collection = {}

        if mx_collection.get('collection_grp_id'):
            data_collection['dataCollectionGroupId'] = \
                mx_collection['collection_grp_id']
        if mx_collection.get('pos_id'):
            data_collection['POSITIONID'] = mx_collection['pos_id']
        if mx_collection.get('motor_pos_id'):
            data_collection['startPositionId'] = mx_collection['motor_pos_id']
        if mx_collection.get('collection_id'):
            data_collection['dataCollectionId'] = mx_collection['collection_id']
        if mx_collection.get('sessionId'):
            data_collection['SESSIONID'] = mx_collection['sessionId']
        if mx_collection.get('sample_reference'):
            data_collection['BLSAMPLEID'] = \
                mx_collection['sample_reference']['blSampleId']
        if mx_collection.get('fileinfo'):
            if mx_collection['fileinfo'].get('directory'):
                data_collection['imageDirectory'] = \
                    mx_collection['fileinfo']['directory']
            if mx_collection['fileinfo'].get('prefix'):
                data_collection['imagePrefix'] = mx_collection['fileinfo']['prefix']
            if mx_collection['fileinfo'].get('suffix'):
                data_collection['imageSuffix'] = mx_collection['fileinfo']['suffix']
            if mx_collection['fileinfo'].get('run_number'):
                data_collection['dataCollectionNumber'] = \
                    mx_collection['fileinfo']['run_number']
        if mx_collection.get('wavelength'):
            data_collection['wavelength'] = mx_collection['wavelength']
        if mx_collection.get('collection_start_time'):
            data_collection['startTime'] = mx_collection['collection_start_time']
        if mx_collection.get('collection_end_time'):
            data_collection['endTime'] = mx_collection['collection_end_time']
        if mx_collection.get('status'):
            data_collection['runStatus'] = mx_collection['status']
        if mx_collection.get('oscillation_sequence'):
            if mx_collection.get('oscillation_sequence')[0].get('number_of_images'):
                data_collection['numberOfImages'] = \
                    mx_collection['oscillation_sequence'][0]['number_of_images']
            if mx_collection.get('oscillation_sequence')[0].get('exposure_time'):
                data_collection['exposureTime'] = \
                    mx_collection['oscillation_sequence'][0]['exposure_time']
            if mx_collection.get('oscillation_sequence')[0].get('start_image_number'):
                data_collection['startImageNumber'] = \
                    mx_collection['oscillation_sequence'][0]['start_image_number']
            if mx_collection.get('oscillation_sequence')[0].get('start'):
                data_collection['axisStart'] = \
                    mx_collection['oscillation_sequence'][0]['start']
            if mx_collection.get('oscillation_sequence')[0].get('range'):
                data_collection['axisRange'] = \
                    mx_collection['oscillation_sequence'][0]['range']
            if mx_collection.get('oscillation_sequence')[0].get('end'):
                data_collection['axisEnd'] = \
                    mx_collection['oscillation_sequence'][0]['end']
            if mx_collection.get('oscillation_sequence')[0].get('overlap'):
                data_collection['overlap'] = \
                    mx_collection['oscillation_sequence'][0]['overlap']
        if mx_collection.get('resolution'):
            data_collection['resolution'] = mx_collection['resolution']
        if mx_collection.get('detectorDistance'):
            data_collection['detectorDistance'] = mx_collection['detectorDistance']
        if mx_collection.get('transmission'):
            data_collection['transmission'] = mx_collection['transmission']
        if mx_collection.get('flux'):
            data_collection['flux'] = mx_collection['flux']
        if mx_collection.get('flux_end'):
            data_collection['flux_end'] = mx_collection['flux_end']
        if mx_collection.get('beamSizeAtSampleX'):
            data_collection['beamSizeAtSampleX'] = mx_collection['beamSizeAtSampleX']
        if mx_collection.get('beamSizeAtSampleY'):
            data_collection['beamSizeAtSampleY'] = mx_collection['beamSizeAtSampleY']
        if mx_collection.get('actualCenteringPosition'):
            data_collection['actualCenteringPosition'] = \
                mx_collection['actualCenteringPosition']
        if mx_collection.get('xBeam'):
            data_collection['xBeam'] = mx_collection['xBeam']
        if mx_collection.get('yBeam'):
            data_collection['yBeam'] = mx_collection['yBeam']
        if mx_collection.get('rotation_axis'):
            data_collection['rotationAxis'] = mx_collection['rotation_axis']

        return data_collection

    @hwo_header_log
    def get_proposal(self, proposal_code: str, proposal_number: int):
        """
        Returns a dict which represent a proposal, with other related information,
        identified by its code and number.
        The related information, retrieved from the ISPyB database, are: Sessions,
        Person (PI) and Laboratory

        The status of the database operations are returned in Status.

        :param proposal_code: The proposal code
        :type proposal_code: str
        :param proposal_number: The proposal number
        :type proposal_number: int

        :returns: The dict {Proposal, Person, Laboratory, Sessions, Status}.
        :rtype: dict
        """

        prop_info = {}
        if self.__disabled:
            return prop_info
        try:
            with self.SqlAlchemySession() as sql_session:
                db_proposal: Proposal = \
                    sql_session.query(Proposal)\
                        .filter(Proposal.proposalCode == proposal_code) \
                        .filter(Proposal.proposalNumber == proposal_number) \
                        .join(BLSession) \
                        .one()
                db_blsession_ls: list[BLSession] = \
                    sql_session.query(BLSession) \
                        .filter(BLSession.proposalId == db_proposal.proposalId)

                proposal = model.Proposal.from_ispyb_orm(db_proposal)
                person = model.Person.from_ispyb_orm(db_proposal.Person)
                lab = model.Laboratory.from_ispyb_orm(db_proposal.Person.Laboratory)
                sessions_ls = []
                for session in db_blsession_ls:
                    sessions_ls.append(model.Session.from_ispyb_orm(session))
                prop_info = model.ProposalInfo(status={"code": "ok"}, Person=person,
                                               Proposal=proposal, Session=sessions_ls,
                                               Laboratory=lab)
        except SQLAlchemyError as e:
            self.log.exception(f"Error occurred retrieving proposal information"
                               " by code and number")
            prop_info = model.ProposalInfo(status=model.Status(
                code="error", msg=str(e)))

        return prop_info.dict(exclude_unset=True)

    @hwo_header_log
    def get_session_by_id(self, session_id):
        with self.SqlAlchemySession() as sql_session:
            db_session: BLSession = sql_session \
                .query(BLSession) \
                .filter(BLSession.sessionId == session_id).one()
        return db_session

    @hwo_header_log
    def get_proposals_by_user(self, user_name):
        """
        Returns a list of all the proposals (and other information related to them)
        associated to a user.
        The related information, retrieved from the ISPyB database, are: Sessions,
        Person and Laboratory

        The status of the database operations are returned in Status.

        :param user_name: Username of the user (usually the e-mail)
        :type user_name: str
        :return: List of dict {Proposal, Person, Laboratory, Sessions, Status}.
        :rtype: list
        """

        try:
            with self.SqlAlchemySession() as sql_session:
                db_person: Person = sql_session \
                    .query(Person) \
                    .filter(Person.login == user_name).first()
                db_blsession_ls: list[BLSession] = \
                    sql_session.query(BLSession) \
                        .filter(BLSession.beamLineName == self.beamline_name) \
                        .join(SessionHasPerson) \
                        .join(Person) \
                        .filter(Person.personId == db_person.personId) \
                        .all()
                props_info_ls = []

                # Loop over all user sessions
                for db_session in db_blsession_ls:
                    session = model.Session.from_ispyb_orm(db_session)
                    person = model.Person.from_ispyb_orm(db_person)
                    proposal = model.Proposal.from_ispyb_orm(db_session.Proposal)
                    laboratory = model.Laboratory.from_ispyb_orm(db_person.Laboratory)

                    # The current session will be added to a new or an existing
                    # ProposalInfo
                    for prop_info in props_info_ls:
                        if prop_info.Proposal.proposalId == proposal.proposalId:
                            prop_info.Session.append(session)
                            break
                    else:
                        props_info_ls.append(
                            model.ProposalInfo(status={"code": "ok"}, Person=person,
                                               Proposal=proposal, Session=[session],
                                               Laboratory=laboratory))
            props_info_ls = [prop_info.dict(exclude_unset=True)
                             for prop_info in props_info_ls]
        except SQLAlchemyError as e:
            self.log.exception("Error occurred retrieving proposal information"
                               " by code and number")
            props_info_ls = [model.ProposalInfo(status=model.Status(
                code="error", msg=str(e)))]
        return props_info_ls

    @hwo_header_log
    def get_protein(self, proposal_id: int, acronym: str,
                    sql_session: DBSession = None):
        """
        Retrieve Protein object from ISPyB database

        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None
        :param proposal_id: Proposal associated to the protein
        :type proposal_id: int
        :param acronym: Protein acronym
        :type acronym: str
        :return: Protein found or None
        :rtype: ispyb.sqlalchemy.Protein | None
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        # It should be just one even if there is no unique constraint in ISPyB db
        db_protein: Protein = \
            sql_session.query(Protein) \
                .filter(Protein.proposalId == proposal_id) \
                .filter(Protein.acronym == acronym) \
                .first()
        return db_protein

    @hwo_header_log
    def get_last_dc_run_number(self, image_prefix, sql_session: DBSession = None):
        session_id = int(HWR.beamline.session.session_id)
        img_prefix_regex = f"^(ref-)?{image_prefix}(_wedge-.+)?$"
        if not sql_session:
            sql_session: DBSession = next(self.get_db_session())
        last_run_number = \
            sql_session.query(func.max(DataCollection.dataCollectionNumber)) \
                .filter(DataCollection.SESSIONID == session_id) \
                .filter(DataCollection.imagePrefix.regexp_match(img_prefix_regex)) \
                .scalar()
        return last_run_number if last_run_number else 0

    @hwo_header_log
    def get_samples(self, proposal_id, session_id):
        samples_list = []
        try:
            with self.SqlAlchemySession() as sql_session:
                sql_session: DBSession
                db_samples: typing.List[BLSample] = \
                    sql_session.query(BLSample) \
                        .join(Crystal) \
                        .join(Protein) \
                        .filter(Protein.proposalId == proposal_id) \
                        .all()
                for db_sample in db_samples:
                    db_dcg: DataCollectionGroup = \
                        sql_session.query(DataCollectionGroup) \
                            .join(BLSample) \
                            .filter(BLSample.blSampleId == db_sample.blSampleId) \
                            .first()
                    sample_info = {
                        'cellA': db_sample.Crystal.cell_a,
                        'cellAlpha': db_sample.Crystal.cell_alpha,
                        'cellB': db_sample.Crystal.cell_b,
                        'cellBeta': db_sample.Crystal.cell_beta,
                        'cellC': db_sample.Crystal.cell_c,
                        'cellGamma': db_sample.Crystal.cell_gamma,
                        'containerSampleChangerLocation':
                            db_sample.Container.sampleChangerLocation
                            if db_sample.Container else '0',  # 0 = Manual mount
                        'crystalSpaceGroup': db_sample.Crystal.spaceGroup,
                        'proteinAcronym': db_sample.Crystal.Protein.acronym,
                        'sampleId': db_sample.blSampleId,
                        'sampleLocation': db_sample.location,
                        'sampleName': db_sample.name,
                        'smiles': db_sample.SMILES,
                        'code': db_sample.code
                    }
                    if db_dcg:
                        sample_info['experimentType'] = db_dcg.experimentType
                    if db_sample.DiffractionPlan:
                        sample_info['diffractionPlan'] = {
                            "diffractionPlanId":
                                db_sample.DiffractionPlan.diffractionPlanId,
                            "experimentKind":
                                db_sample.DiffractionPlan.experimentKind,
                            "numberOfPositions":
                                db_sample.DiffractionPlan.numberOfPositions,
                            "observedResolution":
                                db_sample.DiffractionPlan.observedResolution,
                            "preferredBeamDiameter":
                                db_sample.DiffractionPlan.preferredBeamDiameter,
                            "radiationSensitivity":
                                db_sample.DiffractionPlan.radiationSensitivity,
                            "requiredCompleteness":
                                db_sample.DiffractionPlan.requiredCompleteness,
                            "requiredMultiplicity":
                                db_sample.DiffractionPlan.requiredMultiplicity,
                            "requiredResolution":
                                db_sample.DiffractionPlan.requiredResolution,
                        }
                    samples_list.append(sample_info)
                return samples_list

        except mysql.connector.errors.Error:
            self.log.exception(f"Failed to connect to ISPyB DB getting the "
                               f"samples of the proposal with id: {proposal_id}")
            return
        except ispyb.ISPyBException:
            self.log.exception(f"Failed to interface with ISPyB DB getting the "
                               f"samples of the proposal with id: {proposal_id}")
            return
        except Exception:
            self.log.exception(f"Unexpected error occurred getting the samples"
                               f" of the proposal with id: {proposal_id} from "
                               f"ISPyB")
            return

    def is_manual_sample(self, sample_info):
        return sample_info['lims_location'] == '0:00'

    @hwo_header_log
    def get_session_samples(self, session_id, sample_name=None, acronym=None,
                            sql_session: DBSession = None):

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_query = sql_session.query(BLSample) \
            .join(DataCollection,
                  BLSample.blSampleId == DataCollection.BLSAMPLEID) \
            .filter(DataCollection.SESSIONID == session_id)
        if sample_name is not None:
            db_query = db_query.filter(BLSample.name == sample_name)
        if acronym is not None:
            db_query = db_query.join(Crystal) \
                .join(Protein) \
                .filter(Protein.acronym == acronym)
        db_samples: list[BLSample] = db_query.all()
        return db_samples

    @hwo_header_log
    def get_session_local_contact(self, session_id):
        """
        Response example
        {
            "personId": 1,
            "laboratoryId": 1,
            "login": None,
            "familyName": "operator on ID14eh1",
        }
        """

        pass

    @hwo_header_log
    def _insert_protein(self, protein_dict: dict, sql_session: DBSession = None):
        """

        :param protein_dict:
        :type protein_dict:
        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None
        :return: Protein just inserted
        :rtype: ispyb.sqlalchemy.Protein
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_protein = Protein(**protein_dict)
        sql_session.add(db_protein)
        sql_session.commit()
        sql_session.refresh(db_protein)
        self.log.info(f"Record inserted in the ISPyB \"protein\" table."
                      f"\nRECORD:\n{db_protein.__dict__}")
        return db_protein

    @hwo_header_log
    def _insert_crystal(self, crystal_dict: dict, sql_session: DBSession = None):
        """

        :param crystal_dict:
        :type crystal_dict: dict
        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None
        :return: Crystal just inserted
        :rtype: ispyb.sqlalchemy.Crystal
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_crystal = Crystal(**crystal_dict)
        sql_session.add(db_crystal)
        sql_session.commit()
        sql_session.refresh(db_crystal)
        self.log.info(f"Record inserted in the ISPyB \"crystal\" table."
                      f"\nRECORD:\n{db_crystal.__dict__}")
        return db_crystal

    @hwo_header_log
    def _insert_sample(self, sample_dict: dict, sql_session: DBSession = None):
        """
        Insert a record in the ISPyB "sample" table

        :param sample_dict:
        :type sample_dict:
        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None
        :return: BLSample just inserted
        :rtype: ispyb.sqlalchemy.BLSample
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_samp = BLSample(**sample_dict)
        sql_session.add(db_samp)
        sql_session.commit()
        sql_session.refresh(db_samp)
        self.log.info(f"Record inserted in the ISPyB \"sample\" table."
                      f"\nRECORD:\n{db_samp.__dict__}")
        return db_samp

    @hwo_header_log
    def _insert_grid_info(self, grid_info_dict: dict, sql_session: DBSession = None):
        """
        Insert a record in the ISPyB "gridinfo" table.

        :param grid_info_dict:
        :type grid_info_dict: dict
        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None

        :returns: GridInfo
        :rtype: ispyb.sqlalchemy.GridInfo
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_grid_info = GridInfo(**grid_info_dict)
        sql_session.add(db_grid_info)
        sql_session.commit()
        sql_session.refresh(db_grid_info)
        self.log.info(f"Record inserted in the ISPyB \"gridinfo\" table\n"
                      f"RECORD:\n{db_grid_info.__dict__}")
        return db_grid_info

    @hwo_header_log
    def _insert_motor_positions(self, motor_pos_dict: dict,
                                sql_session: DBSession = None):
        """
        Insert a record in ISPyB "motorposition" table

        :param motor_pos_dict: A dict representing ISPyB MotorPosition object
        :type motor_pos_dict: dict
        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None
        :return: MotorPosition
        :rtype: ispyb.sqlalchemy.MotorPosition
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_motor_pos = MotorPosition(**motor_pos_dict)
        sql_session.add(db_motor_pos)
        sql_session.commit()
        sql_session.refresh(db_motor_pos)
        self.log.info(f"Record inserted in the ISPyB \"motorposition\" table.\n"
                      f"RECORD:\n{db_motor_pos.__dict__}")
        return db_motor_pos

    @hwo_header_log
    def _insert_position(self, pos_dict: dict, sql_session: DBSession = None):
        """
        Insert a record in ISPyB "position" table

        :param pos_dict: A dict representing ISPyB Position object
        :type pos_dict: dict
        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None
        :return:
        :rtype: Position
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_pos = Position(**pos_dict)
        sql_session.add(db_pos)
        sql_session.commit()
        sql_session.refresh(db_pos)
        self.log.info(f"Record inserted in the ISPyB \"position\" table."
                      f"\nRECORD:\n{db_pos.__dict__}")
        return db_pos

    @hwo_header_log
    def _insert_data_collection_group(self, dcg_dict, sql_session: DBSession = None):
        """
        Insert a record in ISPyB "datacollectiongroup" table.

        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None
        :param dcg_dict: A dict representing ISPyB DataCollectionGroup object
        :type dcg_dict: dict

        :returns: DataCollectionGroup
        :rtype: DataCollectionGroup
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_dcg = DataCollectionGroup(**dcg_dict)
        sql_session.add(db_dcg)
        sql_session.commit()
        sql_session.refresh(db_dcg)
        self.log.info(f"Record inserted in the ISPyB \"datacollectiongroup\" "
                      f"table.\nRECORD:\n{db_dcg.__dict__}")
        return db_dcg

    @hwo_header_log
    def _insert_data_collection(self, dc_dict: dict, sql_session: DBSession = None):
        """
        Insert a record in the ISPyB "datacollection" table

        :param dc_dict:
        :type dc_dict: dict
        :param sql_session: DB session
        :type sql_session: sqlalchemy.orm.Session | None

        :returns: DataCollection
        :rtype: ispyb.sqlalchemy.DataCollection
        """

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_data_collection = DataCollection(**dc_dict)
        sql_session.add(db_data_collection)
        sql_session.commit()
        sql_session.refresh(db_data_collection)
        self.log.info(f"Record inserted in the ISPyB \"datacollection\" table.\n"
                      f"RECORD:\n{db_data_collection.__dict__}")
        return db_data_collection

    @hwo_header_log
    def _update_grid_info(self, grid_info_dict: dict,
                          sql_session: DBSession = None):
        if not sql_session:
            sql_session = next(self.get_db_session())
        db_grid_info = sql_session.query(GridInfo) \
            .filter(GridInfo.gridInfoId == grid_info_dict['gridInfoId']) \
            .one()
        for key, value in grid_info_dict.items():
            if getattr(db_grid_info, key) != value:
                setattr(db_grid_info, key, value)
        sql_session.commit()
        sql_session.refresh(db_grid_info)
        self.log.info(f"Record updated in the ISPyB \"gridinfo\" table.\n"
                      f"RECORD [id {grid_info_dict['gridInfoId']}]: \n{grid_info_dict}")
        return db_grid_info

    @hwo_header_log
    def _update_motor_positions(self, motor_pos_dict: dict,
                                sql_session: DBSession = None):

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_motor_pos = sql_session.query(MotorPosition) \
            .filter(MotorPosition.motorPositionId == motor_pos_dict['motorPositionId']) \
            .one()
        for key, value in motor_pos_dict.items():
            if getattr(db_motor_pos, key) != value:
                setattr(db_motor_pos, key, value)
        sql_session.commit()
        sql_session.refresh(db_motor_pos)
        self.log.info(f"Record updated in the ISPyB \"motorposition\" table.\n"
                      f"RECORD [id {motor_pos_dict['motorPositionId']}]:"
                      f" \n{db_motor_pos.__dict__}")
        return db_motor_pos

    @hwo_header_log
    def _update_position(self, pos_dict: dict, sql_session: DBSession = None):

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_pos = sql_session.query(Position) \
            .filter(Position.positionId == pos_dict['positionId']) \
            .one()
        for key, value in pos_dict.items():
            if getattr(db_pos, key) != value:
                setattr(db_pos, key, value)
        sql_session.commit()
        sql_session.refresh(db_pos)
        self.log.info(f"Record updated in the ISPyB \"position\" table.\n"
                      f"RECORD [id {pos_dict['positionId']}]: \n{db_pos.__dict__}")
        return db_pos

    @hwo_header_log
    def _update_data_collection_group(self, dcg_dict: dict,
                                      sql_session: DBSession = None):
        if not sql_session:
            sql_session = next(self.get_db_session())
        db_dcg = sql_session.query(DataCollectionGroup) \
            .filter(DataCollectionGroup.dataCollectionGroupId == dcg_dict['dataCollectionGroupId']) \
            .one()
        for key, value in dcg_dict.items():
            if getattr(db_dcg, key) != value:
                setattr(db_dcg, key, value)
        sql_session.commit()
        sql_session.refresh(db_dcg)
        self.log.info(f"Record updated in the ISPyB \"datacollectiongroup\" table.\n"
                      f"RECORD [id {dcg_dict['dataCollectionGroupId']}]: "
                      f"\n{db_dcg.__dict__}")
        return db_dcg

    @hwo_header_log
    def _update_data_collection(self, dc_dict: dict, sql_session: DBSession = None):

        if not sql_session:
            sql_session = next(self.get_db_session())
        db_dc = sql_session.query(DataCollection)\
            .filter(DataCollection.dataCollectionId == dc_dict['dataCollectionId'])\
            .one()
        for key, value in dc_dict.items():
            if getattr(db_dc, key) != value:
                setattr(db_dc, key, value)
        sql_session.commit()
        sql_session.refresh(db_dc)
        self.log.info(f"Record updated in the ISPyB \"datacollection\" table.\n"
                      f"RECORD [id {dc_dict['dataCollectionId']}]: \n{db_dc.__dict__}")
        return db_dc

    @hwo_header_log
    def update_data_collection(self, mx_collection, grid_dict = None):
        """
        Updates the datacollection mx_collection, this requires that the
        collectionId attribute is set and exists in the database.

        :param mx_collection: The dictionary with collections parameters.
        :type mx_collection: dict

        :returns: None
        """
        with self.SqlAlchemySession() as sql_session:
            sql_session: DBSession

            # DataCollection
            dc_dict = self.extract_data_collection_from_mx_collection(mx_collection)
            self._update_data_collection(dc_dict, sql_session)

            # DataCollectionGroup
            dcg_dict = self.extract_data_collection_group_from_mx_collection(mx_collection)
            self._update_data_collection_group(dcg_dict, sql_session)

            if mx_collection['experiment_type'] == 'Mesh':
                grid_dict = self.extract_grid_info(grid_dict, mx_collection)
                self._update_grid_info(grid_dict, sql_session)

            # Position
            pos_dict = self.extract_position_from_mx_collection(mx_collection)
            self._update_position(pos_dict, sql_session)

            # Motor Position (Starting position)
            motor_pos_dict = self.extract_motor_position_from_mx_collection(mx_collection)
            self._update_motor_positions(motor_pos_dict, sql_session)


    @hwo_header_log
    def update_bl_sample(self, bl_sample):
        """
        Creates or stos a BLSample entry.

        :param sample_dict: A dictonary with the properties for the entry.
        :type sample_dict: dict
        # NBNB update doc string
        """

        pass

    @hwo_header_log
    def store_data_collection_group_grid(self, dcg_id, grid_dict):
        mx_acquisition: MXAcquisition = self.ispyb_api_conn.mx_acquisition

        # TODO check if XRD2 setup it's fine
        grid_info_id = None
        dcg_grid_params = {}
        #dcg_grid_params = mx_acquisition.get_dcg_grid_params()
        dcg_grid_params['parentid'] = dcg_id
        dcg_grid_params['dxInMm'] = grid_dict['cell_width'] * 0.001  # This is actually step size not full length
        # On XRD2 ths is alternative was commented => grid_dict['dx_mm'] / grid_dict['steps_x']
        dcg_grid_params['dyInMm'] = grid_dict['cell_height'] * 0.001 # Same above
        dcg_grid_params['stepsX'] = grid_dict['steps_x']  # Number of step in the raster
        dcg_grid_params['stepsY'] = grid_dict['steps_y']
        dcg_grid_params['meshAngle'] = grid_dict['motor_positions']['phi']
        dcg_grid_params['pixelsPerMicronX'] = 1000 / grid_dict['pixels_per_mm'][0]
        dcg_grid_params['pixelsPerMicronY'] = 1000 / grid_dict['pixels_per_mm'][1]
        dcg_grid_params['snapshotOffsetXPixel'] = grid_dict['screen_coord'][0]
        dcg_grid_params['snapshotOffsetYPixel'] = grid_dict['screen_coord'][1]
        dcg_grid_params['orientation'] = 'horizontal'
        dcg_grid_params['snaked'] = True  # hardcoded for now
        grid_info_id = mx_acquisition.upsert_dcg_grid(list(dcg_grid_params.values()))
        self.log.info(f"Record inserted in the ISPyB \"gridinfo\" table.\n"
                      f"RECORD [id: {grid_info_id}]:\n{dcg_grid_params}")
        self.log.exception(f"Failed to insert a record in the ISPyB \"gridinfo\""
                           f" table.\nRECORD:\n{dcg_grid_params}")
        return grid_info_id

    @hwo_header_log
    def store_data_collection(self, mx_collection, grid_dict = None, bl_config=None):
        """
        Stores the data collection mx_collection, and the beamline setup
        if provided.

        :param mx_collection: The data collection parameters.
        :type mx_collection: dict

        :param bl_config: The beamline setup.
        :type bl_config: dict

        :returns: None


        """

        self.log.debug("Data collection parameters stored in ISPyB: %s"
                       % str(mx_collection))
        self.log.debug("Beamline setup stored in ISPyB: %s" % str(bl_config))

        print("---- MX collection ---")
        print(mx_collection)

        with self.SqlAlchemySession() as sql_session:
            sql_session: DBSession

            # DataCollectionGroup
            dcg_dict = self.extract_data_collection_group_from_mx_collection(mx_collection)
            db_dcg = self._insert_data_collection_group(dcg_dict, sql_session)
            mx_collection['collection_grp_id'] = db_dcg.dataCollectionGroupId

            if mx_collection['experiment_type'] == 'Mesh':
                grid_dict = self.extract_grid_info(grid_dict, mx_collection)
                self._insert_grid_info(grid_dict, sql_session)

            # TODO !!! controlla se il focus viene aggiunto in caso di spostamento
            #  del detector focus !!!

            # Position
            pos_dict = self.extract_position_from_mx_collection(mx_collection)
            db_pos = self._insert_position(pos_dict, sql_session)
            mx_collection['pos_id'] = db_pos.positionId

            # Motor Position (Starting position)
            motor_pos_dict = self.extract_motor_position_from_mx_collection(mx_collection)
            db_motor_pos = self._insert_motor_positions(motor_pos_dict, sql_session)
            mx_collection['motor_pos_id'] = db_motor_pos.motorPositionId

            # DataCollection
            dc_dict = self.extract_data_collection_from_mx_collection(mx_collection)
            db_data_collection = self._insert_data_collection(dc_dict, sql_session)

        return db_data_collection.dataCollectionId, None

    def store_image(self, image_dict):
        """
        Stores the image (image parameters) <image_dict>

        :param image_dict: A dictonary with image pramaters.
        :type image_dict: dict

        :returns: None
        """

        pass

    @hwo_header_log
    def store_energy_scan(self, energyscan_dict):

        pass

    @hwo_header_log
    def associate_bl_sample_and_energy_scan(self, entry_dict):

        pass

    @hwo_header_log
    def get_data_collection(self, data_collection_id):
        """
        Retrives the data collection with id <data_collection_id>

        :param data_collection_id: Id of data collection.
        :type data_collection_id: int

        :rtype: dict
        """

        pass

    @hwo_header_log
    def dc_link(self, dc_id):
        """
        Get the LIMS link the data collection with id <id>.

        :param str dc_id: Data collection ID
        :returns: The link to the data collection
        """

        url = None

        if self.base_result_url is not None:
            path = "/dc/visit/{pcode}{pnumber}-{visit_num}/id/{dc_id}"
            path = path.format(
                pcode=HWR.beamline.session.proposal_code,
                pnumber=HWR.beamline.session.proposal_number,
                visit_num=HWR.beamline.session.visit_num,
                dc_id=dc_id,
            )

            url = urljoin(self.base_result_url, path)

        return url

    @hwo_header_log
    def store_xfe_spectrum(self, xfespectrum_dict):
        """
        Stores a xfe spectrum.

        :returns: A dictionary with the xfe spectrum id.
        :rtype: dict

        """

        pass

    @hwo_header_log
    def disable(self):

        self.__disabled = True

    @hwo_header_log
    def enable(self):

        self.__disabled = False

    @hwo_header_log
    def _store_data_collection_group(self, group_data):

        pass

    @hwo_header_log
    def store_autoproc_program(self, autoproc_program_dict):

        pass

    @hwo_header_log
    def store_workflow(self, *args, **kwargs):

        return 1, 1, 1

    @hwo_header_log
    def _store_workflow(self, info_dict):

        pass

    @hwo_header_log
    def store_workflow_step(self, *args, **kwargs):

        return None

    @hwo_header_log
    def store_image_quality_indicators(self, image_dict):

        pass

    @hwo_header_log
    def set_image_quality_indicators_plot(self, collection_id, plot_path, csv_path):

        pass

    @hwo_header_log
    def store_beamline_setup(self, session_id, bl_config):
        """
        Stores the beamline setup dict <bl_config>.

        :param session_id: The session id that the bl_config
                           should be associated with.
        :type session_id: int

        :param bl_config: The dictonary with beamline settings.
        :type bl_config: dict

        :returns beamline_setup_id: The database id of the beamline setup.
        :rtype: str
        """

        pass

    # Bindings to methods called from older bricks.
    getProposal = get_proposal
    getSessionLocalContact = get_session_local_contact
    storeDataCollection = store_data_collection
    storeBeamLineSetup = store_beamline_setup
    getDataCollection = get_data_collection
    updateBLSample = update_bl_sample
    associateBLSampleAndEnergyScan = associate_bl_sample_and_energy_scan
    updateDataCollection = update_data_collection
    storeImage = store_image
    storeEnergyScan = store_energy_scan
    storeXfeSpectrum = store_xfe_spectrum

    @hwo_header_log
    def store_robot_action(self, robot_action_dict):
        """Stores robot action"""
        pass



