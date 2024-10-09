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

import os
import socket
from typing import Tuple

from ispyb.sqlalchemy import BLSession

from mxcubecore import hwo_header_log
from mxcubecore.BaseHardwareObjects import HardwareObject
import mxcubecore.HardwareRepository as HWR
from mxcubecore.model.queue_model_objects import PathTemplate, DataCollection


class ElettraSession(HardwareObject):
    def __init__(self, name):
        HardwareObject.__init__(self, name)

        self.synchrotron_name = None
        self.beamline_name = None
        self.tag = None
        self._session_id = None
        self.visit_num = None
        self.proposal_code = None
        self.proposal_number = None
        self.proposal_id = None
        self.in_house_users = []
        self.endstation_name = None
        self.session_start_date = None
        self.user_group = ""
        self.email_extension = None
        self.template = None

        self.default_precision = 5
        self.suffix = None

        self.base_directory = None
        self.base_process_directory = None
        self.base_archive_directory = None
        self.prefix_folder_name = None
        self.raw_data_folder_name = None
        self.processed_data_folder_name = None
        self.archived_data_folder = None

    # Framework-2 method, inherited from HardwareObject and called
    # by the framework after the object has been initialized.
    @hwo_header_log
    def init(self):
        self.synchrotron_name = self.get_property("synchrotron_name")
        self.beamline_name = self.get_property("beamline_name")
        self.tag = self.get_property("tag")
        self.endstation_name = self.get_property("endstation_name").lower()

        self.suffix = self["file_info"].get_property("file_suffix")
        self.template = self["file_info"].get_property("file_template")

        self.base_directory = self["file_info"].get_property("base_directory").strip()
        self.prefix_folder_name = \
            self["file_info"].get_property("prefix_folder_name", "").strip()
        self.raw_data_folder_name = \
            self["file_info"].get_property("raw_data_folder_name").strip()
        self.processed_data_folder_name = \
            self["file_info"].get_property("processed_data_folder_name").strip()

        # Archive is on tape library (not directly reachable)
        self.archived_data_folder = ""

        email_extension = self.get_property("email_extension")
        if email_extension:
            self.email_extension = email_extension
        else:
            try:
                domain = socket.getfqdn().split(".")
                self.email_extension = ".".join((domain[-2], domain[-1]))
            except (TypeError, IndexError):
                pass

        precision = int(self["file_info"].get_property("precision",
                                                       self.default_precision))

        # Init PathTemplate
        PathTemplate.set_data_base_path(self.base_directory)
        PathTemplate.set_precision(precision)
        PathTemplate.set_path_template_style(self.synchrotron_name, self.template)
        PathTemplate.set_archive_path(self.base_directory, self.archived_data_folder)

    @property
    def session_id(self):
        return self._session_id

    @session_id.setter
    def session_id(self, sess_id: int):

        # Each time the session change the "visit_number" must change
        db_session: BLSession = HWR.beamline.lims.get_session_by_id(sess_id)
        self.visit_num = db_session.visit_number

        self._session_id = sess_id

    @hwo_header_log
    def get_investigation(self):

        if self.proposal_number and self.visit_num:
            return f"{self.proposal_number}-{self.visit_num}"
        else:
            raise ValueError

    @hwo_header_log
    def prepare_directories(self, proposal_info):
        HWR.beamline.lims.lims_rest.vuo_client.create_user_invest_from_prop(
            self.tag, self.proposal_number, inv_name=self.get_investigation())

    @hwo_header_log
    def get_base_data_directory(self):
        return os.path.join(self.base_directory, self.get_investigation())

    @hwo_header_log
    def get_path_with_proposal_as_root(self, path: str) -> str:
        """
        Strips the begining of the path so that it starts with
        the proposal folder as root

        :path: The full path
        :returns: Path stripped so that it starts with proposal
        """

        return path.split(self.base_directory)[1]

    @hwo_header_log
    def get_base_image_directory(self):
        """
        :returns: The base path for images.
        :rtype: str
        """

        try:
            return self.get_base_data_directory()
        except ValueError:
            return ""

    @hwo_header_log
    def get_base_process_directory(self):
        """
        :returns: The base path for procesed data.
        :rtype: str
        """

        try:
            return self.get_base_data_directory()
        except ValueError:
            return ""

    @hwo_header_log
    def get_image_directory(self, sub_dir: str = "") -> str:
        """
        Returns the full path to images

        :param sub_dir: sub directory relative to path returned
                       by get_base_image_directory

        :returns: The full path to images.
        """

        directory = self.get_base_data_directory()

        if sub_dir:
            sub_dir = sub_dir.replace(" ", "").replace(":", "-")
            directory = os.path.join(directory, sub_dir)

            '''
            # Get the next run number
            run_numbers = [0]
            for path_templates in HWR.beamline.queue_model.get_path_templates():
                for pt in path_templates:
                    if isinstance(pt, PathTemplate):
                        # Checked because it can be something different e.g. DataCollection
                        pt: PathTemplate
                        if pt.directory.startswith(directory):
                            run_numbers.append(pt.run_number)
            run_number = str(max(run_numbers) + 1)
            '''

            directory = os.path.join(directory, self.prefix_folder_name,
                                     self.raw_data_folder_name)
        return directory

    @hwo_header_log
    def get_process_directory(self, sub_dir: str = "") -> str:
        """
        Returns the full path to processed data,

        :param sub_dir: sub directory relative to path returned
                       by get_base_proccess_directory

        :returns: The full path to processed data.
        """
        directory = self.get_base_process_directory()

        if sub_dir:
            sub_dir = sub_dir.replace(" ", "").replace(":", "-")
            directory = os.path.join(directory, sub_dir, self.prefix_folder_name,
                                     self.processed_data_folder_name)

        return directory

    @hwo_header_log
    def get_full_path(self, subdir: str = "", tag: str = "") -> Tuple[str, str]:
        """
        Returns the full path to both image and processed data.
        The path(s) returned will follow the convention:

          <base_direcotry>/<subdir>/run_<NUMBER>_<tag>

        Where NUMBER is a automaticaly sequential number and
        base_directory the path returned by get_base_image/process_direcotry

        :param subdir: subdirecotry
        :param tag: tag for

        :returns: Tuple with the full path to image and processed data
        """

        return self.get_image_directory(subdir), self.get_process_directory(subdir)

    @hwo_header_log
    def get_default_prefix(self, sample_data_node=None, generic_name=False):
        """
        Returns the default prefix, using sample data such as the
        acronym as parts in the prefix.

        :param sample_data_node: The data node to get additional
                                 information from, (which will be
                                 added to the prefix).
        :type sample_data_node: Sample


        :returns: The default prefix.
        :rtype: str
        """

        proposal = self.get_proposal()
        prefix = proposal

        if sample_data_node:
            if sample_data_node.has_lims_data():
                protein_acronym = sample_data_node.crystals[0].protein_acronym
                name = sample_data_node.name
                if protein_acronym:
                    if name:
                        prefix = "%s-%s" % (protein_acronym, name)
                    else:
                        prefix = protein_acronym
                else:
                    prefix = name or ""
        elif generic_name:
            prefix = "<acronym>-<name>"
        #
        return prefix

    @hwo_header_log
    def get_default_subdir(self, sample_data: dict) -> str:
        """
        Gets the default sub-directory based on sample information

        Args:
           sample_data: Lims sample dictionary

        Returns:
           Sub-directory path string
        """

        # TODO !!! TO BE DECIDED !!! subdir = sample name or sample name + acronym

        if isinstance(sample_data, dict):
            sample_name = sample_data.get("sampleName", "")
            # protein_acronym = sample_data.get("proteinAcronym", "")
        else:
            sample_name = sample_data.name
            # protein_acronym = sample_data.crystals[0].protein_acronym

        subdir = "%s" % sample_name

        return subdir.replace(":", "-")

    @hwo_header_log
    def get_archive_directory(self):
        archive_directory = os.path.join(
            self["file_info"].get_property("archive_base_directory"),
            self["file_info"].get_property("archive_folder"),)
        return archive_directory

    @hwo_header_log
    def get_proposal(self):
        """
        :returns: The proposal, 'local-user' if no proposal is
                  available
        :rtype: str
        """

        proposal = f"{self.proposal_code} - {self.get_investigation()}"
        return proposal

    @hwo_header_log
    def is_inhouse(self, proposal_code=None, proposal_number=None):
        """
        Determines if a given proposal is considered to be inhouse.

        :param proposal_code: Proposal code
        :type propsal_code: str

        :param proposal_number: Proposal number
        :type proposal_number: str

        :returns: True if the proposal is inhouse, otherwise False.
        :rtype: bool
        """

        if not proposal_code:
            proposal_code = self.proposal_code

        if not proposal_number:
            proposal_number = self.proposal_number

        if (proposal_code, proposal_number) in self.in_house_users:
            return True
        else:
            return False

    @hwo_header_log
    def set_user_group(self, group_name):
        """
        :param group_name: Name of user group
        :type group_name: str
        """
        self.user_group = str(group_name)

    @hwo_header_log
    def get_group_name(self):
        """
        :returns: Name of user group
        :rtype: str
        """
        return self.user_group

    @hwo_header_log
    def clear_session(self):
        self._session_id = None
        self.visit_num = None
        self.proposal_code = None
        self.proposal_number = None
        self.proposal_id = None
        self.in_house_users = []
