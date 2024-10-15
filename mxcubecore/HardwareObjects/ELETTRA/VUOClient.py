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

from urllib.parse import urljoin
import yaml
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore import HardwareRepository as HWR
from mxcubecore import hwo_header_log

from vuo.client import Client
from vuo.exceptions import VUOException


_CONNECTION_ERROR_MSG = (
    "Could not connect to VUO, please verify that "
    + "the server is running and that your "
    + "configuration is correct"
)
_NO_TOKEN_MSG = "Could not connect to VUO, no valid REST token available."


class VUOClient(HardwareObject):
    """
    Web-service client for VUO.
    """

    def __init__(self, name):
        HardwareObject.__init__(self, name)
        self.tag = None
        self.vuo_client = None
        self.beamline_name = None
        self.base_result_url = None

    @hwo_header_log
    def init(self):

        self.beamline_name = HWR.beamline.session.beamline_name
        self.tag = HWR.beamline.session.tag
        try:
            self.base_result_url = self.get_property("base_result_url").strip()
        except AttributeError:
            self.log.warning("Base result url not set in the configuration file")

    def authenticate(self, user, password):
        """
        Authenticate with RESTfull services, updates the authentication token,
        username and password used internally by this object.

        :param str user: Username
        :param str password: Password
        :returns: None

        """

        try:
            self.vuo_client = Client(user, password)
            msg = f"User \"{user}\" has been authenticated by VUO"
            self.log.info(msg)
        except VUOException as e:
            msg = f"User \"{user}\" has not been authenticated by VUO"
            self.log.exception(msg)
            raise e

    @hwo_header_log
    def dc_link(self, dc_id):
        """
        Get the LIMS link the data collection with id <id>.

        :param str did: Data collection ID
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
    def get_dc(self, dc_id):
        """
        Get data collection with id <dc_id>

        :param int dc_id: The collection id
        :returns: Data collection dict
        """

        dc_dict = {}

        return dc_dict

    @hwo_header_log
    def get_dc_thumbnail(self, image_id):
        """
        Get the image data for image with id <image_id>

        :param int image_id: The image id
        :returns: tuple on the form (file name, base64 encoded data)
        """

        fname, data = ("", "")
        return fname, data
