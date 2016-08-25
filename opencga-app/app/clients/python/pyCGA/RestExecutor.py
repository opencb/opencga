import json
import os
import requests
import logging
from pyCGA.Exceptions import LoginException, ServerResponseException
from pathlib import Path

__author__ = 'antonior'


class WS:

    def __init__(self, token=None, version="v1", instance="opencga"):

        home = Path(os.getenv("HOME"))

        if token is None:
            opencga_dir = home.joinpath(".opencga", "openCGA.json")
            if not Path.exists(opencga_dir):
                raise LoginException()
            fd = open(opencga_dir.as_posix())
            session = json.load(fd)
        else:
            session = token

        self.session_id = session["sid"]
        self.host = session["host"]
        self.debug_path = home.joinpath(".opencga", "pyCGA.log").as_posix()

        if "debug" in session:
            self.debug = session["debug"]
        else:
            self.debug = False

        if "instance" in session:
            self.instance = session["instance"]
        else:
            self.instance = instance
        self.pre_url = os.path.join(self.host, self.instance, "webservices", "rest", version)
        self.r_session = requests.Session()

    @staticmethod
    def check_server_response(response):
        if response == 200 or response == 500:
            return True
        else:
            return False

    def get_result(self, response):
        if response["response"][0]["numResults"] == -1:
            logging.error(response["response"][0]["errorMsg"])
            raise ServerResponseException(response["response"][0]["errorMsg"])
        else:
            return response["response"][0]["result"]

    def run_ws(self, url):
        """

        :param url:
        :return: :raise StandardError:
        """

        response = self.r_session.get(url)
        if self.check_server_response(response.status_code):
            return self.get_result(response.json())
        else:
            logging.error("WS Failed, status: " + str(response.status_code))
            raise ServerResponseException("WS Failed, status: " + str(response.status_code))

    def run_ws_post(self, url, data):
        """

        :param url:
        :return:
        """
        response = self.r_session.post(url, json=data)
        if self.check_server_response(response.status_code):
            return self.get_result(response.json())
        else:
            logging.error("WS Failed, status: " + str(response.status_code))
            raise Exception("WS Failed, status: " + str(response.status_code))

    def general_method(self, ws_category1, action, item_id1=None, ws_category2=None, item_id2=None, data=None,
                       **options):
        """
        This is a wildcard method, if some of the ws in catalog are not implemented in this python wrapper you can
        always use the general method of the corresponding class.

        :param ws_category1: category of ws, i.e: Users, projects, studies...
        :param action: name of the method to be called
        :param item_id1: item_id1
        :param ws_category2: subcategory: acls, groups...
        :param item_id2: item_id2
        :rtype : list of dict
        :param options: this argument is a dictionary with parameters to be used in the ws
        :return: list of results
        """

        if self.debug:
            fdw = open(self.debug_path, "a")

        # TODO: Add pagination
        if data is None and "limit" not in options:
            options["limit"] = -1

        url = os.path.join(self.pre_url, ws_category1)

        if item_id1:
            url = os.path.join(url, item_id1)

        if ws_category2:
            url = os.path.join(url, ws_category2)

        if item_id2:
            url = os.path.join(url, item_id2)

        url = os.path.join(url, action)

        options_string = ""
        if options:
            options_string = "&".join([option_name + "=" + str(options[option_name]) for option_name in options])

        if self.session_id:
            url = os.path.join(url, "?sid=" + self.session_id + "&" + options_string)

        if self.debug:
            fdw.write(url + "\n")

        if data:
            result = self.run_ws_post(url, data)
        else:
            result = self.run_ws(url)

        return result


