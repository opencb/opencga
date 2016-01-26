import json
import os
import requests
import logging
from pyCGA.Exceptions import LoginException, ServerResponseException
from pathlib import Path

__author__ = 'antonior'


class WS:

    def __init__(self, token=None, version="v1", instance="opencga"):

        if token is None:
            HOME = Path(os.getenv("HOME"))
            opencga_dir = HOME.joinpath(".opencga", "openCGA.json")
            if not Path.exists(opencga_dir):
                raise LoginException()
            fd = open(opencga_dir.as_posix())
            session = json.load(fd)
        else:
            session = token
        self.session_id = session["sid"]
        self.host = session["host"]
        self.instance = instance
        self.pre_url = os.path.join(self.host, self.instance, "webservices", "rest", version)

    @staticmethod
    def check_server_response(response):
        if response == 200 or response == 500:
            return True
        else:
            return False

    def get_result(self, response):
        if response["response"][0]["numResults"] == -1:
            logging.error(response["error"])
            raise ServerResponseException(response["response"][0]["errorMsg"])
        else:
            return response["response"][0]["result"]

    def run_ws(self, url):
        """

        :param url:
        :return: :raise StandardError:
        """

        response = requests.get(url)
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
        response = requests.post(url, json=data)
        if self.check_server_response(response.status_code):
            return self.get_result(response.json())
        else:
            raise Exception("WS Failed, status: " + str(response.status_code))

    def general_method(self, ws_category, method_name, item_id=None, data=None, **options):
        """
        This is a wildcard method, if some of the ws in catalog are not implemented in this python wrapper you can
        always use the general method of the corresponding class.

        :rtype : list of dict
        :param ws_category: category of ws, i.e: Users, projects, studies...
        :param method name: name of the method to be called
        :param item_id: item_id
        :param options: this argument is a dictionary with parameters to be used in the ws
        :return: list of results
        """

        options_string = ""
        if options:
            options_string = "&".join([option_name + "=" + options[option_name] for option_name in options])

        if item_id:
            url = os.path.join(self.pre_url, ws_category, item_id, method_name, "?sid=" + self.session_id + "&" + options_string)
        else:
            url = os.path.join(self.pre_url, ws_category, method_name, "?sid=" + self.session_id + "&" + options_string)
        print(url)

        if data:
            return self.run_ws_post(url, data)
        else:
            result = self.run_ws(url)
        return result


