import json
import os
import requests
import logging
from pyCGA.Exceptions import LoginException, ServerResponseException, WSErrorException
from pathlib import Path
from requests_toolbelt import threaded

__author__ = 'antonior'


class WS:
    def __init__(self, token=None, version="v1", instance="opencga"):

        """
        WS class is initialize using the information in the file .opencga/openCGA.json created after the login,
        but also a token object could be pass and it will be used.

        :type instance: str
        :type version: str
        :type token: dict
        :param token: contains the information to initialize a WS object
        :param version: ws version
        :param instance: instance to use

        - Example:
        .. code-block:: python

            # This will use the information in the login file
            ws = WS()

            # This will use the token passed
            token = {"host": "opencga_host", "user": "username", "sid": "XXXXXXXXXXXXXX"}
            ws = WS(token=token, version="v2", instance="other_instance")

        """
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

    @staticmethod
    def get_result(response):
        if response["response"][0]["numResults"] == -1:
            logging.error(response["response"][0]["errorMsg"])
            raise WSErrorException(response["response"][0]["errorMsg"])
        else:
            if "skip" in response["queryOptions"]:
                skipped = int(response["queryOptions"]["skip"])
            else:
                skipped = 0

            num_results = response["response"][0]["numResults"]
            total_results = response["response"][0]["numTotalResults"]

            return total_results, skipped, num_results, response["response"][0]["result"]

    def get_url_pool(self, url, skipped, result_limit, total_results, step=1000):
        urls = []
        skips = range(skipped + total_results, result_limit, step)
        for skip in skips:
            if skip + step <= result_limit:
                def_url = url + "&limit=" + str(step) + "&skip=" + str(skip)
                urls.append(def_url)
            else:
                def_url = url + "&limit=" + str(result_limit - skip) + "&skip=" + str(skip)
                urls.append(def_url)
        logging.info("Prepared pool of queries: \n" + "\n".join(urls))
        return urls

    def run_ws(self, url, skip=0, limit=-1, n_threads=8, step=1000):
        """

        :param url:
        :raise StandardError:
        """
        limit = int(limit)
        if limit > 0:
            result_limit = limit
        else:
            result_limit = -1

        if limit > step or limit == -1:
            limit = step

        all_results = []
        if "variant" in url:
            url_limits = url + "&limit=" + str(limit) + "&skip=" + str(skip) + "&skipCount=false"
        else:
            url_limits = url + "&limit=" + str(limit) + "&skip=" + str(skip)

        logging.info("Sending the first query: " + url_limits)
        response = self.r_session.get(url_limits)

        if self.check_server_response(response.status_code):
            total_result, skipped, num_results, results = self.get_result(response.json())
            yield results
            remaining_total_results = total_result - (num_results + skipped)
            remaining_limit_results = result_limit - (num_results + skipped)

            if remaining_total_results != 0 and remaining_limit_results != 0:
                if result_limit == -1 or (total_result < result_limit):
                    urls = self.get_url_pool(url, skipped, total_result, num_results, step=step)
                else:
                    urls = self.get_url_pool(url, skipped, result_limit, num_results, step=step)

                urls_to_get = [{'method': 'GET', 'url': url} for url in urls]
                for url_chunck in [urls_to_get[i:i+n_threads] for i in xrange(0, len(urls_to_get), n_threads)]:
                    try:
                        responses, errors = threaded.map(url_chunck, num_processes=n_threads, session=self.r_session)
                    except TypeError:
                        responses, errors = threaded.map(url_chunck, num_processes=n_threads)

                    for response in responses:

                        if self.check_server_response(response.status_code):
                            yield self.get_result(response.json())[3]

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
            raise ServerResponseException("WS Failed, status: " + str(response.status_code))

    def general_method(self, ws_category, method_name, item_id=None, data=None, use_buffer=False, pag=1000, limit=-1, skip=0, **options):
        """
        This is a wildcard method, if some of the ws in catalog are not implemented in this python wrapper you can
        always use the general method of the corresponding class.


        :type options: dict
        :type skip: int
        :type limit: int
        :type pag: int
        :type use_buffer: bool
        :type data: dict
        :type item_id: str
        :type method_name: str
        :type ws_category: str
        :param ws_category: category of ws, i.e: Users, projects, studies...
        :param method_name: name of the method to be called, i.e: create, search. info...
        :param item_id: Some of the webservice methods required a item id, if it is the case you need to provide it using this parameter
        :param data: If this parameter is provided post method is assumed
        :param skip: This parameter is used to skip result from the query
        :param limit: This parameter is used to limit the number of results
        :param pag: Size of each page
        :param use_buffer: If false all result will be stored in the same array and retrieved at once. If true an iterator will be returned the size of the result each iteration is the size of the page
        :param options: this argument is a dictionary with parameters to be used in the ws (see examples)

        :return: This method can return a list of results or a iterator of list of results (see use_buffer)
        :rtype: list of dict

        - Examples:
        .. code-block:: python

            # This is an example of a query to search files in a specific path
            ws = WS()
            found_files = ws.general_method("files", "search", studyId="2", path="~path/to/files/")

            # This is an example of a query using the item_id
            file_info = ws.general_method("files", "info", item_id="6")

            # This is an example using a post method
            updated_files = ws.general_method("files", "update", data={"attributes": {"new_field": "new_value"}})

            # This is an example using a custom pagination and the buffer, this example will show all the file names in the DB in batches of ten
            for batch in ws.general_method("files", "search", studyId="2", pag=10, use_buffer=True, type="FILE"):
                for file_result in batch:
                    print file_result["name"]
        """

        options_string = ""
        if options:
            options_string = "&".join([option_name + "=" + str(options[option_name]) for option_name in options])

        if item_id:
            url = os.path.join(self.pre_url, ws_category, item_id, method_name,
                               "?sid=" + self.session_id + "&" + options_string)
        else:
            url = os.path.join(self.pre_url, ws_category, method_name, "?sid=" + self.session_id + "&" + options_string)

        if self.debug:
            fdw = open(self.debug_path, "a")
            fdw.write(url + "\n")
            fdw.close()

        try:
            if data:
                result = self.run_ws_post(url, data)
            else:
                result =[]
                if use_buffer:
                    return self.run_ws(url, skip=skip, limit=limit, step=pag)
                for batch in self.run_ws(url, skip=skip, limit=limit, step=pag):
                    result += batch

            return result

        except ServerResponseException or WSErrorException:
            logging.error(ServerResponseException.message)
            print ServerResponseException.message
