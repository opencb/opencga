from pyopencga.opencga_config import ConfigClient
#from pyopencga.rest_clients._all_rest_clients import *

from pyopencga.rest_clients.user_client import Users
from pyopencga.rest_clients.project_client import Projects
from pyopencga.rest_clients.study_client import Studies
# from rest_clients.file_client import Files
# from rest_clients.sample_client import Samples
# from rest_clients.cohort_client import Cohorts
# from rest_clients.family_client import Families
# from rest_clients.job_client import Jobs
# from rest_clients.individual_client import Individuals
# from rest_clients.clinical_client import Clinical
# from rest_clients.variableset_client import VariableSets
# from rest_clients.alignment_client import AnalysisAlignment
# from rest_clients.variant_client import AnalysisVariant
# from rest_clients.ga4gh_client import GA4GH
# from rest_clients.meta_client import Meta


class OpenCGAClient(object):
    def __init__(self, configuration, user=None, pwd=None, session_id=None,
                 anonymous=False, on_retry=None, auto_refresh=True):
        """
        :param on_retry: callback to be called with client retries an operation.
            It must accept parameters: client, exc_type, exc_val, exc_tb, call
        """
        self.auto_refresh = auto_refresh
        self.configuration = ConfigClient(configuration, on_retry)
        self.on_retry = on_retry
        self.clients = []
        self.user_id = user  # if user and session_id are supplied, we can log out
        if anonymous:
            self._login_handler = None
            self.session_id = None
        else:
            if user and pwd:
                self._login_handler = self._make_login_handler(user, pwd)
                self._login()
            else:
                if session_id is None:
                    msg = "User/password or session_id must be supplied"
                    raise Exception(msg)
                self._login_handler = None
                self.session_id = session_id
        self._create_clients()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()

    def _create_clients(self):

        ## undef all
        self.users = None
        self.projects = None
        self.studies = None
        self.files = None
        self.samples = None
        self.cohorts = None
        self.families = None
        self.jobs = None
        self.individuals = None
        self.clinical = None
        self.variable_sets = None
        self.analysis_alignment = None
        self.analysis_variant = None
        self.ga4gh = None
        self.meta = None

        ## prepare for @properties 
        self.users = Users(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.projects = Projects(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.studies = Studies(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.files = Files(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.samples = Samples(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.cohorts = Cohorts(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.families = Families(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.jobs = Jobs(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.individuals = Individuals(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.clinical = Clinical(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.variable_sets = VariableSets(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.analysis_alignment = AnalysisAlignment(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.analysis_variant = AnalysisVariant(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.ga4gh = GA4GH(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        # self.meta = Meta(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)

        self.clients = [self.users, self.projects, self.studies, self.files,
                        self.samples, self.cohorts, self.families, self.jobs,
                        self.individuals, self.variable_sets, self.clinical,
                        self.analysis_alignment, self.analysis_variant,
                        self.ga4gh, self.meta]

        for client in self.clients:
            # only retry the ones with objects
            if client is not None:
                print("#[DEBUGGING]: Adding methods to class User")
                client.on_retry = self.on_retry

    def _make_login_handler(self, user, pwd):
        """
        Returns a closure that performs the log-in. This will be called on retries
        if the current session ever expires.
        The reason for using a closure and not a normal function is that a normal
        function would require storing the password in a field. It is more secure
        not to do so. This way, the password stored in the closure is inaccessible
        to other code
        """

        def login_handler(refresh=False):
            self.user_id = user
            if refresh:
                self.session_id = Users(self.configuration, session_id=self.session_id).refresh_token(user=user).get().sessionId
            else:
                self.session_id = Users(self.configuration).login(user=user, pwd=pwd).get().sessionId

            for client in self.clients:
                client.session_id = self.session_id  # renew the client's session id
            return self.session_id

        return login_handler

    def _login(self):
        assert self._login_handler, "Can't login without username and password provided"
        self._login_handler()

    def logout(self):
        self.session_id = None

