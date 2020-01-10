import getpass
import time
import sys
import re

from pyopencga.opencga_config import ClientConfiguration
from pyopencga.rest_clients.admin_client import Admin
from pyopencga.rest_clients.alignment_client import Alignment
from pyopencga.rest_clients.clinical_client import Clinical
from pyopencga.rest_clients.cohorts_client import Cohorts
from pyopencga.rest_clients.families_client import Families
from pyopencga.rest_clients.files_client import Files
from pyopencga.rest_clients.ga4gh_client import GA4GH
from pyopencga.rest_clients.individuals_client import Individuals
from pyopencga.rest_clients.jobs_client import Jobs
from pyopencga.rest_clients.meta_client import Meta
from pyopencga.rest_clients.panels_client import Panels
from pyopencga.rest_clients.projects_client import Projects
from pyopencga.rest_clients.samples_client import Samples
from pyopencga.rest_clients.studies_client import Studies
from pyopencga.rest_clients.variant_operations_client import VariantOperations
from pyopencga.rest_clients.users_client import Users
from pyopencga.rest_clients.variant_client import Variant


class OpencgaClient(object):
    def __init__(self, configuration, token=None, on_retry=None, auto_refresh=True):
        """
        :param on_retry: callback to be called with client retries an operation.
            It must accept parameters: client, exc_type, exc_val, exc_tb, call
        """

        if not isinstance(configuration, ClientConfiguration):
            raise ValueError('Expected ClientConfiguration instance')

        self.configuration = configuration
        self.auto_refresh = auto_refresh
        self.on_retry = on_retry
        self.clients = []
        self.user_id = None  # if user and session_id are supplied, we can log out
        self._login_handler = None
        self.token = token
        self._create_clients()
        self._check_versions()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()

    def _check_versions(self):
        server_version = self.meta.about().get_result(0)['Version'].split('-')[0]
        client_version = re.findall(r'Client version: (.+)\n', str(self.meta.__doc__))[0]
        ansi_reset = "\033[0m"
        ansi_red = "\033[31m"
        ansi_yellow = "\033[33m"
        if tuple(server_version.split('.')[:2]) < tuple(client_version.split('.')[:2]):
            msg = '[WARNING]: Client version ({}) is higher than server version ({}).' \
                  ' Some client features may not be implemented in the server.\n'.format(client_version, server_version)
            sys.stdout.write('{}{}{}'.format(ansi_red, msg, ansi_reset))
        elif tuple(server_version.split('.')[:2]) > tuple(client_version.split('.')[:2]):
            msg = '[INFO]: Client version ({}) is lower than server version ({}).\n'.format(client_version, server_version)
            sys.stdout.write('{}{}{}'.format(ansi_yellow, msg, ansi_reset))

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
        self.alignment = None
        self.variant = None
        self.clinical = None
        self.ga4gh = None
        self.meta = None
        self.admin = None
        self.panels = None
        self.variant_operations = None

        ## [TODO] convert to @properties
        self.users = Users(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.projects = Projects(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.studies = Studies(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.files = Files(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.samples = Samples(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.cohorts = Cohorts(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.families = Families(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.jobs = Jobs(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.individuals = Individuals(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.alignment = Alignment(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.variant = Variant(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.clinical = Clinical(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.ga4gh = GA4GH(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.meta = Meta(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.admin = Admin(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.panels = Panels(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.variant_operations = VariantOperations(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)

        self.clients = [self.users, self.projects, self.studies, self.files,
                        self.samples, self.cohorts, self.families, self.jobs,
                        self.individuals, self.alignment, self.variant, self.clinical,
                        self.ga4gh, self.meta, self.admin, self.panels, self.variant_operations]

        for client in self.clients:
            # only retry the ones with objects (instantiated clients)
            if client is not None:
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
                self.token = Users(self.configuration).login(user=user, data={}).get_result(0)['token']
            else:
                self.token = Users(self.configuration).login(user=user, data={'password': pwd}).get_result(0)['token']

            for client in self.clients:
                client.token = self.token  # renew the client's token
            return self.token

        return login_handler

    def login(self, user=None, password=None):
        if user is not None:
            if password is None:
                password = getpass.getpass()
            self._login_handler = self._make_login_handler(user, password)

        assert self._login_handler, "Can't login without username and password provided"
        self._login_handler()

    def logout(self):
        self.token = None
        for client in self.clients:
            client.token = self.token

    def wait_for_job(self, response=None, study=None, job_id=None, retry_seconds=10):
        if response is not None:
            study = response['studyUuid']
            job_id = response['uuid']

        if study is None or job_id is None:
            raise ValueError('Argument "response" or arguments "study" and "job_id" must be provided')

        if len(job_id.split(',')) > 1:
            raise ValueError('Only one job ID is allowed')

        retry_seconds = retry_seconds if retry_seconds >= 10 else 10
        while True:
            job_info = self.jobs.info(study=study, jobs=job_id, include='status').get_result(0)
            if job_info['status']['name'] in ['ERROR', 'ABORTED']:
                raise ValueError('{} ({}): {}'.format(
                    job_info['status']['name'], job_info['status']['date'], job_info['status']['message']
                ))
            elif job_info['status']['name'] in ['DONE']:
                break
            time.sleep(retry_seconds)

    def help(self, category_name=None, parameters=False):
        help_text = []
        help_json = self.meta.api().get_result(0)
        if category_name is None:
            help_text.append('Available categories:')
            help_text += ['{}- {}'.format(' '*4, category['name']) for category in help_json]
        else:
            for category in help_json:
                if category_name == category['name']:
                    help_text.append('{} endpoints:'.format(category['name']))
                    for endpoint in category['endpoints']:
                        help_text.append('{}- {} ({}): {}'.format(
                            ' '*4, endpoint['path'], endpoint['method'], endpoint['description'])
                        )
                        if parameters:
                            help_text += ['{}- {} ({}): {}'.format(
                                ' '*8, param['name'], param['type'], param['description']
                            ) for param in endpoint['parameters']]
        sys.stdout.write('\n'.join(help_text) + '\n')
