import getpass
import time
import sys
import re

from pyopencga.opencga_config import ClientConfiguration
from pyopencga.rest_clients.admin_client import Admin
from pyopencga.rest_clients.alignment_client import Alignment
from pyopencga.rest_clients.clinical_client import Clinical
from pyopencga.rest_clients.cohort_client import Cohort
from pyopencga.rest_clients.family_client import Family
from pyopencga.rest_clients.file_client import File
from pyopencga.rest_clients.ga4gh_client import GA4GH
from pyopencga.rest_clients.individual_client import Individual
from pyopencga.rest_clients.job_client import Job
from pyopencga.rest_clients.meta_client import Meta
from pyopencga.rest_clients.disease_panel_client import DiseasePanel
from pyopencga.rest_clients.project_client import Project
from pyopencga.rest_clients.sample_client import Sample
from pyopencga.rest_clients.study_client import Study
from pyopencga.rest_clients.variant_operation_client import VariantOperation
from pyopencga.rest_clients.user_client import User
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
        self.user_id = None
        self._login_handler = None
        self.token = token
        self.refreshToken = None
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
        self.users = User(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.projects = Project(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.studies = Study(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.files = File(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.jobs = Job(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.samples = Sample(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.individuals = Individual(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.families = Family(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.cohorts = Cohort(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.disease_panels = DiseasePanel(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.alignments = Alignment(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.variants = Variant(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.clinical = Clinical(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.variant_operations = VariantOperation(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.meta = Meta(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.ga4gh = GA4GH(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)
        self.admin = Admin(self.configuration, self.token, self._login_handler, auto_refresh=self.auto_refresh)

        self.clients = [
            self.users, self.projects, self.studies, self.files, self.jobs,
            self.samples, self.individuals, self.families, self.cohorts,
            self.disease_panels, self.alignments, self.variants, self.clinical,
            self.variant_operations, self.meta, self.ga4gh, self.admin
        ]

        for client in self.clients:
            client.on_retry = self.on_retry

    def _make_login_handler(self, user, password):
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
                data = {'refreshToken': self.refresh_token}
            else:
                data = {'user': user, 'password': password}
            tokens = User(self.configuration).login(data=data).get_result(0)
            self.token = tokens['token']
            self.refresh_token = tokens['refreshToken']
            for client in self.clients:
                client.token = self.token
            return self.token
        return login_handler

    def login(self, user=None, password=None):
        if user is not None:
            if password is None:
                password = getpass.getpass()

        try:
            assert user and password
        except AssertionError:
            raise ValueError("User and password required")

        self._login_handler = self._make_login_handler(user, password)
        self._login_handler()

    def logout(self):
        self.token = None
        for client in self.clients:
            client.token = self.token

    def wait_for_job(self, response=None, study_id=None, job_id=None, retry_seconds=10):
        if response is not None:
            study_id = response['results'][0]['study']['id']
            job_id = response['results'][0]['id']

        if response is None and (study_id is None or job_id is None):
            raise ValueError('Argument "response" or arguments "study" and "job_id" must be provided')

        if len(job_id.split(',')) > 1:
            raise ValueError('Only one job ID is allowed')

        retry_seconds = retry_seconds if retry_seconds >= 10 else 10
        while True:
            job_info = self.jobs.info(study=study_id, jobs=job_id).get_result(0)
            if job_info['internal']['status']['name'] in ['ERROR', 'ABORTED']:
                raise ValueError('{} ({}): {}'.format(
                    job_info['status']['name'], job_info['status']['date'],
                    job_info['status']['message']
                ))
            elif job_info['internal']['status']['name'] in ['DONE']:
                break
            time.sleep(retry_seconds)

    def _get_help_info(self, client_name=None, parameters=False):
        info = []
        for client in self.clients:
            # Name
            cls_name = type(client).__name__
            client_method = re.sub(r'(?<!^)(?=[A-Z])', '_', cls_name).lower() \
                if cls_name != 'GA4GH' else cls_name.lower()
            client_method = 'get_' + client_method + '_client'

            if client_name is not None and client_name != cls_name:
                continue

            # Description and path
            class_docstring = client.__doc__
            cls_desc = re.findall('(.+)\n +Client version', class_docstring)[0]
            cls_desc = cls_desc.strip().replace('This class contains methods', 'Client')
            cls_path = re.findall('PATH: (.+)\n', class_docstring)[0]

            # Methods
            methods = []
            method_names = [method_name for method_name in dir(client)
                            if callable(getattr(client, method_name))
                            and not method_name.startswith('_')]
            for method_name in method_names:
                if client_name is None:
                    continue
                method_docstring = getattr(client, method_name).__doc__
                desc = re.findall('(.+)\n +PATH', method_docstring, re.DOTALL)
                desc = re.sub(' +', ' ', desc[0].replace('\n', ' ').strip())
                path = re.findall('PATH: (.+)\n', method_docstring)[0]

                args = []
                arguments = re.findall(
                    ' +:param (.+)', method_docstring, re.DOTALL
                )
                if arguments and parameters:
                    arguments = arguments[0].replace('\n', ' ').strip()
                    arguments = re.sub(' +', ' ', arguments)
                    arguments = arguments.split(' :param ')
                    for parameter in arguments:
                        param_info = parameter.split(' ', 2)
                        args.append({
                            'name': param_info[1].rstrip(':'),
                            'type': param_info[0],
                            'desc': param_info[2]
                        })
                methods.append({
                    'name': method_name,
                    'desc': desc,
                    'path': path,
                    'params': args
                })

            info.append(
                {'class_name': cls_name, 'client_method': client_method,
                 'desc': cls_desc, 'path': cls_path, 'methods': methods}
            )
        return info

    def help(self, client_name=None, show_parameters=False):
        help_txt = []

        info = self._get_help_info(client_name, show_parameters)
        if client_name is None:
            help_txt += ['Available clients:']
            for client in info:
                txt = '{}- {}: {} ({}). USAGE: opencga_client.{}()'
                help_txt += [txt.format(
                    ' '*4, client['class_name'], client['desc'],
                    client['path'], client['client_method']
                )]
        else:
            for client in info:
                help_txt += ['{}: {} ({}). USAGE: opencga_client.{}()'.format(
                    client['class_name'], client['desc'], client['path'],
                    client['client_method']
                )]
                help_txt += ['{}Available methods:'.format(' '*4)]
                for method in client['methods']:
                    help_txt += ['{}- {}: {} ({})'.format(
                        ' '*8, method['name'], method['desc'], method['path']
                    )]
                    if not show_parameters:
                        continue
                    for param in method['params']:
                        help_txt += ['{}* {} ({}): {}'.format(
                            ' ' * 12, param['name'], param['type'],
                            param['desc']
                        )]
        sys.stdout.write('\n'.join(help_txt) + '\n')

    def get_user_client(self):
        return self.users

    def get_project_client(self):
        return self.projects

    def get_study_client(self):
        return self.studies

    def get_file_client(self):
        return self.files

    def get_job_client(self):
        return self.jobs

    def get_sample_client(self):
        return self.samples

    def get_individual_client(self):
        return self.individuals

    def get_family_client(self):
        return self.families

    def get_cohort_client(self):
        return self.cohorts

    def get_disease_panel_client(self):
        return self.disease_panels

    def get_alignment_client(self):
        return self.alignments

    def get_variant_client(self):
        return self.variants

    def get_clinical_client(self):
        return self.clinical

    def get_variant_operation_client(self):
        return self.variant_operations

    def get_meta_client(self):
        return self.meta

    def get_ga4gh_client(self):
        return self.ga4gh

    def get_admin_client(self):
        return self.admin
