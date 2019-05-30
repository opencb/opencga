import json

import requests
import yaml

from pyopencga.retry import retry


class ConfigClient(object):
    """
    Configuration class shared between OpenCGA python clients

    usage:     
        >>> from pyopencga.opencga_config import ConfigClient
        >>> config_file = "/path/config.yml" # it can accept .json
        >>> ConfigClient(configuration, on_retry)

    """
    
    def __init__(self, config_input=None, on_retry=None):
        """

        :param config_input: a dict, or a yaml/json file.  {'version': 'v1', 'rest': {'hosts': ['http://bioinfodev.hpc.cam.ac.uk/opencga-test']}} 
        :param on_retry: A callback to be invoked when an operation is retried It must accept parameters: client, exc_type, exc_val, exc_tb, call
        """
        
        ## [TODO] Mocking a default
        _tmp_host = ['http://bioinfodev.hpc.cam.ac.uk/opencga-test']
        _tmp_config = {
                    'host': _tmp_host,
                    'version': 'v1'
                }
        
        # Default config params
        self._configuration_input = config_input
        self._hosts = _tmp_host
        self._config = _tmp_config
        self._retry_config = None
        self._on_retry = on_retry

        # If config info is provided, override default config params
        if config_input is not None:
            if isinstance(config_input, dict):
                self._override_config_params_from_dict(config_input)
            else:
                self._override_config_params_from_file(config_input)
        else:
            self._config['host'] = self._get_available_host()

    def _override_config_params_from_file(self, config_fpath):
        """Overrides config params if config file is provided"""
        try:
            config_fhand = open(config_fpath, 'r')
        except:
            msg = 'Unable to read file "' + config_fpath + '"'
            raise IOError(msg)

        config_dict = None
        if config_fpath.endswith('.yml') or config_fpath.endswith('.yaml'):
            config_dict = yaml.safe_load(config_fhand)

        if config_fpath.endswith('.json'):
            config_dict = json.loads(config_fhand.read())

        self._override_config_params_from_dict(config_dict)

        config_fhand.close()

    def _override_config_params_from_dict(self, config_dict):
        """Overrides config params if a dict is provided"""
        if config_dict is not None:
            if 'retry' in config_dict:
                self._retry_config = dict(
                    max_attempts=config_dict['retry']['max_attempts'],
                    min_retry_seconds=config_dict['retry']['min_retry_seconds'],
                    max_retry_seconds=config_dict['retry']['max_retry_seconds'],
                )
            if 'rest' in config_dict:
                if 'hosts' in config_dict['rest']:
                    self._hosts = config_dict['rest']['hosts']
                    self._config['host'] = self._get_available_host_retry()
            if 'version' in config_dict:
                self._config['version'] = config_dict['version']

        else:
            msg = 'No opencga_configuration parameters found'
            raise ValueError(msg)

    def _get_available_host_retry(self):
        def notify_retry(exc_type, exc_val, exc_tb):
            if self._on_retry:
                self._on_retry(self, exc_type, exc_val, exc_tb,
                               dict(config=self._configuration_input))

        return retry(
            self._get_available_host, self.max_attempts, self.min_retry_secs,
            self.max_retry_secs, on_retry=notify_retry)

    def _get_available_host(self):
        """Returns the first available host"""
        available_host = None
        for host in self._hosts:
            if not (host.startswith('http://') or host.startswith('https://')):
                host = 'http://' + host
            try:
                r = requests.head(host, timeout=2)
                if r.status_code == 302:
                    available_host = host
                    break
            except requests.ConnectionError:
                pass

        if available_host is None:
            msg = 'No available host found'
            raise Exception(msg)

        else:
            return available_host

    @staticmethod
    def get_basic_config_dict(service_url):
        return {'version': 'v1',
                'rest': {'hosts': [service_url]}}

    @property
    def version(self):
        return self._config['version']

    @version.setter
    def version(self, new_version):
        self._config['version'] = new_version

    @property
    def host(self):
        return self._config['host']

    @host.setter
    def host(self, new_host):
        if not (new_host.startswith('http://') or
                    new_host.startswith('https://')):
            new_host = 'http://' + new_host
        self._config['host'] = new_host

    @property
    def configuration(self):
        return self._config

    @property
    def configuration_input(self):
        return self._configuration_input

    @property
    def retry_config(self):
        return self._retry_config

    @retry_config.setter
    def retry_config(self, new_retry_config):
        self._retry_config = new_retry_config

    @property
    def max_attempts(self):
        """Returns configured max_attempts or 1 if not configured"""
        return self._retry_config['max_attempts'] if self._retry_config else 1

    @property
    def min_retry_secs(self):
        """Returns configured min_retry_seconds or 0 if not configured"""
        return self._retry_config['min_retry_seconds'] if self._retry_config else 0

    @property
    def max_retry_secs(self):
        """Returns configured max_retry_seconds or 0 if not configured"""
        return self._retry_config['max_retry_seconds'] if self._retry_config else 0
