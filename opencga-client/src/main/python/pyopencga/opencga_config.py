import os
import json
import requests
import yaml


class ClientConfiguration(object):
    """
    Configuration class shared between OpenCGA python clients
    """
    
    def __init__(self, config_input):
        """
        :param config_input: a dict, or a yaml/json file containing an OpenCGA valid client configuration.
        """

        # Default config params
        self._configuration_input = config_input
        self._retry_config = None

        if isinstance(config_input, dict):
            self._config = config_input
        else:
            self._config = self._get_dictionary_from_file(config_input)

        self._validate_configuration(self._config)

    @staticmethod
    def _get_dictionary_from_file(config_fpath):
        config_fhand = open(config_fpath, 'r')

        config_dict = None
        if config_fpath.endswith('.yml') or config_fpath.endswith('.yaml'):
            config_dict = yaml.safe_load(config_fhand)
        if config_fpath.endswith('.json'):
            config_dict = json.loads(config_fhand.read())

        config_fhand.close()

        return config_dict

    def _validate_configuration(self, config):
        if config is None:
            raise ValueError('Missing configuration dictionary')

        if 'rest' not in config:
            raise ValueError('Missing "rest" field from configuration dictionary. Please, pass a valid OpenCGA configuration dictionary')

        if 'host' not in config['rest'] or not config['rest']['host']:
            raise ValueError('Missing or empty "host" in OpenCGA configuration')

        self._validate_host(config['rest']['host'])

    @staticmethod
    def _validate_host(host):
        if not (host.startswith('http://') or host.startswith('https://')):
            host = 'http://' + host
        try:
            r = requests.head(host, timeout=2)
            if r.status_code == 302:
                return
        except requests.ConnectionError:
            raise Exception('Unreachable host ' + host)

    @property
    def host(self):
        return self._config['rest']['host']

    @host.setter
    def host(self, new_host):
        if not (new_host.startswith('http://') or new_host.startswith('https://')):
            new_host = 'http://' + new_host
        self._config['rest']['host'] = new_host

    @property
    def version(self):
        return self._config['version'] if 'version' in self._config else 'v2'

    @property
    def cookies(self):
        if 'cookies' in self._config and self._config['cookies']:
            python_session_fhand = open(os.path.expanduser("~/.opencga/python_session.json"), 'r')
            session_info = json.loads(python_session_fhand.read())
            return session_info['cookies']
        else:
            return None

    @property
    def configuration(self):
        return self._config

    @property
    def max_attempts(self):
        """Returns configured max_attempts or 1 if not configured"""
        return 1

    @property
    def min_retry_secs(self):
        """Returns configured min_retry_seconds or 0 if not configured"""
        return 0

    @property
    def max_retry_secs(self):
        """Returns configured max_retry_seconds or 0 if not configured"""
        return 0
