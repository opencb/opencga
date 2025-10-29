import os
import json
import requests
import yaml


class ClientConfiguration(object):
    """PyOpenCGA configuration"""
    
    def __init__(self, config_input):
        """
        :param dict or str config_input: a dict, or a yaml/json file containing an OpenCGA valid client configuration.
        """

        if isinstance(config_input, dict):
            self._config = config_input
        else:
            self._config = self._get_dictionary_from_file(config_input)

        self.get_sso_login_info()

        self._validate_configuration()

    @staticmethod
    def _get_dictionary_from_file(config_fpath):
        config_fhand = open(config_fpath, 'r')

        if config_fpath.endswith('.yml') or config_fpath.endswith('.yaml'):
            config_dict = yaml.safe_load(config_fhand)
        elif config_fpath.endswith('.json'):
            config_dict = json.loads(config_fhand.read())
        else:
            raise NotImplementedError('Input must be a dictionary, a yaml file (.yml or .yaml) or a json file (.json)')

        config_fhand.close()

        return config_dict

    def get_sso_login_info(self):
        # Checking if SSO login is specified
        if (('sso_login' in self._config and self._config['sso_login']) or
                ('cookies' in self._config and self._config['cookies'])):

            # Getting session file name
            if 'name' in self._config['rest'] and self._config['rest']['name']:
                host_name = self._config['rest']['name']
            else:
                host_name = 'opencga'
            python_session_fhand = open(os.path.expanduser("~/.opencga/{}_session.json".format(host_name)), 'r')

            # Loading info from session file
            session_info = json.loads(python_session_fhand.read())
            self._config['sso_login'] = True
            self._config['cookies'] = session_info['attributes']['cookies']
            self._config['token'] = session_info['token']

    def _validate_configuration(self):
        if self._config is None:
            raise ValueError('Missing configuration dictionary.')

        if 'rest' not in self._config:
            raise ValueError('Missing configuration field "rest".')

        if 'host' not in self._config['rest'] or not self._config['rest']['host']:
            raise ValueError('Missing configuration field "host".')

        self._validate_host()

    def _validate_host(self):
        try:
            r = requests.head(self.host, timeout=2, verify=not self.tlsAllowInvalidCertificates)
            if r.status_code == 302:
                return
        except requests.exceptions.SSLError:
            raise Exception('Invalid SSL certificate from "{}"'.format(self.host))
        except requests.ConnectionError:
            raise Exception('Unreachable host "{}"'.format(self.host))

    @property
    def host(self):
        return self._config['rest']['host']

    @host.setter
    def host(self, new_host):
        self._config['rest']['host'] = new_host

    @property
    def tlsAllowInvalidCertificates(self):
        if ('tlsAllowInvalidCertificates' in self._config['rest']
                and self._config['rest']['tlsAllowInvalidCertificates'] is not None):
            return self._config['rest']['tlsAllowInvalidCertificates']
        else:
            return False

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
    def host(self):
        return self._config['rest']['host']

    @host.setter
    def host(self, host):
        self._config['rest']['host'] = host
        self._validate_host()

    @property
    def tlsAllowInvalidCertificates(self):
        if ('tlsAllowInvalidCertificates' in self._config['rest'] and
                self._config['rest']['tlsAllowInvalidCertificates'] is not None):
            return self._config['rest']['tlsAllowInvalidCertificates']
        else:
            return False

    @tlsAllowInvalidCertificates.setter
    def tlsAllowInvalidCertificates(self, tlsAllowInvalidCertificates):
        self._config['rest']['tlsAllowInvalidCertificates'] = tlsAllowInvalidCertificates

    @property
    def version(self):
        return self._config['version'] if 'version' in self._config else 'v2'

    @version.setter
    def version(self, version):
        self._config['version'] = version

    @property
    def sso_login(self):
        if 'sso_login' in self._config and self._config['sso_login']:
            return self._config['sso_login']
        else:
            return False

    @sso_login.setter
    def sso_login(self, sso_login):
        if isinstance(str, sso_login):
            self._config['sso_login'] = sso_login.lower() == 'true'
        if isinstance(bool, sso_login):
            self._config['sso_login'] = sso_login

    @property
    def cookies(self):
        if 'cookies' in self._config and self._config['cookies']:
            return self._config['cookies']
        else:
            return None

    @cookies.setter
    def cookies(self, cookies):
        self._config['cookies'] = cookies

    @property
    def token(self):
        if 'token' in self._config and self._config['token']:
            return self._config['token']
        else:
            return None

    @token.setter
    def token(self, token):
        self._config['token'] = token

    @property
    def max_attempts(self):
        """Returns configured max_attempts or 1 if not configured"""
        if 'max_attempts' in self._config and self._config['max_attempts']:
            return self._config['max_attempts']
        else:
            return 1

    @max_attempts.setter
    def max_attempts(self, max_attempts):
        self._config['max_attempts'] = max_attempts

    @property
    def min_retry_secs(self):
        """Returns configured min_retry_seconds or 0 if not configured"""
        if 'min_retry_secs' in self._config and self._config['min_retry_secs']:
            return self._config['min_retry_secs']
        else:
            return 0

    @min_retry_secs.setter
    def min_retry_secs(self, min_retry_secs):
        self._config['min_retry_secs'] = min_retry_secs

    @property
    def max_retry_secs(self):
        """Returns configured max_retry_seconds or 0 if not configured"""
        if 'max_retry_secs' in self._config and self._config['max_retry_secs']:
            return self._config['max_retry_secs']
        else:
            return 0

    @max_retry_secs.setter
    def max_retry_secs(self, max_retry_secs):
        self._config['max_retry_secs'] = max_retry_secs
