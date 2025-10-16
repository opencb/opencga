import os
import requests

from pyopencga.commons import execute, _create_rest_url, snake_to_camel_case
from pyopencga.rest_response import RestResponse
from pyopencga.retry import retry


class _ParentRestClient(object):
    """Queries the REST service given the different query params"""

    def __init__(self, configuration, token=None, login_handler=None,
                 auto_refresh=True):
        self.auto_refresh = auto_refresh
        self._cfg = configuration
        self.token = token
        self.refresh_token = None
        self.login_handler = login_handler
        self.on_retry = None

    def _client_login_handler(self):
        if self.login_handler:
            self.token, self.refresh_token = self.login_handler()

    def _refresh_token_client(self):
        if self.login_handler:
            self.token, self.refresh_token = self.login_handler(refresh=True)

    @staticmethod
    def _get_query_id_str(query_ids):
        if query_ids is None:
            return None
        elif isinstance(query_ids, list):
            return ','.join(map(str, query_ids))
        else:
            return str(query_ids)

    def _rest_retry(self, method, category, resource, query_id=None, subcategory=None,
                    second_query_id=None, data=None, dont_retry=None,
                    **options):

        query_ids_str = self._get_query_id_str(query_id)

        def exec_retry():
            return execute(config=self._cfg,
                           sid=self.token,
                           category=category,
                           subcategory=subcategory,
                           method=method,
                           query_id=query_ids_str,
                           second_query_id=second_query_id,
                           resource=resource,
                           data=data,
                           options=options)

        def notify_retry(exc_type, exc_val, exc_tb):
            if self.on_retry is not None:
                self.on_retry(self, exc_type, exc_val, exc_tb, dict(
                    method=method, resource=resource, query_id=query_id,
                    category=category, subcategory=subcategory,
                    second_query_id=second_query_id, data=data,
                    options=options
                ))

        response = retry(
            exec_retry, self._cfg.max_attempts, self._cfg.min_retry_secs,
            self._cfg.max_retry_secs, login_handler=self.login_handler,
            on_retry=notify_retry, dont_retry=dont_retry
        )

        if self.auto_refresh:
            self._refresh_token_client()
        return RestResponse(response)

    def _get(self, category, resource, query_id=None, subcategory=None,
             second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry(
            method='get', category=category, resource=resource, query_id=query_id,
            subcategory=subcategory, second_query_id=second_query_id,
            **options
        )

    def _post(self, category, resource, data=None, query_id=None, subcategory=None,
              second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        # Special treatment for the "/{apiVersion}/files/upload" endpoint
        if category == 'files' and resource == 'upload':
            response = self._upload(category=category, resource=resource, **options)
            return RestResponse(response)
        else:
            if data is not None:
                return self._rest_retry(
                    method='post', category=category, resource=resource, query_id=query_id,
                    subcategory=subcategory, second_query_id=second_query_id,
                    data=data, **options
                )
            else:
                return self._rest_retry(
                    method='post', category=category, resource=resource, query_id=query_id,
                    subcategory=subcategory, second_query_id=second_query_id,
                    **options
                )

    def _delete(self, category, resource, query_id=None, subcategory=None,
                second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry(
            method='delete', category=category, resource=resource, query_id=query_id,
            subcategory=subcategory, second_query_id=second_query_id,
            **options
        )

    def _upload(self, category, resource, **options):
        """Upload files"""

        # Checking that the parameter file contains the file path to upload
        if 'file' not in options or not isinstance(options['file'], str):
            raise ValueError('To upload a file, please specify the file path as the "file" parameter.')

        # Creating URL and headers
        url, header = _create_rest_url(host=self._cfg.host, version=self._cfg.version, sid=self.token,
                                       category=category, resource=resource, options=options)

        # Creating data
        data = {}
        for k, v in options.items():
            if k == 'file':  # Param "file" is not included in data
                continue
            data[snake_to_camel_case(k)] = v

        # Uploading
        fpath = os.path.realpath(os.path.expanduser(options['file']))
        with open(fpath, "rb") as f:
            fhand = {"file": (fpath, f, "application/octet-stream")}
            response = requests.post(url, headers=header, files=fhand, data=data or None)
            if response.status_code != 200:
                raise Exception(response.content)

        return response.json()
