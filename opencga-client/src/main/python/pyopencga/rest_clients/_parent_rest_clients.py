from pyopencga.commons import execute
from pyopencga.rest_response import RestResponse
from pyopencga.retry import retry


class _ParentRestClient(object):
    """Queries the REST service given the different query params"""

    def __init__(self, configuration, category, token=None, login_handler=None, auto_refresh=True):
        """
        :param login_handler: a parameterless method that can log in this connector
        and return a session id
        """
        self.auto_refresh = auto_refresh
        self._cfg = configuration
        self._category = category
        self.token = token
        self.login_handler = login_handler
        self.on_retry = None

    def _client_login_handler(self):
        if self.login_handler:
            self.token = self.login_handler()

    def _refresh_token_client(self):
        if self.login_handler:
            self.token = self.login_handler(refresh=True)

    @staticmethod
    def _get_query_id_str(query_ids):
        if query_ids is None:
            return None
        elif isinstance(query_ids, list):
            return ','.join(map(str, query_ids))
        else:
            return str(query_ids)

    def _rest_retry(self, method, resource, query_id=None, subcategory=None,
                    second_query_id=None, data=None, dont_retry=None, **options):
        """Invokes the specified HTTP method, with retries if they are specified in the configuration
        :return: an instance of OpenCGAResponseList"""

        query_ids_str = self._get_query_id_str(query_id)

        def exec_retry():
            return execute(host=self._cfg.host,
                           version=self._cfg.version,
                           sid=self.token,
                           category=self._category,
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
                    category=self._category, subcategory=subcategory,
                    second_query_id=second_query_id, data=data,
                    options=options
                ))

        response = retry(
            exec_retry, self._cfg.max_attempts, self._cfg.min_retry_secs, self._cfg.max_retry_secs,
            login_handler=self._client_login_handler if self.login_handler else None,
            on_retry=notify_retry, dont_retry=dont_retry)

        if self.auto_refresh:
            self._refresh_token_client()
        return RestResponse(response)

    def _get(self, resource, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry(method='get', resource=resource, query_id=query_id, subcategory=subcategory,
                                second_query_id=second_query_id, **options)

    def _post(self, resource, data=None, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        if data is not None:
            return self._rest_retry(method='post', resource=resource, query_id=query_id, subcategory=subcategory,
                                    second_query_id=second_query_id, data=data, **options)
        else:
            return self._rest_retry(method='post', resource=resource, query_id=query_id, subcategory=subcategory,
                                    second_query_id=second_query_id, **options)

    def _delete(self, resource, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry(method='delete', resource=resource, query_id=query_id, subcategory=subcategory,
                                second_query_id=second_query_id, **options)
