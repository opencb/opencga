from pyopencga.retry import retry
from pyopencga.commons import execute, QueryResponse

class _ParentRestClient(object):
    """Queries the REST service given the different query params"""

    def __init__(self, configuration, category, session_id=None, login_handler=None, auto_refresh=True):
        """
        :param login_handler: a parameterless method that can log in this connector
        and return a session id
        """
        self.auto_refresh = auto_refresh
        self._cfg = configuration
        self._category = category
        self.session_id = session_id
        self.login_handler = login_handler
        self.on_retry = None

    def _client_login_handler(self):
        if self.login_handler:
            self.session_id = self.login_handler()

    def _refresh_token_client(self):
        if self.login_handler:
            self.session_id = self.login_handler(refresh=True)

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
                           sid=self.session_id,
                           category=self._category,
                           subcategory=subcategory,
                           method=method,
                           query_id=query_ids_str,
                           second_query_id=second_query_id,
                           resource=resource,
                           data=data,
                           options=options)

        def notify_retry(exc_type, exc_val, exc_tb):
            if self.on_retry:
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
        return QueryResponse(response)

    def _get(self, resource, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry('get', resource, query_id, subcategory, second_query_id, **options)

    def _post(self, resource, data, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry('post', resource, query_id, subcategory, second_query_id, data=data, **options)

    def _delete(self, resource, data, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry('delete', resource, query_id, subcategory, second_query_id, **options)


class _ParentBasicCRUDClient(_ParentRestClient):
    def info(self, query_id, **options):
        return self._get('info', query_id=query_id, **options)

    def create(self, data, **options):
        return self._post('create', data=data, **options)

    def update(self, query_id, data, **options):
        return self._post('update', query_id=query_id, data=data, **options)

    def delete(self, **options):
        return self._delete('delete', **options)


class _ParentAclRestClient(_ParentRestClient):
    def acl(self, query_id, **options):
        """
        acl info

        :param query_id:
        :param options:
        """

        return self._get('acl', query_id=query_id, **options)

    def update_acl(self, memberId, data, **options):
        """
        update acl

        :param query_id:
        :param options:
        """

        return self._post('acl', subcategory='update', second_query_id=memberId, data=data, **options)


class _ParentAnnotationSetRestClient(_ParentRestClient):

    def update_annotations(self, query_id, annotationset_id, data, **options):
        """
        update annotations from an AnnotationSet

        :param query_id: Entry identifier.
        :param annotationset_id: AnnotationSet id.
        :param data: Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only
        the key 'remove' containing the comma separated variables to be removed as a value when the action is REMOVE
        or a json with only the key 'reset' containing the comma separated variables that will be set to the default
        value when the action is RESET
        :param options: QueryParam options
        """

        return self._post('annotationSets', query_id=query_id, second_query_id=annotationset_id,
                            subcategory='annotations/update', data=data, **options)

