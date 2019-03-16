from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient 

class Panels(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains the methods for Panels webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'panels'
        super(Panels, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def search(self, **options):
        """
        Panel search
        URL: /{apiVersion}/panels/search
        """

        return self._get('search', **options)

