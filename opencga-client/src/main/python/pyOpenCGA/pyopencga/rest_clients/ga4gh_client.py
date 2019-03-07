from time import sleep

from pyopencga.commons import execute, OpenCGAResponseList
from pyopencga.opencgaconfig import ConfigClient
from pyopencga.retry import retry
from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient, _ParentBasicCRUDClient, _ParentAclRestClient,  _ParentAnnotationSetRestClient  

class GA4GH(_ParentRestClient):
    """
    This class contains method for GA4GH ws
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "ga4gh"
        super(GA4GH, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def responses(self, **options):
        """
        Beacon webservices
        URL: /{apiVersion}/ga4gh/responses
        """

        return self._get('responses', **options)

    def reads_search(self, data, **options):
        """
        <PEDNING>
        URL: /{apiVersion}/ga4gh/reads/search
        """

        return self._post('read', subcategory='search', data=data, **options)

    def variants_search(self, data, **options):
        """
        <PENDING>
        URL: /{apiVersion}/ga4gh/variants/search
        """

        return self._post('variant', subcategory='search', data=data, **options)


