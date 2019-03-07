from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient  

class Samples(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Samples webservice
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'samples'
        super(Samples, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def search(self, **options):
        """
        Sample search method
        URL: /{apiVersion}/samples/search
        """

        return self._get('search', **options)

    def get_stats(self, **options):
        """
        Fetch catalog sample stats
        URL: /{apiVersion}/samples/stats
        """

        return self._get('stats', **options)

    def load_from_ped(self, **options):
        """
        Load samples from a ped file [EXPERIMENTAL]
        URL: /{apiVersion}/samples/load
        """

        return self._get('load', **options)

    def delete(self, **options):
        """
        Delete existing samples
        URL: /{apiVersion}/samples/delete
        """

        return self._delete('delete', **options)
