from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient  

class Cohorts(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Cohorts ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'cohorts'
        super(Cohorts, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def search(self, **options):
        """
        Search cohorts
        URL: /{apiVersion}/cohorts/search
        """

        return self._get('search', **options)

    def get_stats(self, **options):
        """
        Fetch catalog cohort stats
        URL: /{apiVersion}/cohorts/stats
        """

        return self._get('stats', **options)

    def samples(self, cohort, **options):
        """
        Get samples from cohort [DEPRECATED]
        URL: /{apiVersion}/cohorts/{cohort}/samples

        :param cohort: cohort id
        """

        return self._get('samples', query_id=cohort, **options)

