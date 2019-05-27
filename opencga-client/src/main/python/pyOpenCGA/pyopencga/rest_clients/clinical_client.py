from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient, _ParentBasicCRUDClient, _ParentAclRestClient


class Interpretations(_ParentRestClient):
    """
    This class contains the Interpretations client with methods for the
    Analysis - Clinical webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'analysis/clinical'
        super(Interpretations, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def tool_tiering(self, **options):
        """
        GEL Tiering interpretation analysis (PENDING)
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/tiering
        """

        return self._get('interpretation', subcategory='tools/tiering', **options)

    def tool_team(self, **options):
        """
        TEAM interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/team
        """

        return self._get('interpretation', subcategory='tools/team', **options)

    def tool_custom(self, **options):
        """
        Interpretation custom analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/custom
        """

        return self._get('interpretation', subcategory='tools/custom', **options)

    def stats(self, **options):
        """
        Clinical interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/stats
        """

        return self._get('interpretation', subcategory='stats', **options)

    def query(self, **options):
        """
        Query for reported variants
        URL: /{apiVersion}/analysis/clinical/interpretation/query
        """

        return self._get('interpretation', subcategory='query', **options)

    def index(self, **options):
        """
        Index clinical analysis interpretations in the clinical variant database
        URL: /{apiVersion}/analysis/clinical/interpretation/index
        """

        return self._get('interpretation', subcategory='index', **options)

    def update(self, clinical_analysis, data, action, **options):
        """
        Add or remove Interpretations to/from a Clinical Analysis
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/update

        :param clinical_analysis: clinical analysis id
        :param action: Action to be performed if the array of interpretations is being updated [ADD, REMOVE]
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='update', data=data, action=action, **options)

    def update_interpretation(self, clinical_analysis, interpretation, data, **options):
        """
        Update Interpretation fields
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/update

        :param clinical_analysis: clinical analysis id
        :param interpretation: interpretation id
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='update',
                          subquery_id=interpretation, data=data, **options)

    def update_comments(self, clinical_analysis, interpretation, data, action, **options):
        """
        Update comments of an Interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/comments/update

        :param action: Action to be performed [ADD, SET, REMOVE]
        """

        return self._post('interpretations', query_id=clinical_analysis, subquery_id=interpretation,
                          subcategory='comments/update', data=data, action=action, **options)

    def update_reported_variants(self, clinical_analysis, interpretation, data, **options):
        """
        Update reported variants of an interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/reportedVariants/update
        """

        return self._post('interpretations', query_id=clinical_analysis, subquery_id=interpretation,
                           subcategory='reportedVariants/update', data=data, **options)

class Clinical(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains methods for the Analysis - Clinical webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category  = 'analysis/clinical'
        super(Clinical, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

        self.configuration = configuration
        self.session_id = session_id
        self.login_handler = login_handler
        self._create_clients()

    def _create_clients(self):

        ## undef all
        self.interpretations = None
        self.reports = None
        self.cva = None

        ## [TODO] convert to @properties
        ## [@dgp] SHould I add auto_refresh = self.auto_refresh ??
        self.interpretations = Interpretations(self.configuration, self.session_id, self.login_handler)
        ## self.reports = Reports(configuration, session_id, login_handler, *args, **kwargs)
        ## self.cva = CVAs(configuration, session_id, login_handler, *args, **kwargs)

        self.clients = [self.interpretations]

        for client in self.clients:
            # only retry the ones with objects (instatiated clients)
            if client is not None:
                client.on_retry = self.on_retry

    def search(self, **options):
        """
        Clinical analysis search.
        URL: /{apiVersion}/analysis/clinical/search
        """

        return self._get('search', **options)
