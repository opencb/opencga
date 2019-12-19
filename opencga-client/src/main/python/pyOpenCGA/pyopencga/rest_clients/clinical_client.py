from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient, _ParentBasicCRUDClient, _ParentAclRestClient
from pyopencga.commons import deprecated

class Interpretations(_ParentRestClient, _ParentAclRestClient):
    """
    This class contains the Interpretations client with methods for the
    Analysis - Clinical webservices
    """

    def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):
        _category = 'analysis/clinical'
        super(Interpretations, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)

    @deprecated
    def tool_tiering(self, **options):
        return self.run_tiering(**options)

    def run_tiering(self, **options):
        """
        GEL Tiering interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/tiering/run
        """

        return self._post('interpretation/tiering/run', **options)

    @deprecated
    def tool_team(self, **options):
        return self.run_team(**options)

    def run_team(self, **options):
        """
        TEAM interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/team/run
        """

        return self._post('interpretation/team/run', **options)

    @deprecated
    def tool_custom(self, **options):
        return self.run_custom(**options)

    def run_custom(self, **options):
        """
        TEAM interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/custom/run
        """

        return self._post('interpretation/custom/run', **options)

    def run_cancer_tiering(self, **options):
        """
        Cancer Tiering interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/cancerTiering/run
        """

        return self._post('interpretation/cancerTiering/run', **options)

    def primary_findings(self, **options):
        """
        Search for secondary findings for a given query
        URL: /{apiVersion}/analysis/clinical/interpretation/primaryFindings
        """

        return self._get('interpretation/primaryFindings', **options)

    def secondary_findings(self, **options):
        """
        Search for secondary findings for a given sample
        URL: /{apiVersion}/analysis/clinical/interpretation/secondaryFindings
        """

        return self._get('interpretation/secondaryFindings', **options)

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

    def update_interpretation(self, clinical_analysis, data, action, **options):
        """
        Add or remove Interpretations to/from a Clinical Analysis
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/update

        :param clinical_analysis: clinical analysis id
        :param action: Action to be performed if the array of interpretations is being updated [ADD, REMOVE]
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='update', data=data, action=action, **options)

    def update_interpretation_data(self, clinical_analysis, interpretation, data, **options):
        """
        Update Interpretation fields
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/update

        :param clinical_analysis: clinical analysis id
        :param interpretation: interpretation id
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='update',
                          subquery_id=interpretation, data=data, **options)

    def update_interpretation_comments(self, clinical_analysis, interpretation, data, **options):
        """
        Update comments of an Interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/comment/update

        :param clinical_analysis: clinical analysis id
        :param interpretation: interpretation id
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='comment/update',
                          subquery_id=interpretation, data=data, **options)

    def update_interpretation_primary_findings(self, clinical_analysis, interpretation, data, **options):
        """
        Update reported variants of an interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/primaryFindings/update

        :param clinical_analysis: clinical analysis id
        :param interpretation: interpretation id
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='primaryFindings/update',
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

    def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):
        _category  = 'analysis/clinical'
        super(Clinical, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)

        self.configuration = configuration
        self.token = token
        self.login_handler = login_handler
        self._create_clients()

    def _create_clients(self):

        ## undef all
        self.interpretations = None
        self.reports = None
        self.cva = None

        ## [TODO] convert to @properties
        ## [@dgp] SHould I add auto_refresh = self.auto_refresh ??
        self.interpretations = Interpretations(self.configuration, self.token, self.login_handler)
        ## self.reports = Reports(configuration, token, login_handler, *args, **kwargs)
        ## self.cva = CVAs(configuration, token, login_handler, *args, **kwargs)

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

        if 'skipCount' not in options and ('count' not in options or options['count'] is False):
            options['skipCount'] = True

        return self._get('search', **options)
