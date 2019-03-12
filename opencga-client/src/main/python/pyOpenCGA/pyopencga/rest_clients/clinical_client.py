from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient 

class Clinical(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains methods for the Analysis - Clinical webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category  = 'analysis/clinical'
        super(Clinical, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def search(self, **options):
        """
        Clinical analysis search.
        URL: /{apiVersion}/analysis/clinical/search
        """

        return self._get('search', **options)

    def tiering_interpretation(self, **options):
        """
        GEL Tiering interpretation analysis (PENDING)
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/tiering
        """

        return self._get('interpretation', subcategory='tools/tiering', **options)

    def team_interpretation(self, **options):
        """
        TEAM interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/team
        """

        return self._get('interpretation', subcategory='tools/team', **options)

    def custom_interpretation(self, **options):
        """
        Interpretation custom analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/custom
        """

        return self._get('interpretation', subcategory='tools/custom', **options)

    def interpretation_stats(self, **options):
        """
        Clinical interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/stats
        """

        return self._get('interpretation', subcategory='stats', **options)

    def query_interpretation(self, **options):
        """
        Query for reported variants
        URL: /{apiVersion}/analysis/clinical/interpretation/query
        """

        return self._get('interpretation', subcategory='query', **options)

    def index_interpretation(self, **options):
        """
        Index clinical analysis interpretations in the clinical variant database
        URL: /{apiVersion}/analysis/clinical/interpretation/index
        """

        return self._get('interpretation', subcategory='index', **options)

    def update_interpretations(self, clinical_analysis, data, action, **options):
        """
        Add or remove Interpretations to/from a Clinical Analysis
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/update
        
        :param clinical_analysis: clinical analysis id
        :param action: Action to be performed if the array of interpretations is being updated [ADD, REMOVE]
        """
        
        return self._post('interpretations', query_id=clinical_analysis, subcategory='update',
                           data=data, action=action, **options)

    def update_interpretation(self, clinical_analysis, interpretation, data, **options):
        """
        Update Interpretation fields
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/update
        
        :param clinical_analysis: clinical analysis id
        :param interpretation: interpretation id
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='update',
                          subquery_id=interpretation, data=data, **options)

    def update_reported_variants(self, clinical_analysis, interpretation, data, **options):
        """
        Update reported variants of an interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/reportedVariants/update
        """

        return self._post('interpretations', query_id=clinical_analysis, subquery_id=interpretation,
                           subcategory='reportedVariants/update', data=data, **options)

    def update_interpretation_comments(self, clinical_analysis, interpretation, data, action, **options):
        """
        Update comments of an Interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/comments/update

        :param action: Action to be performed [ADD, SET, REMOVE]
        """

        return self._post('interpretations', query_id=clinical_analysis, subquery_id=interpretation,
                          subcategory='comments/update', data=data, action=action, **options)

