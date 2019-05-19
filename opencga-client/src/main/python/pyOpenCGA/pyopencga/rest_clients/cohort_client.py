from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient

class Cohorts(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Cohorts ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'cohorts'
        super(Cohorts, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def aggregation_stats(self, **options):
        """
        Fetch catalog cohort stats
        URL: /{apiVersion}/cohorts/aggregationStats

        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param type: Type
        :param creationYear: Creation year
        :param creationMonth: Creation month (JANUARY, FEBRUARY...)
        :param creationDay: Creation day
        :param creationDayOfWeek: Creation day of week (MONDAY, TUESDAY...)
        :param numSamples: Number of samples
        :param status: Status
        :param release: Release
        :param annotation: Annotation, e.g: key1=value(;key2=value)
        :param default: Calculate default stats (Boolean, default is False)
        :param field: List of fields separated by semicolons, e.g.: studies;type.
            For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1
        """

        return self._get('aggregationStats', **options)

    def search(self, **options):
        """
        Search cohorts
        URL: /{apiVersion}/cohorts/search

        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param name: [DEPRECATED] Name of the cohort
        :param type: Cohort type (see available values below)
        :param creationDate: Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param status: Status
        :param annotation: Annotation, e.g: key1=value(;key2=value)
        :param samples: Sample list
        :param skipCount: Skip count (Boolean, default is False)
        :param release: Release value
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude:Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (Boolean, default is False)
        :param flattenAnnotations: Flatten the annotations? (Boolean, default is False)

        * Available cohort type values:
        [
        'CASE_CONTROL',
        'CASE_SET',
        'CONTROL_SET',
        'PAIRED',
        'PAIRED_TUMOR',
        'AGGREGATE',
        'TIME_SERIES',
        'FAMILY',
        'TRIO',
        'COLLECTION'
        ]
        """

        return self._get('search', **options)

    def samples(self, cohort, **options):
        """
        Get samples from cohort [DEPRECATED]
        The usage of this webservice is discouraged. /{cohorts}/info is expected
        to be used with &include=samples query parameter to approximately simulate
        this same behaviour.
        URL: /{apiVersion}/cohorts/{cohort}/samples

        :param cohort: cohort id
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (Boolean, default is False)
        """

        return self._get('samples', query_id=cohort, **options)
