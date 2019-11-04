from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient

class Families(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains methods for the Families websevices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'families'
        super(Families, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def aggregation_stats(self, **options):
        """
        Fetch catalog family stats
        URL: /{apiVersion}/families/aggregationStats

        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param creationYear: creation year
        :param creationMonth: creation month (JANUARY, FEBRUARY...)
        :param creationDay: creation day
        :param creationDayOfWeek: creation day of week (MONDAY, TUESDAY...)
        :param status: status
        :param phenotypes: phenotypes
        :param release: release
        :param version: version
        :param numMembers: number of members
        :param expectedSize: expected size
        :param annotation: annotation, e.g: key1=value(;key2=value)
        :param default: calculate default stats
        :param field: list of fields separated by semicolons, e.g.: studies;type.
            For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1
        """

        return self._get('aggregationStats', **options)

    def search(self, **options):
        """
        Search families
        URL: /{apiVersion}/families/search

        :param study: study [[user@]project:]study where study and project can be either the id or alias.
        :param name: family name
        :param parentalConsanguinity: parental consanguinity
        :param members: comma separated list of individual ids or names
        :param samples: comma separated list of sample ids or names
        :param phenotypes: comma separated list of phenotype ids or names
        :param disorders: comma separated list of disorder ids or names
        :param creationDate: creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param annotationsetName: DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}
        :param variableSet: DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}
        :param annotation: annotation, e.g: key1=value(;key2=value)
        :param skipCount: skip count
        :param release: release value (Current release from the moment the families were first created)
        :param snapshot: snapshot value (Latest version of families in the specified release)
        :param include: fields included in the response, whole JSON path must be provided
        :param exclude: fields excluded in the response, whole JSON path must be provided
        :param limit: number of results to be returned in the queries
        :param skip: number of results to skip in the queries
        :param count: total number of results
        :param flattenAnnotations: flatten the annotations?
        """

        return self._get('search', **options)
