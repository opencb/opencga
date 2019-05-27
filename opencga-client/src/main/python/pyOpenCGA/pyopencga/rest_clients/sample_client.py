from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient

class Samples(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Samples webservice
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'samples'
        super(Samples, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def aggregation_stats(self, **options):
        """
        Fetch catalog sample stats
        URL: /{apiVersion}/samples/aggregationStats

        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param source: Source
        :param creationYear: Creation year
        :param creationMonth: Creation month (JANUARY, FEBRUARY...)
        :param creationDay: Creation day
        :param creationDayOfWeek: Creation day of week (MONDAY, TUESDAY...)
        :param status: Status
        :param type: Type
        :param phenotypes: Phenotypes
        :param release: Release
        :param version: Version
        :param somatic: Somatic (Boolean, True/False)
        :param annotation: Annotation, e.g: key1=value(;key2=value)
        :param default: Calculate default stats (Boolean, False as default)
        :param field: List of fields separated by semicolons, e.g.: studies;type. For nested
            fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1
        """

        return self._get('aggregationStats', **options)

    def search(self, **options):
        """
        Sample search method
        URL: /{apiVersion}/samples/search

        :param study: Study [[user@]project:]{study1,study2|*} where studies and project can be either the id or alias.
        :param name: [DEPRECATED] name
        :param source: source
        :param type: type
        :param somatic: boolean (True, False)
        :param individual: Individual id or name
        :param creationDate: Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param phenotypes: Comma separated list of phenotype ids or names
        :param annotationsetName: DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}
        :param variableSet: [DEPRECATED] Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}
        :param annotation: Annotation, e.g: key1=value(;key2=value)
        :param attributes: Text attributes (Format: sex=male,age>20 ...)
        :param nattributes: Numerical attributes (Format: sex=male,age>20 ...)
        :param skipCount: Skip count (Boolean, False as default)
        :param release: Release value (Current release from the moment the samples were first created)
        :param snapshot: Snapshot value (Latest version of samples in the specified release)
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (Boolean, False as default)
        :param includeIndividual: Include Individual object as an attribute (this replaces old lazy parameter) (Boolean, False as default)
        :param flattenAnnotations: Flatten the annotations? (Boolean, False as default)
        """

        return self._get('search', **options)

    def load(self, file, **options):
        """
        Load samples from a ped file [EXPERIMENTAL]
        URL: /{apiVersion}/samples/load

        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param file: Filename of study linked file
        :param variableSet: variableSet
        """

        options['file'] = file

        return self._get('load', **options)
