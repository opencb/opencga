from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient,  _ParentAnnotationSetRestClient

class Individuals(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains methods for the Individuals webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'individuals'
        super(Individuals, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def aggregation_stats(self, **options):
        """
        Fetch catalog individual stats
        URL: /{apiVersion}/individuals/aggregationStats

        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param hasFather: has father (Bool, default=None)
        :param hasMother: has mother (Bool, deafult=None)
        :param numMultiples: number of multiples
        :param multiplesType: multiples type
        :param sex: sex
        :param karyotypicSex: karyotypic sex
        :param ethnicity: ethnicity
        :param population: population
        :param creationYear: creation year
        :param creationMonth: creation month (JANUARY, FEBRUARY...)
        :param creationDay: creation day
        :param creationDayOfWeek: creation day of week (MONDAY, TUESDAY...)
        :param status: status
        :param lifeStatus: life status
        :param affectationStatus: affectation status
        :param phenotypes: phenotypes
        :param numSamples: number of samples
        :param parentalConsanguinity: parental consanguinity (Bool, default=None)
        :param release: release
        :param version: version
        :param annotation: annotation, e.g: key1=value(;key2=value)
        :param default: calculate default stats (Bool, deafult=false)
        :param field: list of fields separated by semicolons, e.g.: studies;type. For nested fields use >>,
            e.g.: studies>>biotype;type;numSamples[0..10]:1
        """

        return self._get('aggregationStats', **options)

    def search(self, **options):
        """
        Search for individuals
        URL: /{apiVersion}/individuals/search

        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param id: id
        :param name: name
        :param father: father
        :param mother: mother
        :param samples: comma separated list of sample ids or names
        :param sex: sex
        :param ethnicity: ethnicity
        :param disorders: comma separated list of disorders ids or names
        :param population.name: population name
        :param population.subpopulation: subpopulation name
        :param population.description: population description
        :param phenotypes: comma separated list of phenotype ids or names
        :param karyotypicSex: karyotypic sex (deafult = None)
            ['UNKNOWN', 'XX', 'XY', 'XO', 'XXY', 'XXX', 'XXYY', 'XXXY', 'XXXX', 'XYY', 'OTHER']
        :param lifeStatus: life status (deafult = None) ['ALIVE', 'ABORTED', 'DECEASED', 'UNBORN', 'STILLBORN', 'MISCARRIAGE', 'UNKNOWN']
        :param affectationStatus: affectation status (default = None) ['CONTROL', 'AFFECTED', 'UNAFFECTED', 'UNKNOWN']
        :param creationDate: creation date (Format: yyyyMMddHHmmss)
        :param modificationDate: Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param annotationsetName: DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}
        :param variableSet: DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}
        :param annotation: annotation, e.g: key1=value(;key2=value)
        :param skipCount: skip count (Bool, deafult=false)
        :param release: release value (Current release from the moment the individuals were first created)
        :param snapshot: snapshot value (Latest version of individuals in the specified release)
        :param include: fields included in the response, whole JSON path must be provided
        :param exclude: fields excluded in the response, whole JSON path must be provided
        :param limit: number of results to be returned in the queries
        :param skip: number of results to skip in the queries
        :param count: total number of results (Bool, deafult=false)
        :param flattenAnnotations: flatten the annotations? (Bool, default=false)
        """

        return self._get('search', **options)
