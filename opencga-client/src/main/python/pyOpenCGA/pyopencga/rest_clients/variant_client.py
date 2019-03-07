from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient

class AnalysisVariant(_ParentRestClient):
    """
    This class contains method for AnalysisVariant ws
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "analysis/variant"
        super(AnalysisVariant, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def index(self, file, **options):
        """
        Index variant files
        URL: /{apiVersion}/analysis/variant/index

        :param file: comma separated list of file ids (files or directories)
        """
        options['file'] = file

        return self._get('index', **options)

    def validate_vcf(self, file, data, **options):
        """
        Validate a VCF file
        URL: /{apiVersion}/analysis/variant/validate

        :param file: VCF file id, name or path
        """

        options['file'] = file
        
        return self._post('validate', data=data, **options)

    def get_stats(self, **options):
        """
        Fetch variant stats
        URL: /{apiVersion}/analysis/variant/stats
        """

        return self._get('stats', **options)

    def get_samples_from_variants(self, **options):
        """
        Get samples given a set of variants
        URL: /{apiVersion}/analysis/variant/samples
        """

        return self._get('samples', **options)

    def get_metadata(self, **options):
        """
        <PENDING>
        URL: /{apiVersion}/analysis/variant/metadata
        """

        return self._get('metadata', **options)

    def query(self, data, **options):
        """
        Fetch variants from a VCF/gVCF file
        URL: /{apiVersion}/analysis/variant/query
        """

        return self._post('query', data=data, **options)

    def calculate_family_genotypes(self, family, mode_of_inheritance, disease, **options):
        """
        Calculate the possible genotypes for the members of a family
        URL: /{apiVersion}/analysis/variant/familyGenotypes
        
        :param family: family id
        :param mode_of_inheritance: mode of inheritance
        :param disease: disease id
        """

        options['family'] = family
        options['modeOfInheritance'] = mode_of_inheritance
        options['disease'] = disease
        
        return self._get('familyGenotypes', **options)

    def facet(self, **options): ## [DEPRECATED]
        """
        This method has been renamed, use endpoint /stats instead [DEPRECATED]
        URL: /{apiVersion}/analysis/variant/facet
        """

        return self._get('facet', **options)

    def calculate_variant_stats(self, **options): ## [PENDING]
        """
        Calculate variant stats [PENDING]
        URL: /{apiVersion}/analysis/variant/cohortStats
        """

        return self._get('cohortStats', **options)

    def query_variant_annotations(self, **options):
        """
        Query variant annotations from any saved versions
        URL: /{apiVersion}/analysis/variant/annotation/query
        """

        return self._get('annotation', subcategory='query', **options)

    def variant_annotations_metadata(self, **options):
        """
        Read variant annotations metadata from any saved versions
        URL: /{apiVersion}/analysis/variant/annotation/metadata
        """

        return self._get('annotation', subcategory='metadata', **options)

