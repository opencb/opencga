from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient

class Alignment(_ParentRestClient):
    """
    This class contains methods for the AnalysisAlignment webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'analysis/alignment'
        super(Alignment, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def index(self, file, **options):
        """
        Index alignment files
        URL: /{apiVersion}/analysis/alignment/index

        :param file: Comma separated list of file ids (files or directories)
        """

        options['file'] = file

        return self._get('index', **options)

    def stats(self, file, **options):
        """
        Fetch the stats of an alignment file
        URL: /{apiVersion}/analysis/alignment/stats

        :param file: file id or name in Catalog
        """

        options['file'] = file

        return self._get('stats', **options)

    def query(self, file, region, **options):
        """
        Fetch alignments from a BAM file
        URL: /{apiVersion}/analysis/alignment/query

        :param file: file id or name in Catalog
        :param region: comma-separated list of regions 'chr:start-end'
        """

        options['file'] = file
        options['region'] = region

        return self._get('query', **options)

    def coverage(self, file, **options):
        """
        Fetch the coverage of an alignment file
        URL: /{apiVersion}/analysis/alignment/coverage

        :param file: file id or name in Catalog
        """

        options['file'] = file

        return self._get('coverage', **options)

