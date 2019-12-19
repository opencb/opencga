from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient
from pyopencga.commons import deprecated


class Alignment(_ParentRestClient):
    """
    This class contains methods for the AnalysisAlignment webservices
    """

    def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):
        _category = 'analysis/alignment'
        super(Alignment, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)

    def index(self, file, **options):
        """
        Index alignment files
        URL: /{apiVersion}/analysis/alignment/index

        :param file: Comma separated list of file ids (files or directories)
        """

        options['file'] = file

        return self._post('index', **options)

    @deprecated
    def stats(self, file, **options):
        return self.stats_info(file, **options)

    def stats_run(self, file, **options):
        """
        Compute stats for a given alignment file
        URL: /{apiVersion}/analysis/alignment/stats/run

        :param file: file id or name in Catalog
        """

        options['file'] = file

        return self._post('stats/run', **options)

    def stats_info(self, file, **options):
        """
        Show the stats for a given alignment file
        URL: /{apiVersion}/analysis/alignment/stats/info

        :param file: file id or name in Catalog
        """

        options['file'] = file

        return self._get('stats/info', **options)

    def stats_query(self, **options):
        """
        Fetch alignment files according to their stats
        URL: /{apiVersion}/analysis/alignment/stats/query
        """

        return self._get('stats/query', **options)

    def bwa_run(self, **options):
        """
        BWA is a software package for mapping low-divergent sequences against a
        large reference genome.
        URL: /{apiVersion}/analysis/alignment/bwa/run
        """

        return self._post('bwa/run', **options)

    def samtools_run(self, **options):
        """
        Samtools is a program for interacting with high-throughput sequencing
        data in SAM, BAM and CRAM formats.
        URL: /{apiVersion}/analysis/alignment/samtools/run
        """

        return self._post('samtools/run', **options)

    def deeptools_run(self, **options):
        """
        Deeptools is a suite of python tools particularly developed for the
        efficient analysis of high-throughput sequencing data, such as ChIP-seq,
        RNA-seq or MNase-seq.
        URL: /{apiVersion}/analysis/alignment/deeptools/run
        """

        return self._post('deeptools/run', **options)

    def query(self, file, **options):
        """
        Search over indexed alignments
        URL: /{apiVersion}/analysis/alignment/query

        :param file: file id or name in Catalog
        :param region: comma-separated list of regions 'chr:start-end'
        """

        options['file'] = file

        return self._get('query', **options)

    def coverage_run(self, file, **options):
        """
        Compute coverage for a list of alignment files
        URL: /{apiVersion}/analysis/alignment/coverage/run

        :param file: file id or name in Catalog
        """

        options['file'] = file

        return self._post('coverage/run', **options)

    def coverage_query(self, file, **options):
        """
        Query the coverage of an alignment file for regions or genes
        URL: /{apiVersion}/analysis/alignment/coverage/query

        :param file: file id or name in Catalog
        """

        options['file'] = file

        return self._get('coverage/query', **options)

    def coverage_ratio(self, file1, file2, **options):
        """
        Compute coverage ratio from file #1 vs file #2, (e.g. somatic vs germline)
        URL: /{apiVersion}/analysis/alignment/coverage/ratio

        :param file1: file1 id or name in Catalog
        :param file2: file2 id or name in Catalog
        """

        options['file1'] = file1
        options['file2'] = file2

        return self._get('coverage/ratio', **options)
