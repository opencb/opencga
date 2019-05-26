from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient

class Alignment(_ParentRestClient):
    """
    This class contains methods for the AnalysisAlignment webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'analysis/alignment'
        super(Alignment, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def log2_coverage_ratio(self, file1, file2, **options):
        """
        Compute log2 coverage ratio from file #1 and file #2
        URL: /{apiVersion}/analysis/alignment/log2CoverageRatio

        :param file1: File #1 (e.g., somatic file ID or name in Catalog)
        :param file2: File #2 (e.g., germline file ID or name in Catalog)
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param region: Comma separated list of regions 'chr:start-end'
        :param gene: Comma separated list of genes
        :param geneOffset: Gene offset (to extend the gene region at up and downstream) (default 500 bp)
        :param onlyExons: Only exons
        :param exonOffset:Exon offset (to extend the exon region at up and downstream) (default 50 bp)
        :param windowSize: Window size (default 1)
        """

        options['file1'] = file1
        options['file2'] = file2

        return self._get('log2CoverageRatio', **options)

    def stats(self, file, **options):
        """
        Fetch the stats of an alignment file
        URL: /{apiVersion}/analysis/alignment/stats

        :param file: Id of the alignment file in catalog
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param region: Comma separated list of regions 'chr:start-end'
        :param minMapQ: Minimum mapping quality
        :param contained: Only alignments completely contained within boundaries of region (bool: ['true','false'])
        """

        options['file'] = file

        return self._get('stats', **options)

    def index(self, file, **options):
        """
        Index alignment files
        URL: /{apiVersion}/analysis/alignment/index

        :param file: Comma separated list of file ids (files or directories)
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param outDir: Output directory id
        :param transform: Boolean indicating that only the transform step will be run (bool: ['true','false' default])
        :param load: Boolean indicating that only the load step will be run (bool: ['true','false' default])
        """

        options['file'] = file

        return self._get('index', **options)

    def query(self, file, region, **options):
        """
        Fetch alignments from a BAM file
        URL: /{apiVersion}/analysis/alignment/query

        :param file: File ID or name in Catalog
        :param study: Study [[user@]project:]study where study and project can be either the Id or alias
        :param region: Comma-separated list of regions 'chr:start-end'
        :param minMapQ: Minimum mapping quality
        :param maxNM: Maximum number of mismatches
        :param maxNH: Maximum number of hits
        :param properlyPaired: Return only properly paired alignments (bool: ['true','false' default])
        :param maxInsertSize: Maximum insert size
        :param skipUnmapped: Skip unmapped alignments (bool: ['true','false' default])
        :param skipDuplicated: Skip duplicated alignments (bool: ['true','false' default])
        :param contained: Return alignments contained within boundaries of region (bool: ['true','false' default])
        :param mdField: Force SAM MD optional field to be set with the alignments (bool: ['true','false' default])
        :param binQualities: Compress the nucleotide qualities by using 8 quality levels
        :param limit: Max number of results to be returned
        :param skip: Number of results to skip
        :param count: Return total number of results (bool: ['true','false' default])
        """

        options['file'] = file
        options['region'] = region

        return self._get('query', **options)

    def coverage(self, file, **options):
        """
        Fetch the coverage of an alignment file
        URL: /{apiVersion}/analysis/alignment/coverage

        :param file: File ID or name in Catalog
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param region: Comma separated list of regions 'chr:start-end'
        :param gene: Comma separated list of genes
        :param geneOffset: Gene offset (to extend the gene region at up and downstream) (default 500 bp)
        :param onlyExons: Only exons (bool: ['true','false' default])
        :param exonOffset: Exon offset (to extend the exon region at up and downstream) (default 50 bp)
        :param threshold: Range of coverage values to be reported. Minimum and maximum values are separated by '-', e.g.: 20-40 (for coverage
           values greater or equal to 20 and less or equal to 40). A single value means to report coverage values greater or equal to that value.
        :param windowSize: Window size (if a threshold is provided, window size must be 1) (default 1)
        """

        options['file'] = file

        return self._get('coverage', **options)
