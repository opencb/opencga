from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient


class Variant(_ParentRestClient):
    """
    This class contains method for AnalysisVariant ws
    """

    def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):
        _category = "analysis/variant"
        super(Variant, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)

    def index(self, **options):
        """
        Index variant files into the variant storage
        URL: /{apiVersion}/analysis/variant/index

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('index', **options)

    def delete_file(self, **options):
        """
        Remove variant files from the variant storage
        URL: /{apiVersion}/analysis/variant/file/delete

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        :param file: Files to remove
        :param resume: Resume a previously failed indexation
        """

        return self._delete('file/delete', **options)

    def aggregation_stats(self, **options):
        """
        Calculate and fetch aggregation stats
        URL: /{apiVersion}/analysis/variant/aggregationStats

        :param fields: List of facet fields separated by semicolons,
            e.g.: studies;type. For nested faceted fields use >>,
            e.g.: chromosome>>type;percentile(gerp)
        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the
            format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome
            name or regions in the format chr:start-end, e.g.: 2,3:100000-200000
        :param type: List of types, accepted values are SNV, MNV, INDEL, SV,
            CNV, INSERTION, DELETION, e.g. SNV,INDEL
        :param reference: Reference allele
        :param alternate: Main alternate allele
        :param project: Project [user@]project where project can be either the ID
            or the alias
        :param study: Filter variants from the given studies, these can be either
            the numeric ID or the alias with the format user@project:study
        :param cohort: Select variants with calculated stats for the selected cohorts
        :param maf: Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}.
            e.g. ALL<=0.4
        :param mgf: Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}.
            e.g. ALL<=0.4
        :param missingAlleles: Number of missing alleles:
            [{study:}]{cohort}[<|>|<=|>=]{number}
        :param missingGenotypes: Number of missing genotypes:
            [{study:}]{cohort}[<|>|<=|>=]{number}
        :param annotationExists: Return only annotated variants (Boolean, False/True)
        :param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...).
            This is an alias to 'xref' parameter
        :param ct: List of SO consequence types, e.g. missense_variant,stop_lost or
            SO:0001583,SO:0001578
        :param xref: List of any external reference, these can be genes, proteins or
            variants. Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, ...
        :param biotype: List of biotypes, e.g. protein_coding
        :param proteinSubstitution: Protein substitution scores include SIFT and PolyPhen.
            You can query using the score {protein_score}[<|>|<=|>=]{number} or the
            description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant
        :param conservation: Filter by conservation score:
            {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1
        :param populationFrequencyAlt: Alternate Population Frequency:
            {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyRef: Reference Population Frequency:
            {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyMaf: Population minor allele frequency:
            {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param transcriptionFlag: List of transcript annotation flags. e.g. CCDS, basic,
            cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno
        :param geneTraitId: List of gene trait association id. e.g. "umls:C0007222", "OMIM:269600"
        :param go: List of GO (Gene Ontology) terms. e.g. "GO:0002020"
        :param expression: List of tissues of interest. e.g. "lung"
        :param proteinKeyword: List of Uniprot protein variant annotation keywords
        :param drug: List of drug names
        :param functionalScore: Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3
        :param clinicalSignificance: Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic
        :param customAnnotation: Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}
        :param trait: List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...
        """

        return self._get('aggregationStats', **options)

    def samples(self, **options):
        """
        Get samples given a set of variants
        URL: /{apiVersion}/analysis/variant/samples

        :param study: Study where all the samples belong to
        :param sample:List of samples to check. By default, all samples
        :param sampleAnnotation: Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456
        :param genotype: Genotypes that the sample must have to be selected (default 0/1,1/1)
        :param all: Samples must be present in ALL variants or in ANY variant. (Boolean, default is False)
        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome
            name or regions in the format chr:start-end, e.g.: 2,3:100000-200000
        :param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter
        :param type: List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL
        :param ct: List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578
        :param populationFrequencyAlt: Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        """

        return self._get('samples', **options)

    def metadata(self, **options):
        """
        <PENDING>
        URL: /{apiVersion}/analysis/variant/metadata

        :param project: Project [user@]project where project can be either the
            ID or the alias
        :param study: Filter variants from the given studies, these can be either
            the numeric ID or the alias with the format user@project:study
        :param file: Filter variants from the files specified. This will set
            includeFile parameter when not provided
        :param sample: Filter variants where the samples contain the variant
            (HET or HOM_ALT). Accepts AND (;) and OR (,) operators. This
            will automatically set 'includeSample' parameter when not provided
        :param includeStudy: List of studies to include in the result. Accepts
            'all' and 'none'.
        :param includeFile: List of files to be returned. Accepts 'all' and 'none'.
        :param includeSample: List of samples to be included in the result.
            Accepts 'all' and 'none'.
        :param include: Fields included in the response, whole JSON path must
            be provided
        :param exclude: Fields excluded in the response, whole JSON path must
            be provided
        """

        return self._get('metadata', **options)

    def query(self, **options):
        """
        Fetch variants from a VCF/gVCF file
        URL: /{apiVersion}/analysis/variant/query

        :param groupBy: Group variants by: [ct, gene, ensemblGene]
        :param histogram: Calculate histogram (Boolean, default is False). Requires one region.
        :param interval: Histogram interval size (default 2000)
        :param rank: Ranks different entities with the most number of variants. Rank by: [ct, gene, ensemblGene]
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (Boolean, False/True)
        :param skipCount: Do not count total number of results (Boolean, False/True)
        :param sort: Sort the results (Boolean, False/True)
        :param summary: Fast fetch of main variant parameters (Boolean, False/True)
        :param approximateCount: Get an approximate count, instead of an exact total count. Reduces execution time (Boolean, False/True)
        :param approximateCountSamplingSize: Sampling size to get the approximate
            count. Larger values increase accuracy but also increase execution time
        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome
            name or regions in the format chr:start-end, e.g.: 2,3:100000-200000
        :param type: List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL
        :param reference: Reference allele
        :param alternate: Main alternate allele
        :param project: Project [user@]project where project can be either the ID or the alias
        :param study: Filter variants from the given studies, these can be
            either the numeric ID or the alias with the format user@project:study
        :param file: Filter variants from the files specified. This will set includeFile parameter when not provided
        :param filter: Specify the FILTER for any of the files. If 'file' filter
            is provided, will match the file and the filter. e.g.: PASS,LowGQX
        :param qual: Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. e.g.: >123.4
        :param info: Filter by INFO attributes from file.
            [{file}:]{key}{op}{value}[,;]* . If no file is specified, will use
            all files from "file" filter. e.g. AN>200 or file_1.vcf:AN>200;file_2.vcf:AN<10 .
            Many INFO fields can be combined. e.g. file_1.vcf:AN>200;DB=true;file_2.vcf:AN<10
        :param sample: Filter variants where the samples contain the variant
            (HET or HOM_ALT). Accepts AND (;) and OR (,) operators. This will
            automatically set 'includeSample' parameter when not provided
        :param genotype: Samples with a specific genotype:
            {samp_1}:{gt_1}(,{gt_n})(;{samp_n}:{gt_1}(,{gt_n}))*
            e.g. HG0097:0/0;HG0098:0/1,1/1. Genotype aliases accepted: HOM_REF,
            HOM_ALT, HET, HET_REF, HET_ALT and MISS e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT.
            This will automatically set 'includeSample' parameter when not provided
        :param format: Filter by any FORMAT field from samples.
            [{sample}:]{key}{op}{value}[,;]* . If no sample is specified, will
            use all samples from "sample" or "genotype" filter. e.g. DP>200 or
            HG0097:DP>200,HG0098:DP<10 . Many FORMAT fields can be combined.
            e.g. HG0097:DP>200;GT=1/1,0/1,HG0098:DP<10
        :param sampleAnnotation: Selects some samples using metadata information
            from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith
        :param sampleMetadata: Return the samples metadata group by study.
            Sample names will appear in the same order as their corresponding genotypes. (Boolean, False/True)
        :param unknownGenotype: Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]
        :param cohort: Select variants with calculated stats for the selected cohorts
        :param maf: Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param mgf: Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param missingAlleles: Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param missingGenotypes: Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param family: Filter variants where any of the samples from the given family contains the variant (HET or HOM_ALT)
        :param familyPhenotype: Specify the phenotype to use for the mode of inheritance
        :param modeOfInheritance: Filter by mode of inheritance from a given
            family. Accepted values: [ monoallelic, monoallelicIncompletePenetrance,
            biallelic, biallelicIncompletePenetrance, XlinkedBiallelic,
            XlinkedMonoallelic, Ylinked ]
        :param includeStudy: List of studies to include in the result. Accepts 'all' and 'none'.
        :param includeFile: List of files to be returned. Accepts 'all' and 'none'.
        :param includeSample: List of samples to be included in the result. Accepts 'all' and 'none'.
        :param includeFormat: List of FORMAT names from Samples Data to include in the output. e.g: DP,AD. Accepts 'all' and 'none'.
        :param includeGenotype: Include genotypes, apart of other formats defined with includeFormat
        :param annotationExists: Return only annotated variants
        :param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter
        :param ct: List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578
        :param xref: List of any external reference, these can be genes, proteins or
            variants. Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, ...
        :param biotype: List of biotypes, e.g. protein_coding
        :param proteinSubstitution: Protein substitution scores include SIFT and
            PolyPhen. You can query using the score {protein_score}[<|>|<=|>=]{number}
            or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant
        :param conservation: Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1
        :param populationFrequencyAlt: Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyRef: Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyMaf: Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param transcriptionFlag: List of transcript annotation flags.
            e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno
        :param geneTraitId: List of gene trait association id. e.g. "umls:C0007222" , "OMIM:269600"
        :param go: List of GO (Gene Ontology) terms. e.g. "GO:0002020"
        :param expression: List of tissues of interest. e.g. "lung"
        :param proteinKeyword: List of Uniprot protein variant annotation keywords
        :param drug: List of drug names
        :param functionalScore: Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3
        :param clinicalSignificance: Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic
        :param customAnnotation: Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}
        :param panel: Filter by genes from the given disease panel
        :param trait: List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...
        """

        return self._get('query', **options)

    def export(self, data, **options):
        """
        Filter and export variants from the variant storage to a file
        URL: /{apiVersion}/analysis/variant/export

        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields included in the response, whole JSON path must be provided
        :param limit: Number of results to be returned
        :param skip: Number of results to skip
        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        :param body: Variant export params

        """

        return self._post('export', data=data, **options)

    def delete_sample_variant_stats(self, **options):
        """
        Delete sample variant stats from a sample.
        URL: /{apiVersion}/analysis/variant/sample/stats/delete

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param sample: Sample
        """

        return self._delete('sample/stats/delete', **options)

    def cohort_stats_run(self, data, **options):
        """
        Compute cohort variant stats for the selected list of samples.
        URL: /{apiVersion}/analysis/variant/cohort/stats/run

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags

        """

        return self._post('cohort/stats/run', data=data, **options)

    def cohort_stats_info(self, **options):
        """
        Read cohort variant stats from list of cohorts.
        URL: /{apiVersion}/analysis/variant/cohort/stats/info

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param cohort: Comma separated list of cohort names or ids up to a maximum of 100
        """

        return self._get('cohort/stats/info', **options)

    def cohort_stats_delete(self, **options):
        """
        Delete cohort variant stats from a cohort.
        URL: /{apiVersion}/analysis/variant/cohort/stats/delete

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param cohort: Comma separated list of cohort names or ids up to a maximum of 100
        """

        return self._delete('cohort/stats/delete', **options)

    def gwas_run(self, data, **options):
        """
        Run a Genome Wide Association Study between two cohorts.
        URL: /{apiVersion}/analysis/variant/gwas/run

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('gwas/run', data=data, **options)

    def plink_run(self, data, **options):
        """
        Plink is a whole genome association analysis toolset, designed to
        perform a range of basic, large-scale analyses.
        URL: /{apiVersion}/analysis/variant/plink/run

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('plink/run', data=data, **options)

    def rvtests_run(self, data, **options):
        """
        Rvtests is a flexible software package for genetic association studies.
        URL: /{apiVersion}/analysis/variant/rvtests/run

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('rvtests/run', data=data, **options)

    def variant_stats_run(self, data, **options):
        """
        Compute variant stats for any cohort and any set of variants.
        Optionally, index the result in the variant storage database.
        URL: /{apiVersion}/analysis/variant/stats/run

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('stats/run', data=data, **options)

    def variant_stats_export(self, data, **options):
        """
        Export calculated variant stats and frequencies
        URL: /{apiVersion}/analysis/variant/stats/export

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('stats/export', data=data, **options)

    def calculate_family_genotypes(self, mode_of_inheritance, **options):
        """
        Calculate the possible genotypes for the members of a family
        URL: /{apiVersion}/analysis/variant/familyGenotypes

        :param mode_of_inheritance: mode of inheritance
        :param disease: disease id
        """

        options['modeOfInheritance'] = mode_of_inheritance

        return self._get('familyGenotypes', **options)

    def query_variant_annotations(self, **options):
        """
        Query variant annotations from any saved versions
        URL: /{apiVersion}/analysis/variant/annotation/query

        :param annotationId: Default is 'CURRENT'. Annotation identifier
        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the
            format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome
            name or regions in the format chr:start-end, e.g.: 2,3:100000-200000
        :param include: Fields included in the response, whole JSON path must
            be provided
        :param exclude: Fields excluded in the response, whole JSON path must
            be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        """

        return self._get('annotation', subcategory='query', **options)

    def variant_annotations_metadata(self, **options):
        """
        Read variant annotations metadata from any saved versions
        URL: /{apiVersion}/analysis/variant/annotation/metadata

        :param annotationId: Annotation identifier
        :param project: Project [user@]project where project can be either the ID
            or the alias
        """

        return self._get('annotation', subcategory='metadata', **options)

    def variant_sample_run(self, data, **options):
        """
        Get samples given a set of variants
        URL: /{apiVersion}/analysis/variant/sample/run

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('sample/run', data=data, **options)

    def variant_sample_data(self, **options):
        """
        Get sample data of a given variant
        URL: /{apiVersion}/analysis/variant/sample/data

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param variant: Variant
        :param merge: Do not group by genotype. Return all genotypes merged.
        :param genotype: Genotypes that the sample must have to be selected
        """

        return self._get('sample/data', **options)

    def sample_variant_stats_run(self, data, **options):
        """
        Compute sample variant stats for the selected list of samples.
        URL: /{apiVersion}/analysis/variant/sample/stats/run

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param jobName: Job name
        :param Job ID or UUID: Job Description
        :param jobTags: Job tags
        """

        return self._post('sample/stats/run', data=data, **options)

    def sample_variant_stats_info(self, **options):
        """
        Read sample variant stats from list of samples.
        URL: /{apiVersion}/analysis/variant/sample/stats/info

        :param study: Study [[user@]project:]s…e either the ID or UUID
        :param sample: Comma separated list sample IDs or UUIDs up to a maximum of 100
        """

        return self._get('sample/stats/info', **options)

    def annotation_query(self, **options):
        """
        Query variant annotations from any saved versions
        URL: /{apiVersion}/analysis/variant/annotation/query

        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome
        :param annotation_id: Annotation identifier
        """

        return self._get('annotation/query', **options)
