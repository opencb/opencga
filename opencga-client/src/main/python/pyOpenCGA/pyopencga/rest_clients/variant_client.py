from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient

class Variant(_ParentRestClient):
    """
    This class contains method for AnalysisVariant ws
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "analysis/variant"
        super(Variant, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def index(self, file, **options):
        """
        Index variant files
        URL: /{apiVersion}/analysis/variant/index

        :param file: comma separated list of file ids (files or directories)
        :param study: Study [[user@]project:]study where study and project can
            be either the id or alias
        :param outDir: Output directory id
        :param transform: Boolean (False/True) indicating that only the transform
            step will be run
        :param load: Boolean (False/True) indicating that only the load step will
            be run
        :param merge: Currently two levels of merge are supported: "basic" mode
            merge genotypes of the same variants while "advanced" merge multiallelic
            and overlapping variants. Available values ['ADVANCED' as default, 'BASIC']
        :param includeExtraFields: Index including other FORMAT fields. Use "all",
            "none", or CSV with the fields to load.
        :param aggregated: Default 'none'. Type of aggregated VCF file: none, basic,
            EVS or ExAC
        :param calculateStats: Calculate indexed variants statistics after the
            load step (Boolean False/True).
        :param annotate: Annotate indexed variants after the load step (Boolean False/True)
        :param overwrite: Overwrite annotations already present in variants
            (Boolean False/True)
        :param indexSearch: Add files to the secondary search index (Boolean False/True)
        :param resume: Resume a previously failed indexation (Boolean False/True)
        :param loadSplitData: Indicate that the variants from a sample (or group of
            samples) split into different files (by chromosome, by type, ...)
            (Boolean False/True)
        :param skipPostLoadCheck: Do not execute post load checks over the database
        """

        options['file'] = file

        return self._get('index', **options)

    # def validate(self, file, data, **options):
    #     """
    #     Validate a VCF file ??
    #     URL: /{apiVersion}/analysis/variant/validate
    #
    #     :param file: VCF file id, name or path
    #     """
    #
    #     options['file'] = file
    #
    #     return self._post('validate', data=data, **options)

    def aggregation_stats(self, **options):
        """
        Fetch variant stats
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

    def calculate_family_genotypes(self, family, mode_of_inheritance, **options):
        """
        Calculate the possible genotypes for the members of a family
        URL: /{apiVersion}/analysis/variant/familyGenotypes

        :param family: family id
        :param mode_of_inheritance: mode of inheritance
        :param disease: disease id
        """

        options['family'] = family
        options['modeOfInheritance'] = mode_of_inheritance

        return self._get('familyGenotypes', **options)

    # def calculate_variant_stats(self, **options): ## [PENDING]
    #     """
    #     Calculate variant stats [PENDING]
    #     URL: /{apiVersion}/analysis/variant/cohortStats
    #     """
    #
    #     return self._get('cohortStats', **options)

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
