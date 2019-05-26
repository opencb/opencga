from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient, _ParentBasicCRUDClient, _ParentAclRestClient


class Interpretations(_ParentRestClient):
    """
    This class contains the Interpretations client with methods for the
    Analysis - Clinical webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'analysis/clinical'
        super(Interpretations, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def secondary_findings(self, **options):
        """
        Search for secondary findings for a list of samples
        URL: /{apiVersion}/analysis/clinical/interpretation/secondaryFindings

        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param sample: Comma separated list of samples
        """

        return self._get('interpretation', subcategory='secondaryFindings', **options)

    def index(self, **options):
        """
        Index clinical analysis interpretations in the clinical variant database
        URL: /{apiVersion}/analysis/clinical/interpretation/index

        :param interpretationId: Comma separated list of interpretation IDs to be indexed in the clinical variant database
        :param clinicalAnalysisId: Comma separated list of clinical analysis IDs to be indexed in the clinical variant database
        :param false: Reset the clinical variant database and import the specified interpretations (bool: ['true', 'false'], default None)
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        """

        return self._get('interpretation', subcategory='index', **options)

    def query(self, **options):
        """
        Query for reported variants
        URL: /{apiVersion}/analysis/clinical/interpretation/query

        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (bool: ['true', 'false'])
        :param skipCount: Do not count total number of results (bool: ['true', 'false'])
        :param sort: Sort the results (bool: ['true', 'false'])
        :param summary: Fast fetch of main variant parameters (bool: ['true', 'false'])
        :param approximateCount: Get an approximate count, instead of an exact total count. Reduces execution time (bool: ['true', 'false'])
        :param approximateCountSamplingSize: Sampling size to get the approximate count. Larger values increase accuracy but also increase execution time
        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000
        :param type: List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL
        :param reference: Reference allele
        :param alternate: Main alternate allele
        :param project: Project [user@]project where project can be either the ID or the alias
        :param study: Filter variants from the given studies, these can be either the numeric ID or the alias with the format user@project:study
        :param file: Filter variants from the files specified. This will set includeFile parameter when not provided
        :param filter: Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter. e.g.: PASS,LowGQX
        :param qual: Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. e.g.: >123.4
        :param info: Filter by INFO attributes from file. [{file}:]{key}{op}{value}[,;]* . If no file is specified, will use all files from
            "file" filter. e.g. AN>200 or file_1.vcf:AN>200;file_2.vcf:AN<10 . Many INFO fields can be combined.
            e.g. file_1.vcf:AN>200;DB=true;file_2.vcf:AN<10
        :param sample: Filter variants where the samples contain the variant (HET or HOM_ALT). Accepts AND (;) and OR (,) operators. This
            will automatically set 'includeSample' parameter when not provided
        :param genotype: Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})(;{samp_n}:{gt_1}(,{gt_n}))* e.g. HG0097:0/0;HG0098:0/1,1/1.
            Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. Genotype aliases
            accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. This will automatically
            set 'includeSample' parameter when not provided
        :param format: Filter by any FORMAT field from samples. [{sample}:]{key}{op}{value}[,;]* . If no sample is specified, will use all
            samples from "sample" or "genotype" filter. e.g. DP>200 or HG0097:DP>200,HG0098:DP<10 . Many FORMAT fields can be combined.
            e.g. HG0097:DP>200;GT=1/1,0/1,HG0098:DP<10
        :param sampleAnnotation: Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith
        :param sampleMetadata: Return the samples metadata group by study. Sample names will appear in the same order as their corresponding
            genotypes. (bool: ['true','false'])
        :param unknownGenotype: Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]
        :param sampleLimit: Limit the number of samples to be included in the result
        :param sampleSkip: Skip some samples from the result. Useful for sample pagination.
        :param cohort: Select variants with calculated stats for the selected cohorts
        :param cohortStatsRef: Reference Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsAlt: Alternate Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsMaf: Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsMgf: Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param missingAlleles: Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param missingGenotypes: Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param includeStudy: List of studies to include in the result. Accepts 'all' and 'none'.
        :param includeFile: List of files to be returned. Accepts 'all' and 'none'.
        :param includeSample: List of samples to be included in the result. Accepts 'all' and 'none'.
        :param includeFormat: List of FORMAT names from Samples Data to include in the output. e.g: DP,AD. Accepts 'all' and 'none'.
        :param includeGenotype: Include genotypes, apart of other formats defined with includeFormat
        :param annotationExists: Return only annotated variants (bool: ['true','false'])
        :param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter
        :param ct: List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578
        :param xref: List of any external reference, these can be genes, proteins or variants. Accepted IDs include HGNC, Ensembl genes,
            dbSNP, ClinVar, HPO, Cosmic, ...
        :param biotype: List of biotypes, e.g. protein_coding
        :param proteinSubstitution: Protein substitution scores include SIFT and PolyPhen. You can query using the score
            {protein_score}[<|>|<=|>=]{number} or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant
        :param conservation: Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1
        :param populationFrequencyAlt: Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyRef: Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyMaf: Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param transcriptFlag: List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF,
            seleno
        :param geneTraitId: List of gene trait association id. e.g. "umls:C0007222" , "OMIM:269600"
        :param go: List of GO (Gene Ontology) terms. e.g. "GO:0002020"
        :param expression: List of tissues of interest. e.g. "lung"
        :param proteinKeyword: List of Uniprot protein variant annotation keywords
        :param drug: List of drug names
        :param functionalScore: Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3
        :param clinicalSignificance: Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic
        :param customAnnotation: Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}
        :param trait: List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...
        :param clinicalAnalysisId: List of clinical analysis IDs
        :param clinicalAnalysisName: List of clinical analysis names
        :param clinicalAnalysisDescr: Clinical analysis description
        :param clinicalAnalysisFiles: List of clinical analysis files
        :param clinicalAnalysisProbandId: List of proband IDs
        :param clinicalAnalysisProbandDisorders: List of proband disorders
        :param clinicalAnalysisProbandPhenotypes: List of proband phenotypes
        :param clinicalAnalysisFamilyId: List of family IDs
        :param clinicalAnalysisFamMemberIds: List of clinical analysis family member IDs
        :param interpretationId: List of interpretation IDs
        :param interpretationSoftwareName: List of interpretation software names
        :param interpretationSoftwareVersion: List of interpretation software versions
        :param interpretationAnalystName: List of interpretation analysist names
        :param interpretationPanels: List of interpretation panels
        :param interpretationDescription: Interpretation description
        :param interpretationDependencies: List of interpretation dependency, format: name:version, e.g. cellbase:4.0
        :param interpretationFilters: List of interpretation filters
        :param interpretationComments: List of interpretation comments
        :param interpretationCreationDate: Iinterpretation creation date (including date ranges)
        :param reportedVariantDeNovoQualityScore: List of reported variant de novo quality scores
        :param reportedVariantComments: List of reported variant comments
        :param reportedEventPhenotypeNames: List of reported event phenotype names
        :param reportedEventConsequenceTypeIds: List of reported event consequence type IDs
        :param reportedEventXrefs: List of reported event phenotype xRefs
        :param reportedEventPanelIds: List of reported event panel IDs
        :param reportedEventAcmg: List of reported event ACMG
        :param reportedEventClinicalSignificance: List of reported event clinical significance
        :param reportedEventDrugResponse: List of reported event drug response
        :param reportedEventTraitAssociation: List of reported event trait association
        :param reportedEventFunctionalEffect: List of reported event functional effect
        :param reportedEventTumorigenesis: List of reported event tumorigenesis
        :param reportedEventOtherClassification: List of reported event other classification
        :param reportedEventRolesInCancer: List of reported event roles in cancer
        """

        return self._get('interpretation', subcategory='query', **options)

    def stats(self, **options):
        """
        Clinical interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/stats

        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param clinicalAnalysisId: Clinical Analysis Id
        :param disease: Disease (HPO term)
        :param familyId: Family ID
        :param subjectIds: Comma separated list of subject IDs
        :param type: Clinical analysis type, e.g. DUO, TRIO, ...
        :param panelId: Panel ID
        :param panelVersion: Panel version
        :param save: Save interpretation in Catalog (bool: ['true','false'])
        :param interpretationId: ID of the stored interpretation
        :param interpretationName: Name of the stored interpretation
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (bool: ['true','false'])
        :param skipCount: Do not count total number of results (bool: ['true','false'])
        :param sort: Sort the results (bool: ['true','false'])
        :param summary: Fast fetch of main variant parameters (bool: ['true','false'])
        :param approximateCount: Get an approximate count, instead of an exact total count. Reduces execution time (bool: ['true','false'])
        :param approximateCountSamplingSize: Sampling size to get the approximate count. Larger values increase accuracy but also increase
            execution time
        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000
        :param type: List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL
        :param reference: Reference allele
        :param alternate: Main alternate allele
        :param project: Project [user@]project where project can be either the ID or the alias
        :param study: Filter variants from the given studies, these can be either the numeric ID or the alias with the format user@project:study
        :param file: Filter variants from the files specified. This will set includeFile parameter when not provided
        :param filter: Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter. e.g.: PASS,LowGQX
        :param qual: Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. e.g.: >123.4
        :param info: Filter by INFO attributes from file. [{file}:]{key}{op}{value}[,;]* . If no file is specified, will use all files from
            "file" filter. e.g. AN>200 or file_1.vcf:AN>200;file_2.vcf:AN<10 . Many INFO fields can be combined.
            e.g. file_1.vcf:AN>200;DB=true;file_2.vcf:AN<10
        :param sample: Filter variants where the samples contain the variant (HET or HOM_ALT). Accepts AND (;) and OR (,) operators. This
            will automatically set 'includeSample' parameter when not provided
        :param genotype: Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})(;{samp_n}:{gt_1}(,{gt_n}))* e.g. HG0097:0/0;HG0098:0/1,1/1.
            Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. Genotype aliases
            accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. This will automatically
            set 'includeSample' parameter when not provided
        :param format: Filter by any FORMAT field from samples. [{sample}:]{key}{op}{value}[,;]* . If no sample is specified, will use all
            samples from "sample" or "genotype" filter. e.g. DP>200 or HG0097:DP>200,HG0098:DP<10 . Many FORMAT fields can be combined.
            e.g. HG0097:DP>200;GT=1/1,0/1,HG0098:DP<10
        :param sampleAnnotation: Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith
        :param sampleMetadata: Return the samples metadata group by study. Sample names will appear in the same order as their corresponding
            genotypes. (bool: ['true','false'])
        :param unknownGenotype: Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]
        :param sampleLimit: Limit the number of samples to be included in the result
        :param sampleSkip: Skip some samples from the result. Useful for sample pagination.
        :param cohort: Select variants with calculated stats for the selected cohorts
        :param cohortStatsRef: Reference Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsAlt: Alternate Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsMaf: Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsMgf: Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param missingAlleles: Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param missingGenotypes: Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param includeStudy: List of studies to include in the result. Accepts 'all' and 'none'.
        :param includeFile: List of files to be returned. Accepts 'all' and 'none'.
        :param includeSample: List of samples to be included in the result. Accepts 'all' and 'none'.
        :param includeFormat: List of FORMAT names from Samples Data to include in the output. e.g: DP,AD. Accepts 'all' and 'none'.
        :param includeGenotype: Include genotypes, apart of other formats defined with includeFormat
        :param annotationExists: Return only annotated variants (bool: ['true','false'])
        :param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter
        :param ct: List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578
        :param xref: List of any external reference, these can be genes, proteins or variants. Accepted IDs include HGNC, Ensembl genes, dbSNP,
            ClinVar, HPO, Cosmic, ...
        :param biotype: List of biotypes, e.g. protein_coding
        :param proteinSubstitution: Protein substitution scores include SIFT and PolyPhen. You can query using the score
            {protein_score}[<|>|<=|>=]{number} or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant
        :param conservation: Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1
        :param populationFrequencyAlt: Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyRef: Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyMaf: Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param transcriptFlag: List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF,
            seleno
        :param geneTraitId: List of gene trait association id. e.g. "umls:C0007222" , "OMIM:269600"
        :param go: List of GO (Gene Ontology) terms. e.g. "GO:0002020"
        :param expression: List of tissues of interest. e.g. "lung"
        :param proteinKeyword: List of Uniprot protein variant annotation keywords
        :param drug: List of drug names
        :param functionalScore: Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3
        :param clinicalSignificance: Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic
        :param customAnnotation: Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}
        :param trait: List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...
        :param field: Facet field for categorical fields
        :param fieldRange: Facet field range for continuous fields
        """

        return self._get('interpretation', subcategory='stats', **options)

    def tool_team(self, **options):
        """
        TEAM interpretation analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/team

        :param study: Study [[user@]project:]study
        :param clinicalAnalysisId: Clinical Analysis ID
        :param panelIds: Comma separated list of disease panel IDs
        :param familySegregation: Filter by mode of inheritance from a given family. Accepted values: [ monoallelic,
            monoallelicIncompletePenetrance, biallelic, biallelicIncompletePenetrance, XlinkedBiallelic, XlinkedMonoallelic,
            Ylinked, MendelianError, DeNovo, CompoundHeterozygous ]
        :param save: Save interpretation in Catalog
        :param includeLowCoverage: Include low coverage regions
        :param maxLowCoverage: (default 20) Max. low coverage
        """

        return self._get('interpretation', subcategory='tools/team', **options)

    def tool_tiering(self, **options):
        """
        GEL Tiering interpretation analysis (PENDING)
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/tiering

        :param study: Study [[user@]project:]study
        :param clinicalAnalysisId: Clinical Analysis ID
        :param panelIds: Comma separated list of disease panel IDs
        :param penetrance: Penetrance ['COMPLETE' default,'INCOMPLETE']
        :param save: Save interpretation in Catalog (bool: ['true','false'])
        :param includeLowCoverage: Include low coverage regions (bool: ['true','false' default])
        :param maxLowCoverage: (default 20) Max. low coverage
        """

        return self._get('interpretation', subcategory='tools/tiering', **options)

    def tool_custom(self, **options):
        """
        Interpretation custom analysis
        URL: /{apiVersion}/analysis/clinical/interpretation/tools/custom

        :param clinicalAnalysisId: Clinical Analysis ID
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param penetrance: Penetrance ['COMPLETE' default,'INCOMPLETE']
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param skip: Number of results to skip in the queries
        :param sort: Sort the results (bool: ['true','false'])
        :param summary: Fast fetch of main variant parameters (bool: ['true','false'])
        :param includeLowCoverage: Include low coverage regions (bool: ['true','false' default])
        :param maxLowCoverage: (default 20)Max. low coverage
        :param skipDiagnosticVariants: Skip diagnostic variants (bool: ['true','false' default])
        :param skipUntieredVariants: Skip variants without tier assigned (bool: ['true','false' default])
        :param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T
        :param region: List of regions, these can be just a single chromosome name or regions in the format chr:start-end,
            e.g.: 2,3:100000-200000
        :param type: List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL
        :param reference: Reference allele
        :param alternate: Main alternate allele
        :param project: Project [user@]project where project can be either the ID or the alias
        :param file: Filter variants from the files specified. This will set includeFile parameter when not provided
        :param filter: Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter.
            e.g.: PASS,LowGQX
        :param qual: Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. e.g.: >123.4
        :param info: Filter by INFO attributes from file. [{file}:]{key}{op}{value}[,;]* . If no file is specified, will use all files from
            "file" filter. e.g. AN>200 or file_1.vcf:AN>200;file_2.vcf:AN<10 . Many INFO fields can be combined.
            e.g. file_1.vcf:AN>200;DB=true;file_2.vcf:AN<10
        :param sample: Filter variants where the samples contain the variant (HET or HOM_ALT). Accepts AND (;) and OR (,) operators. This
            will automatically set 'includeSample' parameter when not provided
        :param genotype: Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})(;{samp_n}:{gt_1}(,{gt_n}))* e.g. HG0097:0/0;HG0098:0/1,1/1.
            Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. Genotype aliases
            accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. This will automatically
            set 'includeSample' parameter when not provided
        :param format: Filter by any FORMAT field from samples. [{sample}:]{key}{op}{value}[,;]* . If no sample is specified, will use all
            samples from "sample" or "genotype" filter. e.g. DP>200 or HG0097:DP>200,HG0098:DP<10 . Many FORMAT fields can be combined.
            e.g. HG0097:DP>200;GT=1/1,0/1,HG0098:DP<10
        :param sampleAnnotation: Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith
        :param sampleMetadata: Return the samples metadata group by study. Sample names will appear in the same order as their corresponding
            genotypes (bool: ['true','false']).
        :param unknownGenotype: Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]
        :param sampleLimit: Limit the number of samples to be included in the result
        :param sampleSkip: Skip some samples from the result. Useful for sample pagination.
        :param cohort: Select variants with calculated stats for the selected cohorts
        :param cohortStatsRef: Reference Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsAlt: Alternate Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsMaf: Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param cohortStatsMgf: Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4
        :param missingAlleles: Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param missingGenotypes: Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}
        :param family: Filter variants where any of the samples from the given family contains the variant (HET or HOM_ALT)
        :param familyDisorder: Specify the disorder to use for the family segregation
        :param familySegregation: Filter by mode of inheritance from a given family. Accepted values: [ monoallelic,
            monoallelicIncompletePenetrance, biallelic, biallelicIncompletePenetrance, XlinkedBiallelic, XlinkedMonoallelic, Ylinked,
            MendelianError, DeNovo, CompoundHeterozygous ]
        :param familyMembers: Sub set of the members of a given family
        :param familyProband: Specify the proband child to use for the family segregation
        :param includeStudy: List of studies to include in the result. Accepts 'all' and 'none'.
        :param includeFile: List of files to be returned. Accepts 'all' and 'none'.
        :param includeSample: List of samples to be included in the result. Accepts 'all' and 'none'.
        :param includeFormat: List of FORMAT names from Samples Data to include in the output. e.g: DP,AD. Accepts 'all' and 'none'.
        :param includeGenotype: Include genotypes, apart of other formats defined with includeFormat
        :param annotationExists: Return only annotated variants (bool: ['true','false'])
        :param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter
        :param ct: List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578
        :param xref: List of any external reference, these can be genes, proteins or variants. Accepted IDs include HGNC, Ensembl genes,
            dbSNP, ClinVar, HPO, Cosmic, ...
        :param biotype: List of biotypes, e.g. protein_coding
        :param proteinSubstitution: Protein substitution scores include SIFT and PolyPhen. You can query using the score
            {protein_score}[<|>|<=|>=]{number} or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant
        :param conservation: Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1
        :param populationFrequencyAlt: Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyRef: Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param populationFrequencyMaf: Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01
        :param transcriptFlag: List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF,
            seleno
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

        return self._get('interpretation', subcategory='tools/custom', **options)

    def update_interpretation(self, clinical_analysis, interpretation, data, **options):
        """
        Update Interpretation fields
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/update

        :param clinical_analysis: clinical analysis id
        :param interpretation: interpretation id
        :param study: [[user@]project:]study id
        :param data: JSON containing clinical interpretation information (check seagger data model)
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='update',
                          subquery_id=interpretation, data=data, **options)

    def update_comments(self, clinical_analysis, interpretation, data, **options):
        """
        Update comments of an Interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/comments/update

        :param clinical_analysis: clinical analysis id
        :param study: [[user@]project:]study id
        :param interpretation: Interpretation id
        :param action: Action to be performed ['ADD' default, 'SET', 'REMOVE']
        :param data: JSON containing a list of comments

        data = [
          {
            "author": "string",
            "type": "string",
            "text": "string",
            "message": "string",
            "date": "string"
          }
        ]
        """

        return self._post('interpretations', query_id=clinical_analysis, subquery_id=interpretation,
                          subcategory='comments/update', data=data, **options)

    def update_primary_findings(self, **options):
        """
        Update reported variants of an interpretation
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/primaryFindings/update

        :param clinical_analysis: clinical analysis id
        :param study: [[user@]project:]study id
        :param interpretation: Interpretation id
        :param action: Action to be performedi ['ADD' dafault, 'SET', 'REMOVE'].
        :param data: JSON containing a list of reported variants (check swagger data model)
        """

        return self._post('interpretation', query_id=clinical_analysis, subquery_id=interpretation,
                          subcategory='primaryFindings/update', data=data, **options)

    def update(self, clinical_analysis, data, **options):
        """
        Add or remove Interpretations to/from a Clinical Analysis
        URL: /{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/update

        :param clinical_analysis: clinical analysis id
        :param interpretation: interpretation id
        :param study: [[user@]project:]study id
        :param data: JSON containing clinical interpretation information (check seagger data model)
        """

        return self._post('interpretations', query_id=clinical_analysis, subcategory='update', data=data, **options)

class Clinical(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains methods for the Analysis - Clinical webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category  = 'analysis/clinical'
        super(Clinical, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

        self.configuration = configuration
        self.session_id = session_id
        self.login_handler = login_handler
        self._create_clients()

    def _create_clients(self):

        ## undef all
        self.interpretations = None
        self.reports = None
        self.cva = None

        ## [TODO] convert to @properties
        ## [@dgp] SHould I add auto_refresh = self.auto_refresh ??
        self.interpretations = Interpretations(self.configuration, self.session_id, self.login_handler)
        ## self.reports = Reports(configuration, session_id, login_handler, *args, **kwargs)
        ## self.cva = CVAs(configuration, session_id, login_handler, *args, **kwargs)

        self.clients = [self.interpretations]

        for client in self.clients:
            # only retry the ones with objects (instatiated clients)
            if client is not None:
                client.on_retry = self.on_retry

    def search(self, **options):
        """
        Clinical analysis search.
        URL: /{apiVersion}/analysis/clinical/search

        :param study: Study [[user@]project:]{study} where study and project can be either the id or alias.
        :param type: Clinical analysis type
        :param priority: Priority
        :param status: Clinical analysis status
        :param creationDate: Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param dueDate: Due date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param description: Description
        :param family: Family id
        :param proband: Proband id
        :param sample: Proband sample
        :param analystAssignee: Clinical analyst assignee
        :param disorder: Disorder id or name
        :param flags: Flags
        :param release: Release value
        :param attributes: Text attributes (Format: sex=male,age>20 ...)
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (bool: ['true,'false' default])
        """

        return self._get('search', **options)
