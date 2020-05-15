
################################################################################
#' AnalysisVariantClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing AnalysisVariant
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("analysisvariantClient", "OpencgaR", function(OpencgaR, action, params=NULL, ...) {
    category <- "analysis/variant"
    switch(action,
        # Endpoint: /{apiVersion}/analysis/variant/aggregationStats
        # @param savedFilter: Use a saved filter at User level.
        # @param region: List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000.
        # @param type: List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL.
        # @param project: Project [user@]project where project can be either the ID or the alias.
        # @param study: Filter variants from the given studies, these can be either the numeric ID or the alias with the format user@project:study.
        # @param file: Filter variants from the files specified. This will set includeFile parameter when not provided.
        # @param filter: Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter. e.g.: PASS,LowGQX.
        # @param sample: Filter variants by sample genotype. This will automatically set 'includeSample' parameter when not provided. This filter accepts multiple 3 forms: 1) List of samples: Samples that contain the main variant. Accepts AND (;) and OR (,) operators.  e.g. HG0097,HG0098 . 2) List of samples with genotypes: {sample}:{gt1},{gt2}. Accepts AND (;) and OR (,) operators.  e.g. HG0097:0/0;HG0098:0/1,1/1 . Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. When filtering by multi-allelic genotypes, any secondary allele will match, regardless of its position e.g. 1/2 will match with genotypes 1/2, 1/3, 1/4, .... Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS  e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT . 3) Sample with segregation mode: {sample}:{segregation}. Only one sample accepted.Accepted segregation modes: [ monoallelic, monoallelicIncompletePenetrance, biallelic, biallelicIncompletePenetrance, XlinkedBiallelic, XlinkedMonoallelic, Ylinked, MendelianError, DeNovo, CompoundHeterozygous ]. Value is case insensitive. e.g. HG0097:DeNovo Sample must have parents defined and indexed. .
        # @param genotype: Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1. Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. When filtering by multi-allelic genotypes, any secondary allele will match, regardless of its position e.g. 1/2 will match with genotypes 1/2, 1/3, 1/4, .... Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS  e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. This will automatically set 'includeSample' parameter when not provided.
        # @param sampleAnnotation: Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith.
        # @param cohort: Select variants with calculated stats for the selected cohorts.
        # @param cohortStatsRef: Reference Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsAlt: Alternate Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsMaf: Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsMgf: Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsPass: Filter PASS frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL>0.8.
        # @param missingAlleles: Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}.
        # @param missingGenotypes: Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}.
        # @param score: Filter by variant score: [{study:}]{score}[<|>|<=|>=]{number}.
        # @param family: Filter variants where any of the samples from the given family contains the variant (HET or HOM_ALT).
        # @param familyDisorder: Specify the disorder to use for the family segregation.
        # @param familySegregation: Filter by segregation mode from a given family. Accepted values: [ monoallelic, monoallelicIncompletePenetrance, biallelic, biallelicIncompletePenetrance, XlinkedBiallelic, XlinkedMonoallelic, Ylinked, MendelianError, DeNovo, CompoundHeterozygous ].
        # @param familyMembers: Sub set of the members of a given family.
        # @param familyProband: Specify the proband child to use for the family segregation.
        # @param annotationExists: Return only annotated variants.
        # @param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter.
        # @param ct: List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578.
        # @param xref: List of any external reference, these can be genes, proteins or variants. Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, ...
        # @param biotype: List of biotypes, e.g. protein_coding.
        # @param proteinSubstitution: Protein substitution scores include SIFT and PolyPhen. You can query using the score {protein_score}[<|>|<=|>=]{number} or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant.
        # @param conservation: Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1.
        # @param populationFrequencyAlt: Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.
        # @param populationFrequencyRef: Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.
        # @param populationFrequencyMaf: Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.
        # @param transcriptFlag: List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno.
        # @param geneTraitId: List of gene trait association id. e.g. "umls:C0007222" , "OMIM:269600".
        # @param go: List of GO (Gene Ontology) terms. e.g. "GO:0002020".
        # @param expression: List of tissues of interest. e.g. "lung".
        # @param proteinKeyword: List of Uniprot protein variant annotation keywords.
        # @param drug: List of drug names.
        # @param functionalScore: Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3.
        # @param clinicalSignificance: Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic.
        # @param customAnnotation: Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}.
        # @param trait: List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...
        # @param fields: List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: chromosome>>type;percentile(gerp).
        aggregationStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="aggregationStats", params=params, httpMethod="GET", as.queryParam=NULL,
                ...),
        # Endpoint: /{apiVersion}/analysis/variant/annotation/metadata
        # @param annotationId: Annotation identifier.
        # @param project: Project [user@]project where project can be either the ID or the alias.
        metadataAnnotation=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="annotation",
                subcategoryId=NULL, action="metadata", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/annotation/query
        # @param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T.
        # @param region: List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000.
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param annotationId: Annotation identifier.
        queryAnnotation=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="annotation",
                subcategoryId=NULL, action="query", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/cohort/stats/delete
        # @param study: study.
        # @param cohort: Cohort id or name.
        deleteCohortStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="cohort/stats",
                subcategoryId=NULL, action="delete", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/cohort/stats/info
        # @param study: study.
        # @param cohort: Comma separated list of cohort names or ids up to a maximum of 100.
        infoCohortStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="cohort/stats",
                subcategoryId=NULL, action="info", params=params, httpMethod="GET", as.queryParam=c("cohort"), ...),
        # Endpoint: /{apiVersion}/analysis/variant/cohort/stats/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Cohort variant stats params.
        runCohortStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="cohort/stats",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/export/run
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Variant export params.
        runExport=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="export",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/family/genotypes
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param family: Family id.
        # @param clinicalAnalysis: Clinical analysis id.
        # @param modeOfInheritance: Mode of inheritance.
        # @param penetrance: Penetrance.
        # @param disorder: Disorder id.
        genotypesFamily=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="family",
                subcategoryId=NULL, action="genotypes", params=params, httpMethod="GET",
                as.queryParam=c("modeOfInheritance"), ...),
        # Endpoint: /{apiVersion}/analysis/variant/file/delete
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param file: Files to remove.
        # @param resume: Resume a previously failed indexation.
        deleteFile=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="file",
                subcategoryId=NULL, action="delete", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/gatk/run
        # @param study: study.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: gatk params.
        runGatk=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="gatk",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/geneticChecks/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Genetic checks analysis params.
        runGeneticChecks=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="geneticChecks",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/gwas/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Gwas analysis params.
        runGwas=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="gwas",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/index/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobDescription: Job description.
        # @param jobTags: Job tags.
        # @param data: Variant index params.
        runIndex=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="index",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/inferredSex/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Inferred sex analysis params.
        runInferredSex=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="inferredSex",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/knockout/run
        # @param study: study.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Gene knockout analysis params.
        runKnockout=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="knockout",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/mendelianError/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Mendelian error analysis params.
        runMendelianError=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL,
                subcategory="mendelianError", subcategoryId=NULL, action="run", params=params, httpMethod="POST",
                as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/metadata
        # @param project: Project [user@]project where project can be either the ID or the alias.
        # @param study: Filter variants from the given studies, these can be either the numeric ID or the alias with the format user@project:study.
        # @param file: Filter variants from the files specified. This will set includeFile parameter when not provided.
        # @param sample: Filter variants by sample genotype. This will automatically set 'includeSample' parameter when not provided. This filter accepts multiple 3 forms: 1) List of samples: Samples that contain the main variant. Accepts AND (;) and OR (,) operators.  e.g. HG0097,HG0098 . 2) List of samples with genotypes: {sample}:{gt1},{gt2}. Accepts AND (;) and OR (,) operators.  e.g. HG0097:0/0;HG0098:0/1,1/1 . Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. When filtering by multi-allelic genotypes, any secondary allele will match, regardless of its position e.g. 1/2 will match with genotypes 1/2, 1/3, 1/4, .... Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS  e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT . 3) Sample with segregation mode: {sample}:{segregation}. Only one sample accepted.Accepted segregation modes: [ monoallelic, monoallelicIncompletePenetrance, biallelic, biallelicIncompletePenetrance, XlinkedBiallelic, XlinkedMonoallelic, Ylinked, MendelianError, DeNovo, CompoundHeterozygous ]. Value is case insensitive. e.g. HG0097:DeNovo Sample must have parents defined and indexed. .
        # @param includeStudy: List of studies to include in the result. Accepts 'all' and 'none'.
        # @param includeFile: List of files to be returned. Accepts 'all' and 'none'.
        # @param includeSample: List of samples to be included in the result. Accepts 'all' and 'none'.
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        metadata=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="metadata", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/mutationalSignature/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Mutational signature analysis params.
        runMutationalSignature=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL,
                subcategory="mutationalSignature", subcategoryId=NULL, action="run", params=params, httpMethod="POST",
                as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/plink/run
        # @param study: study.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Plink params.
        runPlink=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="plink",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/query
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param count: Get the total number of results matching the query. Deactivated by default.
        # @param sort: Sort the results.
        # @param summary: Fast fetch of main variant parameters.
        # @param approximateCount: Get an approximate count, instead of an exact total count. Reduces execution time.
        # @param approximateCountSamplingSize: Sampling size to get the approximate count. Larger values increase accuracy but also increase execution time.
        # @param savedFilter: Use a saved filter at User level.
        # @param id: List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T.
        # @param region: List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000.
        # @param type: List of types, accepted values are SNV, MNV, INDEL, SV, CNV, INSERTION, DELETION, e.g. SNV,INDEL.
        # @param reference: Reference allele.
        # @param alternate: Main alternate allele.
        # @param project: Project [user@]project where project can be either the ID or the alias.
        # @param study: Filter variants from the given studies, these can be either the numeric ID or the alias with the format user@project:study.
        # @param file: Filter variants from the files specified. This will set includeFile parameter when not provided.
        # @param filter: Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter. e.g.: PASS,LowGQX.
        # @param qual: Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. e.g.: >123.4.
        # @param fileData: Filter by file data (i.e. INFO column from VCF file). [{file}:]{key}{op}{value}[,;]* . If no file is specified, will use all files from "file" filter. e.g. AN>200 or file_1.vcf:AN>200;file_2.vcf:AN<10 . Many INFO fields can be combined. e.g. file_1.vcf:AN>200;DB=true;file_2.vcf:AN<10.
        # @param sample: Filter variants by sample genotype. This will automatically set 'includeSample' parameter when not provided. This filter accepts multiple 3 forms: 1) List of samples: Samples that contain the main variant. Accepts AND (;) and OR (,) operators.  e.g. HG0097,HG0098 . 2) List of samples with genotypes: {sample}:{gt1},{gt2}. Accepts AND (;) and OR (,) operators.  e.g. HG0097:0/0;HG0098:0/1,1/1 . Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. When filtering by multi-allelic genotypes, any secondary allele will match, regardless of its position e.g. 1/2 will match with genotypes 1/2, 1/3, 1/4, .... Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS  e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT . 3) Sample with segregation mode: {sample}:{segregation}. Only one sample accepted.Accepted segregation modes: [ monoallelic, monoallelicIncompletePenetrance, biallelic, biallelicIncompletePenetrance, XlinkedBiallelic, XlinkedMonoallelic, Ylinked, MendelianError, DeNovo, CompoundHeterozygous ]. Value is case insensitive. e.g. HG0097:DeNovo Sample must have parents defined and indexed. .
        # @param genotype: Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1. Unphased genotypes (e.g. 0/1, 1/1) will also include phased genotypes (e.g. 0|1, 1|0, 1|1), but not vice versa. When filtering by multi-allelic genotypes, any secondary allele will match, regardless of its position e.g. 1/2 will match with genotypes 1/2, 1/3, 1/4, .... Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS  e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. This will automatically set 'includeSample' parameter when not provided.
        # @param sampleData: Filter by any SampleData field from samples. [{sample}:]{key}{op}{value}[,;]* . If no sample is specified, will use all samples from "sample" or "genotype" filter. e.g. DP>200 or HG0097:DP>200,HG0098:DP<10 . Many FORMAT fields can be combined. e.g. HG0097:DP>200;GT=1/1,0/1,HG0098:DP<10.
        # @param sampleAnnotation: Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith.
        # @param sampleMetadata: Return the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.
        # @param unknownGenotype: Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.].
        # @param sampleLimit: Limit the number of samples to be included in the result.
        # @param sampleSkip: Skip some samples from the result. Useful for sample pagination.
        # @param cohort: Select variants with calculated stats for the selected cohorts.
        # @param cohortStatsRef: Reference Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsAlt: Alternate Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsMaf: Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsMgf: Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.
        # @param cohortStatsPass: Filter PASS frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL>0.8.
        # @param missingAlleles: Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}.
        # @param missingGenotypes: Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}.
        # @param score: Filter by variant score: [{study:}]{score}[<|>|<=|>=]{number}.
        # @param family: Filter variants where any of the samples from the given family contains the variant (HET or HOM_ALT).
        # @param familyDisorder: Specify the disorder to use for the family segregation.
        # @param familySegregation: Filter by segregation mode from a given family. Accepted values: [ monoallelic, monoallelicIncompletePenetrance, biallelic, biallelicIncompletePenetrance, XlinkedBiallelic, XlinkedMonoallelic, Ylinked, MendelianError, DeNovo, CompoundHeterozygous ].
        # @param familyMembers: Sub set of the members of a given family.
        # @param familyProband: Specify the proband child to use for the family segregation.
        # @param includeStudy: List of studies to include in the result. Accepts 'all' and 'none'.
        # @param includeFile: List of files to be returned. Accepts 'all' and 'none'.
        # @param includeSample: List of samples to be included in the result. Accepts 'all' and 'none'.
        # @param includeSampleData: List of Sample Data keys (i.e. FORMAT column from VCF file) from Sample Data to include in the output. e.g: DP,AD. Accepts 'all' and 'none'.
        # @param includeGenotype: Include genotypes, apart of other formats defined with includeFormat.
        # @param includeSampleId: Include sampleId on each result.
        # @param annotationExists: Return only annotated variants.
        # @param gene: List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter.
        # @param ct: List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578.
        # @param xref: List of any external reference, these can be genes, proteins or variants. Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, ...
        # @param biotype: List of biotypes, e.g. protein_coding.
        # @param proteinSubstitution: Protein substitution scores include SIFT and PolyPhen. You can query using the score {protein_score}[<|>|<=|>=]{number} or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant.
        # @param conservation: Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1.
        # @param populationFrequencyAlt: Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.
        # @param populationFrequencyRef: Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.
        # @param populationFrequencyMaf: Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.
        # @param transcriptFlag: List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno.
        # @param geneTraitId: List of gene trait association id. e.g. "umls:C0007222" , "OMIM:269600".
        # @param go: List of GO (Gene Ontology) terms. e.g. "GO:0002020".
        # @param expression: List of tissues of interest. e.g. "lung".
        # @param proteinKeyword: List of Uniprot protein variant annotation keywords.
        # @param drug: List of drug names.
        # @param functionalScore: Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3.
        # @param clinicalSignificance: Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic.
        # @param customAnnotation: Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}.
        # @param panel: Filter by genes from the given disease panel.
        # @param trait: List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...
        query=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="query", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/relatedness/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Relatedness analysis params.
        runRelatedness=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="relatedness",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/rvtests/run
        # @param study: study.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: rvtest params.
        runRvtests=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="rvtests",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/sample/eligibility/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: .
        runSampleEligibility=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL,
                subcategory="sample/eligibility", subcategoryId=NULL, action="run", params=params, httpMethod="POST",
                as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/sample/query
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param variant: Variant.
        # @param study: Study where all the samples belong to.
        # @param genotype: Genotypes that the sample must have to be selected.
        querySample=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="sample",
                subcategoryId=NULL, action="query", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/sample/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Sample variant filter params.
        runSample=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="sample",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/sample/stats/delete
        # @param study: study.
        # @param sample: Sample.
        deleteSampleStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="sample/stats",
                subcategoryId=NULL, action="delete", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/sample/stats/info
        # @param study: Study where all the samples belong to.
        # @param sample: Comma separated list sample IDs or UUIDs up to a maximum of 100.
        infoSampleStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="sample/stats",
                subcategoryId=NULL, action="info", params=params, httpMethod="GET", as.queryParam=c("sample"), ...),
        # Endpoint: /{apiVersion}/analysis/variant/sample/stats/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Sample variant stats params.
        runSampleStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="sample/stats",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/stats/export/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Variant stats export params.
        runStatsExport=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="stats/export",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/analysis/variant/stats/run
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param jobId: Job ID. It must be a unique string within the study. An id will be autogenerated automatically if not provided.
        # @param jobDescription: Job description.
        # @param jobDependsOn: Comma separated list of existing job ids the job will depend on.
        # @param jobTags: Job tags.
        # @param data: Variant stats params.
        runStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="stats",
                subcategoryId=NULL, action="run", params=params, httpMethod="POST", as.queryParam=NULL, ...),
    )
})