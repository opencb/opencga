% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Variant-methods.R
\name{variantClient,OpencgaR-method}
\alias{variantClient,OpencgaR-method}
\title{VariantClient methods}
\usage{
\S4method{variantClient}{OpencgaR}(OpencgaR, endpointName, params = NULL, ...)
}
\arguments{
\item{annotationId}{Annotation identifier.}

\item{clinicalAnalysis}{Clinical analysis id.}

\item{modeOfInheritance}{Mode of inheritance.}

\item{penetrance}{Penetrance.}

\item{disorder}{Disorder id.}

\item{resume}{Resume a previously failed indexation.}

\item{job}{Job ID or UUID.}

\item{fitting}{Compute the relative proportions of the different mutational signatures demonstrated by the tumour.}

\item{include}{Fields included in the response, whole JSON path must be provided.}

\item{exclude}{Fields excluded in the response, whole JSON path must be provided.}

\item{count}{Get the total number of results matching the query. Deactivated by default.}

\item{sort}{Sort the results.}

\item{summary}{Fast fetch of main variant parameters.}

\item{approximateCount}{Get an approximate count, instead of an exact total count. Reduces execution time.}

\item{approximateCountSamplingSize}{Sampling size to get the approximate count. Larger values increase accuracy but also increase execution time.}

\item{id}{List of IDs, these can be rs IDs (dbSNP) or variants in the format chrom:start:ref:alt, e.g. rs116600158,19:7177679:C:T.}

\item{reference}{Reference allele.}

\item{alternate}{Main alternate allele.}

\item{qual}{Specify the QUAL for any of the files. If 'file' filter is provided, will match the file and the qual. e.g.: >123.4.}

\item{fileData}{Filter by file data (i.e. FILTER, QUAL and INFO columns from VCF file). [{file}:]{key}{op}{value}\link{,;}* . If no file is specified, will use all files from "file" filter. e.g. AN>200 or file_1.vcf:AN>200;file_2.vcf:AN<10 . Many fields can be combined. e.g. file_1.vcf:AN>200;DB=true;file_2.vcf:AN<10,FILTER=PASS,LowDP.}

\item{sampleMetadata}{Return the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.}

\item{unknownGenotype}{Returned genotype for unknown genotypes. Common values: \link{0/0, 0|0, ./.}.}

\item{sampleLimit}{Limit the number of samples to be included in the result.}

\item{sampleSkip}{Skip some samples from the result. Useful for sample pagination.}

\item{cohort}{Select variants with calculated stats for the selected cohorts.}

\item{cohortStatsRef}{Reference Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.}

\item{cohortStatsAlt}{Alternate Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.}

\item{cohortStatsMaf}{Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.}

\item{cohortStatsMgf}{Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL<=0.4.}

\item{cohortStatsPass}{Filter PASS frequency: [{study:}]{cohort}[<|>|<=|>=]{number}. e.g. ALL>0.8.}

\item{missingAlleles}{Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}.}

\item{missingGenotypes}{Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}.}

\item{score}{Filter by variant score: [{study:}]{score}[<|>|<=|>=]{number}.}

\item{includeStudy}{List of studies to include in the result. Accepts 'all' and 'none'.}

\item{includeFile}{List of files to be returned. Accepts 'all' and 'none'. If undefined, automatically includes files used for filtering. If none, no file is included.}

\item{includeSample}{List of samples to be included in the result. Accepts 'all' and 'none'. If undefined, automatically includes samples used for filtering. If none, no sample is included.}

\item{includeSampleData}{List of Sample Data keys (i.e. FORMAT column from VCF file) from Sample Data to include in the output. e.g: DP,AD. Accepts 'all' and 'none'.}

\item{includeGenotype}{Include genotypes, apart of other formats defined with includeFormat.}

\item{includeSampleId}{Include sampleId on each result.}

\item{annotationExists}{Return only annotated variants.}

\item{gene}{List of genes, most gene IDs are accepted (HGNC, Ensembl gene, ...). This is an alias to 'xref' parameter.}

\item{xref}{List of any external reference, these can be genes, proteins or variants. Accepted IDs include HGNC, Ensembl genes, dbSNP, ClinVar, HPO, Cosmic, ...}

\item{proteinSubstitution}{Protein substitution scores include SIFT and PolyPhen. You can query using the score {protein_score}[<|>|<=|>=]{number} or the description {protein_score}[~=|=]{description} e.g. polyphen>0.1,sift=tolerant.}

\item{conservation}{Filter by conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1.}

\item{populationFrequencyRef}{Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.}

\item{populationFrequencyMaf}{Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.}

\item{geneTraitId}{List of gene trait association id. e.g. "umls:C0007222" , "OMIM:269600".}

\item{go}{List of GO (Gene Ontology) terms. e.g. "GO:0002020".}

\item{expression}{List of tissues of interest. e.g. "lung".}

\item{proteinKeyword}{List of Uniprot protein variant annotation keywords.}

\item{drug}{List of drug names.}

\item{functionalScore}{Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3.}

\item{customAnnotation}{Custom annotation: {key}[<|>|<=|>=]{number} or {key}[~=|=]{text}.}

\item{panel}{Filter by genes from the given disease panel.}

\item{panelModeOfInheritance}{Filter genes from specific panels that match certain mode of inheritance. Accepted values : \link{ autosomalDominant, autosomalRecessive, XLinkedDominant, XLinkedRecessive, YLinked, mitochondrial, deNovo, mendelianError, compoundHeterozygous }.}

\item{panelConfidence}{Filter genes from specific panels that match certain confidence. Accepted values : \link{ high, medium, low, rejected }.}

\item{panelRoleInCancer}{Filter genes from specific panels that match certain role in cancer. Accepted values : \link{ both, oncogene, tumorSuppressorGene, fusion }.}

\item{panelFeatureType}{Filter elements from specific panels by type. Accepted values : \link{ gene, region, str, variant }.}

\item{panelIntersection}{Intersect panel genes and regions with given genes and regions from que input query. This will prevent returning variants from regions out of the panel.}

\item{trait}{List of traits, based on ClinVar, HPO, COSMIC, i.e.: IDs, histologies, descriptions,...}

\item{savedFilter}{Use a saved filter at User level.}

\item{sampleAnnotation}{Selects some samples using metadata information from Catalog. e.g. age>20;phenotype=hpo:123,hpo:456;name=smith.}

\item{family}{Filter variants where any of the samples from the given family contains the variant (HET or HOM_ALT).}

\item{familyDisorder}{Specify the disorder to use for the family segregation.}

\item{familySegregation}{Filter by segregation mode from a given family. Accepted values: \link{ autosomalDominant, autosomalRecessive, XLinkedDominant, XLinkedRecessive, YLinked, mitochondrial, deNovo, mendelianError, compoundHeterozygous }.}

\item{familyMembers}{Sub set of the members of a given family.}

\item{familyProband}{Specify the proband child to use for the family segregation.}

\item{field}{List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: chromosome>>type . Accepted values: chromosome, type, genotype, consequenceType, biotype, clinicalSignificance, dp, qual, filter.}

\item{limit}{Number of results to be returned.}

\item{skip}{Number of results to skip.}

\item{variant}{Variant.}

\item{genotype}{Genotypes that the sample must have to be selected.}

\item{region}{List of regions, these can be just a single chromosome name or regions in the format chr:start-end, e.g.: 2,3:100000-200000.}

\item{type}{List of types, accepted values are SNV, MNV, INDEL, SV, COPY_NUMBER, COPY_NUMBER_LOSS, COPY_NUMBER_GAIN, INSERTION, DELETION, DUPLICATION, TANDEM_DUPLICATION, BREAKEND, e.g. SNV,INDEL.}

\item{file}{Filter variants from the files specified. This will set includeFile parameter when not provided.}

\item{filter}{Specify the FILTER for any of the files. If 'file' filter is provided, will match the file and the filter. e.g.: PASS,LowGQX.}

\item{sampleData}{Filter by any SampleData field from samples. [{sample}:]{key}{op}{value}\link{,;}* . If no sample is specified, will use all samples from "sample" or "genotype" filter. e.g. DP>200 or HG0097:DP>200,HG0098:DP<10 . Many FORMAT fields can be combined. e.g. HG0097:DP>200;GT=1/1,0/1,HG0098:DP<10.}

\item{ct}{List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578. Accepts aliases 'loss_of_function' and 'protein_altering'.}

\item{biotype}{List of biotypes, e.g. protein_coding.}

\item{transcriptFlag}{List of transcript flags. e.g. canonical, CCDS, basic, LRG, MANE Select, MANE Plus Clinical, EGLH_HaemOnc, TSO500.}

\item{populationFrequencyAlt}{Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}. e.g. 1kG_phase3:ALL<0.01.}

\item{clinical}{Clinical source: clinvar, cosmic.}

\item{clinicalSignificance}{Clinical significance: benign, likely_benign, likely_pathogenic, pathogenic.}

\item{clinicalConfirmedStatus}{Clinical confirmed status.}

\item{filterTranscript}{Do filter transcripts when obtaining transcript counts.}

\item{sample}{Sample ID.}

\item{project}{Project \link{user@}project where project can be either the ID or the alias.}

\item{study}{Study [\link{user@}project:]study where study and project can be either the ID or UUID.}

\item{jobId}{Job ID. It must be a unique string within the study. An ID will be autogenerated automatically if not provided.}

\item{jobDescription}{Job description.}

\item{jobDependsOn}{Comma separated list of existing job IDs the job will depend on.}

\item{jobTags}{Job tags.}

\item{data}{Variant stats params.}
}
\description{
This function implements the OpenCGA calls for managing Analysis - Variant.
The following table summarises the available \emph{actions} for this client:\tabular{llr}{
   endpointName \tab Endpoint WS \tab parameters accepted \cr
   aggregationStats \tab /{apiVersion}/analysis/variant/aggregationStats \tab savedFilter, region, type, project, study, cohort, cohortStatsRef, cohortStatsAlt, cohortStatsMaf, cohortStatsMgf, cohortStatsPass, missingAlleles, missingGenotypes, score, annotationExists, gene, ct, xref, biotype, proteinSubstitution, conservation, populationFrequencyAlt, populationFrequencyRef, populationFrequencyMaf, transcriptFlag, geneTraitId, go, expression, proteinKeyword, drug, functionalScore, clinical, clinicalSignificance, clinicalConfirmedStatus, customAnnotation, trait, field \cr
   metadataAnnotation \tab /{apiVersion}/analysis/variant/annotation/metadata \tab annotationId, project \cr
   queryAnnotation \tab /{apiVersion}/analysis/variant/annotation/query \tab id, region, include, exclude, limit, skip, annotationId \cr
   runCircos \tab /{apiVersion}/analysis/variant/circos/run \tab study, body\link{*} \cr
   deleteCohortStats \tab /{apiVersion}/analysis/variant/cohort/stats/delete \tab study, cohort \cr
   infoCohortStats \tab /{apiVersion}/analysis/variant/cohort/stats/info \tab study, cohort\link{*} \cr
   runCohortStats \tab /{apiVersion}/analysis/variant/cohort/stats/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runExport \tab /{apiVersion}/analysis/variant/export/run \tab include, exclude, project, study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   genotypesFamily \tab /{apiVersion}/analysis/variant/family/genotypes \tab study, family, clinicalAnalysis, modeOfInheritance\link{*}, penetrance, disorder \cr
   runFamilyQc \tab /{apiVersion}/analysis/variant/family/qc/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   deleteFile \tab /{apiVersion}/analysis/variant/file/delete \tab jobId, jobDescription, jobDependsOn, jobTags, study, file, resume \cr
   runGatk \tab /{apiVersion}/analysis/variant/gatk/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runGenomePlot \tab /{apiVersion}/analysis/variant/genomePlot/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runGwas \tab /{apiVersion}/analysis/variant/gwas/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runIndex \tab /{apiVersion}/analysis/variant/index/run \tab study, jobId, jobDependsOn, jobDescription, jobTags, body\link{*} \cr
   runIndividualQc \tab /{apiVersion}/analysis/variant/individual/qc/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runInferredSex \tab /{apiVersion}/analysis/variant/inferredSex/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   queryKnockoutGene \tab /{apiVersion}/analysis/variant/knockout/gene/query \tab limit, skip, study, job \cr
   queryKnockoutIndividual \tab /{apiVersion}/analysis/variant/knockout/individual/query \tab limit, skip, study, job \cr
   runKnockout \tab /{apiVersion}/analysis/variant/knockout/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runMendelianError \tab /{apiVersion}/analysis/variant/mendelianError/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   metadata \tab /{apiVersion}/analysis/variant/metadata \tab project, study, file, sample, includeStudy, includeFile, includeSample, include, exclude \cr
   queryMutationalSignature \tab /{apiVersion}/analysis/variant/mutationalSignature/query \tab study, sample, ct, biotype, fileData, filter, qual, region, gene, panel, panelModeOfInheritance, panelConfidence, panelFeatureType, panelRoleInCancer, panelIntersection, fitting \cr
   runMutationalSignature \tab /{apiVersion}/analysis/variant/mutationalSignature/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runPlink \tab /{apiVersion}/analysis/variant/plink/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   query \tab /{apiVersion}/analysis/variant/query \tab include, exclude, limit, skip, count, sort, summary, approximateCount, approximateCountSamplingSize, savedFilter, id, region, type, reference, alternate, project, study, file, filter, qual, fileData, sample, genotype, sampleData, sampleAnnotation, sampleMetadata, unknownGenotype, sampleLimit, sampleSkip, cohort, cohortStatsRef, cohortStatsAlt, cohortStatsMaf, cohortStatsMgf, cohortStatsPass, missingAlleles, missingGenotypes, score, family, familyDisorder, familySegregation, familyMembers, familyProband, includeStudy, includeFile, includeSample, includeSampleData, includeGenotype, includeSampleId, annotationExists, gene, ct, xref, biotype, proteinSubstitution, conservation, populationFrequencyAlt, populationFrequencyRef, populationFrequencyMaf, transcriptFlag, geneTraitId, go, expression, proteinKeyword, drug, functionalScore, clinical, clinicalSignificance, clinicalConfirmedStatus, customAnnotation, panel, panelModeOfInheritance, panelConfidence, panelRoleInCancer, panelFeatureType, panelIntersection, trait \cr
   runRelatedness \tab /{apiVersion}/analysis/variant/relatedness/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runRvtests \tab /{apiVersion}/analysis/variant/rvtests/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   aggregationStatsSample \tab /{apiVersion}/analysis/variant/sample/aggregationStats \tab savedFilter, region, type, project, study, file, filter, sample, genotype, sampleAnnotation, family, familyDisorder, familySegregation, familyMembers, familyProband, ct, biotype, populationFrequencyAlt, clinical, clinicalSignificance, clinicalConfirmedStatus, field \cr
   runSampleEligibility \tab /{apiVersion}/analysis/variant/sample/eligibility/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runSampleQc \tab /{apiVersion}/analysis/variant/sample/qc/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   querySample \tab /{apiVersion}/analysis/variant/sample/query \tab limit, skip, variant, study, genotype \cr
   runSample \tab /{apiVersion}/analysis/variant/sample/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   querySampleStats \tab /{apiVersion}/analysis/variant/sample/stats/query \tab region, type, study, file, filter, sampleData, ct, biotype, transcriptFlag, populationFrequencyAlt, clinical, clinicalSignificance, clinicalConfirmedStatus, study, filterTranscript, sample\link{*} \cr
   runSampleStats \tab /{apiVersion}/analysis/variant/sample/stats/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runStatsExport \tab /{apiVersion}/analysis/variant/stats/export/run \tab project, study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   runStats \tab /{apiVersion}/analysis/variant/stats/run \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
}
}
\section{Endpoint /{apiVersion}/analysis/variant/aggregationStats}{

Calculate and fetch aggregation stats.
}

\section{Endpoint /{apiVersion}/analysis/variant/annotation/metadata}{

Read variant annotations metadata from any saved versions.
}

\section{Endpoint /{apiVersion}/analysis/variant/annotation/query}{

Query variant annotations from any saved versions.
}

\section{Endpoint /{apiVersion}/analysis/variant/circos/run}{

Generate a Circos plot for a given sample.
}

\section{Endpoint /{apiVersion}/analysis/variant/cohort/stats/delete}{

Delete cohort variant stats from a cohort.
}

\section{Endpoint /{apiVersion}/analysis/variant/cohort/stats/info}{

Read cohort variant stats from list of cohorts.
}

\section{Endpoint /{apiVersion}/analysis/variant/cohort/stats/run}{

Compute cohort variant stats for the selected list of samples.
}

\section{Endpoint /{apiVersion}/analysis/variant/export/run}{

Filter and export variants from the variant storage to a file.
}

\section{Endpoint /{apiVersion}/analysis/variant/family/genotypes}{

Calculate the possible genotypes for the members of a family.
}

\section{Endpoint /{apiVersion}/analysis/variant/family/qc/run}{

Run quality control (QC) for a given family. It computes the relatedness scores among the family members.
}

\section{Endpoint /{apiVersion}/analysis/variant/file/delete}{

\link{DEPRECATED} Use operation/variant/delete.
}

\section{Endpoint /{apiVersion}/analysis/variant/gatk/run}{

GATK is a Genome Analysis Toolkit for variant discovery in high-throughput sequencing data. Supported Gatk commands: HaplotypeCaller.
}

\section{Endpoint /{apiVersion}/analysis/variant/genomePlot/run}{

Generate a genome plot for a given sample.
}

\section{Endpoint /{apiVersion}/analysis/variant/gwas/run}{

Run a Genome Wide Association Study between two cohorts.
}

\section{Endpoint /{apiVersion}/analysis/variant/index/run}{

\link{DEPRECATED} Use operation/variant/index.
}

\section{Endpoint /{apiVersion}/analysis/variant/individual/qc/run}{

Run quality control (QC) for a given individual. It includes inferred sex and  mendelian errors (UDP).
}

\section{Endpoint /{apiVersion}/analysis/variant/inferredSex/run}{

Infer sex from chromosome mean coverages.
}

\section{Endpoint /{apiVersion}/analysis/variant/knockout/gene/query}{

Fetch values from KnockoutAnalysis result, by genes.
}

\section{Endpoint /{apiVersion}/analysis/variant/knockout/individual/query}{

Fetch values from KnockoutAnalysis result, by individuals.
}

\section{Endpoint /{apiVersion}/analysis/variant/knockout/run}{

Obtains the list of knocked out genes for each sample.
}

\section{Endpoint /{apiVersion}/analysis/variant/mendelianError/run}{

Run mendelian error analysis to infer uniparental disomy regions.
}

\section{Endpoint /{apiVersion}/analysis/variant/metadata}{

.
}

\section{Endpoint /{apiVersion}/analysis/variant/mutationalSignature/query}{

Run mutational signature analysis for a given sample. Use context index.
}

\section{Endpoint /{apiVersion}/analysis/variant/mutationalSignature/run}{

Run mutational signature analysis for a given sample.
}

\section{Endpoint /{apiVersion}/analysis/variant/plink/run}{

Plink is a whole genome association analysis toolset, designed to perform a range of basic, large-scale analyses.
}

\section{Endpoint /{apiVersion}/analysis/variant/query}{

Filter and fetch variants from indexed VCF files in the variant storage.
}

\section{Endpoint /{apiVersion}/analysis/variant/relatedness/run}{

Compute a score to quantify relatedness between samples.
}

\section{Endpoint /{apiVersion}/analysis/variant/rvtests/run}{

Rvtests is a flexible software package for genetic association studies. Supported RvTests commands: rvtest, vcf2kinship.
}

\section{Endpoint /{apiVersion}/analysis/variant/sample/aggregationStats}{

Calculate and fetch sample aggregation stats.
}

\section{Endpoint /{apiVersion}/analysis/variant/sample/eligibility/run}{

Filter samples by a complex query involving metadata and variants data.
}

\section{Endpoint /{apiVersion}/analysis/variant/sample/qc/run}{

Run quality control (QC) for a given sample. It includes variant stats, and if the sample is somatic, mutational signature and genome plot are calculated.
}

\section{Endpoint /{apiVersion}/analysis/variant/sample/query}{

Get sample data of a given variant.
}

\section{Endpoint /{apiVersion}/analysis/variant/sample/run}{

Get samples given a set of variants.
}

\section{Endpoint /{apiVersion}/analysis/variant/sample/stats/query}{

Obtain sample variant stats from a sample.
}

\section{Endpoint /{apiVersion}/analysis/variant/sample/stats/run}{

Compute sample variant stats for the selected list of samples.
}

\section{Endpoint /{apiVersion}/analysis/variant/stats/export/run}{

Export calculated variant stats and frequencies.
}

\section{Endpoint /{apiVersion}/analysis/variant/stats/run}{

Compute variant stats for any cohort and any set of variants.
}

\seealso{
\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
\link{*}: Required parameter
}
