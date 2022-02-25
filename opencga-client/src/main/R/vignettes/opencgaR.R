## ----install, eval=FALSE, echo=TRUE-------------------------------------------
#  ## Install opencgaR
#  # install.packages("opencgaR_2.2.0.tar.gz", repos=NULL, type="source")

## ----message=FALSE, warning=FALSE, include=FALSE------------------------------
library(opencgaR)
library(glue)
library(dplyr)
library(tidyr)

## ----eval=TRUE, results='hide'------------------------------------------------
## Initialise connection using a configuration in R list
conf <- list(version="v2", rest=list(host="https://ws.opencb.org/opencga-prod"))
con <- initOpencgaR(opencgaConfig=conf)

## ----eval=FALSE, results='hide'-----------------------------------------------
#  ## Initialise connection using a configuration file (in YAML or JSON format)
#  # conf <- "/path/to/conf/client-configuration.yml"
#  # con <- initOpencgaR(opencgaConfig=conf)

## ---- results='hide'----------------------------------------------------------
# Log in
con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser", 
                    autoRenew = TRUE, verbose = FALSE, showToken = FALSE)

## ---- results='hide'----------------------------------------------------------
projects <- projectClient(OpencgaR = con, endpointName = "search")
getResponseResults(projects)[[1]][,c("id","name", "description")]

## ---- results='hide'----------------------------------------------------------
projects <- projectClient(OpencgaR=con, project="population", endpointName="studies")
getResponseResults(projects)[[1]][,c("id","name", "description")]

## ---- results='hide'----------------------------------------------------------
study <- "population:1000g"

# Study name
study_result <- studyClient(OpencgaR = con, studies = study, endpointName = "info")
study_name <- getResponseResults(study_result)[[1]][,"name"]
# Number of samples in this study
count_samples <- sampleClient(OpencgaR = con, endpointName = "search", 
                              params=list(study=study, count=TRUE, limit=0))
num_samples <- getResponseNumMatches(count_samples)
# Number of individuals
count_individuals <- individualClient(OpencgaR = con, endpointName = "search", 
                                      params=list(study=study, count=TRUE, limit=0))
num_individuals <- getResponseNumMatches(count_individuals)
# Number of clinical cases
count_cases <- clinicalClient(OpencgaR = con, endpointName = "search", 
                              params=list(study=study, count=TRUE, limit=0))
num_cases <- getResponseNumMatches(count_cases)
# Number of variants
count_variants <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, count=TRUE, limit=0))
num_variants <- getResponseNumMatches(count_variants)

to_print <- "Study name: {study_name}
            - Number of samples: {num_samples}
            - Number of individuals: {num_individuals}
            - Number of cases: {num_cases}
            - Number of variants: {num_variants}"
glue(to_print)

## ---- results='hide'----------------------------------------------------------
# Retrieve the first variant
variant_example <- variantClient(OpencgaR = con, endpointName = "query", 
                                 params=list(study=study, limit=1, type="SNV"))
variant_id <- getResponseResults(variant_example)[[1]][,"id"]
glue("Variant example: {variant_id}")

## ----message=FALSE, warning=FALSE, results='hide'-----------------------------
# Retrieve the samples that have this variant
variant_result <- variantClient(OpencgaR = con, endpointName = "querySample", 
                                params=list(study=study, variant=variant_id))

glue("Number of samples with this variant: 
     {getResponseAttributes(variant_result)$numTotalSamples}")
data_keys <- unlist(getResponseResults(variant_result)[[1]][, "studies"][[1]][, "sampleDataKeys"])

df <- getResponseResults(variant_result)[[1]][, "studies"][[1]][, "samples"][[1]] %>% 
        select(-fileIndex) %>% unnest_wider(data)
colnames(df) <- c("samples", data_keys)
df

## ---- results='hide', message=FALSE, warning=FALSE, include=FALSE-------------
# Fetch the samples with a specific genotype
genotype <- "0/1,1/1"
variant_result <- variantClient(OpencgaR = con, endpointName = "querySample", 
                                params=list(study=study, variant=variant_id, 
                                            genotype=genotype))
glue("Number of samples with genotype {genotype} in this variant: 
     {getResponseAttributes(variant_result)$numTotalSamples}")

## ---- results='hide'----------------------------------------------------------
# Search for homozygous alternate samples
genotype <- "1/1"
variant_result <- variantClient(OpencgaR = con, endpointName = "querySample", 
                                params=list(study=study, variant=variant_id, 
                                            genotype=genotype))
glue("Number of samples with genotype {genotype} in this variant: 
     {getResponseAttributes(variant_result)$numTotalSamples}")

## ---- results='hide'----------------------------------------------------------
# Search for homozygous alternate samples
region <- "1:62273600-62273700"
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, region=region, limit=100))

glue("Number of variants in region {region}: {getResponseNumResults(variant_result)}")
getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by gene
genes <- "HCN3,MTOR"
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, gene=genes, limit=10))
getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by xref
snp <- "rs147394986"
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, xref=snp, limit=5))
getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by missense variants and stop_gained
ct <- "missense_variant,stop_gained"
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, ct=ct, limit=5))
glue("Number of missense and stop gained variants: {getResponseNumMatches(variant_result)}")
getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by LoF (alias created containing 9 different CTs)
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, ct="lof", limit=5))
glue("Number of LoF variants: {getResponseNumMatches(variant_result)}")
getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by population alternate frequency
population_frequency_alt <- '1kG_phase3:ALL<0.001'
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, 
                                            populationFrequencyAlt=population_frequency_alt, 
                                            limit=5))
glue("Number of variants matching the filter: {getResponseNumMatches(variant_result)}")
# getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by two population alternate frequency
## Remember to use commas for OR and semi-colon for AND
population_frequency_alt <- '1kG_phase3:ALL<0.001;GNOMAD_GENOMES:ALL<0.001'
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, 
                                            populationFrequencyAlt=population_frequency_alt, 
                                            limit=5))
glue("Number of variants matching the filter: {getResponseNumMatches(variant_result)}")

# getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by population alternate frequency using a range of values
population_frequency_alt <- '1kG_phase3:ALL>0.001;1kG_phase3:ALL<0.005'
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, 
                                            populationFrequencyAlt=population_frequency_alt, 
                                            limit=5))
glue("Number of variants matching the filter: {getResponseNumMatches(variant_result)}")

# getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by Consequence Type, Clinical Significance and population frequency
ct <- 'lof,missense_variant'
clinicalSignificance <- 'likely_pathogenic,pathogenic'
populationFrequencyAlt <- '1kG_phase3:ALL<0.01;GNOMAD_GENOMES:ALL<0.01'
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, limit=5, ct=ct, 
                                            clinicalSignificance=clinicalSignificance,
                                            populationFrequencyAlt=population_frequency_alt))
glue("Number of variants matching the filter: {getResponseNumMatches(variant_result)}")

# getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- results='hide'----------------------------------------------------------
## Filter by cohort ALL frequency
cohort_stats <- paste0(study, ':ALL<0.001')
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, cohortStatsAlt=cohort_stats, 
                                            limit=5))
glue("Number of variants matching the filter: {getResponseNumMatches(variant_result)}")

# getResponseResults(variant_result)[[1]] %>% select(-names, -studies, -annotation)

## ---- eval=FALSE--------------------------------------------------------------
#  ## Filter by custom cohorts' frequency
#  cohort_stats = study + ':MY_COHORT_A<0.001'
#  variant_result <- variantClient(OpencgaR = con, endpointName = "query",
#                                  params=list(study=study, cohortStatsAlt=cohort_stats,
#                                              limit=5))
#  glue("Number of variants matching the filter: {getResponseNumMatches(variant_result)}")
#  
#  ## Filter by more than one cohort frequency and consequence type
#  cohort_stats = study + ':MY_COHORT_A<0.001' + ';' + study + ':MY_COHORT_B<0.001'
#  variant_result <- variantClient(OpencgaR = con, endpointName = "query",
#                                  params=list(study=study, cohortStatsAlt=cohort_stats,
#                                              limit=5))
#  glue("Number of variants matching the filter: {getResponseNumMatches(variant_result)}")

## ---- results='hide'----------------------------------------------------------
variant_result <- variantClient(OpencgaR = con, endpointName = "query", 
                                params=list(study=study, id=variant_id))
getResponseResults(variant_result)[[1]]$studies[[1]]$stats[[1]][,c('cohortId', 'alleleCount',
                                                              'altAlleleCount', 'altAlleleFreq')]
glue("Variant example: {variant_id}")

## ---- results='hide', fig.width=7---------------------------------------------
## Aggregate by number of LoF variants per gene
variants_agg <- variantClient(OpencgaR = con, endpointName = "aggregationStats", 
                              params=list(study=study, ct='lof', fields="genes"))
df <- getResponseResults(variants_agg)[[1]]$buckets[[1]]
barplot(height = df$count, names.arg = df$value, las=2, cex.names = 0.6)

