################################################################################
## UserClient
setGeneric("userClient", function(OpencgaR, user, filterId, action, params=NULL, ...)
    standardGeneric("userClient"))

################################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, projects, project, action, params=NULL, ...)
    standardGeneric("projectClient"))

################################################################################
## StudyClient
setGeneric("studyClient", function(OpencgaR, studies, variableSet, group, members, study, action, params=NULL, ...)
    standardGeneric("studyClient"))

################################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, annotationSet, file, files, members, folder, action, params=NULL, ...)
    standardGeneric("fileClient"))

################################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, members, jobs, job, action, params=NULL, ...)
    standardGeneric("jobClient"))

################################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, members, annotationSet, samples, sample, action, params=NULL, ...)
    standardGeneric("sampleClient"))

################################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, members, individuals, individual, annotationSet, action, params=NULL, ...)
    standardGeneric("individualClient"))

################################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, members, annotationSet, families, family, action, params=NULL, ...)
    standardGeneric("familyClient"))

################################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, members, annotationSet, cohort, cohorts, action, params=NULL, ...)
    standardGeneric("cohortClient"))

################################################################################
## PanelClient
setGeneric("panelClient", function(OpencgaR, members, panels, action, params=NULL, ...)
    standardGeneric("panelClient"))

################################################################################
## AnalysisAlignmentClient
setGeneric("analysisalignmentClient", function(OpencgaR, action, params=NULL, ...)
    standardGeneric("analysisalignmentClient"))

################################################################################
## AnalysisVariantClient
setGeneric("analysisvariantClient", function(OpencgaR, action, params=NULL, ...)
    standardGeneric("analysisvariantClient"))

################################################################################
## AnalysisClinicalClient
setGeneric("analysisclinicalClient", function(OpencgaR, members, clinicalAnalysis, clinicalAnalyses, action, params=NULL, ...)
    standardGeneric("analysisclinicalClient"))

################################################################################
## OperationClient
setGeneric("operationClient", function(OpencgaR, action, params=NULL, ...)
    standardGeneric("operationClient"))

################################################################################
## MetaClient
setGeneric("metaClient", function(OpencgaR, action, params=NULL, ...)
    standardGeneric("metaClient"))

################################################################################
## GA4GHClient
setGeneric("ga4ghClient", function(OpencgaR, file, study, action, params=NULL, ...)
    standardGeneric("ga4ghClient"))

