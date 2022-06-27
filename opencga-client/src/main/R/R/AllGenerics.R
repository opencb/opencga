# ##############################################################################
## UserClient
setGeneric("userClient", function(OpencgaR, users, filterId, user, endpointName, params=NULL, ...)
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, projects, project, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
setGeneric("studyClient", function(OpencgaR, templateId, group, members, variableSet, study, studies, endpointName, params=NULL, ...)
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, members, annotationSet, file, folder, files, endpointName, params=NULL, ...)
    standardGeneric("fileClient"))

# ##############################################################################
## ExecutionClient
setGeneric("executionClient", function(OpencgaR, executions, job, members, jobs, endpointName, params=NULL, ...)
    standardGeneric("executionClient"))

# ##############################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, job, members, jobs, endpointName, params=NULL, ...)
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, samples, members, annotationSet, sample, endpointName, params=NULL, ...)
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, individuals, annotationSet, members, individual, endpointName, params=NULL, ...)
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, family, families, members, annotationSet, endpointName, params=NULL, ...)
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, cohorts, members, annotationSet, cohort, endpointName, params=NULL, ...)
    standardGeneric("cohortClient"))

# ##############################################################################
## DiseasePanelClient
setGeneric("diseasepanelClient", function(OpencgaR, panels, members, endpointName, params=NULL, ...)
    standardGeneric("diseasepanelClient"))

# ##############################################################################
## AlignmentClient
setGeneric("alignmentClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("alignmentClient"))

# ##############################################################################
## VariantClient
setGeneric("variantClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("variantClient"))

# ##############################################################################
## ClinicalClient
setGeneric("clinicalClient", function(OpencgaR, members, interpretation, clinicalAnalyses, clinicalAnalysis, interpretations, endpointName, params=NULL, ...)
    standardGeneric("clinicalClient"))

# ##############################################################################
## VariantOperationClient
setGeneric("variantoperationClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("variantoperationClient"))

# ##############################################################################
## MetaClient
setGeneric("metaClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("metaClient"))

# ##############################################################################
## GA4GHClient
setGeneric("ga4ghClient", function(OpencgaR, file, study, endpointName, params=NULL, ...)
    standardGeneric("ga4ghClient"))

# ##############################################################################
## AdminClient
setGeneric("adminClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("adminClient"))

