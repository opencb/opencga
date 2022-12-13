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
setGeneric("studyClient", function(OpencgaR, studies, group, templateId, variableSet, study, members, endpointName, params=NULL, ...)
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, annotationSet, folder, file, files, members, endpointName, params=NULL, ...)
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, jobs, job, members, endpointName, params=NULL, ...)
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, annotationSet, sample, samples, members, endpointName, params=NULL, ...)
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, annotationSet, individuals, individual, members, endpointName, params=NULL, ...)
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, families, annotationSet, family, members, endpointName, params=NULL, ...)
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, annotationSet, cohort, cohorts, members, endpointName, params=NULL, ...)
    standardGeneric("cohortClient"))

# ##############################################################################
## PanelClient
setGeneric("panelClient", function(OpencgaR, panels, members, endpointName, params=NULL, ...)
    standardGeneric("panelClient"))

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
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalysis, clinicalAnalyses, interpretations, interpretation, members, endpointName, params=NULL, ...)
    standardGeneric("clinicalClient"))

# ##############################################################################
## OperationClient
setGeneric("operationClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("operationClient"))

# ##############################################################################
## MetaClient
setGeneric("metaClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("metaClient"))

# ##############################################################################
## GA4GHClient
setGeneric("ga4ghClient", function(OpencgaR, study, file, endpointName, params=NULL, ...)
    standardGeneric("ga4ghClient"))

# ##############################################################################
## AdminClient
setGeneric("adminClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("adminClient"))

