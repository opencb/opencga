# ##############################################################################
## UserClient
setGeneric("userClient", function(OpencgaR, filterId, user, users, endpointName, params=NULL, ...)
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, project, projects, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
setGeneric("studyClient", function(OpencgaR, members, study, templateId, group, studies, variableSet, endpointName, params=NULL, ...)
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, files, annotationSet, members, folder, file, endpointName, params=NULL, ...)
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, members, jobs, job, endpointName, params=NULL, ...)
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, members, annotationSet, samples, sample, endpointName, params=NULL, ...)
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, members, individual, individuals, annotationSet, endpointName, params=NULL, ...)
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, members, annotationSet, family, families, endpointName, params=NULL, ...)
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, members, annotationSet, cohorts, cohort, endpointName, params=NULL, ...)
    standardGeneric("cohortClient"))

# ##############################################################################
## PanelClient
setGeneric("panelClient", function(OpencgaR, members, panels, endpointName, params=NULL, ...)
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
setGeneric("clinicalClient", function(OpencgaR, interpretation, clinicalAnalyses, members, clinicalAnalysis, interpretations, endpointName, params=NULL, ...)
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

