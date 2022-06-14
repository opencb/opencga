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
setGeneric("studyClient", function(OpencgaR, studies, group, variableSet, study, members, templateId, endpointName, params=NULL, ...)
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, annotationSet, folder, files, members, file, endpointName, params=NULL, ...)
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, jobs, members, job, endpointName, params=NULL, ...)
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, sample, members, samples, annotationSet, endpointName, params=NULL, ...)
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, individuals, individual, members, annotationSet, endpointName, params=NULL, ...)
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, family, members, families, annotationSet, endpointName, params=NULL, ...)
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, cohort, cohorts, members, annotationSet, endpointName, params=NULL, ...)
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
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalysis, interpretations, interpretation, members, clinicalAnalyses, endpointName, params=NULL, ...)
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

