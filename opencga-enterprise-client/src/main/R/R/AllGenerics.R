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
setGeneric("studyClient", function(OpencgaR, members, templateId, studies, group, study, variableSet, endpointName, params=NULL, ...)
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, files, annotationSet, members, file, folder, endpointName, params=NULL, ...)
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, members, job, jobs, endpointName, params=NULL, ...)
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, annotationSet, sample, members, samples, endpointName, params=NULL, ...)
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, annotationSet, individual, members, individuals, endpointName, params=NULL, ...)
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, annotationSet, members, family, families, endpointName, params=NULL, ...)
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, annotationSet, members, cohorts, cohort, endpointName, params=NULL, ...)
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
setGeneric("clinicalClient", function(OpencgaR, members, interpretations, interpretation, clinicalAnalysis, clinicalAnalyses, endpointName, params=NULL, ...)
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
## CvaClient
setGeneric("cvaClient", function(OpencgaR, caseId, endpointName, params=NULL, ...)
    standardGeneric("cvaClient"))

# ##############################################################################
## AdminClient
setGeneric("adminClient", function(OpencgaR, user, endpointName, params=NULL, ...)
    standardGeneric("adminClient"))

