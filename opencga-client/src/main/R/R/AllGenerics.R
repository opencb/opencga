# ##############################################################################
## UserClient
<<<<<<< HEAD
setGeneric("userClient", function(OpencgaR, user, users, filterId, endpointName, params=NULL, ...)
=======
setGeneric("userClient", function(OpencgaR, users, user, filterId, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, projects, project, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
<<<<<<< HEAD
setGeneric("studyClient", function(OpencgaR, variableSet, group, members, study, studies, templateId, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, templateId, studies, group, members, study, variableSet, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, annotationSet, members, folder, files, file, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, files, folder, file, members, annotationSet, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
<<<<<<< HEAD
setGeneric("jobClient", function(OpencgaR, members, jobs, job, endpointName, params=NULL, ...)
=======
setGeneric("jobClient", function(OpencgaR, jobs, members, job, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, annotationSet, members, sample, samples, endpointName, params=NULL, ...)
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
<<<<<<< HEAD
setGeneric("individualClient", function(OpencgaR, individuals, members, annotationSet, individual, endpointName, params=NULL, ...)
=======
setGeneric("individualClient", function(OpencgaR, individual, annotationSet, members, individuals, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, annotationSet, family, members, families, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, family, annotationSet, members, families, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
<<<<<<< HEAD
setGeneric("cohortClient", function(OpencgaR, cohorts, members, annotationSet, cohort, endpointName, params=NULL, ...)
=======
setGeneric("cohortClient", function(OpencgaR, cohorts, cohort, members, annotationSet, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
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
<<<<<<< HEAD
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalyses, members, interpretation, clinicalAnalysis, interpretations, endpointName, params=NULL, ...)
=======
setGeneric("clinicalClient", function(OpencgaR, interpretations, interpretation, clinicalAnalysis, members, clinicalAnalyses, endpointName, params=NULL, ...)
>>>>>>> release-2.2.x
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
setGeneric("ga4ghClient", function(OpencgaR, file, study, endpointName, params=NULL, ...)
    standardGeneric("ga4ghClient"))

# ##############################################################################
## AdminClient
setGeneric("adminClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("adminClient"))

