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
<<<<<<< HEAD
setGeneric("studyClient", function(OpencgaR, group, study, templateId, variableSet, studies, members, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, templateId, group, members, study, variableSet, studies, endpointName, params=NULL, ...)
>>>>>>> release-2.4.x
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, annotationSet, files, members, folder, file, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, files, members, folder, file, annotationSet, endpointName, params=NULL, ...)
>>>>>>> release-2.4.x
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
<<<<<<< HEAD
setGeneric("jobClient", function(OpencgaR, job, members, jobs, endpointName, params=NULL, ...)
=======
setGeneric("jobClient", function(OpencgaR, jobs, job, members, endpointName, params=NULL, ...)
>>>>>>> release-2.4.x
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
<<<<<<< HEAD
setGeneric("sampleClient", function(OpencgaR, annotationSet, sample, members, samples, endpointName, params=NULL, ...)
=======
setGeneric("sampleClient", function(OpencgaR, annotationSet, samples, sample, members, endpointName, params=NULL, ...)
>>>>>>> release-2.4.x
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
<<<<<<< HEAD
setGeneric("individualClient", function(OpencgaR, annotationSet, individuals, members, individual, endpointName, params=NULL, ...)
=======
setGeneric("individualClient", function(OpencgaR, individuals, annotationSet, members, individual, endpointName, params=NULL, ...)
>>>>>>> release-2.4.x
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, families, annotationSet, members, family, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, families, annotationSet, family, members, endpointName, params=NULL, ...)
>>>>>>> release-2.4.x
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
<<<<<<< HEAD
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalysis, interpretations, interpretation, clinicalAnalyses, members, endpointName, params=NULL, ...)
=======
setGeneric("clinicalClient", function(OpencgaR, interpretations, interpretation, members, clinicalAnalyses, clinicalAnalysis, endpointName, params=NULL, ...)
>>>>>>> release-2.4.x
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

