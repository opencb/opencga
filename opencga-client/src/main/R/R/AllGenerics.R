# ##############################################################################
## UserClient
<<<<<<< HEAD
setGeneric("userClient", function(OpencgaR, filterId, user, users, endpointName, params=NULL, ...)
=======
setGeneric("userClient", function(OpencgaR, filterId, users, user, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, projects, project, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
<<<<<<< HEAD
setGeneric("studyClient", function(OpencgaR, group, members, study, studies, templateId, variableSet, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, members, templateId, studies, variableSet, group, study, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, members, annotationSet, folder, file, files, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, folder, file, members, files, annotationSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, members, jobs, job, endpointName, params=NULL, ...)
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
<<<<<<< HEAD
setGeneric("sampleClient", function(OpencgaR, annotationSet, samples, members, sample, endpointName, params=NULL, ...)
=======
setGeneric("sampleClient", function(OpencgaR, members, sample, samples, annotationSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
<<<<<<< HEAD
setGeneric("individualClient", function(OpencgaR, individual, members, individuals, annotationSet, endpointName, params=NULL, ...)
=======
setGeneric("individualClient", function(OpencgaR, members, individual, annotationSet, individuals, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, family, families, members, annotationSet, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, members, families, family, annotationSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
<<<<<<< HEAD
setGeneric("cohortClient", function(OpencgaR, cohort, members, cohorts, annotationSet, endpointName, params=NULL, ...)
=======
setGeneric("cohortClient", function(OpencgaR, members, cohort, annotationSet, cohorts, endpointName, params=NULL, ...)
>>>>>>> develop
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
setGeneric("clinicalClient", function(OpencgaR, members, study, clinicalAnalyses, interpretation, interpretations, clinicalAnalysis, endpointName, params=NULL, ...)
=======
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalysis, members, interpretation, clinicalAnalyses, interpretations, endpointName, params=NULL, ...)
>>>>>>> develop
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

