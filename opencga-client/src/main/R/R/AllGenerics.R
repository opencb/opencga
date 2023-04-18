# ##############################################################################
## UserClient
<<<<<<< HEAD
setGeneric("userClient", function(OpencgaR, filterId, user, users, endpointName, params=NULL, ...)
=======
setGeneric("userClient", function(OpencgaR, user, users, filterId, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, projects, project, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
<<<<<<< HEAD
setGeneric("studyClient", function(OpencgaR, studies, study, group, templateId, members, variableSet, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, studies, templateId, study, variableSet, members, group, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, file, folder, members, annotationSet, files, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, file, files, annotationSet, folder, members, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
<<<<<<< HEAD
setGeneric("jobClient", function(OpencgaR, job, members, jobs, endpointName, params=NULL, ...)
=======
setGeneric("jobClient", function(OpencgaR, job, jobs, members, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
<<<<<<< HEAD
setGeneric("sampleClient", function(OpencgaR, members, annotationSet, sample, samples, endpointName, params=NULL, ...)
=======
setGeneric("sampleClient", function(OpencgaR, annotationSet, samples, sample, members, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
<<<<<<< HEAD
setGeneric("individualClient", function(OpencgaR, members, annotationSet, individual, individuals, endpointName, params=NULL, ...)
=======
setGeneric("individualClient", function(OpencgaR, individuals, annotationSet, individual, members, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, annotationSet, members, families, family, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, annotationSet, families, family, members, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
<<<<<<< HEAD
setGeneric("cohortClient", function(OpencgaR, annotationSet, members, cohort, cohorts, endpointName, params=NULL, ...)
=======
setGeneric("cohortClient", function(OpencgaR, annotationSet, cohorts, cohort, members, endpointName, params=NULL, ...)
>>>>>>> develop
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
setGeneric("clinicalClient", function(OpencgaR, interpretation, clinicalAnalysis, clinicalAnalyses, members, interpretations, endpointName, params=NULL, ...)
=======
setGeneric("clinicalClient", function(OpencgaR, interpretations, clinicalAnalysis, members, clinicalAnalyses, interpretation, endpointName, params=NULL, ...)
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

