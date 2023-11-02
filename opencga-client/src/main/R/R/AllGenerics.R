# ##############################################################################
## UserClient
<<<<<<< HEAD
setGeneric("userClient", function(OpencgaR, filterId, users, user, endpointName, params=NULL, ...)
=======
setGeneric("userClient", function(OpencgaR, user, filterId, users, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, projects, project, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
<<<<<<< HEAD
setGeneric("studyClient", function(OpencgaR, variableSet, templateId, members, study, studies, group, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, members, templateId, study, variableSet, group, studies, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, folder, annotationSet, file, members, files, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, members, folder, annotationSet, files, file, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, jobs, members, job, endpointName, params=NULL, ...)
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
<<<<<<< HEAD
setGeneric("sampleClient", function(OpencgaR, samples, annotationSet, sample, members, endpointName, params=NULL, ...)
=======
setGeneric("sampleClient", function(OpencgaR, members, sample, annotationSet, samples, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
<<<<<<< HEAD
setGeneric("individualClient", function(OpencgaR, individual, annotationSet, members, individuals, endpointName, params=NULL, ...)
=======
setGeneric("individualClient", function(OpencgaR, members, individuals, individual, annotationSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, families, members, family, annotationSet, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, members, families, family, annotationSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
<<<<<<< HEAD
setGeneric("cohortClient", function(OpencgaR, cohort, cohorts, members, annotationSet, endpointName, params=NULL, ...)
=======
setGeneric("cohortClient", function(OpencgaR, members, cohorts, cohort, annotationSet, endpointName, params=NULL, ...)
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
setGeneric("clinicalClient", function(OpencgaR, interpretation, members, interpretations, clinicalAnalyses, clinicalAnalysis, endpointName, params=NULL, ...)
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
setGeneric("adminClient", function(OpencgaR, user, endpointName, params=NULL, ...)
    standardGeneric("adminClient"))

