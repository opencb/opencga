# ##############################################################################
## UserClient
<<<<<<< HEAD
setGeneric("userClient", function(OpencgaR, users, user, filterId, endpointName, params=NULL, ...)
=======
setGeneric("userClient", function(OpencgaR, users, filterId, user, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, projects, project, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
<<<<<<< HEAD
setGeneric("studyClient", function(OpencgaR, members, study, studies, variableSet, templateId, group, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, members, templateId, studies, group, study, variableSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, members, folder, file, annotationSet, files, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, files, annotationSet, members, file, folder, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
<<<<<<< HEAD
setGeneric("jobClient", function(OpencgaR, jobs, members, job, endpointName, params=NULL, ...)
=======
setGeneric("jobClient", function(OpencgaR, members, job, jobs, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
<<<<<<< HEAD
setGeneric("sampleClient", function(OpencgaR, annotationSet, members, samples, sample, endpointName, params=NULL, ...)
=======
setGeneric("sampleClient", function(OpencgaR, annotationSet, sample, members, samples, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
<<<<<<< HEAD
setGeneric("individualClient", function(OpencgaR, annotationSet, members, individual, individuals, endpointName, params=NULL, ...)
=======
setGeneric("individualClient", function(OpencgaR, annotationSet, individual, members, individuals, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, family, annotationSet, members, families, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, annotationSet, members, family, families, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
<<<<<<< HEAD
setGeneric("cohortClient", function(OpencgaR, cohort, annotationSet, members, cohorts, endpointName, params=NULL, ...)
=======
setGeneric("cohortClient", function(OpencgaR, annotationSet, members, cohorts, cohort, endpointName, params=NULL, ...)
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
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalysis, members, interpretations, clinicalAnalyses, interpretation, endpointName, params=NULL, ...)
=======
setGeneric("clinicalClient", function(OpencgaR, members, interpretations, interpretation, clinicalAnalysis, clinicalAnalyses, endpointName, params=NULL, ...)
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
## CvdbClient
setGeneric("cvdbClient", function(OpencgaR, endpointName, params=NULL, ...)
    standardGeneric("cvdbClient"))

# ##############################################################################
## AdminClient
setGeneric("adminClient", function(OpencgaR, user, endpointName, params=NULL, ...)
    standardGeneric("adminClient"))

