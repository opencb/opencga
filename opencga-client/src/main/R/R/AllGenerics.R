# ##############################################################################
## OrganizationClient
setGeneric("organizationClient", function(OpencgaR, organization, endpointName, params=NULL, ...)
    standardGeneric("organizationClient"))

# ##############################################################################
## UserClient
<<<<<<< HEAD
setGeneric("userClient", function(OpencgaR, user, users, filterId, endpointName, params=NULL, ...)
=======
setGeneric("userClient", function(OpencgaR, user, filterId, users, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("userClient"))

# ##############################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, project, projects, endpointName, params=NULL, ...)
    standardGeneric("projectClient"))

# ##############################################################################
## StudyClient
<<<<<<< HEAD
setGeneric("studyClient", function(OpencgaR, members, study, group, variableSet, templateId, studies, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, group, members, templateId, studies, study, variableSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, members, folder, file, annotationSet, files, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, file, annotationSet, members, files, folder, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
<<<<<<< HEAD
setGeneric("jobClient", function(OpencgaR, jobs, members, job, endpointName, params=NULL, ...)
=======
setGeneric("jobClient", function(OpencgaR, job, members, jobs, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
<<<<<<< HEAD
setGeneric("sampleClient", function(OpencgaR, annotationSet, members, sample, samples, endpointName, params=NULL, ...)
=======
setGeneric("sampleClient", function(OpencgaR, members, annotationSet, sample, samples, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
<<<<<<< HEAD
setGeneric("individualClient", function(OpencgaR, individual, annotationSet, members, individuals, endpointName, params=NULL, ...)
=======
setGeneric("individualClient", function(OpencgaR, individual, members, annotationSet, individuals, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, families, annotationSet, members, family, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, members, annotationSet, families, family, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
<<<<<<< HEAD
setGeneric("cohortClient", function(OpencgaR, annotationSet, members, cohort, cohorts, endpointName, params=NULL, ...)
=======
setGeneric("cohortClient", function(OpencgaR, cohort, members, annotationSet, cohorts, endpointName, params=NULL, ...)
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
setGeneric("clinicalClient", function(OpencgaR, interpretations, members, interpretation, clinicalAnalysis, annotationSet, clinicalAnalyses, endpointName, params=NULL, ...)
=======
setGeneric("clinicalClient", function(OpencgaR, annotationSet, clinicalAnalysis, interpretations, members, clinicalAnalyses, interpretation, endpointName, params=NULL, ...)
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
setGeneric("adminClient", function(OpencgaR, user, endpointName, params=NULL, ...)
    standardGeneric("adminClient"))

