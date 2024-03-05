# ##############################################################################
## OrganizationClient
setGeneric("organizationClient", function(OpencgaR, organization, endpointName, params=NULL, ...)
    standardGeneric("organizationClient"))

# ##############################################################################
## UserClient
<<<<<<< HEAD
setGeneric("userClient", function(OpencgaR, users, filterId, user, endpointName, params=NULL, ...)
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
setGeneric("studyClient", function(OpencgaR, study, studies, variableSet, group, members, templateId, endpointName, params=NULL, ...)
=======
setGeneric("studyClient", function(OpencgaR, templateId, study, members, studies, group, variableSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("studyClient"))

# ##############################################################################
## FileClient
<<<<<<< HEAD
setGeneric("fileClient", function(OpencgaR, annotationSet, file, files, members, folder, endpointName, params=NULL, ...)
=======
setGeneric("fileClient", function(OpencgaR, annotationSet, members, files, file, folder, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("fileClient"))

# ##############################################################################
## JobClient
<<<<<<< HEAD
setGeneric("jobClient", function(OpencgaR, members, job, jobs, endpointName, params=NULL, ...)
=======
setGeneric("jobClient", function(OpencgaR, members, jobs, job, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("jobClient"))

# ##############################################################################
## SampleClient
<<<<<<< HEAD
setGeneric("sampleClient", function(OpencgaR, members, annotationSet, sample, samples, endpointName, params=NULL, ...)
=======
setGeneric("sampleClient", function(OpencgaR, members, sample, samples, annotationSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("sampleClient"))

# ##############################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, members, annotationSet, individuals, individual, endpointName, params=NULL, ...)
    standardGeneric("individualClient"))

# ##############################################################################
## FamilyClient
<<<<<<< HEAD
setGeneric("familyClient", function(OpencgaR, members, annotationSet, families, family, endpointName, params=NULL, ...)
=======
setGeneric("familyClient", function(OpencgaR, members, family, families, annotationSet, endpointName, params=NULL, ...)
>>>>>>> develop
    standardGeneric("familyClient"))

# ##############################################################################
## CohortClient
<<<<<<< HEAD
setGeneric("cohortClient", function(OpencgaR, members, annotationSet, cohort, cohorts, endpointName, params=NULL, ...)
=======
setGeneric("cohortClient", function(OpencgaR, members, cohorts, annotationSet, cohort, endpointName, params=NULL, ...)
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
setGeneric("clinicalClient", function(OpencgaR, interpretations, clinicalAnalyses, clinicalAnalysis, members, interpretation, endpointName, params=NULL, ...)
=======
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalysis, annotationSet, clinicalAnalyses, interpretation, members, interpretations, endpointName, params=NULL, ...)
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

