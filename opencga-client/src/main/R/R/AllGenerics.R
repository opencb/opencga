################################################################################
## UserClient
setGeneric("userClient", function(OpencgaR, user, action, params=NULL)
    standardGeneric("userClient"))

setGeneric("userConfigClient", function(OpencgaR, user, name, action, 
                                        params=NULL)
    standardGeneric("userConfigClient"))

################################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, project, action, params=NULL)
    standardGeneric("projectClient"))

################################################################################
## StudiesClient
setGeneric("studyClient", function(OpencgaR, study, action, params=NULL)
    standardGeneric("studyClient"))

setGeneric("studyGroupClient", function(OpencgaR, study, group, action, 
                                        params=NULL)
    standardGeneric("StudyGroupClient"))

setGeneric("studyAclClient", function(OpencgaR, study, memberId, action, 
                                      params=NULL)
    standardGeneric("StudyAclClient"))

################################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, f, action, params=NULL)
    standardGeneric("FileClient"))

setGeneric("fileAclClient", function(OpencgaR, f, memberId, action, params=NULL)
    standardGeneric("FileAclClient"))

################################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, jobId, action, params=NULL)
    standardGeneric("jobClient"))

setGeneric("jobAclClient", function(OpencgaR, jobId, memberId, action, 
                                    params=NULL)
    standardGeneric("jobAclClient"))

################################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, family, action, params=NULL)
    standardGeneric("familyClient"))

setGeneric("familyAnnotationClient", function(OpencgaR, family, 
                                              annotationsetName, action, 
                                              params=NULL)
    standardGeneric("familyAnnotationClient"))

################################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, individual, action, 
                                        params=NULL)
    standardGeneric("individualClient"))

setGeneric("individualAnnotationClient", function(OpencgaR, individual, 
                                              annotationsetName, action, 
                                              params=NULL)
    standardGeneric("individualAnnotationClient"))

################################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, cohort, action, params=NULL)
    standardGeneric("individualClient"))

setGeneric("cohortAnnotationClient", function(OpencgaR, cohort, 
                                              annotationsetName, action, 
                                              params=NULL)
    standardGeneric("cohortAnnotationClient"))

################################################################################
## ClinicalClient
setGeneric("clinicalClient", function(OpencgaR, clinicalAnalysis, action, 
                                      params=NULL)
    standardGeneric("clinicalClient"))

################################################################################
## MetaClient
setGeneric("metaClient", function(OpencgaR, action, params=NULL)
    standardGeneric("metaClient"))

################################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, sample, action, params=NULL) 
    standardGeneric("sampleClient"))

setGeneric("sampleAnnotationClient", function(OpencgaR, sample, 
                                              annotationsetName, action, 
                                              params=NULL) 
    standardGeneric("sampleAnnotationClient"))

################################################################################
## AnalysisVariantClient
setGeneric("AnalysisVariantClient", function(OpencgaR, action, params=NULL) 
    standardGeneric("AnalysisVariantClient"))
