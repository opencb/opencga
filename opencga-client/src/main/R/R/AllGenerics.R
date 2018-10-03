################################################################################
## UserClient
setGeneric("userClient", function(OpencgaR, user, action, params=NULL, ...)
    standardGeneric("userClient"))

setGeneric("userConfigClient", function(OpencgaR, user, name, action, params=NULL, ...)
    standardGeneric("userConfigClient"))

setGeneric("userFilterClient", function(OpencgaR, user, name=NULL, action, params=NULL, ...)
    standardGeneric("userFilterClient"))

################################################################################
## ProjectClient
setGeneric("projectClient", function(OpencgaR, project, action, params=NULL, ...)
    standardGeneric("projectClient"))

################################################################################
## StudyClient
setGeneric("studyClient", function(OpencgaR, study, action, params=NULL, ...)
    standardGeneric("studyClient"))

setGeneric("studyGroupClient", function(OpencgaR, study, group=NULL, action, params=NULL, ...)
    standardGeneric("studyGroupClient"))

setGeneric("studyAclClient", function(OpencgaR, study, memberId, action, params=NULL, ...)
    standardGeneric("studyAclClient"))

setGeneric("studyVariablesetClient", function(OpencgaR, study, variableSet=NULL, action, params=NULL, ...)
    standardGeneric("studyVariablesetClient"))

setGeneric("studyPermissionRulesClient", function(OpencgaR, study, action, params=NULL, ...)
    standardGeneric("studyPermissionRulesClient"))

# setGeneric("studyVariablesetFieldClient", function(OpencgaR, variableSet, action, params=NULL, ...)
#     standardGeneric("studyVariablesetFieldClient"))

################################################################################
## FileClient
setGeneric("fileClient", function(OpencgaR, f, action, params=NULL, ...)
    standardGeneric("fileClient"))

setGeneric("fileAclClient", function(OpencgaR, f, memberId, action, params=NULL, ...)
    standardGeneric("fileAclClient"))

setGeneric("fileAnnotationsetClient", function(OpencgaR, f, annotationSet, action, params=NULL, ...)
    standardGeneric("fileAnnotationsetClient"))

################################################################################
## JobClient
setGeneric("jobClient", function(OpencgaR, jobId, action, params=NULL, ...)
    standardGeneric("jobClient"))

setGeneric("jobAclClient", function(OpencgaR, jobId, memberId, action, params=NULL, ...)
    standardGeneric("jobAclClient"))

################################################################################
## FamilyClient
setGeneric("familyClient", function(OpencgaR, family, action, params=NULL, ...)
    standardGeneric("familyClient"))

setGeneric("familyAnnotationsetClient", function(OpencgaR, family, annotationSet, 
                                              action, params=NULL, ...)
    standardGeneric("familyAnnotationsetClient"))

setGeneric("familyAclClient", function(OpencgaR, memberIds, action, params=NULL, ...)
    standardGeneric("familyAclClient"))

################################################################################
## IndividualClient
setGeneric("individualClient", function(OpencgaR, individual, action, 
                                        params=NULL, ...)
    standardGeneric("individualClient"))

setGeneric("individualAnnotationsetClient", function(OpencgaR, individual, annotationSet, 
                                                  action, params=NULL, ...)
    standardGeneric("individualAnnotationsetClient"))

setGeneric("individualAclClient", function(OpencgaR, memberIds, action, params=NULL, ...)
    standardGeneric("individualAclClient"))

################################################################################
## SampleClient
setGeneric("sampleClient", function(OpencgaR, sample, action, params=NULL, ...) 
    standardGeneric("sampleClient"))

setGeneric("sampleAnnotationsetClient", function(OpencgaR, sample, annotationSet, 
                                              action, params=NULL, ...) 
    standardGeneric("sampleAnnotationsetClient"))

setGeneric("sampleAclClient", function(OpencgaR, memberIds, action, params=NULL, ...)
    standardGeneric("sampleAclClient"))

################################################################################
## CohortClient
setGeneric("cohortClient", function(OpencgaR, cohort, action, params=NULL, ...)
    standardGeneric("cohortClient"))

setGeneric("cohortAnnotationsetClient", function(OpencgaR, cohort, annotationSet, 
                                              action, params=NULL, ...)
    standardGeneric("cohortAnnotationsetClient"))

setGeneric("cohortAclClient", function(OpencgaR, memberIds, action, params=NULL, ...)
    standardGeneric("cohortAclClient"))

################################################################################
## ClinicalAnalysisClient
setGeneric("clinicalAnalysisClient", function(OpencgaR, clinicalAnalysis, action, 
                                      params=NULL, ...)
    standardGeneric("clinicalAnalysisClient"))

setGeneric("clinicalAnalysisInterpretationClient", function(OpencgaR, clinicalAnalysis, 
                                                             action, params=NULL, ...)
    standardGeneric("clinicalAnalysisInterpretationClient"))

################################################################################
## DiseasePanelClient
setGeneric("diseasePanelClient", function(OpencgaR, diseasePanel, action, 
                                          params=NULL, ...)
    standardGeneric("diseasePanelClient"))

setGeneric("diseasePanelAclClient", function(OpencgaR, members, action, params=NULL, ...)
    standardGeneric("diseasePanelAclClient"))

################################################################################
## AnalysisAlignmentClient
setGeneric("analysisAlignmentClient", function(OpencgaR, action, params=NULL, ...) 
    standardGeneric("analysisAlignmentClient"))

################################################################################
## AnalysisVariantClient
setGeneric("analysisVariantClient", function(OpencgaR, action, params=NULL, ...) 
    standardGeneric("analysisVariantClient"))

setGeneric("analysisVariantAnnotationClient", function(OpencgaR, action, params=NULL, ...) 
    standardGeneric("analysisVariantAnnotationClient"))

################################################################################
## AnalysisClinicalInterpretationClient
setGeneric("analysisClinicalInterpretationClient", function(OpencgaR, action, params=NULL, ...) 
    standardGeneric("analysisClinicalInterpretationClient"))

################################################################################
## AnalysisToolClient
setGeneric("analysisToolClient", function(OpencgaR, action, params=NULL, ...) 
    standardGeneric("analysisToolClient"))

################################################################################
## MetaClient
setGeneric("metaClient", function(OpencgaR, action, params=NULL, ...)
    standardGeneric("metaClient"))

