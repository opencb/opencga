################################################################################
#' ClinicalAnalysisClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Clinical Analyses
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param clinicalAnalysis a character string or vector containing clinical analysis IDs
#' @param action action to be performed on the sample(s)
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("clinicalAnalysisClient", "OpencgaR", function(OpencgaR, clinicalAnalysis,
                                                 action, params=NULL, ...) {
    category <- "analysis"
    subcategory <- "clinical"
    switch(action,
           search=fetchOpenCGA(object=OpencgaR, category=category, subcategory= subcategory,
                               action=action, params=params, httpMethod="GET", ...),
           info=fetchOpenCGA(object=OpencgaR, category=category, subcategory= subcategory,
                             subcategoryId=clinicalAnalysis, action=action, 
                             params=params, httpMethod="GET", ...),
           create=fetchOpenCGA(object=OpencgaR, category=category, subcategory= subcategory,
                               action=action, params=params, httpMethod="POST", ...),
           update=fetchOpenCGA(object=OpencgaR, category=category, subcategory= subcategory,
                               subcategoryId=clinicalAnalysis,
                               action=action, params=params, httpMethod="POST", ...)
    )
})

#' @export
setMethod("clinicalAnalysisInterpretationClient", "OpencgaR", function(OpencgaR, clinicalAnalysis,
                                                         action, params=NULL, ...) {
    category <- "clinical"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, 
                               categoryId=clinicalAnalysis,action=action, params=params, 
                               as.queryParam="interpretationAction", httpMethod="POST", ...)
    )
})

#' @export
setMethod("clinicalAnalysisInterpretationToolClient", "OpencgaR", function(OpencgaR, action, 
                                                                           params=NULL, ...) {
    category <- "analysis/clinical"
    subcategory <- "interpretation/tools"
    switch(action,
           custom=fetchOpenCGA(object=OpencgaR, category=category, subcategory=subcategory,
                               action=action, params=params, httpMethod="GET", ...)
    )
})
