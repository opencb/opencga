################################################################################
#' FileClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Files
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param f a character string or a vector containing file ids, folder ids, fields 
#' or a path. When specifying paths they must be separated by ":" instead of "/".
#' Mandatory when using studyClient with all actions except "create" and "search" 
#' and all actions in studyGroupClient
#' @param memberId a character or a vector contaning user or group ids to 
#' work on. Mandatory when using studyAclClient
#' @param action action to be performed on the files
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("fileClient", "OpencgaR", function(OpencgaR, f, action, params=NULL, ...) {
    category <- "files"
    switch(action,
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                             action=action, params=params, httpMethod="GET", ...),
           scan=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                             action=action, params=params, httpMethod="GET", ...),
           tree=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                             action=action, params=params, httpMethod="GET", ...),
           formats=fetchOpenCGA(object=OpencgaR, category=category,  
                                action=action, params=params, httpMethod="GET", ...),
           search=fetchOpenCGA(object=OpencgaR, category=category, 
                               action=action, params=params, httpMethod="GET", ...),
           stats=fetchOpenCGA(object=OpencgaR, category=category, 
                               action=action, params=params, httpMethod="GET", ...),
           list=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                             action=action, params=params, httpMethod="GET", ...),
           acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                            action=action, params=params, httpMethod="GET", ...),
           bioformats=fetchOpenCGA(object=OpencgaR, category=category,  
                                   action=action, params=params, httpMethod="GET", ...),
           refresh=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                                action=action, params=params, httpMethod="GET", ...),
           content=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                                action=action, params=params, httpMethod="GET", ...),
           download=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                                 action=action, params=params, httpMethod="GET", ...),
           grep=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                             action=action, params=params, httpMethod="GET", ...),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                               action=action, params=params, 
                               as.queryParam=c("samplesAction", "annotationSetsAction"), 
                               httpMethod="POST", ...),
           upload=fetchOpenCGA(object=OpencgaR, category=category, 
                               action=action, params=params, httpMethod="POST", ...),
           create=fetchOpenCGA(object=OpencgaR, category=category,  
                               action=action, params=params, httpMethod="POST", ...)
    )
})

#' @export
setMethod("fileAclClient", "OpencgaR", function(OpencgaR, f, memberId, action, params=NULL, ...) {
    category <- "files"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, subcategory="acl", 
                               subcategoryId=memberId, action=action, 
                               params=params, httpMethod="POST", ...)
    )
})

#' @export
setMethod("fileAnnotationsetClient", "OpencgaR", function(OpencgaR, f, annotationSet, action, params=NULL, ...) {
    category <- "files"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=f, 
                               subcategory="annotationSets", subcategoryId=annotationSet, 
                               action="annotations/update", params=params, 
                               as.queryParam="action", httpMethod="POST", ...)
    )
})

