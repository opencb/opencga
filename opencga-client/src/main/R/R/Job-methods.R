################################################################################
#' JobClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Jobs
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param jobId a character string or a vector containing job ids.
#' @param memberId a character or a vector contaning user or group ids to 
#' work on. Mandatory when using jobAclClient
#' @param action action to be performed on the jobs
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("jobClient", "OpencgaR", function(OpencgaR, jobId, action, params=NULL, ...) {
    category <- "jobs"
    switch(action,
           groupBy=fetchOpenCGA(object=OpencgaR, category=category,  
                                action="groupBy", params=params, httpMethod="GET", ...),
           search=fetchOpenCGA(object=OpencgaR, category=category, 
                               action="search", params=params, httpMethod="GET", ...),
           acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=jobId, 
                            action="acl", params=params, httpMethod="GET", ...),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=jobId, 
                               action="delete", params=params, httpMethod="GET", ...),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=jobId, 
                             action="info", params=params, httpMethod="GET", ...),
           visit=fetchOpenCGA(object=OpencgaR, category=category, categoryId=jobId, 
                              action="visit", params=params, httpMethod="GET", ...),
           create=fetchOpenCGA(object=OpencgaR, category=category, 
                               action="create", params=params, httpMethod="POST", ...)
    )
})

#' @export
setMethod("jobAclClient", "OpencgaR", function(OpencgaR, jobId, memberId, action, params=NULL, ...){
    category <- "jobs"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=jobId, 
                               subcategory="acl", subcategoryId=memberId, 
                               action="update", params=params, httpMethod="POST", ...)
    )
})

