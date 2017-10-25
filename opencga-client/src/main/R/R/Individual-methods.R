################################################################################
#' IndividualClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Individuals
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param individual a character string or a vector containing individual ids
#' @param annotationsetName a character string with the annotationset name. Only 
#' necessary when updating and deleting using the individualAnnotationClient
#' @param memberId a character or a vector contaning user or group ids to 
#' work on. Mandatory when using individualAclClient
#' @param action action to be performed on the individual(s)
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("individualClient", "OpencgaR", function(OpencgaR, individual, action, params=NULL) {
    category <- "individuals"
    switch(action,
           annotationsets=fetchOpenCGA(object=OpencgaR, category=category, 
                                       categoryId=individual, action="annotationsets", 
                                       params=params, httpMethod="GET"),
           groupBy=fetchOpenCGA(object=OpencgaR, category=category, action="groupBy", 
                                params=params, httpMethod="GET"),
           search=fetchOpenCGA(object=OpencgaR, category=category, action="search", 
                               params=params, httpMethod="GET"),
           acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                            action="acl", params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                               action="delete", params=params, httpMethod="GET"),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                             action="info", params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category,  
                               action="create", params=params, httpMethod="POST"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                               action="update", params=params, httpMethod="POST")
    )
})

setMethod("individualAnnotationsetClient", "OpencgaR", function(OpencgaR, individual, 
                                                             annotationsetName, action, 
                                                             params=NULL) {
    category <- "individuals"
    switch(action,
           search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                               subcategory="annotationsets", action="search", 
                               params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action="delete", params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                               subcategory="annotationsets", action="create", 
                               params=params, httpMethod="POST", as.queryParam="variableSet"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=individual, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action="update", params=params, httpMethod="POST")
    )
})

setMethod("individualAclClient", "OpencgaR", function(OpencgaR, memberIds, action, params=NULL) {
    category <- "individuals"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, subcategory="acl", 
                               subcategoryId=memberIds, action="update", params=params, 
                               httpMethod="POST")
    )
})
