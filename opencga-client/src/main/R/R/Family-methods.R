################################################################################
#' FamilyClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Jobs
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param family a character string or a vector containing family ids
#' @param annotationsetName a character string with the annotationset name. Only 
#' necessary when updating and deleting using the familyAnnotationClient
#' @param memberId a character or a vector contaning user or group ids to 
#' work on. Mandatory when using familyAclClient
#' @param action action to be performed on the family or families
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("familyClient", "OpencgaR", function(OpencgaR, family, action, params=NULL) {
    category <- "families"
    switch(action,
           annotationsets=fetchOpenCGA(object=OpencgaR, category=category, 
                                       categoryId=family, action="annotationsets", 
                                       params=params, httpMethod="GET"),
           search=fetchOpenCGA(object=OpencgaR, category=category, action="search", 
                               params=params, httpMethod="GET"),
           acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family, 
                            action="acl", params=params, httpMethod="GET"),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family, 
                             action="info", params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category,  
                               action="create", params=params, httpMethod="POST"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family, 
                               action="update", params=params, httpMethod="POST")
    )
})

#' @export
setMethod("familyAnnotationsetClient", "OpencgaR", function(OpencgaR, family, annotationsetName, 
                                                         action, params=NULL) {
    category <- "families"
    switch(action,
           search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family, 
                               subcategory="annotationsets", action="search", 
                               params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action="delete", params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family, 
                               subcategory="annotationsets", action="create", 
                               params=params, httpMethod="POST", as.queryParam="variableSet"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action="update", params=params, httpMethod="POST")
    )
})

#' @export
setMethod("familyAclClient", "OpencgaR", function(OpencgaR, memberIds, action, params=NULL) {
    category <- "families"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, subcategory="acl", 
                               subcategoryId=memberIds, action="update", params=params, 
                               httpMethod="POST")
    )
})

