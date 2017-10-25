################################################################################
#' SampleClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Samples
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param sample a character string or a vector containing sample IDs or names
#' @param annotationsetName a character string with the annotationset name. Only 
#' necessary when updating and deleting using the sampleAnnotationClient
#' @param memberId a character or a vector contaning user or group ids to 
#' work on. Mandatory when using sampleAclClient
#' @param action action to be performed on the sample(s)
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("sampleClient", "OpencgaR", function(OpencgaR, sample, action, params=NULL) {
    category <- "samples"
    switch(action,
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                             action=action, params=params, httpMethod="GET"),
           annotationsets=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                             action=action, params=params, httpMethod="GET"),
           load=fetchOpenCGA(object=OpencgaR, category=category,  
                             action=action, params=params, httpMethod="GET"),
           search=fetchOpenCGA(object=OpencgaR, category=category,  
                               action=action, params=params, httpMethod="GET"),
           acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                            action=action, params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                               action=action, params=params, httpMethod="GET"),
           groupBy=fetchOpenCGA(object=OpencgaR, category=category, 
                                action=action, params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, 
                               action=action, params=params, httpMethod="POST",
                               as.queryParam="individual"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                               action=action, params=params, httpMethod="POST")
    )
})

setMethod("sampleAnnotationsetClient", "OpencgaR", function(OpencgaR, sample, 
                                                         annotationsetName, action, 
                                                         params=NULL) {
    category <- "samples"
    switch(action,
           search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                               subcategory="annotationsets", action=action, 
                               params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action=action, params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                               subcategory="annotationsets", action=action, 
                               params=params, httpMethod="POST", as.queryParam="variableSet"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action=action, params=params, httpMethod="POST")
    )
})

setMethod("sampleAclClient", "OpencgaR", function(OpencgaR, memberIds, action, params=NULL) {
    category <- "samples"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, subcategory="acl", 
                               subcategoryId=memberIds, action="update", params=params, 
                               httpMethod="POST")
    )
})

