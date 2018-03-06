################################################################################
#' CohortClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Cohorts
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param cohort a character string containing the cohort ID or name
#' @param annotationsetName a character string with the annotationset name. Only 
#' necessary when updating and deleting using the sampleAnnotationClient
#' @param memberId a character or a vector contaning user or group ids to 
#' work on. Mandatory when using sampleAclClient
#' @param action action to be performed on the sample(s)
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("cohortClient", "OpencgaR", function(OpencgaR, cohort, action, params=NULL) {
    category <- "cohorts"
    switch(action,
           samples=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                             action=action, params=params, httpMethod="GET"),
           search=fetchOpenCGA(object=OpencgaR, category=category,  
                               action=action, params=params, httpMethod="GET"),
           acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                            action=action, params=params, httpMethod="GET"),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                             action=action, params=params, httpMethod="GET"),
           annotationsets=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                                       action=action, params=params, httpMethod="GET"),
           groupBy=fetchOpenCGA(object=OpencgaR, category=category, 
                                action=action, params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                               action=action, params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, 
                               action=action, params=params, httpMethod="POST",
                               as.queryParam=c("variable", "variableSet")),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                               action=action, params=params, httpMethod="POST")
    )
})

#' @export
setMethod("cohortAnnotationsetClient", "OpencgaR", function(OpencgaR, cohort, 
                                                         annotationsetName, action, 
                                                         params=NULL) {
    category <- "cohorts"
    switch(action,
           search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                               subcategory="annotationsets", action=action, 
                               params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action=action, params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                               subcategory="annotationsets", action=action, 
                               params=params, httpMethod="POST", as.queryParam="variableSet"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=cohort, 
                               subcategory="annotationsets", subcategoryId=annotationsetName, 
                               action=action, params=params, httpMethod="POST")
    )
})


#' @export
setMethod("cohortAclClient", "OpencgaR", function(OpencgaR, memberIds, action, params=NULL) {
    category <- "cohorts"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, subcategory="acl", 
                               subcategoryId=memberIds, action="update", params=params, 
                               httpMethod="POST")
    )
})
