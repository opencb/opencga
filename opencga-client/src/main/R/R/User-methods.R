################################################################################
#' UserClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Users
#' @slot OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @slot user a character or a vector contaning the user ids or aliases to 
#' work on. Mandatory in all actions except "create"
#' @slot name a character or a vector contaning the filter name to work on. 
#' Mandatory in all userConfigClient actions except "create" and all 
#' userFilterClient except "list" and "create"
#' @slot action action to be performed on users
#' @slot params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("userClient", "OpencgaR", function(OpencgaR, user, action, params=NULL) {
    category <- "users"
    switch(action,
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                               action="delete", params=params, httpMethod="GET"),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                             action="info", params=params, httpMethod="GET"),
           projects=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user,
                                 action="projects", params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, action="create", 
                               params=params, httpMethod="POST"),
           login=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                              action="login", params=params, httpMethod="POST"),
           `change-password`=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                                          action="change-password", params=params, 
                                          httpMethod="POST"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                               action="update", params=params, httpMethod="POST")
    )
})

setMethod("userConfigClient", "OpencgaR", function(OpencgaR, user, name, action, params=NULL) {
    category <- "users"
    switch(action,
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                               subcategory="configs", subcategoryId=name, 
                               action="delete", params=params, httpMethod="GET"),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                             subcategory="configs", subcategoryId=name, 
                             action="info", params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                               subcategory="configs", action="create", 
                               params=params, httpMethod="POST")
    )
})

setMethod("userFilterClient", "OpencgaR", function(OpencgaR, user, name, action, params=NULL) {
    category <- "users"
    switch(action,
           list=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                             subcategory="configs/filters", action="list", 
                             params=params, httpMethod="GET"),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                               subcategory="configs/filters", subcategoryId=name, 
                               action="delete", params=params, httpMethod="GET"),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                             subcategory="configs/filters", subcategoryId=name, 
                             action="info", params=params, httpMethod="GET"),
           create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                               subcategory="configs/filters", action="create", 
                               params=params, httpMethod="POST"),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, 
                               subcategory="configs/filters", subcategoryId=name, 
                               action="update", params=params, httpMethod="POST")
    )
})
