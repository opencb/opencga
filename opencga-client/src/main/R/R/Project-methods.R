################################################################################
#' ProjectClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Projects
#' @slot OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @slot project a character or a vector contaning the project ids or aliases to 
#' work on. Mandatory when using "info", "studies", "delete", "increlease" and 
#' "update" actions
#' @slot action action to be performed on the projects
#' @slot params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("projectClient", "OpencgaR", function(OpencgaR, project, action, params=NULL, ...) {
    category <- "projects"
    switch(action,
           search=fetchOpenCGA(object=OpencgaR, category=category, action=action, 
                               params=params, httpMethod = "GET", ...),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project, 
                             action=action, params=params, httpMethod = "GET", ...),
           studies=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project, 
                             action=action, params=params, httpMethod = "GET", ...),
           delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project,
                               action=action, params=params, httpMethod = "GET", ...),
           create=fetchOpenCGA(object=OpencgaR, category=category, action=action, 
                               params=params, httpMethod = "POST", ...),
           increlease=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project, 
                             action=action, params=params, httpMethod = "POST", ...),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project, 
                             action=action, params=params, httpMethod = "POST", ...)
           )
})
