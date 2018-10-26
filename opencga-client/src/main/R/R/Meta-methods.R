################################################################################
#' MetaClient methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Meta
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param action action to be performed on the variableSet(s)
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("metaClient", "OpencgaR", function(OpencgaR, action, params=NULL, ...) {
    category <- "meta"
    switch(action,
           about=fetchOpenCGA(object=OpencgaR, category=category, action=action, 
                                params=params, httpMethod="GET", ...),
           ping=fetchOpenCGA(object=OpencgaR, category=category, action=action, 
                             params=params, httpMethod="GET", ...),
           status=fetchOpenCGA(object=OpencgaR, category=category, action=action, 
                               params=params, httpMethod="GET", ...)
    )
})

