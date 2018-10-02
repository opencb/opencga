################################################################################
#' DiseasePanel methods
#' @include commons.R
#' 
#' @description This function implements the OpenCGA calls for managing Disease Panels
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin 
#' where the connection and session details are stored
#' @param diseasePanel a string or vector containing the disease panel ID(s)
#' @param action action to be performed on the sample(s)
#' @param params list containing additional query or body params
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export

setMethod("diseasePanelClient", "OpencgaR", function(OpencgaR, diseasePanel, action, params=NULL, ...) {
    category <- "diseasePanels"
    switch(action,
           search=fetchOpenCGA(object=OpencgaR, category=category,  
                               action=action, params=params, httpMethod="GET", ...),
           acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=diseasePanel, 
                            action=action, params=params, httpMethod="GET", ...),
           info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=diseasePanel, 
                             action=action, params=params, httpMethod="GET", ...),
           create=fetchOpenCGA(object=OpencgaR, category=category, 
                               action=action, params=params, httpMethod="POST",
                               as.queryParam="importPanelId", ...),
           update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=diseasePanel, 
                               action=action, params=params, as.queryParam="incVersion",
                               httpMethod="POST", ...)
    )
})

#' @export
setMethod("diseasePanelAclClient", "OpencgaR", function(OpencgaR, members, action, 
                                                         params=NULL, ...) {
    category <- "diseasePanels"
    switch(action,
           update=fetchOpenCGA(object=OpencgaR, category=category, subcategory="acl", 
                               subcategoryId=members, action=action, params=params, 
                               httpMethod="POST", ...)
    )
})
