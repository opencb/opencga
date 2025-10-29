################################################################################
#' RestResponse methods
#' 
#' @include AllClasses.R
#' @description The following methods implement the functions to extract 
#' information from the RestResponse object
#' @seealso \url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the 
#' RESTful API documentation \url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}

## Getters ---------------------------------------------------------------------
#' Getters from RestResponse
#' 
#' @export
getApiVersion <- function(restResponse) {
    restResponse@apiVersion
}
#' @export
getTime <- function(restResponse) {
    restResponse@time
}
#' @export
getEvents <- function(restResponse) {
    restResponse@events
}
#' @export
getParams <- function(restResponse) {
    restResponse@params
}
#' @export
getResponses <- function(restResponse) {
    restResponse@responses
}
# Response getters
#' @export
getResponseTime <- function(restResponse) {
    restResponse@responses$time
}
#' @export
getResponseEvents <- function(restResponse) {
    restResponse@responses$events
}
#' @export
getResponseNumResults <- function(restResponse) {
    restResponse@responses$numResults
}
#' @export
getResponseResultType <- function(restResponse) {
    restResponse@responses$resultType
}
#' @export
getResponseNumTotalResults <- function(restResponse) {
    restResponse@responses$numTotalResults
}
#' @export
getResponseNumMatches <- function(restResponse) {
    restResponse@responses$numMatches
}
#' @export
getResponseNumInserted <- function(restResponse) {
    restResponse@responses$numInserted
}
#' @export
getResponseNumUpdated <- function(restResponse) {
    restResponse@responses$numUpdated
}
#' @export
getResponseNumDeleted <- function(restResponse) {
    restResponse@responses$numDeleted
}
#' @export
getResponseAttributes <- function(restResponse) {
    restResponse@responses$attributes
}
#' @export
getResponseResults <- function(restResponse) {
    restResponse@responses$results
}
# ------------------------------------------------------------------------------

#' Get results from RestResponse
#' 
#' @description Fetch the list of results of the response specified in _response_.
#' @param RestResponse an object of class RestResponse obtained after executing 
#' a query to OpenCGA.
#' @param response Position of the response from the array of responses. Default: 1
#' @return the list of results of the specified response. If no _response_ is 
#' given, the results from the first list are returned.
#' @export
setGeneric("results", function(restResponse, response=1) standardGeneric("results"))
setMethod("results", signature="RestResponse", definition=function(restResponse, response=1) {
    getResponseResults(restResponse)[[response]]
})

#' Merge a list of RestResponse Results
#' 
#' @description Merge a list of results from multiple calls to the db
#' @param resultsList a list of results from a RestResponse object
#' @return a character string containing the JSON with the merged results
#' @export
setGeneric("mergeResults", function(resultsList) standardGeneric("mergeResults"))
setMethod("mergeResults", signature=list(), definition=function(resultsList) {
    # resultsMarta = list(list("f1.1", "f2.1", "f3.1"), list("f1.2", "f2.2", "f3.2"), list("f1.3", "f2.3", "f2.3"), list("f1.4", "f2.4", "f3.4"))
    # resultsList <- resultsMarta
    numCall <- length(resultsList)
    numFederation <- length(resultsList[[1]])
    container <- list()
    for (fed_num in 1:numFederation){
        container[[fed_num]] <- list()
        for (call_num in 1:numCall){
            container[[fed_num]][[call_num]] <- resultsList[[call_num]][[fed_num]]
        }
    }
    for (fed_num in 1:numFederation){
        container[[fed_num]] <- jsonlite::rbind_pages(container[[fed_num]])
    }
    # resultsMerged <- jsonlite::rbind_pages(resultsList)
    return(container)
})

#' Merge a list of RestResponses
#' 
#' @description Fetch the list of results of the response specified in _response_.
#' @param RestResponseList a list of RestResponse objects obtained after executing 
#' a query to OpenCGA.
#' @return a merged RestResponse object
#' @export
setGeneric("mergeResponses", function(restResponseList) standardGeneric("mergeResponses"))
setMethod("mergeResponses", signature=list(), definition=function(restResponseList) {
    if (length(restResponseList) == 1){
        return (restResponseList[[1]])
    }else if (length(restResponseList) > 1){
        # Merge apiVersions
        apiVersion <- unique(unlist(lapply(X = restResponseList, FUN = getApiVersion)))
        if (length(apiVersion) > 1) {
            warning("WARNING: More than one api version used in the query.")
            apiVersion <- paste(apiVersion, collapse = ", ")
        }
        # Merge times
        times = sum(sapply(X = restResponseList, FUN = getTime), na.rm = TRUE)
        # Merge events
        events <- lapply(X = restResponseList, FUN = getEvents)
        # Merge params
        params <- getParams(restResponseList[[1]])
        # Merge responses
        responses <- list(time = sum(sapply(X = restResponseList, FUN = getResponseTime), na.rm = TRUE),
                          events = sapply(X = restResponseList, FUN = getResponseEvents),
                          numResults = sum(sapply(X = restResponseList, FUN = getResponseNumResults), na.rm = TRUE),
                          results = mergeResults(resultsList = lapply(X = restResponseList, FUN = getResponseResults)),
                          resultType = unique(sapply(X = restResponseList, FUN = getResponseResultType)),
                          numMatches = sapply(X = restResponseList, FUN = getResponseNumMatches)[1],
                          numInserted = sapply(X = restResponseList, FUN = getResponseNumInserted)[1],
                          numUpdated = sapply(X = restResponseList, FUN = getResponseNumUpdated)[1],
                          numDeleted = sapply(X = restResponseList, FUN = getResponseNumDeleted)[1],
                          attributes = sapply(X = restResponseList, FUN = getResponseAttributes))
        mergedResponse <- new("RestResponse", 
                              apiVersion = apiVersion,
                              time = times,
                              events = events,
                              params = params,
                              responses = responses)
        return(mergedResponse)    
    }else{
        stop("ERROR: No data to process")
    }
})

