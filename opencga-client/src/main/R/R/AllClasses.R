################################################################################
#' OpencgaR Class
#' 
#' @description This is an S4 class which defines the OpencgaR object
#' @details This S4 class holds the default configuration required by OpencgaR 
#' methods to stablish the connection with the web services.
#' @slot host a character specifying the host url. Example: 
#' "http://bioinfo.hpc.cam.ac.uk/opencga-prod/"
#' @slot version a character specifying the API version. Example: "v2"
#' @slot user a character string with you OpenCGA username
#' @slot token string containin your token
#' @slot refreshToken string containin your renew token
#' @slot swagger list with the swagger information of the OpenCGA intance in the
#' spcified host
#' @seealso  \url{https://github.com/opencb/opencga/wiki} 
#' and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
#' @export
opencgaR <- setClass("OpencgaR", slots = c(host="character", 
                                           version="character",
                                           user="character",
                                           token="character",
                                           refreshToken="character",
                                           sessionFile="character",
                                           autoRenew="logical",
                                           showToken="logical",
                                           swagger="list"))


setMethod("show", signature = "OpencgaR", definition = function(object){
    cat("An object of class ", class(object), "\n", sep = "")
    cat(paste("| Host:", object@host))
    cat(paste("\n| Version:", object@version))
    if (.hasSlot(object, "token")) {
        cat(paste("\n| Token:", object@token))
    }
    if (.hasSlot(object, "expirationTime")) {
        cat(paste("\n| Expiration time:", object@expirationTime))
    }
})


################################################################################
#' RestResponse Class
#' 
#' @description This is an internal S4 class which defines the RestResponse object 
#' @details REST web services return the response wrapped in a RestResponse 
#' object. This consists of some metadata and a list of _OpenCGAResult_ objects 
#' called _responses_ containing the data results and metadata requested. 
#' The *first response of the list* will always contain the response of the 
#' OpenCGA federation being directly queried. Any additional response in the 
#' list will belong to other federated servers that could be connected. Each 
#' federated response will contain a list of results ( _OpenCGAResult_ ) 
#' containing the data that has been queried. 
#' @slot apiVersion a character specifying the API version. Example: "v2"
#' @slot time integer especifying the time in milliseconds (ms) that took to 
#' retrieve the information. If the query requires multiple calls, time is added.
#' @slot events list containing any warnings or errors
#' @slot params list containg the specified parameters
#' @slot responses list containing the results from the query. There is one list 
#' per OpenCGA federated servers that could be connected. The first response of 
#' the list will always contain the response of the OpenCGA federation being 
#' directly queried.
#' @seealso \url{http://docs.opencb.org/display/opencga/RESTful+Web+Services#RESTfulWebServices-RESTResponse} 
#' @export

restResponse <- setClass("RestResponse", slots = c(apiVersion = "character",
                                                   time = "integer",
                                                   events = "list",
                                                   params = "list",
                                                   responses = "list"))
