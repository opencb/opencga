################################################################################
#' OpencgaR Class
#' #@include commons.R
#' 
#' @description This is an S4 class which defines the OpencgaR object
#' @details This S4 class holds the default configuration required by OpencgaR 
#' methods to stablish the connection with the web services. By default it is 
#' configured to query HGVA (http://hgva.opencb.org/). 
#' @slot host a character specifying the host url. Default 
#' "http://bioinfo.hpc.cam.ac.uk/hgva"
#' @slot version a character specifying the API version. Default "v1"
#' @seealso  \url{https://github.com/opencb/opencga/wiki} 
#' and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export
opencgaR <- setClass("OpencgaR", slots = c(host="character", 
                                           version="character",
                                           user="character",
                                           sessionId="character"),
                                 prototype = prototype(
                                   host="http://bioinfo.hpc.cam.ac.uk/hgva",
                                   version = "v1",
                                   user=NULL,
                                   sessionId=NULL))

