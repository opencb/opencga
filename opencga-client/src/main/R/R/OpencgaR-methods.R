















################################################################################
#' OpencgaR constructor function
#' @aliases OpencgaR
#' @title OpencgaR
#' 
#' @description  This is a constructor function for the OpencgaR object
#' @details
#' This class defines the OpencgaR object. It holds the default configuration 
#' required by OpencgaR methods to connect to OpenCGA web services. By defult, 
#' it is configured to query HGVA (http://hgva.opencb.org/).
#' @import methods
#' @param host a character specifying the host url, e.g.  
#' "http://bioinfo.hpc.cam.ac.uk/hgva"
#' @param version a character specifying the API version, e.g. "v1"
#' @return An object of class OpencgaR
#' @seealso  \url{https://github.com/opencb/opencga/wiki} 
#' and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export
#' @examples
#'    cga <- OpencgaR()
#'    print(cga)
#' @export

OpencgaConfig <- function(host, version){
    new("OpencgaConfig", host=host, version=version)
}

setMethod("show", signature = "OpencgaR", definition = function(object){
    cat("An object of class ", class(object), "\n", sep = "")
    cat(paste("| Host:", object@host))
    cat(paste("\n| Version:", object@version))
})

################################################################################
#' Read OpenCGA configuration
#'
#' @param conf a list or the path to a file (in "JSON" or "YAML" format) 
#' containing the host and version configurations to set OpenCGA connection
#'
#' @return a OpencgaR object
#' 
#' @examples
#' # Configuration in list format
#' conf <- list(version="v1",
#'              rest=list(host="http://localhost:8080/opencga/"))
#' con <- OpencgaReadConfig(conf)
#' 
#' # Configuration in file format ("YAML" or "JSON")
#' conf <- "/path/to/conf/client-configuration.yml"
#' con <- OpencgaReadConfig(conf)
#' 
#' @export
opencgaReadConfig <- function(conf){
    if (class(conf) == "list"){
        # read from R object
        conf <- readConfList(conf)
    }else if(class(conf) == "character"){
        # read from file
        conf <- readConfFile(conf)
    }
    ocgaConf <- new(Class = "OpencgaR", host=conf$host, version=conf$version,
                    user=NULL, sessionId=NULL)
    return(ocgaConf)
}

readConfList <- function(conf){
    if ("rest" %in% names(conf)){
        if ("host" %in% names(conf$rest)){
            host <- conf$rest$host
        }
    }else{
        stop("Please, specify the 'host' in the 'rest' section")
    }
    if ("version" %in% names(conf)){
        version <- conf$version
    }else{
        version <- "v1"
        #stop("Please, specify the OpenCGA version")
    }
    return(list(host=host, version=version))
}

readConfFile <- function(conf){
    type <- get.config.type(conf)
    print(paste("Reading configuration file in", type, "format", sep = " "))
    conf.obj <- read.config(conf, warn = F)
    
    readConfList(conf.obj)
}
