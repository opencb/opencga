################################################################################
#' OpencgaR init function
#' @aliases initOpencgaR
#' @title initOpencgaR
#' 
#' @description  This function inicializes the OpencgaR object with the 
#' necessary details to generate a connection.
#' @details
#' This class initializes the OpencgaR object. It holds the default configuration 
#' required by OpencgaR methods to connect to OpenCGA web services. By defult, 
#' it is configured to query HGVA (http://hgva.opencb.org/).
#' @param host a character specifying the host url, e.g.  
#' "http://bioinfo.hpc.cam.ac.uk/hgva"
#' @param version a character specifying the API version, e.g. "v1"
#' @return An object of class OpencgaR
#' @seealso  \url{https://github.com/opencb/opencga/wiki} 
#' and the RESTful API documentation 
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export
#' @examples
#'    ocga <- initOpencgaR()
#'    print(ocga)
#' @export

initOpencgaR <- function(host=NULL, version="v1", user="", opencgaConfig=NULL){
    if (is.null(opencgaConfig)){
        # Check values provided
        if (!is.null(host) & user != ""){
            new("OpencgaR", host=host, version=version, user=user, sessionId="")
        }else if(!is.null(host)){
            new("OpencgaR", host=host, version=version, sessionId="")
        }else{
            cat("No connection parameters given. Using HGVA setup.")
            new("OpencgaR")
        }
    }else{
        opencgaReadConfig(opencgaConfig)
    }
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
    if ("user" %in% conf){
        user=conf$user
    }else{
        cat("User name not specified. Please specify it if login is required.")
        user=""
    }
    ocgaConf <- new(Class = "OpencgaR", host=conf$host, version=conf$version,
                    user=user, sessionId="")
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

################################################################################
#' @title Login to OpenCGA Web Services
#' 
#' @description
#' A function to login Opencga web services
#' 
#' @aliases OpencgaLogin
#' @param ocga an object of type OpencgaR generated using initOpencgaR
#' @param userid a charatcer with the username
#' @param passwd a charcter with the user password
#' #@param version a character with the OpenCGA version to use
#' @param interactive whether to launch a graphical interface, FALSE by default
#' #@param ... Any other arguments
#' 
#' @return an Opencga class object
#' 
#' @export
opencgaLogin <- function(opencga, userid=NULL, passwd=NULL, interactive=FALSE){
    if (class(opencga) == "OpencgaR"){
        host <- slot(object = opencga, name = "host")
        version <- slot(object = opencga, name = "version")
    }else{
        stop("Please, provide a valid config object. See initOpencgaR")
    }
    
    if(!endsWith(x = host, suffix = "/")){
        host <- paste0(host, "/")
    }
    if (!grepl("webservices/rest", host)){
        host <- paste0(host, "webservices/rest/")
    }
    baseurl <- paste0(host, version,"/users")
    
    if(interactive==TRUE){
        cred <- user_login()
        user <- cred$user
        passwd <- cred$pass
    }
    baseurl <- paste(baseurl, userid, "login", sep="/")
    
    # Send request
    query <- POST(baseurl, body = list(password = passwd), encode = "json")
    
    # check query status
    warn_for_status(query)
    stop_for_status(query)
    
    res <- content(query)
    sessionId <- res$response[[1]]$result[[1]]$sessionId
    opencga@user <- userid
    opencga@sessionId <- sessionId
    return(opencga)
}