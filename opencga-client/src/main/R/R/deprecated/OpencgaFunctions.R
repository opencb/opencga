# Helper Functions for the Opencga Classes
# URL structure =base+category+action+params
# fetchOpencga
# createURL
# ParseResponse
#base <- "http://localhost:8080/opencga/webservices/rest/v1"

#source("R/.opencgaConfig.R")

#' #' Read OpenCGA configuration
#' #'
#' #' @param conf a list or the path to a file (in "JSON" or "YAML" format) containing the host and version configurations to set OpenCGA connection
#' #'
#' #' @return a OpencgaConfig object
#' #' 
#' #' @examples
#' #' # Configuration in list format
#' #' conf <- list(version="v1",
#' #'              rest=list(host="http://localhost:8080/opencga/"))
#' #' con <- OpencgaReadConfig(conf)
#' #' 
#' #' # Configuration in file format ("YAML" or "JSON")
#' #' conf <- "/path/to/conf/client-configuration.yml"
#' #' con <- OpencgaReadConfig(conf)
#' #' 
#' #' @export
# OpencgaReadConfig <- function(conf){
#   if (class(conf) == "list"){
#     # read from R object
#     conf <- read.conf.list(conf)
#   }else if(class(conf) == "character"){
#     # read from file
#     conf <- read.conf.file(conf)
#   }
#   ocgaConf <- new(Class = "OpencgaConfig", host=conf$host, version=conf$version)
#   return(ocgaConf)
# }


#' @title Login to OpenCGA Web Services
#' 
#' @description
#' A function to login Opencga web services
#' 
#' @aliases OpencgaLogin
#' @param conf an object of type OpencgaConfig
#' @param userid a charatcer with the username
#' @param passwd a charcter with the user password
#' #@param version a character with the OpenCGA version to use
#' @param logical whether to launch a graphical interface, FALSE by default
#' #@param ... Any other arguments
#' 
#' @return an Opencga class object
#' 
#' @export
OpencgaLogin <- function(conf, userid=NULL, passwd=NULL, interactive=FALSE){
  if (class(conf) == "OpencgaConfig"){
    host <- slot(object = conf, name = "host")
    version <- slot(object = conf, name = "version")
  }else{
    stop("Please, provide a valid config object. See OpencgaConfig.")
  }
  
  baseurl <- paste0(host, "/webservices/rest/", version,"/users")
  
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
  return(new("Opencga", baseurl=baseurl, sessionID=sessionId, userID=userid))
}

# Workforce function
excuteOpencga <- function(object, category, id, action, params, ...){
  baseurl <- object@baseurl
  sessionID <- paste0("?sid=",object@sessionID)
  if(is.null(id)){
    id <- NULL
  }else{
    id <- paste0("/", id, sep="")
  }
  
  action <- paste0("/", action)
  category <- paste0("/", category)
  params <- params
  ################################################################
  Oargs <- list(...)
  if(length(args)>0){
    acc <- list()
    j <- 1
    for (i in names(Oargs)){
      tmp <- paste(i,"=", Oargs[[i]], sep="")
      acc[[j]] <- tmp
      j <- j+1
    }
    Nargs <- paste(unlist(acc), collapse = "&")
  }else{
    Nargs <- NULL
  }
  ################################################################
  ## loop to get all the data to be finished
  i=1
  server_limit=2000
  skip=0
  num_results=2000
  container=list()
  while(num_results==server_limit){
    grl <- createURL(baseurl, category, id, action,  sessionID, params, skip, Nargs)
    res_list <- parseJ(grl)
    num_results <- res_list$num_results
    cell <- res_list$data
    container[[i]] <- cell
    skip=skip+2000
    i = i + 1
  }
  ds <- rbind.pages(container)
  ###############################################################
  return(ds)
}


createURL <- function(baseurl, category, id, action, sessionID, params, skip, Nargs){
  noIds <- c("create", "create-folder", "search", "link", "unlink", "content-example", "download-example", "load")
  skip=paste0("skip=",skip)
  baseParam <- paste(sessionID, skip, sep = "&")
  if(!is.null(params)){
    extraParams <- getCgaParam(params)
  }else{
    extraParams <- NULL
  }
  # extrArgs <- list(...)
  allParams <- c(baseParam, extraParams, Nargs)
  allParams <- allParams[!is.null(allParams)]
  allParams <- paste0(allParams, collapse = "&")
  if (action %in%noIds){
    grl <- paste0(baseurl, category, action, allParams)
  }else{
    grl <- paste0(baseurl, category, id, action, allParams)
  }

  return(grl)
}

parseJ <- function(grl){
  res <- fromJSON(grl, flatten = TRUE, simplifyVector = TRUE)
  num_results <- res$response$numResults
  return(list(num_results=num_results,data=as.data.frame(res$response$result)))
}
getParam <- function(object){

  return(param)
}

getCgaParam <- function(object){
            region=object@region
            chromosome=object@chromosome
            gene=object@gene
            maf=object@maf
            mgf=object@mgf
            genotype=object@genotype
            polyphen=object@polyphen
            sift=object@sift
            conservation=object@conservation
            reference=object@reference
            alternate=object@alternate
            so=object@so
            biotype=object@biotype
            limit=object@limit
            files=object@files
            returnedStudies=object@returnedStudies
            returnedSamples=object@returnedSamples
            param=c(region, chromosome, gene, maf,mgf, genotype, polyphen,
                    sift,  conservation, reference, alternate, so, biotype,
                    limit, files, returnedStudies, returnedSamples)
            foundParam <- vector()
            i=1
            for (argument in param){
              if(length(argument)>0){
                foundParam[i] <- argument
                i=i+1
              }
            }
            return(paste(foundParam, collapse="&"))

          }
#' A method to fetch documentation about Opencga Methods
#' 
#' This method allow users to lookup required 
#' to query Opencga Data
#' @param category the Opencga category being queried
#' @param action action intended on that category
#' @return A dataframe
#' @export
getOpencgaDocs <- function(category, action,  requiredOnly=FALSE){
  # take name of the category 'users
  # construct the url by pasting docsurl and the category
  docurl <- 'http://localhost:8080/opencga/webservices/rest/api-docs/'
  category <- stringi::stri_trans_totitle(category)
  drl <- paste0(docurl,category)
  # get the json api docs
  Data <- fromJSON(drl)
  Data <- Data$apis
  # take the name of the action exctract the operations and the parameters
  action <- action
  index <- grep(action, Data$path)
  operations <- Data$operations[[index]]$parameters[[1]]
  if(requiredOnly==TRUE){
   operations <- subset(operations, required==TRUE)
  }else{
    operations <- operations
  }
   operations
}



#' A method to query and manipulate user data in Opencga
#' 
#' This method to query and manipulate user data in Opencga
#'   
#' #' @export
#' OpencgaCreateUser <- function(baseurl, userid, name, passwd, email, organization){
#'   require(jsonlite)
#'   userid <- paste0("userid=", userid)
#'   name <- paste0("name=", name)
#'   email <- paste0("email=", email)
#'   passwd <-paste0("password=", passwd)
#'   organization <- paste0("organization=",organization)
#'   url <- paste(baseurl, "/users/create?",userid, name, email, organization, passwd)
#'   res <- fromJSON(url)
#'   return(res$response$result)
#' }