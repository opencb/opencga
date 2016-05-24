# Helper Functions for the Opencga Classes
# URL structure =base+category+action+params
# fetchOpencga
# createURL
# ParseResponse
require(miniUI)
require(shiny)
require(jsonlite)
base <- "http://localhost:8080/opencga/webservices/rest/v1"
#' @export
OpencgaCreateUser <- function(baseurl, userid, name, passwd, email, organization){
  require(jsonlite)
  userid <- paste0("userid=", userid)
  name <- paste0("name=", name)
  email <- paste0("email=", email)
  passwd <-paste0("password=", passwd)
  organization <- paste0("organization=",organization)
  url <- paste(baseurl, "/users/create?",userid, name, email, organization, passwd)
  res <- fromJSON(url)
  return(res$response$result)
}
#' A function to login Opencga web services
#' @aliases OpencgaLogin
#' @param baseurl a character specifying the host url
#' @param userid a charatcer the username
#' @param passwd a charcter the user password
#' @param logical whether to launch a graphical interface, FALSE by default
#' @return an Opencga class object
#' @export

OpencgaLogin <- function(baseurl, userid=NULL, passwd=NULL, interactive=FALSE){
  require(jsonlite)
  if(interactive==TRUE){
    cred <- user_login()
    url <- paste(baseurl,"/users/",cred$user,"/login","?password=", cred$pass,
                 sep="")
  }else{
    url <- paste(baseurl,"/users/", userid, "/login","?password=", passwd,
                 sep="")
  }
  res <- fromJSON(url)
  userId <- unlist(res$response$result[[1]]$userId)
  sessionId <- unlist(res$response$result[[1]]$sessionId)
  return(new("Opencga",baseurl=baseurl, sessionID=sessionId, userID=userId))
}
# Workforce function
excuteOpencga <- function(object, category, id, action, params){
  baseurl <- object@baseurl
  sessionID <- paste0("?sid=",object@sessionID)
  id <- as.character(id)
  action <- paste0("/", action)
  category <- paste0("/", category, "/")
  params <- params
  ## loop to get all the data to be finished
  i=1
  server_limit=1000
  skip=0
  num_results=1000
  container=list()
  while(num_results==server_limit){
    url <- createURL(baseurl, category, id, action,  sessionID, params, skip)
    res_list <- parseJ(url)
    num_results <- res_list$num_results
    cell <- res_list$data
    container[[i]] <- cell
    skip=skip+1000
    i = i + 1
  }
  ds <- rbind.pages(container)
  ##
  # url <- createURL(baseurl, category, id, action,  sessionID, params)
  # res <- parseJ(url)
  return(ds)
}


createURL <- function(baseurl, category, id, action, sessionID, params, skip){
  noIds <- c("create", "create-folder", "search", "link", "unlink", "content-example", "download-example", "load")
  skip=paste0("skip=",skip)
  baseParam <- paste(sessionID, skip, sep = "&")
  if(!is.null(params)){
    extraParams <- getCgaParam(params)
  }else{
    extraParams <- NULL
  }

  allParams <- paste(baseParam,extraParams, sep="&")
  if (action %in%noIds){
    url <- paste0(baseurl, category, action, allParams)
  }else{
    url <- paste0(baseurl, category, id, action, allParams)
  }

  return(url)
}

parseJ <- function(url){
  require(jsonlite)
  res <- fromJSON(url, flatten = TRUE, simplifyVector = TRUE)
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
