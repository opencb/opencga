# Helper Functions for the Opencga Classes
# URL structure =base+category+action+params
# fetchOpencga
# createURL
# ParseResponse
require(miniUI)
require(shiny)
require(jsonlite)
base <- "http://localhost:8080/opencga/webservices/rest/v1"
source("/home/melsiddieg/R/opencgaR/R/login.R")
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
  sessionID <- paste0("sid=",object@sessionID)
  id <- as.character(id)
  action <- paste0("/", action)
  category <- paste0("/", category, "/")
  url <- paste0(baseurl, category, id, action, "?", sessionID, params=NULL)
  data <- parseJ(url)
  return(list(data=data, url=url) )
}


createURL <- function(base, category, id, action,params){
  paste0(baseurl, category, id, action, "?", sessionID, params=NULL)
  return(url)
}
parseJ <- function(url){
  require(jsonlite)
  res <- fromJSON(url)
  return(as.data.frame(res$response$result))
}
