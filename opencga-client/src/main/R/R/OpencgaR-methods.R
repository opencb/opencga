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
#' @importFrom methods new slot
#' @param host a character specifying the host url, e.g.
#' "http://bioinfo.hpc.cam.ac.uk/hgva"
#' @param version a character specifying the API version, e.g. "v1"
#' @return An object of class OpencgaR
#' @seealso  \url{https://github.com/opencb/opencga/wiki}
#' and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @examples
#' \dontrun{
#' con <- initOpencgaR(host = "http://localhost:8080/opencga/", version = "v1",
#' user = "user")
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#'
#' # Configuration in list format
#' conf <- list(version="v1", rest=list(host="http://localhost:8080/opencga/",
#' user="user"))
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#'
#' # Configuration in file format ("YAML" or "JSON")
#' conf <- "/path/to/conf/client-configuration.yml"
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#' }
#' @export

initOpencgaR <- function(host=NULL, version="v1", user=NULL, opencgaConfig=NULL){
    if (is.null(opencgaConfig)){
        # Check values provided
        if (!is.null(host) & !is.null(user)){
            ocga <- new("OpencgaR", host=host, version=version, user=user, sessionFile="")
        }else if(!is.null(host)){
            ocga <- new("OpencgaR", host=host, version=version, user="", sessionFile="")
        }else{
            cat("No connection parameters given. Using HGVA setup.")
            ocga <- new("OpencgaR")
        }
    }else{
        ocga <- opencgaReadConfig(opencgaConfig)
    }

    # Download swagger
    if(!endsWith(x = ocga@host, suffix = "/")){
        ocga@host <- paste0(ocga@host, "/")
    }
    if (!grepl("webservices/rest", ocga@host)){
        ocga@host <- paste0(ocga@host, "webservices/rest/")
    }
    baseurl <- paste0(ocga@host, "swagger.json")
    swagger <- jsonlite::fromJSON(baseurl)
    ocga@swagger <- swagger
    return(ocga)
}

setMethod("show", signature = "OpencgaR", definition = function(object){
    cat("An object of class ", class(object), "\n", sep = "")
    cat(paste("| Host:", object@host))
    cat(paste("\n| Version:", object@version))
    if (.hasSlot(object, "sessionId")) {
      cat(paste("\n| Token:", object@sessionId))
    }
    if (.hasSlot(object, "expirationTime")) {
      cat(paste("\n| Expiration time:", object@expirationTime))
    }
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
#' \dontrun{
#' con <- initOpencgaR(host = "http://localhost:8080/opencga/", version = "v1", user = "user")
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#'
#' # Configuration in list format
#' conf <- list(version="v1", rest=list(host="http://localhost:8080/opencga/"))
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#'
#' # Configuration in file format ("YAML" or "JSON")
#' conf <- "/path/to/conf/client-configuration.yml"
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#' }

opencgaReadConfig <- function(conf){
    if (class(conf) == "list"){
        # read from R object
        conf <- readConfList(conf)
    }else if(class(conf) == "character"){
        # read from file
        conf <- readConfFile(conf)
    }
    # if ("user" %in% conf){
    #     user=conf$user
    # }else{
    #     cat("User name not specified. Please specify it if login is required.")
    #     user=""
    # }
    ocga <- new(Class = "OpencgaR", host=conf$host, version=conf$version,
                    user="", sessionId="")
    return(ocga)
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
    if(requireNamespace("configr", quietly = TRUE)){
        type <- configr::get.config.type(conf)
        print(paste("Reading configuration file in", type, "format", sep = " "))
        conf.obj <- configr::read.config(conf, warn = F)

        readConfList(conf.obj)
    }
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
#' #@examples
#' \dontrun{
#' con <- initOpencgaR(host = "http://localhost:8080/opencga/", version = "v1", user = "user")
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#'
#' # Configuration in list format
#' conf <- list(version="v1", rest=list(host="http://localhost:8080/opencga/"))
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#'
#' # Configuration in file format ("YAML" or "JSON")
#' conf <- "/path/to/conf/client-configuration.yml"
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#' 
#' 
#' @export
opencgaLogin <- function(opencga, userid=NULL, passwd=NULL, interactive=FALSE, 
                         autoRenew=FALSE, showToken=FALSE){
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
    
    # Interactive login
    if(requireNamespace("miniUI", quietly = TRUE) & requireNamespace("shiny", quietly = TRUE)){
        user_login <- function() {
            ui <- miniUI::miniPage(
                miniUI::gadgetTitleBar("Please enter your username and password"),
                miniUI::miniContentPanel(
                    shiny::textInput("username", "Username"),
                    shiny::passwordInput("password", "Password")))
            
            server <- function(input, output) {
                shiny::observeEvent(input$done, {
                    user <- input$username
                    pass <- input$password
                    res <- list(user=user, pass=pass)
                    shiny::stopApp(res)
                })
                shiny::observeEvent(input$cancel, {
                    shiny::stopApp(stop("No password.", call. = FALSE))
                })
            }
            
            shiny::runGadget(ui, server, viewer=shiny::dialogViewer("user_login"))
        }
    }else{
        print("The 'miniUI' and 'shiny' packages are required to run the 
           interactive login, please install it and try again.
           To install 'miniUI': install.packages('miniUI')
           To install 'shiny': install.packages('shiny')")
    }
    # end interactive login
    
    if(interactive==TRUE){
        cred <- user_login()
        userid <- cred$user
        passwd <- cred$pass
    }

    baseurl <- paste(baseurl, userid, "login", sep="/")

    # Send request
    query <- httr::POST(baseurl, body = list(password = passwd), encode = "json")

    # check query status
    httr::warn_for_status(query)
    httr::stop_for_status(query)

    res <- httr::content(query)
    sessionId <- res$responses[[1]]$results[[1]]$sessionId
    
    opencga@user <- userid
    #opencga@sessionId <- sessionId
    opencga@showToken <- showToken
    opencga@autoRenew <- autoRenew
    
    # get expiration time
    loginInfo <- unlist(strsplit(x=sessionId, split="\\."))[2]
    loginInfojson <- jsonlite::fromJSON(rawToChar(base64enc::base64decode(what=loginInfo)))
    loginTime <- as.character(as.POSIXct(loginInfojson$iat, origin="1970-01-01"), format="%Y%m%d%H%M%S")
    expirationTime <- as.character(as.POSIXct(loginInfojson$exp, origin="1970-01-01"), format="%Y%m%d%H%M%S")
    
    # Create session JSON
    sessionDf <- data.frame(host=opencga@host, version=opencga@version, 
                            user=opencga@user, token=sessionId,
                            login=loginTime, expirationTime=expirationTime)
    sessionJson <- jsonlite::toJSON(sessionDf)
    
    # Get system to define session directory
    if(.Platform$OS.type == "unix") {
        sessionDir <- file.path(Sys.getenv("HOME"), ".opencga", "R", fsep = .Platform$file.sep)
    } else {
        sessionDir <- normalizePath(file.path(Sys.getenv("HOMEDRIVE"),
                                    Sys.getenv("HOMEPATH"), "opencga", "R", 
                                    winslash = .Platform$file.sep))
    }
    
    # Create/update session file
    dir.create(path=sessionDir, showWarnings=FALSE, recursive=TRUE)
    sessionFile <- file.path(sessionDir, "rsession.json", fsep = .Platform$file.sep)
    opencga@sessionFile <- sessionFile
    if(file.exists(sessionFile)){
        sessionTable <- jsonlite::fromJSON(sessionFile)
        sessionTableMatch <- which(sessionTable$host==opencga@host & 
                                   sessionTable$version == opencga@version & 
                                   sessionTable$user == opencga@user)
        if (length(sessionTableMatch) == 0){
            sessionTable <- rbind(sessionTable, sessionDf)
            write(x = jsonlite::toJSON(sessionTable), file = sessionFile)
        }else if (length(sessionTableMatch) == 1){
            sessionTable[sessionTableMatch, "login"] <- loginTime
            sessionTable[sessionTableMatch, "token"] <- sessionId
            sessionTable[sessionTableMatch, "expirationTime"] <- expirationTime
            write(x = jsonlite::toJSON(sessionTable), file = sessionFile)
        }else{
            stop(paste("There is more than one connection to this host in your rsession file. Please, remove any duplicated entries in", 
                       sessionFile))
        }
    }else{
        write(x = sessionJson, file = sessionFile)
    }
    return(opencga)
}


################################################################################
#' @title Logout from OpenCGA Web Services
#'
#' @description
#' A function to logout from Opencga web services
#'
#' @aliases OpencgaLogout
#' @param ocga an object of type OpencgaR generated using opencgaLogin
#' @examples
#' \dontrun{
#' opencgaLogout(con)
#' }
#' @export
opencgaLogout <- function(opencga){
  if (class(opencga) != "OpencgaR"){
    stop("Please, provide a valid config object. See initOpencgaR")
  }
  eval.parent(substitute(opencga@sessionId <- ""))
}


################################################################################
#' @title Get help from OpenCGA Web Services
#'
#' @description
#' A function extract all the available information from Opencga web services
#' at the defined host
#'
#' @aliases OpencgaHelp
#' @param opencga an object of type OpencgaR generated using initOpencga or
#' opencgaLogin
#'
#' @examples
#' \dontrun{
#' con <- initOpencgaR(host = "http://localhost:8080/opencga/", version = "v1")
#' con <- opencgaLogin(opencga = con, userid = "user", passwd = "user_pass")
#' opencgaHelp(con, client="userClient", action="info")
#' }
#' @export
opencgaHelp <- function(opencga, client=NULL, action=NULL){
    if (class(opencga) != "OpencgaR"){
        stop("Please, provide a valid config object. See initOpencgaR")
    }
    if(length(opencga@swagger) == 0) {
        stop("ERROR: Help cannot be displayed. The swagger JSON could not be
              downloaded from your host.\nPlease try initialising your session
              again or have a look at the swagger located in your host.")
    }

    allApis <- names(opencga@swagger$paths)

    # General help: Return all paths
    if (is.null(client) & is.null(action)){
        gsub(pattern="\\/\\{apiVersion\\}", replacement="", x=allApis)

    # Client help: return all possible paths within a method (subcategory != NULL)
    # Action help: return all possible params for the action in the client
    }else{
        switch(client,
               "userClient"=getMethodInfo(opencga, categ="users", subcat=NULL, action=action),
               "userConfigClient"=getMethodInfo(opencga, categ="users", subcat="configs", action=action),
               "userFilterClient"=getMethodInfo(opencga, categ="users", subcat="filters", action=action),
               "projectClient"=getMethodInfo(opencga, categ="projects", subcat=NULL, action=action),
               "studyClient"=getMethodInfo(opencga, categ="studies", subcat=NULL, action=action),
               "studyGroupClient"=getMethodInfo(opencga, categ="studies", subcat="groups", action=action),
               "studyAclClient"=getMethodInfo(opencga, categ="studies", subcat="acl", action=action),
               "studyVariablesetClient"=getMethodInfo(opencga, categ="studies", subcat="variablesets", action=action),
               "studyVariablesetFieldClient"=getMethodInfo(opencga, categ="studies", subcat="variablesets", action=action),
               "fileClient"=getMethodInfo(opencga, categ="files", subcat=NULL, action=action),
               "fileAclClient"=getMethodInfo(opencga, categ="files", subcat="acl", action=action),
               "jobClient"=getMethodInfo(opencga, categ="jobs", subcat=NULL, action=action),
               "jobAclClient"=getMethodInfo(opencga, categ="jobs", subcat="acl", action=action),
               "familyClient"=getMethodInfo(opencga, categ="families", subcat=NULL, action=action),
               "familyAnnotationsetClient"=getMethodInfo(opencga, categ="families", subcat="annotationsets", action=action),
               "familyAclClient"=getMethodInfo(opencga, categ="families", subcat="acl", action=action),
               "individualClient"=getMethodInfo(opencga, categ="individuals", subcat=NULL, action=action),
               "individualAnnotationsetClient"=getMethodInfo(opencga, categ="individuals", subcat="annotationsets", action=action),
               "individualAclClient"=getMethodInfo(opencga, categ="individuals", subcat="acl", action=action),
               "sampleClient"=getMethodInfo(opencga, categ="samples", subcat=NULL, action=action),
               "sampleAnnotationsetClient"=getMethodInfo(opencga, categ="samples", subcat="annnotationsets", action=action),
               "sampleAclClient"=getMethodInfo(opencga, categ="samples", subcat="acl", action=action),
               "cohortClient"=getMethodInfo(opencga, categ="cohorts", subcat=NULL, action=action),
               "cohortAnnotationsetClient"=getMethodInfo(opencga, categ="cohorts", subcat="annotationsets", action=action),
               "cohortAclClient"=getMethodInfo(opencga, categ="cohorts", subcat="acl", action=action),
               "clinicalClient"=getMethodInfo(opencga, categ="clinical", subcat=NULL, action=action),
               "metaClient"=getMethodInfo(opencga, categ="meta", subcat=NULL, action=action),
               "analysisVariantClient"=getMethodInfo(opencga, categ="analysis", subcat="variant", action=action)
        )
    }
}

#' @importFrom utils tail
#' @importFrom rlang .data
getMethodInfo <- function(opencga, categ, subcat, action){
    allApis <- names(opencga@swagger$paths)
    methodsInCategoryLogic <- sapply(strsplit(x = allApis, split = "/"), "[", 3) == categ
    methodsInCategory <- allApis[methodsInCategoryLogic]
    lenParams <- unlist(lapply(X = strsplit(x = allApis, split = "/"), FUN = length))

    filterParams <- function(x){
        x <- subset(x, !.data$name %in% c("apiVersion", "version", "sid", "Authorization"))
        x <- x[, c("name", "in", "required", "type", "description")]
        return(x)
    }

    if (is.null(subcat)){
        simpleMethodsLogic <- lenParams < 6
        simpleMethodsInCat <- allApis[methodsInCategoryLogic & simpleMethodsLogic]

        if(is.null(action)){
            helpResp <- gsub(pattern="\\/\\{apiVersion\\}", replacement="", x=simpleMethodsInCat)
        }else{
            availActions <- sapply(strsplit(x = simpleMethodsInCat, split = "/"), tail, 1)
            if (action %in% availActions){
                selMethodAction <- simpleMethodsInCat[sapply(strsplit(x = simpleMethodsInCat, split = "/"), tail, 1) == action]
                if(grepl(pattern = "DEPRECATED", x = opencga@swagger$paths[selMethodAction][[names(opencga@swagger$paths[selMethodAction])]][[1]]$description)){
                    helpResp <- opencga@swagger$paths[selMethodAction][[names(opencga@swagger$paths[selMethodAction])]][[1]]$description
                }else{
                    helpResp <- opencga@swagger$paths[selMethodAction][[names(opencga@swagger$paths[selMethodAction])]][[1]]$parameters
                    helpResp <- filterParams(helpResp)
                }
            }else{
                helpResp <- paste0("The action '", action, "' could not be found in the specified client.")
            }
        }

    }else{
        complexMethodsLogic <- lenParams >= 6
        subcatInMethodsLogic <- sapply(strsplit(x = allApis, split = "/"), "[", 5) == subcat
        complexMethodsInCat <- allApis[methodsInCategoryLogic & complexMethodsLogic & subcatInMethodsLogic]

        if(is.null(action)){
            helpResp <- gsub(pattern="\\/\\{apiVersion\\}", replacement="", x=complexMethodsInCat)
        }else{
            availActions <- sapply(strsplit(x = complexMethodsInCat, split = "/"), tail, 1)
            if (action %in% availActions){
                selMethodAction <- complexMethodsInCat[sapply(strsplit(x = complexMethodsInCat, split = "/"), tail, 1) == action]
                if(grepl(pattern = "DEPRECATED", x = opencga@swagger$paths[selMethodAction][[names(opencga@swagger$paths[selMethodAction])]][[1]]$description)){
                    helpResp <- opencga@swagger$paths[selMethodAction][[names(opencga@swagger$paths[selMethodAction])]][[1]]$description
                }else{
                    helpResp <- opencga@swagger$paths[selMethodAction][[names(opencga@swagger$paths[selMethodAction])]][[1]]$parameters
                    helpResp <- filterParams(helpResp)
                }
            }else{
                helpResp <- paste0("The action '", action, "' could not be found in the specified client.")
            }
        }
    }
    print(helpResp)
}
