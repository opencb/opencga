################################################################################
#' OpencgaR init function
#' @aliases initOpencgaR
#' @title initOpencgaR
#'
#' @description  This function inicializes the OpencgaR object with the
#' necessary details to generate a connection.
#' @details
#' This class initializes the OpencgaR object. It holds the default configuration
#' required by OpencgaR methods to connect to OpenCGA web services.
#' @importFrom methods new slot
#' @param host a character specifying the host url, e.g.
#' "http://bioinfo.hpc.cam.ac.uk/opencga-prod/"
#' @param version a character specifying the API version, e.g. "v2"
#' @return An object of class OpencgaR
#' @seealso  \url{https://github.com/opencb/opencga/wiki}
#' and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
#' \dontrun{
#' con <- initOpencgaR(host = "http://bioinfo.hpc.cam.ac.uk/opencga-prod/", version = "v2")
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser", showToken = TRUE)
#'
#' # Configuration in list format
#' conf <- list(version="v2", rest=list(host="http://bioinfo.hpc.cam.ac.uk/opencga-prod/"))
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = demouser")
#'
#' # Configuration in file format ("YAML" or "JSON")
#' conf <- "/path/to/conf/client-configuration.yml"
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser")
#' }
#' @export

initOpencgaR <- function(host=NULL, version="v2", user=NULL, opencgaConfig=NULL){
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

################################################################################
#' Read OpenCGA configuration
#'
#' @param conf a list or the path to a file (in "JSON" or "YAML" format)
#' containing the host and version configurations to set OpenCGA connection
#'
#' @return a OpencgaR object
#'
#' \dontrun{
#' con <- initOpencgaR(host = "http://bioinfo.hpc.cam.ac.uk/opencga-prod/", version = "v2")
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser", showToken = TRUE)
#'
#' # Configuration in list format
#' conf <- list(version="v2", rest=list(host="http://bioinfo.hpc.cam.ac.uk/opencga-prod/"))
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = demouser")
#'
#' # Configuration in file format ("YAML" or "JSON")
#' conf <- "/path/to/conf/client-configuration.yml"
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser")
#' }

opencgaReadConfig <- function(conf){
    if (class(conf) == "list"){
        # read from R object
        conf <- readConfList(conf)
    }else if(class(conf) == "character"){
        # read from file
        conf <- readConfFile(conf)
    }
    ocga <- new(Class = "OpencgaR", host=conf$host, version=conf$version,
                    user="", token="", refreshToken="")
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
        version <- "v2"
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
#' \dontrun{
#' con <- initOpencgaR(host = "http://bioinfo.hpc.cam.ac.uk/opencga-prod/", version = "v2")
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser", showToken = TRUE)
#'
#' # Configuration in list format
#' conf <- list(version="v2", rest=list(host="http://bioinfo.hpc.cam.ac.uk/opencga-prod/"))
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = demouser")
#'
#' # Configuration in file format ("YAML" or "JSON")
#' conf <- "/path/to/conf/client-configuration.yml"
#' con <- initOpencgaR(opencgaConfig=conf)
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser")
#' }
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
    baseurl <- paste0(host, version,"/users/login")
    
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

    # Send request
    query <- httr::POST(baseurl, body = list(user=userid, password=passwd), encode = "json")

    # check query status
    httr::warn_for_status(query)
    httr::stop_for_status(query)

    res <- httr::content(query)
    token <- res$responses[[1]]$results[[1]]$token
    refreshToken <- res$responses[[1]]$results[[1]]$refreshToken
    
    opencga@user <- userid
    opencga@token <- token
    opencga@refreshToken <- refreshToken
    opencga@showToken <- showToken
    opencga@autoRenew <- autoRenew
    
    # get expiration time
    loginInfo <- unlist(strsplit(x=token, split="\\."))[2]
    loginInfojson <- jsonlite::fromJSON(rawToChar(base64enc::base64decode(what=loginInfo)))
    loginTime <- as.character(as.POSIXct(loginInfojson$iat, origin="1970-01-01"), format="%Y%m%d%H%M%S")
    expirationTime <- as.character(as.POSIXct(loginInfojson$exp, origin="1970-01-01"), format="%Y%m%d%H%M%S")
    
    # Create session JSON
    sessionDf <- data.frame(host=opencga@host, version=opencga@version, 
                            user=opencga@user, token=opencga@token,
                            refreshToken=opencga@refreshToken,
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
            sessionTable[sessionTableMatch, "token"] <- token
            sessionTable[sessionTableMatch, "refreshToken"] <- refreshToken
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
  eval.parent(substitute(opencga@token <- ""))
  eval.parent(substitute(opencga@refreshToken <- ""))
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
#' con <- initOpencgaR(host = "http://bioinfo.hpc.cam.ac.uk/opencga-prod/", version = "v2")
#' con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser", showToken = TRUE)
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
               "projectClient"=getMethodInfo(opencga, categ="projects", subcat=NULL, action=action),
               "studyClient"=getMethodInfo(opencga, categ="studies", subcat=NULL, action=action),
               "fileClient"=getMethodInfo(opencga, categ="files", subcat=NULL, action=action),
               "jobClient"=getMethodInfo(opencga, categ="jobs", subcat=NULL, action=action),
               "sampleClient"=getMethodInfo(opencga, categ="samples", subcat=NULL, action=action),
               "individualClient"=getMethodInfo(opencga, categ="individuals", subcat=NULL, action=action),
               "familyClient"=getMethodInfo(opencga, categ="families", subcat=NULL, action=action),
               "cohortClient"=getMethodInfo(opencga, categ="cohorts", subcat=NULL, action=action),
               "panelClient"=getMethodInfo(opencga, categ="panels", subcat=NULL, action=action),
               "alignmentClient"=getMethodInfo(opencga, categ="alignment", subcat=NULL, action=action),
               "variantClient"=getMethodInfo(opencga, categ="variant", subcat=NULL, action=action),
               "clinicalClient"=getMethodInfo(opencga, categ="clinical", subcat=NULL, action=action),
               "operationClient"=getMethodInfo(opencga, categ="operation", subcat=NULL, action=action),
               "metaClient"=getMethodInfo(opencga, categ="meta", subcat=NULL, action=action),
               "ga4ghClient"=getMethodInfo(opencga, categ="ga4gh", subcat=NULL, action=action),
               "adminClient"=getMethodInfo(opencga, categ="admin", subcat=NULL, action=action)
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
