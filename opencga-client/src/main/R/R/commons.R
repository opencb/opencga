################################################################################
#' OpencgaR Common functions
#' 
#' @include AllClasses.R
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


fetchOpenCGA <- function(object=object, category=NULL, categoryId=NULL, 
                         subcategory=NULL, subcategoryId=NULL, action=NULL, 
                         params=NULL, httpMethod="GET", skip=0,
                         num_threads=NULL, as.queryParam=NULL, batch_size=1000){
    
    # Need to disable scientific notation to avoid errors in the URL
    options(scipen=999)
    
    # Get connection info
    host <- object@host
    version <- object@version
    
    # real_batch_size <- real_batch_size
    
    if(!endsWith(x = host, suffix = "/")){
        host <- paste0(host, "/")
    }
    if (!grepl("webservices/rest", host)){
        host <- paste0(host, "webservices/rest/")
    }
    
    if(!endsWith(x = version, suffix = "/")){
        version <- paste0(version, "/")
    }
    
    # Format category and subcategory
    if(is.null(category)){
        category <- ""
    }else{
        category <- paste0(category, "/", sep="")
    }
    
    if(is.null(subcategory)){
        subcategory <- ""
    }else{
        subcategory <- paste0(subcategory, "/")
    }
    
    # Format IDs
    if(is.null(categoryId)){
        categoryId <- ""
    }else{
        categoryId <- paste0(categoryId, collapse = ",")
        categoryId <- paste0(categoryId, "/")
    }
    
    if(is.null(subcategoryId)){
        subcategoryId <- ""
    }else{
        subcategoryId <- paste0(subcategoryId, collapse = ",")
        subcategoryId <- paste0(subcategoryId, "/")
    }
    
    # Extract limit from params
    if(is.null(params)){
       limit <- 400000
    }else{
        if(is.null(params$limit)){
            limit <- 400000
        }else{
            limit <- params$limit
        }
    }
    
    # Call server
    i <- 1
    real_batch_size <- min(c(batch_size, limit))
    skip <- 0
    num_results <- real_batch_size
    container <- list()
    count <- 0
    
    # Initialise array of RestResponse objects
    restResponseList = list()
    
    if (is.null(params)){
        params <- list()
    }
    
    while((unlist(num_results) == real_batch_size) && count <= limit){
        pathUrl <- paste0(host, version, category, categoryId, subcategory, 
                          subcategoryId, action)
        
        ## send batch size as limit to callrest
        real_batch_size <- min(c(real_batch_size, limit-count))
        # if(real_batch_size == 0){
        #     break()
        # }
        params$limit <- real_batch_size
        
        # check expiration time
        sessionTable <- jsonlite::fromJSON(object@sessionFile)
        sessionTableMatch <- which(sessionTable$host==object@host & 
                                       sessionTable$version == object@version & 
                                       sessionTable$user == object@user)
        if (length(sessionTableMatch) == 0){
            stop("You are not logged into openCGA. Please, log in before launching a query.")
        }else if (length(sessionTableMatch) == 1){
            token <- sessionTable[sessionTableMatch, "token"]
            expirationTime <- sessionTable[sessionTableMatch, "expirationTime"]
        }else{
            stop(paste("There is more than one connection to this host in your rsession file. Please, remove any duplicated entries in", 
                       object@sessionFile))
        }
        timeNow <- Sys.time()
        timeLeft <- as.numeric(difftime(as.POSIXct(expirationTime, format="%Y%m%d%H%M%S"), timeNow, units="mins"))
        if (timeLeft > 0 & timeLeft <= 5){
            print("INFO: Your session will expire in less than 5 minutes.")
            urlNewToken <- paste0(host, version, "users/login")
            resp <- httr::POST(url=urlNewToken, 
                               httr::add_headers(.headers=c("Content-Type"="application/json",
                                                            "Accept"="application/json",
                                                            "Authorisation"="Bearer ")), 
                               body=list(refreshToken=object@refreshToken),
                               encode = "json")
            content <- httr::content(resp, as="text", encoding = "utf-8")
            if (length(jsonlite::fromJSON(content)$responses$results[[1]]$token) > 0){
                token <- jsonlite::fromJSON(content)$responses$results[[1]]$token
                loginInfo <- unlist(strsplit(x=token, split="\\."))[2]
                loginInfojson <- jsonlite::fromJSON(rawToChar(base64enc::base64decode(what=loginInfo)))
                expirationTime <- as.POSIXct(loginInfojson$exp, origin="1970-01-01")
                expirationTime <- as.character(expirationTime, format="%Y%m%d%H%M%S")
                sessionTable[sessionTableMatch, "token"] <- token
                sessionTable[sessionTableMatch, "expirationTime"] <- expirationTime
                write(x = jsonlite::toJSON(sessionTable), file = object@sessionFile)
                print("Your session has been renewed!")
            }else{
                warning(paste0("WARNING: Your token could not be renewed, your session will expire in ", 
                             round(x = timeLeft, digits = 2), " minutes"))
            }
        }else if(timeLeft <= 0){
            stop("ERROR: Your session has expired, please renew your connection.")
        }

        response <- callREST(pathUrl=pathUrl, params=params, 
                             httpMethod=httpMethod, skip=skip, token=token,
                             as.queryParam=as.queryParam, verbose=object@verbose, 
                             sid=object@showToken)
        
        skip <- skip+real_batch_size
        res_list <- parseResponse(resp=response$resp, content=response$content, 
                                  verbose=object@verbose)
        num_results <- res_list$numResults
        restResponseList <- append(x = restResponseList, values = res_list$restResponse)
        
        count <- count + num_results
        if (isTRUE(object@verbose)){
            print(paste("Number of retrieved documents:", count))
        }
        
        if(num_results == limit){
          break()
        }
    }
    
    # Merge RestResponses
    finalRestResponse <- opencgaR::mergeResponses(restResponseList = restResponseList)
    return(finalRestResponse)
}


## all working functions

## Format query params
get_qparams <- function(params){
    paramsVec <- c()
    for(p in seq_along(params)){
        paramsVec <- append(paramsVec, paste0(names(params)[p], "=", params[p]))
    }
    paramsStr <- paste(paramsVec, collapse = "&")
    return(paramsStr)
}

## Make call to server
callREST <- function(pathUrl, params, httpMethod, skip, token, as.queryParam, 
                     verbose, sid=FALSE){
    content <- list()
    session <- paste("Bearer", token)
    skip=paste0("?skip=", as.character(skip))
    
    # Make GET call
    if (httpMethod == "GET"){
        if (!is.null(params)){
            params <- get_qparams(params)
            fullUrl <- paste0(pathUrl, skip, "&", params)
        }else{
            fullUrl <- paste0(pathUrl, skip)
        }
        
        if (sid){
            fullUrl <- paste0(fullUrl, "&sid=", token)
        }
        
        # Encode the URL
        fullUrl <- httr::build_url(httr::parse_url(fullUrl))
        if (isTRUE(verbose)){
            print(paste("URL:", fullUrl))
        }
        resp <- httr::GET(url=fullUrl,
                          httr::add_headers(.headers=c(Accept="application/json", 
                                                    Authorization=session)),
                          httr::timeout(30))
        
    }else if(httpMethod == "POST"){
    # Make POST call
        if (!is.null(as.queryParam)){
            if(class(as.queryParam) == "character"){
                as.queryParam <- unique(c(as.queryParam, "study", "skip", "limit", "include", "exclude"))
            }
        } else {
          as.queryParam = c("skip", "limit", "include", "exclude")
        }
        
        if (!is.null(params)){
            # extract study as query param
            if (any(as.queryParam %in% names(params))){
                queryParams <- get_qparams(params[which(names(params) %in% as.queryParam)])
                bodyParams <- params[-which(names(params) %in% as.queryParam)]
            }else{
                bodyParams <- params
                queryParams <- ""
            }
        }
        
        if (is.null(params) | queryParams == ""){
            fullUrl <- paste0(pathUrl, skip)
        }else{
            fullUrl <- paste0(pathUrl, skip, "&", queryParams)
        }
        
        if (sid){
            fullUrl <- paste0(fullUrl, "&sid=", token)
        }
        
        # Encode the URL
        fullUrl <- httr::build_url(httr::parse_url(fullUrl))
        if (isTRUE(verbose)){
            print(paste("URL:",fullUrl))
        }
        if (exists("bodyParams")){
            resp <- httr::POST(url=fullUrl, 
                               body=bodyParams,
                               httr::add_headers(.headers=c(Authorization=session)), 
                               encode = "json")
        }else{
            resp <- httr::POST(url=fullUrl, 
                               httr::add_headers(.headers=c(Authorization=session)), 
                               encode = "json")
        }
    }
    
    content <- httr::content(resp, as="text", encoding = "utf-8")
    return(list(resp=resp, content=content))
}

## A function to parse the json data into R dataframes
parseResponse <- function(resp, content, verbose){
    js <- jsonlite::fromJSON(content)
    if (resp$status_code == 200){
        if (!("warning" %in% js[[1]]) || js[[1]]$warning == ""){
            if (isTRUE(verbose)){
                print("Query successful!")
            }
        }else{
            print("Query successful with warnings.")
            print(paste("WARNING:", js[[1]]$warning))
        }
    }else{
        print("Query unsuccessful.")
        print(paste("Category:", httr::http_status(resp)$category))
        print(paste("Reason:", httr::http_status(resp)$reason))
        if (js[[1]]$warning != ""){
            print(paste("WARNING:", js[[1]]$warning))
            print()
        }
        if (js[[1]]$error != ""){
            stop(paste("ERROR:", js[[1]]$error))
        }
    }
    
    #responsesJSON <- jsonlite::toJSON(js$responses)
    myRestResponse = new("RestResponse",
                         apiVersion = js$apiVersion,
                         time = js$time,
                         events = js$events,
                         params = js$params,
                         responses = list(
                             time = js$responses$time,
                             events = js$responses$events,
                             numResults = js$responses$numResults,
                             resultType = js$responses$resultType,
                             numTotalResults = js$responses$numTotalResults,
                             numMatches = js$responses$numMatches,
                             numInserted = js$responses$numInserted,
                             numUpdated = js$responses$numUpdated,
                             numDeleted = js$responses$numDeleted,
                             attributes = js$responses$attributes,
                             results = js$responses$results))
    
    return(list(restResponse = myRestResponse, 
                numResults = js$responses$numResults))
}
###############################################
