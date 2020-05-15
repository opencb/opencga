################################################################################
#' OpencgaR Commons
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


fetchOpenCGA <- function(object=object, category=NULL, categoryId=NULL, 
                         subcategory=NULL, subcategoryId=NULL, action=NULL, 
                         params=NULL, httpMethod="GET", skip=0,
                         num_threads=NULL, as.queryParam=NULL, batch_size=2000){
    
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
    
    if (is.null(params)){
        params <- list()
    }
    
    while((unlist(num_results) == real_batch_size) && count <= limit){
        pathUrl <- paste0(host, version, category, categoryId, subcategory, 
                          subcategoryId, action)
        
        ## send batch size as limit to callrest
        real_batch_size <- min(c(real_batch_size, limit-count))
        if(real_batch_size == 0){
            break()
        }
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
            urlNewToken <- paste0(host, version, "users/", object@user, "/", "login", "?sid=", token)
            resp <- httr::POST(urlNewToken, httr::add_headers(c("Content-Type"="application/json",
                                                          "Accept"="application/json",
                                                          "Authorisation"="Bearer")), body="{}")
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
                             as.queryParam=as.queryParam, sid=object@showToken)
        
        skip <- skip+real_batch_size
        res_list <- parseResponse(resp=response$resp, content=response$content)
        num_results <- res_list$num_results
        tmp_result <- res_list$results
        # There are responses such as AnalysisResults that only return 1 result, not an array of results.
        if (is.data.frame((jsonlite::fromJSON(res_list$results))[[1]])) {
            # For those cases, we will simulate an array of results
            tmp_result <- paste0("[", res_list$results, "]")
        }
        
        result <- as.character(tmp_result)
        
        # remove first [ and last ] from json
        result <- trimws(result)
        result <- substr(result, 4, nchar(result))
        result <- substr(result, 1, nchar(result)-3)
        
        # Add to list
        container[[i]] <- result
        i=i+1
        count <- count + unlist(num_results)
        
        print(paste("Number of retrieved documents:", count))
    }
    
    if (count > 0){
        tryCatch({
            container <- paste(container, collapse = ',')
            container <- paste0("[", container, "]")
            jsonDf <- jsonlite::fromJSON(txt=container)
            return(jsonDf)
        }, error = function(e) {
            print("Constructing data.frame by batches...")
            containerDfTmp <- list()
            countTmp <- 0
            for (i in seq(from = 1, to = length(container), by = 10)){
                iend <- i+9
                if(iend > length(container)){
                    iend <- length(container)
                }
                countTmp <- countTmp + 1
                miniJson <- paste(container[i:iend], collapse = ',')
                miniJson <- paste0("[", miniJson, "]")
                containerDfTmp[[countTmp]] <- jsonlite::fromJSON(miniJson)
            }
            jsonDf <- jsonlite::rbind_pages(containerDfTmp)
            return(jsonDf)
        })
    }
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
callREST <- function(pathUrl, params, httpMethod, skip, token, as.queryParam, sid=FALSE){
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
        print(paste("URL:",fullUrl))
        resp <- httr::GET(fullUrl, httr::add_headers(Accept="application/json", Authorization=session), httr::timeout(300))
        
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
        print(paste("URL:",fullUrl))
        if (exists("bodyParams")){
            resp <- httr::POST(fullUrl, body = bodyParams, 
                        httr::add_headers(`Authorization` = session), encode = "json")
        }else{
            resp <- httr::POST(fullUrl, httr::add_headers(`Authorization` = session), 
                        encode = "json")
        }
    }
    
    content <- httr::content(resp, as="text", encoding = "utf-8")
    return(list(resp=resp, content=content))
}

## A function to parse the json data into R dataframes
parseResponse <- function(resp, content){
    js <- lapply(content, function(x) jsonlite::fromJSON(x))
    if (resp$status_code == 200){
        if (!("warning" %in% js[[1]]) || js[[1]]$warning == ""){
            print("Query successful!")
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

    ares <- lapply(js, function(x)x$responses$results)
    ares <- jsonlite::toJSON(ares)
    nums <- lapply(js, function(x)x$responses$numResults)

    # if (class(ares[[1]][[1]])=="data.frame"){
    #     ds <- lapply(ares, function(x)jsonlite::rbind_pages(x))
    #     
    #     ### Important to get correct vertical binding of dataframes
    #     names(ds) <- NULL
    #     ds <- jsonlite::rbind_pages(ds)
    # }else{
    #     ds <- ares
    #     names(ds) <- NULL
    # }
    return(list(result=ares, num_results=nums))
}
###############################################
