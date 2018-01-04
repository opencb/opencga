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
                         params=NULL, httpMethod="GET", 
                         num_threads=NULL, as.queryParam=NULL){
    # Get connection info
    host <- object@host
    token <- object@sessionId
    version <- object@version
    
    # batch_size <- batch_size
    
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
       limit <- 100000 
    }else{
        if(is.null(params$limit)){
            limit <- 100000
        }else{
            limit <- params$limit
        }
    }
    
    # Call server
    i <- 1
    batch_size <- min(c(1000, limit))
    skip <- 0
    num_results <- batch_size
    container <- list()
    count <- 0
    
    if (is.null(params)){
        params <- list()
    }
    
    while((unlist(num_results) == batch_size) && count <= limit){
        pathUrl <- paste0(host, version, category, categoryId, subcategory, 
                          subcategoryId, action)
        
        ## send batch size as limit to callrest
        batch_size <- min(c(batch_size, limit-count))
        if(batch_size == 0){
            break()
        }
        params$limit <- batch_size
        response <- callREST(pathUrl=pathUrl, params=params, 
                             httpMethod=httpMethod, skip=skip, token=token,
                             as.queryParam=as.queryParam)
        
        skip <- skip+batch_size
        res_list <- parseResponse(resp=response$resp, content=response$content)
        num_results <- res_list$num_results
        result <- res_list$result
        container[[i]] <- result
        i=i+1
        count <- count + unlist(num_results)
        
        print(paste("Number of retrieved documents:", count))
    }
    if(class(container[[1]])=="data.frame"){
        ds <- jsonlite::rbind_pages(container)
    }else{
        ds <- as.data.frame(container[[1]], stringsAsFactors=FALSE, names="result")
    }
    return(ds)
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
callREST <- function(pathUrl, params, httpMethod, skip, token, as.queryParam){
    content <- list()
    session <- paste("Bearer", token)
    skip=paste0("?skip=", skip)
    
    # Make GET call
    if (httpMethod == "GET"){
        if (!is.null(params)){
            params <- get_qparams(params)
            fullUrl <- paste0(pathUrl, skip, "&", params)
        }else{
            fullUrl <- paste0(pathUrl, skip)
        }
        print(paste("URL:",fullUrl))
        resp <- httr::GET(fullUrl, httr::add_headers(Accept="application/json", Authorization=session), httr::timeout(30))
        
    }else if(httpMethod == "POST"){
    # Make POST call
        if (!is.null(as.queryParam)){
            if(class(as.queryParam) == "character"){
                as.queryParam <- unique(c(as.queryParam, "study"))
            }
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
        if (js[[1]]$warning == ""){
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

    ares <- lapply(js, function(x)x$response$result)
    nums <- lapply(js, function(x)x$response$numResults)

    if (class(ares[[1]][[1]])=="data.frame"){
        ds <- lapply(ares, function(x)jsonlite::rbind_pages(x))
        
        ### Important to get correct vertical binding of dataframes
        names(ds) <- NULL
        ds <- jsonlite::rbind_pages(ds)
    }else{
        ds <- ares
        names(ds) <- NULL
    }
    return(list(result=ds, num_results=nums))
}
###############################################
