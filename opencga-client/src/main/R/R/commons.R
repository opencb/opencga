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
                         params=NULL, httpMethod="GET", batch_size=50, 
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
    
    # Call server
    i <- 1
    server_limit <- 1000
    skip <- 0
    num_results <- 1000
    container <- list()
    
    while(unlist(num_results) == server_limit){
        pathUrl <- paste0(host, version, category, categoryId, subcategory, 
                          subcategoryId, action)
        
        content <- callREST(pathUrl=pathUrl, params=params, 
                            httpMethod=httpMethod, skip=skip, token=token,
                            as.queryParam=as.queryParam)
        
        skip <- skip+1000
        res_list <- parseResponse(content=content)
        num_results <- res_list$num_results
        result <- res_list$result
        container[[i]] <- result
        i=i+1
    }
    if(class(container[[1]])=="data.frame"){
        ds <- rbind_pages(container)
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
        #resp <- GET(fullUrl, add_headers(`Accept-Encoding` = "gzip, deflate", 
        #                                 `Authorization` = session), timeout(2))
        print(paste("URL:",fullUrl))
        resp <- GET(fullUrl, add_headers(Accept="application/json", Authorization=session), timeout(2))
        
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
            resp <- POST(fullUrl, body = bodyParams, 
                         add_headers(`Authorization` = session), encode = "json")
        }else{
            resp <- POST(fullUrl, add_headers(`Authorization` = session), 
                         encode = "json")
        }
    }
    
    content <- content(resp, as="text", encoding = "utf-8")
    return(content)
}

## A function to parse the json data into R dataframes
parseResponse <- function(content){
    js <- lapply(content, function(x)fromJSON(x))
    if (js[[1]]$warning != ""){
        print(paste("WARNING:", js[[1]]$warning))
        print()
    }
    if (js[[1]]$error != ""){
        stop(paste("ERROR:", js[[1]]$error))
    }
    ares <- lapply(js, function(x)x$response$result)
    
    nums <- lapply(js, function(x)x$response$numResults)
    
    if (class(ares[[1]][[1]])=="data.frame"){
        if(requireNamespace("pbapply", quietly = TRUE)){
            ds <- pbapply::pblapply(ares,function(x)rbind_pages(x))
        }
        ### Important to get correct vertical binding of dataframes
        names(ds) <- NULL
        ds <- rbind_pages(ds)
    }else{
        ds <- ares
        names(ds) <- NULL
    }
    return(list(result=ds, num_results=nums))
}
###############################################



## a function to read the varinats from a vcf file
readIds <- function(file=file,batch_size,num_threads)
{
    
    ids<- list()
    num_iter<- ceiling(countLines(file)[[1]]/(batch_size*num_threads))
    #batchSize * numThreads
    demo <- TabixFile(file,yieldSize = batch_size*num_threads)
    tbx <- open(demo)
    i <- 1
    while (i <=num_iter) {
        inter <- scanTabix(tbx)[[1]]
        if(length(inter)==0)break
        whim <- lapply(inter, function(x){
            strsplit(x[1],split = "\t")[[1]][c(1,2,4,5)]})
        whish <- sapply(whim, function(x){paste(x,collapse =":")})
        hope <- split(whish, ceiling(seq_along(whish)/batch_size))
        ids[[i]] <- hope
        i <- i+1
    }
    ids <-foreach(k=1:length(ids))%do%{
        foreach(j=1:length(ids[[k]]))%do%{
            ids[[k]][[j]]
        }
    }
    ids <- unlist(ids, recursive = FALSE)
    return(ids)
}

## A function to create URLs
## create a list of character vectors of urls
createURL <- function(httpMethod=httpMethod, host=host, version=version, 
                      category=category, categoryId=categoryId, 
                      subcategory=subcategory, subcategoryId=subcategoryId, 
                      action=action, params=params, skip=0){
    
    # if(is.null(file)){
    skip=paste0("?","skip=",skip)
    param <- paste(skip, param, sep = "&")
    
    if(nchar(species)>1){
        grls <- paste0(host,version, meta, species,"/", categ, subcateg, ids, 
                       resource,param,collapse = "")
    }else{
        grls <- paste0(host,version, meta, species, categ, subcateg, ids, 
                       resource,param,collapse = "")
    }
    # }else{
    #     grls <- list()
    #     gcl <- paste0(host,version,species,categ,subcateg,collapse = "")
    #     
    #     for(i in seq_along(ids)){
    #         hop <- paste(ids[[i]],collapse = ",")
    #         tmp <- paste0(gcl,hop,resource,collapse = ",")
    #         grls[[i]] <- gsub("chr","",tmp)
    #     }
    # }
    return(grls)
}


callREST2 <- function(grls,async=FALSE,num_threads=num_threads)
{
    content <- list()
    if(is.null(file)){
        resp <- GET(grls, add_headers(`Accept-Encoding` = "gzip, deflate"), 
                    timeout(2))
        content <- content(resp, as="text", encoding = "utf-8")
    }else{
        resp <- GET(grls, add_headers(`Accept-Encoding` = "gzip, deflate"))
        content <- content(resp, as="text", encoding = "utf-8")
    }
    return(content)
}


### Docs
#' getCellBaseResourceHelp
#' 
#' A function to get help about available cellbase resources
#' @details This function retrieves available resources for each generic method
#' like getGene, getRegion, getprotein, etc. It help the user see all possible 
#' resources to use with the getGeneric methods 
#' @param object a cellBase class object
#' @param subcategory a character the subcategory to be queried
#' @return character vector of the available resources to that particular 
#' subcategory 
#' @examples 
#' cb <- CellBaseR()
#' # Get help about what resources are available to the getGene method
#' getCellBaseResourceHelp(cb, subcategory="gene")
#' # Get help about what resources are available to the getRegion method
#' getCellBaseResourceHelp(cb, subcategory="region")
#' # Get help about what resources are available to the getXref method
#' getCellBaseResourceHelp(cb, subcategory="id")
#' @export
getCellBaseResourceHelp <- function(object, subcategory){
    host <- object@host
    if(exists('.api', .GlobalEnv)&exists('.tags', .GlobalEnv)){
        getList <- get('.api',envir = .GlobalEnv)
        tags <- get('.tags',envir = .GlobalEnv) 
    }else {
        cbDocsUrl <- paste0(host, "swagger.json")
        Datp <- jsonlite::fromJSON(cbDocsUrl)
        tags <- Datp$tags
        paths <- Datp$paths 
        getList<- lapply(paths, function(x)x$get)
        assign('.api', getList, .GlobalEnv)
        assign('.tags', tags, .GlobalEnv)
    }
    category <- switch (subcategory,
                        gene= "feature",
                        protein= "feature",
                        tf="regulation",
                        variation="feature",
                        variant="genomic",
                        clinical="feature",
                        transcript="feature",
                        id="feature")
    
    ## filtered
    SUBCATEGORIES <- c('gene', 'protein', 'tf', 'variation', 'variant',
                       'clinical', 'transcript', 'id', 'region')
    if(!(subcategory %in% SUBCATEGORIES)){
        cat("Please use one of CellBase Subcategories\n")
        cat(SUBCATEGORIES,'\n')
        stop("Error unknown subcategory")}
    parts <- Filter(Negate(function(x) is.null(unlist(x))), getList)
    cbGetParams <- lapply(parts, function(x)x$parameters)
    catsub <- paste(category,subcategory, sep = "/")
    index <- grep(catsub, names(cbGetParams))
    narrowed <- names(parts)[index]
    patt1 <- paste0(catsub,"/", ".*?/","(.*)" )
    resMatch <- regexec(patt1,narrowed)
    m <- regmatches(narrowed, resMatch)
    res <- sapply(m, function(x)x[2])
    res <- res[!is.na(res)]
    
    res
}
#
# cbCheck <- function(object, category, subcategory, resource){
#   CATEGORIES <- c("feature", "genomic", "regulation")
#   if(!(category %in% CATEGORIES)){
#     stop("Error Unknown category")
#   } 
#   SUBCATEGORIES <- tolower(unlist(tags[[1]]))
#   if(!(subcategory %in% SUBCATEGORIES)){
#     stop("Error Unknown subcategory")
#   } 
#   RESOURCES <- getCellBaseResourceHelp(object, subcategory)
#   if(!(resource %in% RESOURCES)){
#     stop("Error Unknown resource")
#   } 
# }