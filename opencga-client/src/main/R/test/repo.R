
vcfl <- list.load(vcf$url)


# require(BiocParallel)
# fetchCellbase <- function(file=NULL,host=host, version=version, meta=meta,
#                           species=species, categ, subcateg,ids, resource,filters=NULL,
#                           batch_size=NULL,num_threads=NULL,...){
#   # Get the parametrs
#   if(species=="hsapiens/"){
#     batch_size <- batch_size
#   }else{
#     batch_size <- 50
#   }
#   num_threads <- num_threads
#   if(is.null(categ)){
#     categ <- ""
#   }else{
#     categ <- paste0(categ,"/",sep="")
#   }
#   if(is.null(subcateg)){
#     subcateg <- ""
#   }else{
#     subcateg <- paste0(subcateg,"/",sep="")
#   }
#   # How to read the ids from the function parameter
#   if(is.null(file)){
#     if(is.null(ids)){
#       ids <- ""
#     }else{
#       ids <- paste0(ids,collapse = ",")
#       ids <- paste0(ids,"/",collapse = "")
#     }
#     # or from a file
#   }else{
#     cat("\nreading the file....\n")
#     ids <- readIds(file,batch_size = batch_size,num_threads = num_threads)
#   }
#   # in case a vcf file has been specified
#   if(!is.null(file)){
#     container=list()
#     grls <- createURL(file=file, host=host, version=version, species=species,
#                       categ=categ, subcateg=subcateg, ids=ids, resource=resource,...)
#     cat("\ngetting the data....\n")
#     content <- callREST(grls = grls,async=TRUE,num_threads)
#     cat("\nparsing the data....\n")
#     res_list <- parseResponse(content=content,parallel=TRUE,
#                               num_threads=num_threads)
#     ds <- res_list$result
#     cat("Done!")
#
#     # in case of all other methods except for annotateVcf
#   }else{
#     i=1
#     server_limit=1000
#     skip=0
#     num_results=1000
#     container=list()
#     while(is.null(file)&all(unlist(num_results)==server_limit)){
#       grls <- createURL(file=NULL, host=host, version=version, meta=meta,
#                         species=species, categ=categ, subcateg=subcateg, ids=ids,
#                         resource=resource,filters=filters,skip = skip)
#       skip=skip+1000
#       content <- callREST(grls = grls)
#       res_list <- parseResponse(content=content)
#       num_results <- res_list$num_results
#       cell <- res_list$result
#       container[[i]] <- cell
#       i=i+1
#     }
#     ds <- rbind.pages(container)
#   }
#
#
#   return(ds)
# }
# ## all working functions
# ## a function to read the varinats from a vcf file
# readIds <- function(file=file,batch_size,num_threads)
# {
#   require(Rsamtools)
#   #require(pbapply)
#   ids<- list()
#   num_iter<- ceiling(R.utils::countLines(file)[[1]]/(batch_size*num_threads))
#   #batchSize * numThreads
#   demo <- TabixFile(file,yieldSize = batch_size*num_threads)
#   tbx <- open(demo)
#   i <- 1
#   while (i <=num_iter) {
#     inter <- scanTabix(tbx)[[1]]
#     if(length(inter)==0)break
#     whim <- lapply(inter, function(x){
#       strsplit(x[1],split = "\t")[[1]][c(1,2,4,5)]})
#     whish <- sapply(whim, function(x){paste(x,collapse =":")})
#     hope <- split(whish, ceiling(seq_along(whish)/batch_size))
#     ids[[i]] <- hope
#     i <- i+1
#   }
#   #ids <- pbsapply(ids, function(x)lapply(x, function(x)x))
#   require(foreach)
#   ids <-foreach(k=1:length(ids))%do%{
#     foreach(j=1:length(ids[[k]]))%do%{
#       ids[[k]][[j]]
#     }
#   }
#   ids <- unlist(ids, recursive = FALSE)
#   return(ids)
# }
#
# ## A function to create URLs
# ## create a list of character vectors of urls
# createURL <- function(file=NULL, host=host, version=version, meta=meta,
#                       species=species, categ=categ, subcateg=subcateg, ids=ids,
#                       resource=resource, filters=filters,skip=0)
# {
#
#   if(is.null(file)){
#     skip=paste0("?","skip=",skip)
#     filters <- paste(skip,filters,sep = "&")
#     grls <- paste0(host,version, meta, species, categ, subcateg, ids,
#                    resource,filters,collapse = "")
#
#   }else{
#     grls <- list()
#     gcl <- paste0(host,version,species,categ,subcateg,collapse = "")
#
#     for(i in seq_along(ids)){
#       hop <- paste(ids[[i]],collapse = ",")
#       tmp <- paste0(gcl,hop,resource,collapse = ",")
#       grls[[i]] <- gsub("chr","",tmp)
#     }
#   }
#   return(grls)
# }
#
# ## A function to make the API calls
# callREST <- function(grls,async=FALSE,num_threads=num_threads)
# {
#   content <- list()
#   require(RCurl)
#   if(is.null(file)){
#     content <- getURI(grls)
#   }else{
#     require(pbapply)
#     if(async==TRUE){
#       prp <- split(grls,ceiling(seq_along(grls)/num_threads))
#       cat("Preparing The Asynchronus call.............")
#       gs <- pblapply(prp, function(x)unlist(x))
#       cat("Getting the Data...............")
#       content <- pblapply(gs,function(x)getURIAsynchronous(x,perform = Inf))
#       content <- unlist(content)
#
#     }else{
#       content <- pbsapply(grls, function(x)getURI(x))
#
#     }
#   }
#
#
#   return(content)
# }
# ## A function to parse the json data into R dataframes
# parseResponse <- function(content,parallel=FALSE,num_threads=num_threads){
#   require(BiocParallel)
#   require(jsonlite)
#   if(parallel==TRUE){
#     require(parallel)
#     require(doMC)
#     num_cores <-detectCores()/2
#     registerDoMC(num_cores)
#     #
#     # ## Extracting the content in parallel
#     js <- mclapply(content, function(x)fromJSON(x), mc.cores=num_cores)
#     res <- mclapply(js, function(x)x$response$result, mc.cores=num_cores)
#     names(res) <- NULL
#     ind <- sapply(res, function(x)length(x)!=1)
#     res <- res[ind]
#     ds <- mclapply(res, function(x)rbind.pages(x), mc.cores=num_cores)
#     # js <- pblapply(content, function(x)fromJSON(x))
#     # res <- pblapply(js, function(x)x$response$result)
#     # names(res) <- NULL
#     # ds <- foreach(k=1:length(res),.options.multicore=list(preschedule=TRUE),
#     #               .combine=function(...)rbind.pages(list(...)),
#     #               .packages='jsonlite',.multicombine=TRUE) %dopar% {
#     #                 rbind.pages(res[[k]])
#     #               }
#
#     ds <- pblapply(res, function(x)rbind.pages(x))
#     ## Important to get correct merging of dataframe
#     names(ds) <- NULL
#     ds <- rbind.pages(ds)
#     nums <- NULL
#     # js <- lapply(content, function(x)fromJSON(x))
#     # ares <- lapply(js, function(x)x$response$result)
#     # ds <- pblapply(ares,function(x)rbind.pages(x))
#   }else{
#     js <- lapply(content, function(x)fromJSON(x))
#     ares <- lapply(js, function(x)x$response$result)
#     nums <- lapply(js, function(x)x$response$numResults)
#     ds <- pblapply(ares,function(x)rbind.pages(x))
#     ### Important to get correct vertical binding of dataframes
#     names(ds) <- NULL
#     ds <- rbind.pages(ds)
#   }
#   return(list(result=ds,num_results=nums))
# }
