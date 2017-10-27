## OpenCGA configuration

# read.conf.list <- function(conf){
#   if ("rest" %in% names(conf)){
#     if ("host" %in% names(conf$rest)){
#       host <- conf$rest$host
#     }
#   }else{
#     stop("Please, specify the 'host' in the 'rest' section")
#   }
#   if ("version" %in% names(conf)){
#     version <- conf$version
#   }else{
#     version <- "v1"
#     #stop("Please, specify the OpenCGA version")
#   }
#   return(list(host=host, version=version))
# }
# 
# read.conf.file <- function(conf){
#   type <- get.config.type(conf)
#   print(paste("Reading configuration file in", type, "format", sep = " "))
#   conf.obj <- read.config(conf, warn = F)
#   
#   read.conf.list(conf.obj)
# }
