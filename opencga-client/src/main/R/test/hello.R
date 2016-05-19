# # base should be suuplied by the config
# base <- "http://localhost:8080/opencga/webservices/rest/v1"
# userid <- "melsiddieg"
# passwd <- "1234"
# OpencgaLogin <- function(base, userid, passwd){
#   require(jsonlite)
#   url <- paste(base,"/users/",userid,"/login","?password=", passwd, sep="")
#   res <- fromJSON(url)
#   userId <- unlist(res$response$result[[1]]$userId)
#   sessionId <- unlist(res$response$result[[1]]$sessionId)
#   return(list(sessionId=sessionId, userId=userId))
# }
#   test <- OpencgaLogin(base = base, userid = userid, passwd = passwd)
# test
# test$sessionId
# test$userId
#
# cga <- Opencga(baseurl = base, userId = test$userId, sessionId = test$sessionId)
# # see available projects
# library(jsonlite)
# prl <- "http://localhost:8080/opencga/webservices/rest/v1/users/melsiddieg/info?sid=kRO6FIGwvfHVOVN3MXQs"
# ufo <- fromJSON(prl)
# res <- ufo$response$result
# res
# study_info <- NULL
# smrl <- "http://localhost:8080/opencga/webservices/rest/v1/studies/3/samples?sid=LLzpk5G5XUs5YxW3z0et"
#
# vrl <- "http://localhost:8080/opencga/webservices/rest/v1/studies/3/variants?limit=1000&sid=LLzpk5G5XUs5YxW3z0et&histogram=false&interval=2000&merge=false"
# vreq <- fromJSON(vrl)
# smeq <- fromJSON(smrl)
# vares <- as.data.frame(vreq$response$result)
# smes <-as.data.frame(smeq$response$result)
# #
l1 <- OpencgaLogin(baseurl ="http://localhost:8080/opencga/webservices/rest/v1",
                   userid = "melsiddieg", passwd = '1234')
cga <- Opencga(baseurl = "http://localhost:8080/opencga/webservices/rest/v1",
               userID = l1$userId, sessionID = l1$sessionId)
cga
res1 <- OpencgaStudy(object = cga, id = 3, action = "files")
library(rlist)
library(pipeR)
vfl%>>%
  list.filter(annotation$consequenceTypes$geneName=="NOC2L")%>>%length
