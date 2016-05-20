base <- "http://localhost:8080/opencga/webservices/rest/v1"
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
cga <- OpencgaLogin(baseurl = base,interactive = T)
vardata <-OpencgaStudy(object = cga, id = 3, action = "variants")
test <- fromJSON(vardata$url)
str(vardata,1)
