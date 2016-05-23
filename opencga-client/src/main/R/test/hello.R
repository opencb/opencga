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
vardata <-OpencgaStudy(object = l1, id = 3, action = "variants")
varainst <- vardata$data
test <- fromJSON(vardata$url)
str(vardata,1)
sampledata <- OpencgaStudy(object = l1, id = 3, action = "samples")
studyFiles <- OpencgaStudy(object = l1, id = 3, action = "files")
sams <- sampledata$num_results
str(sams,1)
studyInfo <- OpencgaStudy(object = l1, id = 3, action = "info")
userinfo <- OpencgaUser(object = l1, id = "melsiddieg", action = "info")

### fetch variants from a file

test <- OpencgaFiles(object = l1, id = 32, action = "variants")
system.time({
  test2 <- OpencgaFiles(object = l1, id = 32, action = "variants")
})


















p <- profvis({
  test3 <- OpencgaFiles(object = l1, id = 32, action = "variants")
})
p
