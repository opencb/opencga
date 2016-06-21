base <- "http://localhost:8080/opencga/webservices/rest/v1"
cga <- OpencgaLogin(baseurl ="http://localhost:8080/opencga/webservices/rest/v1",
                   userid = "melsiddieg", passwd = '1234')
cga <- Opencga(baseurl = "http://localhost:8080/opencga/webservices/rest/v1",
               userID = cga$userId, sessionID = cga$sessionId)
cga
res1 <- OpencgaStudy(object = cga, id = 2, action = "files")

cga <- OpencgaLogin(baseurl = base,interactive = T)
vardata <-OpencgaStudy(object = cga, id = 3, action = "variants")
varainst <- vardata$data
test <- fromJSON(vardata$url)
str(vardata,1)
sampledata <- OpencgaStudy(object = cga, id = 2, action = "samples")
studyFiles <- OpencgaStudy(object = cga, id = 2, action = "files")
sams <- sampledata$num_results
str(sams,1)
studyInfo <- OpencgaStudy(object = cga, id = 2, action = "info")
userinfo <- OpencgaUser(object = cga, id = "mano", action = "info")

### fetch variants from a file

test <- OpencgaFiles(object = cga, id = 32, action = "variants")
system.time({
  test2 <- OpencgaFiles(object = cga, id = 32, action = "variants")
})

files <- OpencgaStudy(object = cga, id = 3, action = "files")
info <- OpencgaStudy(object = cga, id = 3, action = "info")
samples <- OpencgaStudy(object = cga, id = 3, action = "samples")

filvar <- OpencgaFiles(object = cga, id = 32, action = "variants", params = filt)

summ <- OpencgaStudy(cga, id = 2, action = "summary")








