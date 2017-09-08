library(opencgaR)

## Test reading config
##############################
# Configuration in list format
conf <- list(version="v1",
             rest=list(host="http://localhost:8080/opencga/"))
con <- opencgaReadConfig(conf)

# Configuration in file format ("YAML" or "JSON")
conf <- "/Users/mbleda/appl/opencga/build/conf/client-configuration.yml"
con <- OpencgaReadConfig(conf)






## Test reading config
##############################
# Configuration in list format
conf <- list(version="v1",
             rest=list(host="http://localhost:8080/opencga/"))
con <- OpencgaReadConfig(conf)

# Configuration in file format ("YAML" or "JSON")
conf <- "/Users/mbleda/appl/opencga/build/conf/client-configuration.yml"
con <- OpencgaReadConfig(conf)

## Test connection to OpenCGA
##############################
# Connect to OpenCGA
cga <- OpencgaLogin(conf = con, userid = "user1", passwd = "user1_pass")
cga

debug(OpencgaStudy)


res1 <- OpencgaStudy(object = cga, id = 2, action = "files")

# cga <- OpencgaLogin(baseurl = base,interactive = T)
userProj <- OpencgaUser(cga, category = "user", id="mano",action = "projects")

sampledata <- OpencgaStudy(object = cga, id = 2, action = "samples")
studyFiles <- OpencgaStudy(object = cga, id = 2, action = "files")
studyInfo <- OpencgaStudy(object = cga, id = 2, action = "info")
userinfo <- OpencgaUser(object = cga, id = "mano", action = "info")

### fetch variants from a file
# get all the variants
system.time({
  test <- OpencgaFiles(object = cga, id = 60, action = "variants")
  
})
# construct a variantParam
filt <- cgaVariantParam(region = "1:10522:17522")
#
system.time({
  filteredVars <- OpencgaFiles(cga, id = 60, action = "variants", params = filt)
  })

# A test to create a new project
# get the neccessary help
proj <- getOpencgaDocs(category = "projects", action = "create", requiredOnly = T)
# Hope
hop <- OpencgaProjects(object = cga, id = NULL, action = "create", params = NULL,
                       name="Sudan" ,alias="Sudanese", organization="Opencb")
# Another test
hop2 <- OpencgaProjects(object = cga, action = "create",  name="Sudan2" ,
                        alias="Sudanese2", organization="Opencb")
#
# create  A New study
# get help on the arguments to create a a study
sth <- getOpencgaDocs(category = "studies", action = "create")
#
newS <- OpencgaStudy(object = cga, id = NULL, action = "create", params = NULL,
                     name="Sudan_test", alias="test_S", projectId=72)



system.time({
  test2 <- OpencgaFiles(object = cga, id = 32, action = "variants")
})

files <- OpencgaStudy(object = cga, id = 3, action = "files")
info <- OpencgaStudy(object = cga, id = 3, action = "info")
samples <- OpencgaStudy(object = cga, id = 3, action = "samples")

filvar <- OpencgaFiles(object = cga, id = 32, action = "variants", params = filt)

summ <- OpencgaStudy(cga, id = 2, action = "summary")








