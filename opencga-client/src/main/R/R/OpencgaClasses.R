### Expected classes
# OpencgaClient create login
# OpencgaUser
# OpencgaProject
# OpencgaStudy
# OpencgaFiles
# OpencgaJobs
# OpencgaIndividuals
# OpencgaSamples
# OpencgaVariables
# opencgaCohorts
require(methods)

#' @include OpencgaFunctions.R
#' This class defines the Opencga object
Opencga <- setClass("Opencga", slots = c(baseurl="character",
    userID="character", sessionID="character"))

## A constructor function
Opencga <- function(baseurl, userID, sessionID){
  new("Opencga", baseurl=baseurl, userID=userID, sessionID=sessionID)
}

setMethod("show",signature = "Opencga",definition = function(object){
  cat("|An object of class ", class(object), "\n", sep = "")
  cat("|This object is required by all Opencga Methods")
})

setGeneric("OpencgaUser", function(object, category, id, action, params=NULL)
  standardGeneric("OpencgaUser"))

setMethod("OpencgaUser", "Opencga", definition = function(object,
          id, action, params=NULL){
  category <- "users"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})

###
setGeneric("OpencgaStudy", function(object, id, action, params=NULL)
  standardGeneric("OpencgaStudy"))

setMethod("OpencgaStudy", "Opencga", definition = function(object, id,
                                                          action, params){
 category <- "studies"
 data <- excuteOpencga(object=object, category=category, id=id, action=action,
                       params=NULL)
  return(data)
})
setGeneric("OpencgaProjects", function(object, id, action, params, ...)
  standardGeneric("OpencgaProjects"))

setMethod("OpencgaProjects", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "projects"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaFiles", function(object, id, action, params, ...)
  standardGeneric("OpencgaFiles"))

setMethod("OpencgaFiles", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "files"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaSamples", function(object, id, action, params, ...)
  standardGeneric("OpencgaSamples"))
setMethod("OpencgaSamples", "Opencga",    definition = function(object,
         id, action, params, ...){
  category <- "samples"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaIndividuals", function(object, id, action, params, ...)
  standardGeneric("OpencgaIndividuals"))

setMethod("OpencgaIndividuals", "Opencga",    definition = function(object,
         id, action, params, ...){
  category <- "individuals"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaJobs", function(object, id, action, params, ...)
  standardGeneric("OpencgaJobs"))

setMethod("OpencgaJobs", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "jobs"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaVariables", function(object, id, action, params, ...)
  standardGeneric("OpencgaVariables"))

setMethod("OpencgaVariables", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "variables"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaCohorts", function(object, id, action, params, ...)
  standardGeneric("OpencgaCohorts"))

setMethod("OpencgaCohorts", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "cohorts"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
