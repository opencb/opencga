require(methods)
require(miniUI)
require(shiny)
#' @include OpencgaFunctions.R
#' This class defines the Opencga object
Opencga <- setClass("Opencga", slots = c(baseurl="character",
    userID="character", sessionID="character"))

## A constructor function

Opencga <- function(baseurl, userID, sessionID){
  new("Opencga", baseurl=baseurl, userID=userID, sessionID=sessionID)
}
#' @title A method to query user data
#' @aliases OpencgaUser
#' @description This method allow the user to create update and see user data
#'
#' @export
setMethod("show",signature = "Opencga",definition = function(object){
  cat("|An object of class ", class(object), "\n", sep = "")
  cat("|This object is required by all Opencga Methods")
})

setGeneric("OpencgaUser", function(object, category, id, action, params=NULL)
  standardGeneric("OpencgaUser"))
#' A method to query user data
#' @export
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
#' @export
setMethod("OpencgaStudy", "Opencga", definition = function(object, id,
                                                          action, params){
 category <- "studies"
 data <- excuteOpencga(object=object, category=category, id=id, action=action,
                       params=NULL)
  return(data)
})
setGeneric("OpencgaProjects", function(object, id, action, params, ...)
  standardGeneric("OpencgaProjects"))
#' @export
setMethod("OpencgaProjects", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "projects"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaFiles", function(object, id, action, params, ...)
  standardGeneric("OpencgaFiles"))
#' @export
setMethod("OpencgaFiles", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "files"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaSamples", function(object, id, action, params, ...)
  standardGeneric("OpencgaSamples"))
#' @export
setMethod("OpencgaSamples", "Opencga",    definition = function(object,
         id, action, params, ...){
  category <- "samples"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaIndividuals", function(object, id, action, params, ...)
  standardGeneric("OpencgaIndividuals"))
#' @export
setMethod("OpencgaIndividuals", "Opencga",    definition = function(object,
         id, action, params, ...){
  category <- "individuals"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaJobs", function(object, id, action, params, ...)
  standardGeneric("OpencgaJobs"))
#' @export
setMethod("OpencgaJobs", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "jobs"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaVariables", function(object, id, action, params, ...)
  standardGeneric("OpencgaVariables"))
#' @export
setMethod("OpencgaVariables", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "variables"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
setGeneric("OpencgaCohorts", function(object, id, action, params, ...)
  standardGeneric("OpencgaCohorts"))
#' @export
setMethod("OpencgaCohorts", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "cohorts"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
#' @export
OpencgaParam <- setClass("OpencgaParam", slots = c(region="character",
                          chromosome="character", type="character",
                          gene="character", maf="character", mgf="character",
                          genotype="character", polyphen="character",
                          sift="character", conservation="character",
                          reference="character", alternate="character",
                          so="character", biotype="character", skip="numeric",
                          limit="numeric", files="character"),
                          prototype = prototype(region=character(),
                          chromosome=character(), type=character(),
                          gene=character(), maf=character(),
                          mgf=character(), genotype=character(),
                          polyphen=character(), sift=character(),
                          conservation=character(), reference=character(),
                          alternate=character(), so=character(),
                          so=character(), biotype=character(),
                          skip=numeric(), limit=numeric(),
                          files=character()))


OpencgaParam <- function(region=character(), chromosome=character(),
                         type=character(), gene=character(), maf=character(),
                         mgf=character(), genotype=character(),
                         polyphen=character(), sift=character(),
                         conservation=character(), reference=character(),
                         alternate=character(), so=character(),
                         biotype=character(), skip=numeric(), limit=numeric(),
                         files=character())
{

  new("OpencgaParam", region=region, chromosome=chromosome, type=type,
      gene=gene, maf=maf, mgf=mgf, genotype=genotype, polyphen=polyphen,
      sift=sift, conservation=conservation, reference=reference,
      alternate=alternate, so=so, biotype=biotype, skip=0, limit=1000,
      files=files)
}


