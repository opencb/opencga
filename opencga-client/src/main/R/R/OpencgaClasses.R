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



setMethod("show",signature = "Opencga",definition = function(object){
  cat("|An object of class ", class(object), "\n", sep = "")
  cat("|This object is required by all Opencga Methods")
})

setGeneric("OpencgaUser", function(object, category, id, action, params=NULL)
  standardGeneric("OpencgaUser"))
#' A method to query user data
#' @title A method to query user data
#' @aliases OpencgaUser
#' @description This method allow the user to create, update, and explore user
#'  data and metadata
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
#' A method to query Studies
#' @title A method to query Opencga studies
#' @aliases OpencgaStudy
#' @description This method allow the user to create, update and explore
#' study data and metadta
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
#' A method to query Projects data
#' @title A method to query Opencga Projects
#' @aliases OpencgaStudy
#' @description This method allow the user to create, update and explore
#' projects data and metadta
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
#' @title A method to query Opencga Files
#' @aliases OpencgaFiles
#' @description This method allow the user to create, update and explore
#' files data and metadta
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
#' @title A method to query Opencga Samples
#' @aliases OpencgaSamples
#' @description This method allow the user to create, update and explore
#' Samples data and metadta
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
#' @title A method to query Opencga Individuals
#' @aliases OpencgaIndividuals
#' @description This method allow the user to create, update and explore
#' Individuals data and metadta
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
#' @title A method to query Opencga Jobs
#' @aliases OpencgaJobs
#' @description This method allow the user to create, update and explore
#' jobs data and metadta
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
#' @title A method to query Opencga Variables
#' @aliases OpencgaVariables
#' @description This method allow the user to create, update and explore
#' vaiables data and metadta
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
#' @title A method to query Opencga cohorts
#' @aliases OpencgaCohorts
#' @description This method allow the user to create, update and explore
#' cohorts data and metadta
#' @export
setMethod("OpencgaCohorts", "Opencga",    definition = function(object,
          id, action, params, ...){
  category <- "cohorts"
  data <- excuteOpencga(object=object, category=category, id=id, action=action,
                        params=NULL)
  return(data)
})
#' @title A class to store Opencga parameters for filtering queries
#' @aliases cgaVariantParam
#' @param chromosome the name of the chromosome to be returned
#' @export
cgaVariantParam <- setClass("cgaVariantParam", slots = c(region="character",
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


cgaVariantParam <- function(region=character(), chromosome=character(),
                         type=character(), gene=character(), maf=character(),
                         mgf=character(), genotype=character(),
                         polyphen=character(), sift=character(),
                         conservation=character(), reference=character(),
                         alternate=character(), so=character(),
                         biotype=character(), skip=numeric(), limit=numeric(),
                         files=character())
{

  new("cgaVariantParam", region=region, chromosome=chromosome, type=type,
      gene=gene, maf=maf, mgf=mgf, genotype=genotype, polyphen=polyphen,
      sift=sift, conservation=conservation, reference=reference,
      alternate=alternate, so=so, biotype=biotype, skip=0, limit=1000,
      files=files)
}


