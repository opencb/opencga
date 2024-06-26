% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Cohort-methods.R
\name{cohortClient,OpencgaR-method}
\alias{cohortClient,OpencgaR-method}
\title{CohortClient methods}
\usage{
\S4method{cohortClient}{OpencgaR}(
  OpencgaR,
  members,
  annotationSet,
  cohorts,
  cohort,
  endpointName,
  params = NULL,
  ...
)
}
\arguments{
\item{members}{Comma separated list of user or group ids.}

\item{annotationSet}{AnnotationSet ID to be updated.}

\item{cohorts}{Comma separated list of cohort ids.}

\item{cohort}{Cohort ID.}

\item{creationYear}{Creation year.}

\item{creationMonth}{Creation month (JANUARY, FEBRUARY...).}

\item{creationDay}{Creation day.}

\item{creationDayOfWeek}{Creation day of week (MONDAY, TUESDAY...).}

\item{default}{Calculate default stats.}

\item{variableSetId}{Variable set ID or name.}

\item{path}{Path where the TSV file is located in OpenCGA or where it should be located.}

\item{parents}{Flag indicating whether to create parent directories if they don't exist (only when TSV file was not previously associated).}

\item{annotationSetId}{Annotation set id. If not provided, variableSetId will be used.}

\item{variableSet}{Deprecated: Use /generate web service and filter by annotation.}

\item{variable}{Deprecated: Use /generate web service and filter by annotation.}

\item{field}{Field for which to obtain the distinct values.}

\item{somatic}{Somatic sample.}

\item{individualId}{Individual ID or UUID.}

\item{fileIds}{Comma separated list of file IDs, paths or UUIDs.}

\item{phenotypes}{Comma separated list of phenotype ids or names.}

\item{snapshot}{Snapshot value (Latest version of the entry in the specified release).}

\item{limit}{Number of results to be returned.}

\item{skip}{Number of results to skip.}

\item{count}{Get the total number of results matching the query. Deactivated by default.}

\item{id}{Comma separated list of cohort IDs up to a maximum of 100.}

\item{name}{Comma separated list of cohort names up to a maximum of 100.}

\item{uuid}{Comma separated list of cohort IDs up to a maximum of 100.}

\item{type}{Cohort type.}

\item{creationDate}{creationDate.}

\item{modificationDate}{modificationDate.}

\item{status}{status.}

\item{internalStatus}{internalStatus.}

\item{annotation}{Cohort annotation.}

\item{acl}{acl.}

\item{samples}{Cohort sample IDs.}

\item{numSamples}{Number of samples.}

\item{release}{release.}

\item{member}{User or group id.}

\item{silent}{Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.}

\item{flattenAnnotations}{Flatten the annotations?.}

\item{deleted}{Boolean to retrieve deleted cohorts.}

\item{include}{Fields included in the response, whole JSON path must be provided.}

\item{exclude}{Fields excluded in the response, whole JSON path must be provided.}

\item{samplesAction}{Action to be performed if the array of samples is being updated. Allowed values: \link{'ADD', 'SET', 'REMOVE'}}

\item{annotationSetsAction}{Action to be performed if the array of annotationSets is being updated. Allowed values: \link{'ADD', 'SET', 'REMOVE'}}

\item{includeResult}{Flag indicating to include the created or updated document result in the response.}

\item{study}{Study [\link{user@}project:]study where study and project can be either the ID or UUID.}

\item{action}{Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some annotations; RESET to set some annotations to the default value configured in the corresponding variables of the VariableSet if any. Allowed values: \link{'ADD', 'SET', 'REMOVE', 'RESET', 'REPLACE'}}

\item{data}{Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset' containing the comma separated variables that will be set to the default value when the action is RESET.}
}
\description{
This function implements the OpenCGA calls for managing Cohorts.
The following table summarises the available \emph{actions} for this client:\tabular{llr}{
   endpointName \tab Endpoint WS \tab parameters accepted \cr
   updateAcl \tab /{apiVersion}/cohorts/acl/{members}/update \tab study, members\link{*}, action\link{*}, body\link{*} \cr
   aggregationStats \tab /{apiVersion}/cohorts/aggregationStats \tab study, type, creationYear, creationMonth, creationDay, creationDayOfWeek, numSamples, status, release, annotation, default, field \cr
   loadAnnotationSets \tab /{apiVersion}/cohorts/annotationSets/load \tab study, variableSetId\link{*}, path\link{*}, parents, annotationSetId, body \cr
   create \tab /{apiVersion}/cohorts/create \tab include, exclude, study, variableSet, variable, includeResult, body\link{*} \cr
   distinct \tab /{apiVersion}/cohorts/distinct \tab study, id, name, uuid, type, creationDate, modificationDate, deleted, status, internalStatus, annotation, acl, samples, numSamples, release, field\link{*} \cr
   generate \tab /{apiVersion}/cohorts/generate \tab include, exclude, study, id, somatic, individualId, fileIds, creationDate, modificationDate, internalStatus, status, phenotypes, annotation, acl, release, snapshot, includeResult, body\link{*} \cr
   search \tab /{apiVersion}/cohorts/search \tab include, exclude, limit, skip, count, flattenAnnotations, study, id, name, uuid, type, creationDate, modificationDate, deleted, status, internalStatus, annotation, acl, samples, numSamples, release \cr
   acl \tab /{apiVersion}/cohorts/{cohorts}/acl \tab cohorts\link{*}, study, member, silent \cr
   delete \tab /{apiVersion}/cohorts/{cohorts}/delete \tab study, cohorts\link{*} \cr
   info \tab /{apiVersion}/cohorts/{cohorts}/info \tab include, exclude, flattenAnnotations, cohorts\link{*}, study, deleted \cr
   update \tab /{apiVersion}/cohorts/{cohorts}/update \tab include, exclude, cohorts\link{*}, study, samplesAction, annotationSetsAction, includeResult, body \cr
   updateAnnotationSetsAnnotations \tab /{apiVersion}/cohorts/{cohort}/annotationSets/{annotationSet}/annotations/update \tab cohort\link{*}, study, annotationSet\link{*}, action, body \cr
}
}
\section{Endpoint /{apiVersion}/cohorts/acl/{members}/update}{

Update the set of permissions granted for the member.
}

\section{Endpoint /{apiVersion}/cohorts/aggregationStats}{

Fetch catalog cohort stats.
}

\section{Endpoint /{apiVersion}/cohorts/annotationSets/load}{

Load annotation sets from a TSV file.
}

\section{Endpoint /{apiVersion}/cohorts/create}{

Create a cohort.
}

\section{Endpoint /{apiVersion}/cohorts/distinct}{

Cohort distinct method.
}

\section{Endpoint /{apiVersion}/cohorts/generate}{

Create a cohort based on a sample query.
}

\section{Endpoint /{apiVersion}/cohorts/search}{

Search cohorts.
}

\section{Endpoint /{apiVersion}/cohorts/{cohorts}/acl}{

Return the acl of the cohort. If member is provided, it will only return the acl for the member.
}

\section{Endpoint /{apiVersion}/cohorts/{cohorts}/delete}{

Delete cohorts.
}

\section{Endpoint /{apiVersion}/cohorts/{cohorts}/info}{

Get cohort information.
}

\section{Endpoint /{apiVersion}/cohorts/{cohorts}/update}{

Update some cohort attributes.
}

\section{Endpoint /{apiVersion}/cohorts/{cohort}/annotationSets/{annotationSet}/annotations/update}{

Update annotations from an annotationSet.
}

\seealso{
\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
\link{*}: Required parameter
}
