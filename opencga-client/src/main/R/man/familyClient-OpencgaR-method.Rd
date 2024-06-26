% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Family-methods.R
\name{familyClient,OpencgaR-method}
\alias{familyClient,OpencgaR-method}
\title{FamilyClient methods}
\usage{
\S4method{familyClient}{OpencgaR}(
  OpencgaR,
  members,
  annotationSet,
  families,
  family,
  endpointName,
  params = NULL,
  ...
)
}
\arguments{
\item{members}{Comma separated list of family members.}

\item{annotationSet}{AnnotationSet ID to be updated.}

\item{families}{Comma separated list of family ids.}

\item{family}{Family id.}

\item{propagate}{Propagate family permissions to related individuals and samples.}

\item{creationYear}{Creation year.}

\item{creationMonth}{Creation month (JANUARY, FEBRUARY...).}

\item{creationDay}{Creation day.}

\item{creationDayOfWeek}{Creation day of week (MONDAY, TUESDAY...).}

\item{numMembers}{Number of members.}

\item{default}{Calculate default stats.}

\item{variableSetId}{Variable set ID or name.}

\item{path}{Path where the TSV file is located in OpenCGA or where it should be located.}

\item{parents}{Flag indicating whether to create parent directories if they don't exist (only when TSV file was not previously associated).}

\item{annotationSetId}{Annotation set id. If not provided, variableSetId will be used.}

\item{field}{Field for which to obtain the distinct values.}

\item{limit}{Number of results to be returned.}

\item{skip}{Number of results to skip.}

\item{count}{Get the total number of results matching the query. Deactivated by default.}

\item{id}{Comma separated list family IDs up to a maximum of 100.}

\item{name}{Comma separated list family names up to a maximum of 100.}

\item{uuid}{Comma separated list family UUIDs up to a maximum of 100.}

\item{expectedSize}{Expected size of the family (number of members).}

\item{samples}{Comma separated list of member's samples.}

\item{phenotypes}{Comma separated list of phenotype ids or names.}

\item{disorders}{Comma separated list of disorder ids or names.}

\item{creationDate}{Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.}

\item{modificationDate}{Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.}

\item{internalStatus}{Filter by internal status.}

\item{status}{Filter by status.}

\item{annotation}{Annotation filters. Example: age>30;gender=FEMALE. For more information, please visit http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0.}

\item{acl}{Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators can query by this field. .}

\item{release}{Release when it was created.}

\item{snapshot}{Snapshot value (Latest version of the entry in the specified release).}

\item{member}{User or group id.}

\item{silent}{Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.}

\item{flattenAnnotations}{Flatten the annotations?.}

\item{version}{Comma separated list of family versions. 'all' to get all the family versions. Not supported if multiple family ids are provided.}

\item{deleted}{Boolean to retrieve deleted families.}

\item{include}{Fields included in the response, whole JSON path must be provided.}

\item{exclude}{Fields excluded in the response, whole JSON path must be provided.}

\item{updateRoles}{Update the member roles within the family.}

\item{annotationSetsAction}{Action to be performed if the array of annotationSets is being updated. Allowed values: \link{'ADD', 'SET', 'REMOVE'}}

\item{includeResult}{Flag indicating to include the created or updated document result in the response.}

\item{study}{Study [\link{user@}project:]study where study and project can be either the ID or UUID.}

\item{action}{Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some annotations; RESET to set some annotations to the default value configured in the corresponding variables of the VariableSet if any. Allowed values: \link{'ADD', 'SET', 'REMOVE', 'RESET', 'REPLACE'}}

\item{incVersion}{Create a new version of family.}

\item{data}{Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset' containing the comma separated variables that will be set to the default value when the action is RESET.}
}
\description{
This function implements the OpenCGA calls for managing Families.
The following table summarises the available \emph{actions} for this client:\tabular{llr}{
   endpointName \tab Endpoint WS \tab parameters accepted \cr
   updateAcl \tab /{apiVersion}/families/acl/{members}/update \tab study, members\link{*}, action\link{*}, propagate, body\link{*} \cr
   aggregationStats \tab /{apiVersion}/families/aggregationStats \tab study, creationYear, creationMonth, creationDay, creationDayOfWeek, status, phenotypes, release, version, numMembers, expectedSize, annotation, default, field \cr
   loadAnnotationSets \tab /{apiVersion}/families/annotationSets/load \tab study, variableSetId\link{*}, path\link{*}, parents, annotationSetId, body \cr
   create \tab /{apiVersion}/families/create \tab include, exclude, study, members, includeResult, body\link{*} \cr
   distinct \tab /{apiVersion}/families/distinct \tab study, id, name, uuid, members, expectedSize, samples, phenotypes, disorders, creationDate, modificationDate, deleted, internalStatus, status, annotation, acl, release, snapshot, field\link{*} \cr
   search \tab /{apiVersion}/families/search \tab include, exclude, limit, skip, count, flattenAnnotations, study, id, name, uuid, members, expectedSize, samples, phenotypes, disorders, creationDate, modificationDate, deleted, internalStatus, status, annotation, acl, release, snapshot \cr
   acl \tab /{apiVersion}/families/{families}/acl \tab families\link{*}, study, member, silent \cr
   delete \tab /{apiVersion}/families/{families}/delete \tab study, families\link{*} \cr
   info \tab /{apiVersion}/families/{families}/info \tab include, exclude, flattenAnnotations, families\link{*}, study, version, deleted \cr
   update \tab /{apiVersion}/families/{families}/update \tab include, exclude, families\link{*}, study, incVersion, updateRoles, annotationSetsAction, includeResult, body \cr
   updateAnnotationSetsAnnotations \tab /{apiVersion}/families/{family}/annotationSets/{annotationSet}/annotations/update \tab family\link{*}, study, annotationSet\link{*}, action, incVersion, body \cr
}
}
\section{Endpoint /{apiVersion}/families/acl/{members}/update}{

Update the set of permissions granted for the member.
}

\section{Endpoint /{apiVersion}/families/aggregationStats}{

Fetch catalog family stats.
}

\section{Endpoint /{apiVersion}/families/annotationSets/load}{

Load annotation sets from a TSV file.
}

\section{Endpoint /{apiVersion}/families/create}{

Create family and the individual objects if they do not exist.
}

\section{Endpoint /{apiVersion}/families/distinct}{

Family distinct method.
}

\section{Endpoint /{apiVersion}/families/search}{

Search families.
}

\section{Endpoint /{apiVersion}/families/{families}/acl}{

Returns the acl of the families. If member is provided, it will only return the acl for the member.
}

\section{Endpoint /{apiVersion}/families/{families}/delete}{

Delete existing families.
}

\section{Endpoint /{apiVersion}/families/{families}/info}{

Get family information.
}

\section{Endpoint /{apiVersion}/families/{families}/update}{

Update some family attributes.
}

\section{Endpoint /{apiVersion}/families/{family}/annotationSets/{annotationSet}/annotations/update}{

Update annotations from an annotationSet.
}

\seealso{
\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
\link{*}: Required parameter
}
