% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Panel-methods.R
\name{panelClient,OpencgaR-method}
\alias{panelClient,OpencgaR-method}
\title{PanelClient methods}
\usage{
\S4method{panelClient}{OpencgaR}(OpencgaR, members, panels, endpointName, params = NULL, ...)
}
\arguments{
\item{members}{Comma separated list of user or group ids.}

\item{panels}{Comma separated list of panel ids.}

\item{action}{Action to be performed \link{ADD, SET, REMOVE or RESET}.}

\item{source}{Comma separated list of sources to import panels from. Current supported sources are 'panelapp' and 'cancer-gene-census'.}

\item{field}{Field for which to obtain the distinct values.}

\item{limit}{Number of results to be returned.}

\item{skip}{Number of results to skip.}

\item{count}{Get the total number of results matching the query. Deactivated by default.}

\item{id}{Comma separated list of panel IDs  up to a maximum of 100.}

\item{uuid}{Comma separated list of panel UUIDs  up to a maximum of 100.}

\item{name}{Comma separated list of panel names  up to a maximum of 100.}

\item{disorders}{Comma separated list of disorder ids or names.}

\item{variants}{Comma separated list of variant ids.}

\item{genes}{Comma separated list of gene ids.}

\item{regions}{Comma separated list of regions.}

\item{categories}{Comma separated list of category names.}

\item{tags}{Panel tags.}

\item{status}{Filter by status.}

\item{creationDate}{Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.}

\item{modificationDate}{Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.}

\item{acl}{Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators can query by this field. .}

\item{release}{Release when it was created.}

\item{snapshot}{Snapshot value (Latest version of the entry in the specified release).}

\item{member}{User or group id.}

\item{silent}{Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.}

\item{version}{Comma separated list of panel versions. 'all' to get all the panel versions. Not supported if multiple panel ids are provided.}

\item{deleted}{Boolean to retrieve deleted panels.}

\item{include}{Fields included in the response, whole JSON path must be provided.}

\item{exclude}{Fields excluded in the response, whole JSON path must be provided.}

\item{study}{Study [\link{user@}project:]study where study and project can be either the ID or UUID.}

\item{incVersion}{Create a new version of panel.}

\item{includeResult}{Flag indicating to include the created or updated document result in the response.}

\item{data}{Panel parameters.}
}
\description{
This function implements the OpenCGA calls for managing Disease Panels.
The following table summarises the available \emph{actions} for this client:\tabular{llr}{
   endpointName \tab Endpoint WS \tab parameters accepted \cr
   updateAcl \tab /{apiVersion}/panels/acl/{members}/update \tab study, members\link{*}, action\link{*}, body\link{*} \cr
   create \tab /{apiVersion}/panels/create \tab include, exclude, study, source, id, includeResult, body \cr
   distinct \tab /{apiVersion}/panels/distinct \tab study, id, uuid, name, disorders, variants, genes, regions, categories, tags, deleted, status, creationDate, modificationDate, acl, release, snapshot, field\link{*} \cr
   search \tab /{apiVersion}/panels/search \tab include, exclude, limit, skip, count, study, id, uuid, name, disorders, variants, genes, regions, categories, tags, deleted, status, creationDate, modificationDate, acl, release, snapshot \cr
   acl \tab /{apiVersion}/panels/{panels}/acl \tab panels\link{*}, study, member, silent \cr
   delete \tab /{apiVersion}/panels/{panels}/delete \tab study, panels\link{*} \cr
   info \tab /{apiVersion}/panels/{panels}/info \tab include, exclude, panels\link{*}, study, version, deleted \cr
   update \tab /{apiVersion}/panels/{panels}/update \tab include, exclude, study, panels\link{*}, incVersion, includeResult, body \cr
}
}
\section{Endpoint /{apiVersion}/panels/acl/{members}/update}{

Update the set of permissions granted for the member.
}

\section{Endpoint /{apiVersion}/panels/create}{

Create a panel.
}

\section{Endpoint /{apiVersion}/panels/distinct}{

Panel distinct method.
}

\section{Endpoint /{apiVersion}/panels/search}{

Panel search.
}

\section{Endpoint /{apiVersion}/panels/{panels}/acl}{

Returns the acl of the panels. If member is provided, it will only return the acl for the member.
}

\section{Endpoint /{apiVersion}/panels/{panels}/delete}{

Delete existing panels.
}

\section{Endpoint /{apiVersion}/panels/{panels}/info}{

Panel info.
}

\section{Endpoint /{apiVersion}/panels/{panels}/update}{

Update panel attributes.
}

\seealso{
\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
\link{*}: Required parameter
}
