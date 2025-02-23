% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Admin-methods.R
\name{adminClient,OpencgaR-method}
\alias{adminClient,OpencgaR-method}
\title{AdminClient methods}
\usage{
\S4method{adminClient}{OpencgaR}(OpencgaR, user, endpointName, params = NULL, ...)
}
\arguments{
\item{user}{User ID.}

\item{fields}{Comma separated list of fields by which to group by.}

\item{entity}{Entity to be grouped by. Allowed values: \link{'AUDIT NOTE ORGANIZATION USER PROJECT STUDY FILE SAMPLE JOB INDIVIDUAL COHORT DISEASE_PANEL FAMILY CLINICAL_ANALYSIS INTERPRETATION VARIANT ALIGNMENT CLINICAL EXPRESSION RGA FUNCTIONAL'}}

\item{before}{Object before update.}

\item{after}{Object after update.}

\item{date}{Date <,<=,>,>=(Format: yyyyMMddHHmmss) and yyyyMMddHHmmss-yyyyMMddHHmmss.}

\item{study}{Study [\link{organization@}project:]study where study and project can be either the ID or UUID.}

\item{entryIds}{Comma separated list of entry ids.}

\item{permissions}{Comma separated list of permissions to be retrieved.}

\item{category}{Category corresponding to the id's provided.}

\item{include}{Fields included in the response, whole JSON path must be provided.}

\item{exclude}{Fields excluded in the response, whole JSON path must be provided.}

\item{limit}{Number of results to be returned.}

\item{skip}{Number of results to skip.}

\item{count}{Get the total number of results matching the query. Deactivated by default.}

\item{authenticationId}{Authentication origin ID.}

\item{organization}{Organization id.}

\item{action}{Action to be performed: ADD or REMOVE user to/from groups. Allowed values: \link{'ADD REMOVE'}}

\item{data}{JSON containing the parameters.}
}
\description{
This function implements the OpenCGA calls for managing Admin.
The following table summarises the available \emph{actions} for this client:\tabular{llr}{
   endpointName \tab Endpoint WS \tab parameters accepted \cr
   groupByAudit \tab /{apiVersion}/admin/audit/groupBy \tab count, limit, fields\link{*}, entity\link{*}, action, before, after, date \cr
   installCatalog \tab /{apiVersion}/admin/catalog/install \tab body\link{*} \cr
   jwtCatalog \tab /{apiVersion}/admin/catalog/jwt \tab organization, body\link{*} \cr
   createUsers \tab /{apiVersion}/admin/users/create \tab body\link{*} \cr
   importUsers \tab /{apiVersion}/admin/users/import \tab organization, body\link{*} \cr
   permissionsUsers \tab /{apiVersion}/admin/users/permissions \tab study, entryIds, permissions, category \cr
   searchUsers \tab /{apiVersion}/admin/users/search \tab include, exclude, limit, skip, count, organization, user, authenticationId \cr
   syncUsers \tab /{apiVersion}/admin/users/sync \tab organization, body\link{*} \cr
   usersUpdateGroups \tab /{apiVersion}/admin/users/{user}/groups/update \tab organization, user\link{*}, action, body\link{*} \cr
}
}
\section{Endpoint /{apiVersion}/admin/audit/groupBy}{

Group by operation.
}

\section{Endpoint /{apiVersion}/admin/catalog/install}{

Install OpenCGA database.
}

\section{Endpoint /{apiVersion}/admin/catalog/jwt}{

Change JWT secret key.
}

\section{Endpoint /{apiVersion}/admin/users/create}{

Create a new user.
}

\section{Endpoint /{apiVersion}/admin/users/import}{

Import users or a group of users from LDAP or AAD.
}

\section{Endpoint /{apiVersion}/admin/users/permissions}{

User permissions.
}

\section{Endpoint /{apiVersion}/admin/users/search}{

User search method.
}

\section{Endpoint /{apiVersion}/admin/users/sync}{

Synchronise a group of users from an authentication origin with a group in a study from catalog.
}

\section{Endpoint /{apiVersion}/admin/users/{user}/groups/update}{

Add or remove users from existing groups.
}

\seealso{
\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
\link{*}: Required parameter
}
