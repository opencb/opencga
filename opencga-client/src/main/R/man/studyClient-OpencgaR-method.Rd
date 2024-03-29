% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Study-methods.R
\name{studyClient,OpencgaR-method}
\alias{studyClient,OpencgaR-method}
\title{StudyClient methods}
\usage{
\S4method{studyClient}{OpencgaR}(
  OpencgaR,
  members,
  study,
  variableSet,
  group,
  studies,
  templateId,
  endpointName,
  params = NULL,
  ...
)
}
\arguments{
\item{members}{Comma separated list of user or group ids.}

\item{study}{Study [\link{user@}project:]study where study and project can be either the ID or UUID.}

\item{variableSet}{VariableSet id of the VariableSet to be updated.}

\item{group}{Group name.}

\item{studies}{Comma separated list of Studies [\link{user@}project:]study where study and project can be either the ID or UUID up to a maximum of 100.}

\item{templateId}{Template id.}

\item{project}{Project \link{user@}project where project can be either the ID or the alias.}

\item{name}{Study name.}

\item{alias}{Study alias.}

\item{fqn}{Study full qualified name.}

\item{creationDate}{Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.}

\item{modificationDate}{Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.}

\item{internalStatus}{Filter by internal status.}

\item{attributes}{Attributes.}

\item{release}{Release value.}

\item{member}{User or group id.}

\item{default}{Calculate default stats.}

\item{fileFields}{List of file fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.}

\item{individualFields}{List of individual fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.}

\item{familyFields}{List of family fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.}

\item{sampleFields}{List of sample fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.}

\item{cohortFields}{List of cohort fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.}

\item{jobFields}{List of job fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.}

\item{limit}{Number of results to be returned.}

\item{skip}{Number of results to skip.}

\item{count}{Get the total number of results matching the query. Deactivated by default.}

\item{operationId}{Audit operation UUID.}

\item{userId}{User ID.}

\item{resource}{Resource involved.}

\item{resourceId}{Resource ID.}

\item{resourceUuid}{resource UUID.}

\item{status}{Filter by status.}

\item{date}{Date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.}

\item{silent}{Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.}

\item{entity}{Entity where the permission rules should be applied to.}

\item{jobId}{Job ID. It must be a unique string within the study. An ID will be autogenerated automatically if not provided.}

\item{jobDependsOn}{Comma separated list of existing job IDs the job will depend on.}

\item{jobDescription}{Job description.}

\item{jobTags}{Job tags.}

\item{file}{File to upload.}

\item{include}{Fields included in the response, whole JSON path must be provided.}

\item{exclude}{Fields excluded in the response, whole JSON path must be provided.}

\item{includeResult}{Flag indicating to include the created or updated document result in the response.}

\item{id}{Id of the variableSet to be retrieved. If no id is passed, it will show all the variableSets of the study.}

\item{action}{Action to be performed: ADD or REMOVE a variable. Allowed values: \link{'ADD', 'REMOVE'}}

\item{data}{JSON containing the variable to be added or removed. For removing, only the variable id will be needed.}
}
\description{
This function implements the OpenCGA calls for managing Studies.
The following table summarises the available \emph{actions} for this client:\tabular{llr}{
   endpointName \tab Endpoint WS \tab parameters accepted \cr
   updateAcl \tab /{apiVersion}/studies/acl/{members}/update \tab members\link{*}, action\link{*}, body\link{*} \cr
   create \tab /{apiVersion}/studies/create \tab include, exclude, project, includeResult, body\link{*} \cr
   search \tab /{apiVersion}/studies/search \tab include, exclude, limit, skip, count, project\link{*}, name, id, alias, fqn, creationDate, modificationDate, internalStatus, status, attributes, release \cr
   acl \tab /{apiVersion}/studies/{studies}/acl \tab studies\link{*}, member, silent \cr
   aggregationStats \tab /{apiVersion}/studies/{studies}/aggregationStats \tab studies\link{*}, default, fileFields, individualFields, familyFields, sampleFields, cohortFields, jobFields \cr
   info \tab /{apiVersion}/studies/{studies}/info \tab include, exclude, studies\link{*} \cr
   searchAudit \tab /{apiVersion}/studies/{study}/audit/search \tab include, exclude, limit, skip, count, study\link{*}, operationId, userId, action, resource, resourceId, resourceUuid, status, date \cr
   groups \tab /{apiVersion}/studies/{study}/groups \tab study\link{*}, id, silent \cr
   updateGroups \tab /{apiVersion}/studies/{study}/groups/update \tab study\link{*}, action, body\link{*} \cr
   updateGroupsUsers \tab /{apiVersion}/studies/{study}/groups/{group}/users/update \tab study\link{*}, group\link{*}, action, body\link{*} \cr
   permissionRules \tab /{apiVersion}/studies/{study}/permissionRules \tab study\link{*}, entity\link{*} \cr
   updatePermissionRules \tab /{apiVersion}/studies/{study}/permissionRules/update \tab study\link{*}, entity\link{*}, action, body\link{*} \cr
   runTemplates \tab /{apiVersion}/studies/{study}/templates/run \tab study\link{*}, jobId, jobDependsOn, jobDescription, jobTags, body\link{*} \cr
   uploadTemplates \tab /{apiVersion}/studies/{study}/templates/upload \tab file, study\link{*} \cr
   deleteTemplates \tab /{apiVersion}/studies/{study}/templates/{templateId}/delete \tab study, templateId\link{*} \cr
   update \tab /{apiVersion}/studies/{study}/update \tab include, exclude, study\link{*}, includeResult, body\link{*} \cr
   variableSets \tab /{apiVersion}/studies/{study}/variableSets \tab study\link{*}, id \cr
   updateVariableSets \tab /{apiVersion}/studies/{study}/variableSets/update \tab study\link{*}, action, body\link{*} \cr
   updateVariableSetsVariables \tab /{apiVersion}/studies/{study}/variableSets/{variableSet}/variables/update \tab study\link{*}, variableSet\link{*}, action, body\link{*} \cr
}
}
\section{Endpoint /{apiVersion}/studies/acl/{members}/update}{

Update the set of permissions granted for the member.
}

\section{Endpoint /{apiVersion}/studies/create}{

Create a new study.
}

\section{Endpoint /{apiVersion}/studies/search}{

Search studies.
}

\section{Endpoint /{apiVersion}/studies/{studies}/acl}{

Return the acl of the study. If member is provided, it will only return the acl for the member.
}

\section{Endpoint /{apiVersion}/studies/{studies}/aggregationStats}{

Fetch catalog study stats.
}

\section{Endpoint /{apiVersion}/studies/{studies}/info}{

Fetch study information.
}

\section{Endpoint /{apiVersion}/studies/{study}/audit/search}{

Search audit collection.
}

\section{Endpoint /{apiVersion}/studies/{study}/groups}{

Return the groups present in the study. For owners and administrators only.
}

\section{Endpoint /{apiVersion}/studies/{study}/groups/update}{

Add or remove a group.
}

\section{Endpoint /{apiVersion}/studies/{study}/groups/{group}/users/update}{

Add, set or remove users from an existing group.
}

\section{Endpoint /{apiVersion}/studies/{study}/permissionRules}{

Fetch permission rules.
}

\section{Endpoint /{apiVersion}/studies/{study}/permissionRules/update}{

Add or remove a permission rule.
}

\section{Endpoint /{apiVersion}/studies/{study}/templates/run}{

Execute template.
}

\section{Endpoint /{apiVersion}/studies/{study}/templates/upload}{

Resource to upload a zipped template.
}

\section{Endpoint /{apiVersion}/studies/{study}/templates/{templateId}/delete}{

Delete template.
}

\section{Endpoint /{apiVersion}/studies/{study}/update}{

Update some study attributes.
}

\section{Endpoint /{apiVersion}/studies/{study}/variableSets}{

Fetch variableSets from a study.
}

\section{Endpoint /{apiVersion}/studies/{study}/variableSets/update}{

Add or remove a variableSet.
}

\section{Endpoint /{apiVersion}/studies/{study}/variableSets/{variableSet}/variables/update}{

Add or remove variables to a VariableSet.
}

\seealso{
\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
\link{*}: Required parameter
}
