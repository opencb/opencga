
################################################################################
#' StudyClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing Study
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("studyClient", "OpencgaR", function(OpencgaR, studies, variableSet, group, members, study, action, params=NULL, ...) {
    category <- "studies"
    switch(action,
        # Endpoint: /{apiVersion}/studies/acl/{members}/update
        # @param members: Comma separated list of user or group ids.
        # @param data: JSON containing the parameters to modify ACLs. 'template' could be either 'admin', 'analyst' or 'view_only'.
        updateAcl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=members, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/create
        # @param projectId: Deprecated: Project id.
        # @param project: Project [user@]project where project can be either the ID or the alias.
        # @param data: study.
        create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/search
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param count: Get the total number of results matching the query. Deactivated by default.
        # @param project: Project [user@]project where project can be either the ID or the alias.
        # @param name: Study name.
        # @param id: Study id.
        # @param alias: Study alias.
        # @param fqn: Study full qualified name.
        # @param creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param status: Status.
        # @param attributes: Attributes.
        # @param release: Release value.
        search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="search", params=params, httpMethod="GET", as.queryParam=c("project"), ...),
        # Endpoint: /{apiVersion}/studies/{studies}/acl
        # @param studies: Comma separated list of Studies [[user@]project:]study where study and project can be either the ID or UUID up to a maximum of 100.
        # @param member: User or group id.
        # @param silent: Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.
        acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=studies, subcategory=NULL, subcategoryId=NULL,
                action="acl", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{studies}/aggregationStats
        # @param studies: Comma separated list of studies [[user@]project:]study up to a maximum of 100.
        # @param default: Calculate default stats.
        # @param fileFields: List of file fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param individualFields: List of individual fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param familyFields: List of family fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param sampleFields: List of sample fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param cohortFields: List of cohort fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param jobFields: List of job fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        aggregationStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=studies, subcategory=NULL,
                subcategoryId=NULL, action="aggregationStats", params=params, httpMethod="GET", as.queryParam=NULL,
                ...),
        # Endpoint: /{apiVersion}/studies/{studies}/info
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param studies: Comma separated list of Studies [[user@]project:]study where study and project can be either the ID or UUID up to a maximum of 100.
        info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=studies, subcategory=NULL, subcategoryId=NULL,
                action="info", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{study}/groups
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param id: Group id. If provided, it will only fetch information for the provided group.
        # @param name: [DEPRECATED] Replaced by id.
        # @param silent: Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.
        groups=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory=NULL, subcategoryId=NULL,
                action="groups", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{study}/groups/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param action: Action to be performed: ADD or REMOVE a group. Allowed values: ['ADD', 'REMOVE']
        # @param data: JSON containing the parameters.
        updateGroups=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory="groups",
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{study}/groups/{group}/users/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param group: Group name.
        # @param action: Action to be performed: ADD, SET or REMOVE users to/from a group. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param data: JSON containing the parameters.
        updateUsers=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory="groups",
                subcategoryId=group, action="users/update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{study}/permissionRules
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param entity: Entity where the permission rules should be applied to.
        permissionRules=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory=NULL,
                subcategoryId=NULL, action="permissionRules", params=params, httpMethod="GET",
                as.queryParam=c("entity"), ...),
        # Endpoint: /{apiVersion}/studies/{study}/permissionRules/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param entity: Entity where the permission rules should be applied to.
        # @param action: Action to be performed: ADD to add a new permission rule; REMOVE to remove all permissions assigned by an existing permission rule (even if it overlaps any manual permission); REVERT to remove all permissions assigned by an existing permission rule (keep manual overlaps); NONE to remove an existing permission rule without removing any permissions that could have been assigned already by the permission rule. Allowed values: ['ADD', 'REMOVE', 'REVERT', 'NONE']
        # @param data: JSON containing the permission rule to be created or removed.
        updatePermissionRules=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study,
                subcategory="permissionRules", subcategoryId=NULL, action="update", params=params, httpMethod="POST",
                as.queryParam=c("entity"), ...),
        # Endpoint: /{apiVersion}/studies/{study}/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param data: JSON containing the params to be updated.
        update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory=NULL, subcategoryId=NULL,
                action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{study}/variableSets
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param id: Id of the variableSet to be retrieved. If no id is passed, it will show all the variableSets of the study.
        variableSets=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory=NULL,
                subcategoryId=NULL, action="variableSets", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{study}/variableSets/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param action: Action to be performed: ADD or REMOVE a variableSet. Allowed values: ['ADD', 'REMOVE']
        # @param data: JSON containing the VariableSet to be created or removed.
        updateVariableSets=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study,
                subcategory="variableSets", subcategoryId=NULL, action="update", params=params, httpMethod="POST",
                as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/studies/{study}/variableSets/{variableSet}/variables/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param variableSet: VariableSet id of the VariableSet to be updated.
        # @param action: Action to be performed: ADD or REMOVE a variable. Allowed values: ['ADD', 'REMOVE']
        # @param data: JSON containing the variable to be added or removed. For removing, only the variable id will be needed.
        updateVariables=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory="variableSets",
                subcategoryId=variableSet, action="variables/update", params=params, httpMethod="POST",
                as.queryParam=NULL, ...),
    )
})