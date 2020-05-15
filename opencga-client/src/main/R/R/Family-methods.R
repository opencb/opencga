
################################################################################
#' FamilyClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing Family
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("familyClient", "OpencgaR", function(OpencgaR, members, annotationSet, families, family, action, params=NULL, ...) {
    category <- "families"
    switch(action,
        # Endpoint: /{apiVersion}/families/acl/{members}/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param members: Comma separated list of user or group ids.
        # @param data: JSON containing the parameters to add ACLs.
        updateAcl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=members, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/families/aggregationStats
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param creationYear: Creation year.
        # @param creationMonth: Creation month (JANUARY, FEBRUARY...).
        # @param creationDay: Creation day.
        # @param creationDayOfWeek: Creation day of week (MONDAY, TUESDAY...).
        # @param status: Status.
        # @param phenotypes: Phenotypes.
        # @param release: Release.
        # @param version: Version.
        # @param numMembers: Number of members.
        # @param expectedSize: Expected size.
        # @param annotation: Annotation, e.g: key1=value(,key2=value).
        # @param default: Calculate default stats.
        # @param field: List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1.
        aggregationStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="aggregationStats", params=params, httpMethod="GET", as.queryParam=NULL,
                ...),
        # Endpoint: /{apiVersion}/families/annotationSets/load
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param variableSetId: Variable set id or name.
        # @param path: Path where the TSV file is located in OpenCGA or where it should be located.
        # @param parents: Flag indicating whether to create parent directories if they don't exist (only when TSV file was not previously associated).
        # @param annotationSetId: Annotation set id. If not provided, variableSetId will be used.
        # @param data: JSON containing the 'content' of the TSV file if this has not yet been registered into OpenCGA.
        loadAnnotationSets=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL,
                subcategory="annotationSets", subcategoryId=NULL, action="load", params=params, httpMethod="POST",
                as.queryParam=c("variableSetId","path"), ...),
        # Endpoint: /{apiVersion}/families/create
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param members: Comma separated list of member ids to be associated to the created family.
        # @param data: JSON containing family information.
        create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/families/search
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param count: Get the total number of results matching the query. Deactivated by default.
        # @param flattenAnnotations: Flatten the annotations?.
        # @param study: Study [[user@]project:]study where study and project can be either the id or alias.
        # @param name: Family name.
        # @param parentalConsanguinity: Parental consanguinity.
        # @param members: Comma separated list of individual ids or names.
        # @param samples: Comma separated list sample IDs or UUIDs up to a maximum of 100.
        # @param phenotypes: Comma separated list of phenotype ids or names.
        # @param disorders: Comma separated list of disorder ids or names.
        # @param creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param deleted: Boolean to retrieve deleted families.
        # @param annotationsetName: DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}.
        # @param variableSet: DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}.
        # @param annotation: Annotation, e.g: key1=value(,key2=value).
        # @param acl: Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators can query by this field. .
        # @param release: Release value (Current release from the moment the families were first created).
        # @param snapshot: Snapshot value (Latest version of families in the specified release).
        search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="search", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/families/{families}/acl
        # @param families: Comma separated list of family IDs or names up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param member: User or group id.
        # @param silent: Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.
        acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=families, subcategory=NULL, subcategoryId=NULL,
                action="acl", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/families/{families}/delete
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param families: Comma separated list of family ids.
        delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=families, subcategory=NULL,
                subcategoryId=NULL, action="delete", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/families/{families}/info
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param flattenAnnotations: Flatten the annotations?.
        # @param families: Comma separated list of family IDs or names up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param version: Family version.
        # @param deleted: Boolean to retrieve deleted families.
        info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=families, subcategory=NULL,
                subcategoryId=NULL, action="info", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/families/{families}/update
        # @param families: Comma separated list of family ids.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param incVersion: Create a new version of family.
        # @param updateIndividualVersion: Update all the individual references from the family to point to their latest versions.
        # @param annotationSetsAction: Action to be performed if the array of annotationSets is being updated. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param data: body.
        update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=families, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/families/{family}/annotationSets/{annotationSet}/annotations/update
        # @param family: Family id.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param annotationSet: AnnotationSet id to be updated.
        # @param action: Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some annotations; RESET to set some annotations to the default value configured in the corresponding variables of the VariableSet if any. Allowed values: ['ADD', 'SET', 'REMOVE', 'RESET', 'REPLACE']
        # @param incVersion: Create a new version of family.
        # @param updateSampleVersion: Update all the individual references from the family to point to their latest versions.
        # @param data: Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset' containing the comma separated variables that will be set to the default value when the action is RESET.
        updateAnnotations=fetchOpenCGA(object=OpencgaR, category=category, categoryId=family,
                subcategory="annotationSets", subcategoryId=annotationSet, action="annotations/update", params=params,
                httpMethod="POST", as.queryParam=NULL, ...),
    )
})