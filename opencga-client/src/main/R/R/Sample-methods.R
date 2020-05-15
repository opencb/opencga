
################################################################################
#' SampleClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing Sample
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("sampleClient", "OpencgaR", function(OpencgaR, members, annotationSet, samples, sample, action, params=NULL, ...) {
    category <- "samples"
    switch(action,
        # Endpoint: /{apiVersion}/samples/acl/{members}/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param members: Comma separated list of user or group ids.
        # @param data: JSON containing the parameters to update the permissions. If propagate flag is set to true, it will propagate the permissions defined to the individuals that are associated to the matching samples.
        updateAcl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=members, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/samples/aggregationStats
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param source: Source.
        # @param creationYear: Creation year.
        # @param creationMonth: Creation month (JANUARY, FEBRUARY...).
        # @param creationDay: Creation day.
        # @param creationDayOfWeek: Creation day of week (MONDAY, TUESDAY...).
        # @param status: Status.
        # @param type: Type.
        # @param phenotypes: Phenotypes.
        # @param release: Release.
        # @param version: Version.
        # @param somatic: Somatic.
        # @param annotation: Annotation, e.g: key1=value(,key2=value).
        # @param default: Calculate default stats.
        # @param field: List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1.
        aggregationStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="aggregationStats", params=params, httpMethod="GET", as.queryParam=NULL,
                ...),
        # Endpoint: /{apiVersion}/samples/annotationSets/load
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param variableSetId: Variable set id or name.
        # @param path: Path where the TSV file is located in OpenCGA or where it should be located.
        # @param parents: Flag indicating whether to create parent directories if they don't exist (only when TSV file was not previously associated).
        # @param annotationSetId: Annotation set id. If not provided, variableSetId will be used.
        # @param data: JSON containing the 'content' of the TSV file if this has not yet been registered into OpenCGA.
        loadAnnotationSets=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL,
                subcategory="annotationSets", subcategoryId=NULL, action="load", params=params, httpMethod="POST",
                as.queryParam=c("variableSetId","path"), ...),
        # Endpoint: /{apiVersion}/samples/create
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param individual: DEPRECATED: It should be passed in the body.
        # @param data: JSON containing sample information.
        create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/samples/load
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param file: file.
        # @param variableSet: variableSet.
        load=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="load", params=params, httpMethod="GET", as.queryParam=c("file"), ...),
        # Endpoint: /{apiVersion}/samples/search
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param count: Get the total number of results matching the query. Deactivated by default.
        # @param includeIndividual: Include Individual object as an attribute.
        # @param flattenAnnotations: Flatten the annotations?.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param name: DEPRECATED: name.
        # @param somatic: somatic.
        # @param individual: Individual ID or UUID.
        # @param creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param deleted: Boolean to retrieve deleted samples.
        # @param phenotypes: Comma separated list of phenotype ids or names.
        # @param annotationsetName: DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}.
        # @param variableSet: DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}.
        # @param annotation: Annotation, e.g: key1=value(,key2=value).
        # @param acl: Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators can query by this field. .
        # @param attributes: Text attributes (Format: sex=male,age>20 ...).
        # @param nattributes: Numerical attributes (Format: sex=male,age>20 ...).
        # @param release: Release value (Current release from the moment the samples were first created).
        # @param snapshot: Snapshot value (Latest version of samples in the specified release).
        search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="search", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/samples/{samples}/acl
        # @param samples: Comma separated list sample IDs or UUIDs up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param member: User or group id.
        # @param silent: Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.
        acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=samples, subcategory=NULL, subcategoryId=NULL,
                action="acl", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/samples/{samples}/delete
        # @param force: Force the deletion of samples even if they are associated to files, individuals or cohorts.
        # @param emptyFilesAction: Action to be performed over files that were associated only to the sample to be deleted. Possible actions are NONE, TRASH, DELETE.
        # @param deleteEmptyCohorts: Boolean indicating if the cohorts associated only to the sample to be deleted should be also deleted.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param samples: Comma separated list sample IDs or UUIDs up to a maximum of 100.
        delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=samples, subcategory=NULL,
                subcategoryId=NULL, action="delete", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/samples/{samples}/info
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param includeIndividual: Include Individual object as an attribute.
        # @param flattenAnnotations: Flatten the annotations?.
        # @param samples: Comma separated list sample IDs or UUIDs up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param version: Sample version.
        # @param deleted: Boolean to retrieve deleted samples.
        info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=samples, subcategory=NULL, subcategoryId=NULL,
                action="info", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/samples/{samples}/update
        # @param samples: Comma separated list sample IDs or UUIDs up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param incVersion: Create a new version of sample.
        # @param annotationSetsAction: Action to be performed if the array of annotationSets is being updated. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param data: body.
        update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=samples, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/samples/{sample}/annotationSets/{annotationSet}/annotations/update
        # @param sample: Sample ID.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param annotationSet: AnnotationSet id to be updated.
        # @param action: Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some annotations; RESET to set some annotations to the default value configured in the corresponding variables of the VariableSet if any. Allowed values: ['ADD', 'SET', 'REMOVE', 'RESET', 'REPLACE']
        # @param incVersion: Create a new version of sample.
        # @param data: Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset' containing the comma separated variables that will be set to the default value when the action is RESET.
        updateAnnotations=fetchOpenCGA(object=OpencgaR, category=category, categoryId=sample,
                subcategory="annotationSets", subcategoryId=annotationSet, action="annotations/update", params=params,
                httpMethod="POST", as.queryParam=NULL, ...),
    )
})