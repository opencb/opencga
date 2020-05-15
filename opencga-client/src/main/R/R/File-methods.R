
################################################################################
#' FileClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing File
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("fileClient", "OpencgaR", function(OpencgaR, annotationSet, file, files, members, folder, action, params=NULL, ...) {
    category <- "files"
    switch(action,
        # Endpoint: /{apiVersion}/files/acl/{members}/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param members: Comma separated list of user or group ids.
        # @param data: JSON containing the parameters to add ACLs.
        updateAcl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=members, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/aggregationStats
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param name: Name.
        # @param type: Type.
        # @param format: Format.
        # @param bioformat: Bioformat.
        # @param creationYear: Creation year.
        # @param creationMonth: Creation month (JANUARY, FEBRUARY...).
        # @param creationDay: Creation day.
        # @param creationDayOfWeek: Creation day of week (MONDAY, TUESDAY...).
        # @param status: Status.
        # @param release: Release.
        # @param external: External.
        # @param size: Size.
        # @param software: Software.
        # @param experiment: Experiment.
        # @param numSamples: Number of samples.
        # @param numRelatedFiles: Number of related files.
        # @param annotation: Annotation, e.g: key1=value(,key2=value).
        # @param default: Calculate default stats.
        # @param field: List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1.
        aggregationStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="aggregationStats", params=params, httpMethod="GET", as.queryParam=NULL,
                ...),
        # Endpoint: /{apiVersion}/files/annotationSets/load
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param variableSetId: Variable set id or name.
        # @param path: Path where the TSV file is located in OpenCGA or where it should be located.
        # @param parents: Flag indicating whether to create parent directories if they don't exist (only when TSV file was not previously associated).
        # @param annotationSetId: Annotation set id. If not provided, variableSetId will be used.
        # @param data: JSON containing the 'content' of the TSV file if this has not yet been registered into OpenCGA.
        loadAnnotationSets=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL,
                subcategory="annotationSets", subcategoryId=NULL, action="load", params=params, httpMethod="POST",
                as.queryParam=c("variableSetId","path"), ...),
        # Endpoint: /{apiVersion}/files/bioformats

        bioformats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="bioformats", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/create
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param data: File parameters.
        create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/fetch
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param data: Fetch parameters.
        fetch=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="fetch", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/formats

        formats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="formats", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/link
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param parents: Create the parent directories if they do not exist.
        # @param data: File parameters.
        link=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="link", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/search
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param count: Get the total number of results matching the query. Deactivated by default.
        # @param flattenAnnotations: Boolean indicating to flatten the annotations.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param name: Comma separated list of file names.
        # @param path: Comma separated list of paths.
        # @param type: File type, either FILE or DIRECTORY.
        # @param bioformat: Comma separated Bioformat values. For existing Bioformats see files/bioformats.
        # @param format: Comma separated Format values. For existing Formats see files/formats.
        # @param status: File status.
        # @param directory: Directory under which we want to look for files or folders.
        # @param creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param description: Description.
        # @param tags: Tags.
        # @param size: File size.
        # @param samples: Comma separated list sample IDs or UUIDs up to a maximum of 100.
        # @param jobId: Job id that created the file(s) or folder(s).
        # @param annotation: Annotation, e.g: key1=value(,key2=value).
        # @param acl: Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators can query by this field. .
        # @param deleted: Boolean to retrieve only deleted files.
        # @param attributes: Text attributes (Format: sex=male,age>20 ...).
        # @param release: Release when the file was registered.
        search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="search", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/upload

        upload=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="upload", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{files}/acl
        # @param files: Comma separated list of file ids or names up to a maximum of 100.
        # @param study: Comma separated list of Studies [[user@]project:]study where study and project can be either the ID or UUID up to a maximum of 100.
        # @param member: User or group id.
        # @param silent: Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.
        acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=files, subcategory=NULL, subcategoryId=NULL,
                action="acl", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{files}/delete
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param files: Comma separated list of file ids, names or paths.
        # @param skipTrash: Skip trash and delete the files/folders from disk directly (CANNOT BE RECOVERED).
        delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=files, subcategory=NULL, subcategoryId=NULL,
                action="delete", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{files}/info
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param flattenAnnotations: Flatten the annotations?.
        # @param files: Comma separated list of file ids or names up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param deleted: Boolean to retrieve deleted files.
        info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=files, subcategory=NULL, subcategoryId=NULL,
                action="info", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{files}/unlink
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param files: Comma separated list of file ids, names or paths.
        unlink=fetchOpenCGA(object=OpencgaR, category=category, categoryId=files, subcategory=NULL, subcategoryId=NULL,
                action="unlink", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{files}/update
        # @param files: Comma separated list of file ids, names or paths. Paths must be separated by : instead of /.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param samplesAction: Action to be performed if the array of samples is being updated. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param annotationSetsAction: Action to be performed if the array of annotationSets is being updated. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param relatedFilesAction: Action to be performed if the array of relatedFiles is being updated. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param tagsAction: Action to be performed if the array of tags is being updated. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param data: Parameters to modify.
        update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=files, subcategory=NULL, subcategoryId=NULL,
                action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{file}/annotationSets/{annotationSet}/annotations/update
        # @param file: File id, name or path. Paths must be separated by : instead of /.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param annotationSet: AnnotationSet id to be updated.
        # @param action: Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some annotations; RESET to set some annotations to the default value configured in the corresponding variables of the VariableSet if any. Allowed values: ['ADD', 'SET', 'REMOVE']
        # @param data: Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset' containing the comma separated variables that will be set to the default value when the action is RESET.
        updateAnnotations=fetchOpenCGA(object=OpencgaR, category=category, categoryId=file,
                subcategory="annotationSets", subcategoryId=annotationSet, action="annotations/update", params=params,
                httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{file}/download
        # @param file: File id, name or path. Paths must be separated by : instead of /.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        download=fetchOpenCGA(object=OpencgaR, category=category, categoryId=file, subcategory=NULL,
                subcategoryId=NULL, action="download", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{file}/grep
        # @param file: File uuid, id, or name.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param pattern: String pattern.
        # @param ignoreCase: Flag to perform a case insensitive search.
        # @param maxCount: Stop reading a file after 'n' matching lines. 0 means no limit.
        grep=fetchOpenCGA(object=OpencgaR, category=category, categoryId=file, subcategory=NULL, subcategoryId=NULL,
                action="grep", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{file}/head
        # @param file: File uuid, id, or name.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param offset: Starting byte from which the file will be read.
        # @param lines: Maximum number of lines to be returned.
        head=fetchOpenCGA(object=OpencgaR, category=category, categoryId=file, subcategory=NULL, subcategoryId=NULL,
                action="head", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{file}/image
        # @param file: File ID.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        image=fetchOpenCGA(object=OpencgaR, category=category, categoryId=file, subcategory=NULL, subcategoryId=NULL,
                action="image", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{file}/refresh
        # @param file: File id, name or path. Paths must be separated by : instead of /.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        refresh=fetchOpenCGA(object=OpencgaR, category=category, categoryId=file, subcategory=NULL, subcategoryId=NULL,
                action="refresh", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{file}/tail
        # @param file: File uuid, id, or name.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param lines: Maximum number of lines to be returned.
        tail=fetchOpenCGA(object=OpencgaR, category=category, categoryId=file, subcategory=NULL, subcategoryId=NULL,
                action="tail", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{folder}/list
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param count: Get the total number of results matching the query. Deactivated by default.
        # @param folder: Folder id, name or path.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        list=fetchOpenCGA(object=OpencgaR, category=category, categoryId=folder, subcategory=NULL, subcategoryId=NULL,
                action="list", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/files/{folder}/tree
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: [PENDING] Number of results to be returned.
        # @param folder: Folder id, name or path. Paths must be separated by : instead of /.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param maxDepth: Maximum depth to get files from.
        tree=fetchOpenCGA(object=OpencgaR, category=category, categoryId=folder, subcategory=NULL, subcategoryId=NULL,
                action="tree", params=params, httpMethod="GET", as.queryParam=NULL, ...),
    )
})