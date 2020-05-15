
################################################################################
#' PanelClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing Panel
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("panelClient", "OpencgaR", function(OpencgaR, members, panels, action, params=NULL, ...) {
    category <- "panels"
    switch(action,
        # Endpoint: /{apiVersion}/panels/acl/{members}/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param members: Comma separated list of user or group ids.
        # @param data: JSON containing the parameters to update the permissions.
        updateAcl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=members, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/panels/create
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param source: Comma separated list of sources to import panels from. Current supported sources are 'panelapp' and 'genecensus'.
        # @param id: Comma separated list of panel ids to be imported from the defined source.If 'source' is provided and 'id' is empty, it will import all the panels from the source. When 'id' is provided, only one 'source' will be allowed.
        # @param data: Panel parameters.
        create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/panels/search
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param count: Get the total number of results matching the query. Deactivated by default.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param name: Panel name.
        # @param phenotypes: Panel phenotypes.
        # @param variants: Panel variants.
        # @param genes: Panel genes.
        # @param regions: Panel regions.
        # @param categories: Panel categories.
        # @param tags: Panel tags.
        # @param description: Panel description.
        # @param author: Panel author.
        # @param deleted: Boolean to retrieve deleted panels.
        # @param creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param acl: Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators can query by this field. .
        # @param release: Release value (Current release from the moment the samples were first created).
        # @param snapshot: Snapshot value (Latest version of samples in the specified release).
        search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="search", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/panels/{panels}/acl
        # @param panels: Comma separated list of panel ids up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param member: User or group id.
        # @param silent: Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason.
        acl=fetchOpenCGA(object=OpencgaR, category=category, categoryId=panels, subcategory=NULL, subcategoryId=NULL,
                action="acl", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/panels/{panels}/delete
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param panels: Comma separated list of panel ids.
        delete=fetchOpenCGA(object=OpencgaR, category=category, categoryId=panels, subcategory=NULL,
                subcategoryId=NULL, action="delete", params=params, httpMethod="DELETE", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/panels/{panels}/info
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param panels: Comma separated list of panel ids up to a maximum of 100.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param version: Panel  version.
        # @param deleted: Boolean to retrieve deleted panels.
        info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=panels, subcategory=NULL, subcategoryId=NULL,
                action="info", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/panels/{panels}/update
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param panels: Comma separated list of panel ids.
        # @param incVersion: Create a new version of panel.
        # @param data: Panel parameters.
        update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=panels, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
    )
})