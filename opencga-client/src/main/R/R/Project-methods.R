
################################################################################
#' ProjectClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing Project
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("projectClient", "OpencgaR", function(OpencgaR, projects, project, action, params=NULL, ...) {
    category <- "projects"
    switch(action,
        # Endpoint: /{apiVersion}/projects/create
        # @param data: JSON containing the mandatory parameters.
        create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/projects/search
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param owner: Owner of the project.
        # @param id: Project [user@]project where project can be either the ID or the alias.
        # @param name: Project name.
        # @param fqn: Project fqn.
        # @param organization: Project organization.
        # @param description: Project description.
        # @param study: Study id.
        # @param creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
        # @param status: Status.
        # @param attributes: Attributes.
        search=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="search", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/projects/{projects}/aggregationStats
        # @param projects: Comma separated list of projects [user@]project up to a maximum of 100.
        # @param default: Calculate default stats.
        # @param fileFields: List of file fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param individualFields: List of individual fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param familyFields: List of family fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param sampleFields: List of sample fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param cohortFields: List of cohort fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        # @param jobFields: List of job fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type.
        aggregationStats=fetchOpenCGA(object=OpencgaR, category=category, categoryId=projects, subcategory=NULL,
                subcategoryId=NULL, action="aggregationStats", params=params, httpMethod="GET", as.queryParam=NULL,
                ...),
        # Endpoint: /{apiVersion}/projects/{projects}/info
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param projects: Comma separated list of projects [user@]project up to a maximum of 100.
        info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=projects, subcategory=NULL,
                subcategoryId=NULL, action="info", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/projects/{project}/incRelease
        # @param project: Project [user@]project where project can be either the ID or the alias.
        incRelease=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project, subcategory=NULL,
                subcategoryId=NULL, action="incRelease", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/projects/{project}/studies
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param project: Project [user@]project where project can be either the ID or the alias.
        studies=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project, subcategory=NULL,
                subcategoryId=NULL, action="studies", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/projects/{project}/update
        # @param project: Project [user@]project where project can be either the ID or the alias.
        # @param data: JSON containing the params to be updated. It will be only possible to update organism fields not previously defined.
        update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=project, subcategory=NULL,
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
    )
})