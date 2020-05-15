
################################################################################
#' UserClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing User
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("userClient", "OpencgaR", function(OpencgaR, user, filterId, action, params=NULL, ...) {
    category <- "users"
    switch(action,
        # Endpoint: /{apiVersion}/users/create
        # @param data: JSON containing the parameters.
        create=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/login
        # @param data: JSON containing the authentication parameters.
        login=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL, subcategoryId=NULL,
                action="login", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/password
        # @param data: JSON containing the change of password parameters.
        password=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="password", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/configs
        # @param user: User id.
        # @param name: Unique name (typically the name of the application).
        configs=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory=NULL, subcategoryId=NULL,
                action="configs", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/configs/update
        # @param user: User id.
        # @param action: Action to be performed: ADD or REMOVE a group. Allowed values: ['ADD', 'REMOVE']
        # @param data: JSON containing anything useful for the application such as user or default preferences. When removing, only the id will be necessary.
        updateConfigs=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory="configs",
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/filters
        # @param user: User id.
        # @param id: Filter id. If provided, it will only fetch the specified filter.
        filters=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory=NULL, subcategoryId=NULL,
                action="filters", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/filters/update
        # @param user: User id.
        # @param action: Action to be performed: ADD or REMOVE a group. Allowed values: ['ADD', 'REMOVE']
        # @param data: Filter parameters. When removing, only the 'name' of the filter will be necessary.
        updateFilters=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory="filters",
                subcategoryId=NULL, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/filters/{filterId}/update
        # @param user: User id.
        # @param filterId: Filter id.
        # @param data: Filter parameters.
        updateFilter=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory="filters",
                subcategoryId=filterId, action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/info
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param user: User id.
        info=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory=NULL, subcategoryId=NULL,
                action="info", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/projects
        # @param include: Fields included in the response, whole JSON path must be provided.
        # @param exclude: Fields excluded in the response, whole JSON path must be provided.
        # @param limit: Number of results to be returned.
        # @param skip: Number of results to skip.
        # @param user: User id.
        projects=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory=NULL,
                subcategoryId=NULL, action="projects", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/users/{user}/update
        # @param user: User id.
        # @param data: JSON containing the params to be updated.
        update=fetchOpenCGA(object=OpencgaR, category=category, categoryId=user, subcategory=NULL, subcategoryId=NULL,
                action="update", params=params, httpMethod="POST", as.queryParam=NULL, ...),
    )
})