
# WARNING: AUTOGENERATED CODE
#
#    This code was generated by a tool.
#    Autogenerated on: 2021-02-25 14:31:51
#    
#    Manual changes to this file may cause unexpected behavior in your application.
#    Manual changes to this file will be overwritten if the code is regenerated.


# ##############################################################################
#' AdminClient methods
#' @include AllClasses.R
#' @include AllGenerics.R
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing Admin.

#' The following table summarises the available *actions* for this client:
#'
#' | endpointName | Endpoint WS | parameters accepted |
#' | -- | :-- | --: |
#' | groupByAudit | /{apiVersion}/admin/audit/groupBy | count, limit, fields[*], entity[*], action, before, after, date |
#' | indexStatsCatalog | /{apiVersion}/admin/catalog/indexStats | collection |
#' | installCatalog | /{apiVersion}/admin/catalog/install | body[*] |
#' | jwtCatalog | /{apiVersion}/admin/catalog/jwt | body[*] |
#' | createUsers | /{apiVersion}/admin/users/create | body[*] |
#' | importUsers | /{apiVersion}/admin/users/import | body[*] |
#' | syncUsers | /{apiVersion}/admin/users/sync | body[*] |
#'
#' @md
#' @seealso \url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
#' [*]: Required parameter
#' @export

setMethod("adminClient", "OpencgaR", function(OpencgaR, endpointName, params=NULL, ...) {
    switch(endpointName,

        #' @section Endpoint /{apiVersion}/admin/audit/groupBy:
        #' Group by operation.
        #' @param count Count the number of elements matching the group.
        #' @param limit Maximum number of documents (groups) to be returned.
        #' @param fields Comma separated list of fields by which to group by.
        #' @param entity Entity to be grouped by.
        #' @param action Action performed.
        #' @param before Object before update.
        #' @param after Object after update.
        #' @param date Date <,<=,>,>=(Format: yyyyMMddHHmmss) and yyyyMMddHHmmss-yyyyMMddHHmmss.
        groupByAudit=fetchOpenCGA(object=OpencgaR, category="admin", categoryId=NULL, subcategory="audit",
                subcategoryId=NULL, action="groupBy", params=params, httpMethod="GET",
                as.queryParam=c("fields","entity"), ...),

        #' @section Endpoint /{apiVersion}/admin/catalog/indexStats:
        #' Sync Catalog into the Solr.
        #' @param collection Collection to be indexed (file, sample, individual, family, cohort and/or job). If not provided, all of them will be indexed.
        indexStatsCatalog=fetchOpenCGA(object=OpencgaR, category="admin", categoryId=NULL, subcategory="catalog",
                subcategoryId=NULL, action="indexStats", params=params, httpMethod="POST", as.queryParam=NULL, ...),

        #' @section Endpoint /{apiVersion}/admin/catalog/install:
        #' Install OpenCGA database.
        #' @param data JSON containing the mandatory parameters.
        installCatalog=fetchOpenCGA(object=OpencgaR, category="admin", categoryId=NULL, subcategory="catalog",
                subcategoryId=NULL, action="install", params=params, httpMethod="POST", as.queryParam=NULL, ...),

        #' @section Endpoint /{apiVersion}/admin/catalog/jwt:
        #' Change JWT secret key.
        #' @param data JSON containing the parameters.
        jwtCatalog=fetchOpenCGA(object=OpencgaR, category="admin", categoryId=NULL, subcategory="catalog",
                subcategoryId=NULL, action="jwt", params=params, httpMethod="POST", as.queryParam=NULL, ...),

        #' @section Endpoint /{apiVersion}/admin/users/create:
        #' Create a new user.
        #' @param data JSON containing the parameters.
        createUsers=fetchOpenCGA(object=OpencgaR, category="admin", categoryId=NULL, subcategory="users",
                subcategoryId=NULL, action="create", params=params, httpMethod="POST", as.queryParam=NULL, ...),

        #' @section Endpoint /{apiVersion}/admin/users/import:
        #' Import users or a group of users from LDAP or AAD.
        #' @param data JSON containing the parameters.
        importUsers=fetchOpenCGA(object=OpencgaR, category="admin", categoryId=NULL, subcategory="users",
                subcategoryId=NULL, action="import", params=params, httpMethod="POST", as.queryParam=NULL, ...),

        #' @section Endpoint /{apiVersion}/admin/users/sync:
        #' Synchronise a group of users from an authentication origin with a group in a study from catalog.
        #' @param data JSON containing the parameters.
        syncUsers=fetchOpenCGA(object=OpencgaR, category="admin", categoryId=NULL, subcategory="users",
                subcategoryId=NULL, action="sync", params=params, httpMethod="POST", as.queryParam=NULL, ...),
    )
})