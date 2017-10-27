# ################################################################################VariablesetClient methods
# @include commons.R
# 
# @description This function implements the OpenCGA calls for managing VariableSets
# @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
# where the connection and session details are stored
# @param variableSet a character string or vector containing clinical analysis IDs
# @param action action to be performed on the variableSet(s)
# @param params list containing additional query or body params
# @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
# \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}

# category <- "variableset"
# 
# setMethod("variablesetClient", "OpencgaR", function(OpencgaR, variableSet,
#                                                     action, params=NULL) {
#     switch(action,
#            search=fetchOpenCGA(object=OpencgaR, category=category, 
#                                action=action, params=params, httpMethod="GET"),
#            delete=fetchOpenCGA(object=OpencgaR, category=category, 
#                                categoryId=variableSet, action=action, 
#                                params=params, httpMethod="GET"),
#            info=fetchOpenCGA(object=OpencgaR, category=category, 
#                              categoryId=variableSet, action=action, 
#                              params=params, httpMethod="GET"),
#            summary=fetchOpenCGA(object=OpencgaR, category=category, 
#                                 categoryId=variableSet, action=action, 
#                                 params=params, httpMethod="GET"),
#            create=fetchOpenCGA(object=OpencgaR, category=category, 
#                                action=action, params=params, httpMethod="POST"),
#            update=fetchOpenCGA(object=OpencgaR, category=category, 
#                                categoryId=variableSet, action=action, 
#                                params=params, httpMethod="POST")
#     )
# })
# 
# setMethod("variablesetFieldClient", "OpencgaR", function(OpencgaR, variableSet, 
#                                                          action, params=NULL) {
#     switch(action,
#            delete=fetchOpenCGA(object=OpencgaR, category=category, 
#                                categoryId=variableSet, subcategory="field", 
#                                action=action, params=params, httpMethod="GET"),
#            rename=fetchOpenCGA(object=OpencgaR, category=category, 
#                                categoryId=variableSet, subcategory="field", 
#                                action=action, params=params, httpMethod="GET"),
#            add=fetchOpenCGA(object=OpencgaR, category=category, 
#                              categoryId=variableSet, action=action, 
#                              params=params, httpMethod="POST")
#     )
# })

