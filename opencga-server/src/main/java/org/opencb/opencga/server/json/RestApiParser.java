package org.opencb.opencga.server.json;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.server.json.beans.Category;
import org.opencb.opencga.server.json.beans.Endpoint;
import org.opencb.opencga.server.json.beans.Parameter;
import org.opencb.opencga.server.json.beans.RestApi;
import org.opencb.opencga.server.json.utils.CommandLineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

public class RestApiParser {

    private static Logger logger = LoggerFactory.getLogger(RestApiParser.class);

    public static RestApi getApi(List<Class> classes) {
        RestApi res = new RestApi();
        res.setCategories(getCategories(classes));
        return res;
    }

    public static List<LinkedHashMap<String, Object>> getHelp(List<Class> classes) {
        List<LinkedHashMap<String, Object>> res = new ArrayList<>();
        for (Class clazz : classes) {
            res.add(getHelp(clazz));
        }
        return res;
    }

    public static List<Category> getCategories(List<Class> classes) {
        List<Category> res = new ArrayList<>();
        for (Class clazz : classes) {
            res.add(getCategory(clazz));
        }
        return res;
    }

    private static Category getCategory(Class clazz) {
        Category category = new Category();

        Map<String, String> fieldNames = new HashMap<>();
        category.setName(((Api) clazz.getAnnotation(Api.class)).value());
        category.setPath(((Path) clazz.getAnnotation(Path.class)).value());
        //System.out.println("CATEGORY : " + map.get("name"));

        String category_name = category.getName().toUpperCase() + "_";
        List<Endpoint> endpoints = new ArrayList<>();
        for (Method method : clazz.getMethods()) {
            Path pathAnnotation = method.getAnnotation(Path.class);
            String httpMethod = "GET";
            if (method.getAnnotation(POST.class) != null) {
                httpMethod = "POST";
            } else {
                if (method.getAnnotation(DELETE.class) != null) {
                    httpMethod = "DELETE";
                }
            }

            ApiOperation apiOperationAnnotation = method.getAnnotation(ApiOperation.class);
            if (pathAnnotation != null && apiOperationAnnotation != null && !apiOperationAnnotation.hidden()) {
                String path = pathAnnotation.value();
                String variablePrefix = category_name + getMethodName(path).toUpperCase() + "_";
                Endpoint endpoint = new Endpoint();
                endpoint.setMethod(httpMethod);
                endpoint.setPath(category.getPath() + pathAnnotation.value());
                endpoint.setResponse(StringUtils.substringAfterLast(apiOperationAnnotation.response().getName().replace("Void", ""),
                        "."));
                String responseClass = apiOperationAnnotation.response().getName().replace("Void", "");
                endpoint.setResponseClass(responseClass.endsWith(";") ? responseClass : responseClass + ";");
                endpoint.setNotes(apiOperationAnnotation.notes());
                endpoint.setDescription(apiOperationAnnotation.value());

                ApiImplicitParams apiImplicitParams = method.getAnnotation(ApiImplicitParams.class);
                List<Parameter> parameters = new ArrayList<>();
                if (apiImplicitParams != null) {
                    for (ApiImplicitParam apiImplicitParam : apiImplicitParams.value()) {
                        Parameter parameter = new Parameter();
                        parameter.setName(apiImplicitParam.name());
                        parameter.setParam(apiImplicitParam.paramType());
                        parameter.setType(apiImplicitParam.dataType());
                        parameter.setTypeClass("java.lang." + StringUtils.capitalize(apiImplicitParam.dataType()));
                        parameter.setAllowedValues(apiImplicitParam.allowableValues());
                        parameter.setRequired(apiImplicitParam.required());
                        parameter.setDefaultValue(apiImplicitParam.defaultValue());
                        parameter.setDescription(apiImplicitParam.value());
                        parameters.add(parameter);
                    }
                }

                java.lang.reflect.Parameter[] methodParameters = method.getParameters();
                if (methodParameters != null) {
                    for (java.lang.reflect.Parameter methodParameter : methodParameters) {
                        ApiParam apiParam = methodParameter.getAnnotation(ApiParam.class);
                        if (apiParam != null && !apiParam.hidden()) {
                            List<Parameter> bodyParams = new ArrayList<>();
                            Parameter parameter = new Parameter();
                            if (methodParameter.getAnnotation(PathParam.class) != null) {
                                parameter.setName(methodParameter.getAnnotation(PathParam.class).value());
                                parameter.setParam("path");
                            } else {
                                if (methodParameter.getAnnotation(QueryParam.class) != null) {
                                    parameter.setName(methodParameter.getAnnotation(QueryParam.class).value());
                                    parameter.setParam("query");
                                } else {
                                    parameter.setName("body");
                                    parameter.setParam("body");
                                }
                            }

                            // Get type in lower case except for 'body' param
                            String type = methodParameter.getType().getName();
                            String typeClass = type;
                            if (typeClass.contains(".")) {
                                String[] split = typeClass.split("\\.");
                                type = split[split.length - 1];
                                if (!parameter.getParam().equals("body")) {
                                    type = type.toLowerCase();

                                    // Complex type different from body are enums
                                    if (type.contains("$")) {
                                        type = "enum";
                                    }
                                } else {
                                    type = "object";
                                    try {
                                        Class<?> aClass = Class.forName(typeClass);
                                        for (Field declaredField : aClass.getDeclaredFields()) {
                                            int modifiers = declaredField.getModifiers();
                                            // Ignore non-private or static fields
                                            if (Modifier.isPrivate(modifiers) && !Modifier.isStatic(modifiers)) {
                                                Parameter innerParam = new Parameter();
                                                innerParam.setName(declaredField.getName());
                                                innerParam.setParam("typeClass");
                                                innerParam.setType(declaredField.getType().getSimpleName());
                                                innerParam.setTypeClass(declaredField.getType().getName() + ";");
                                                innerParam.setAllowedValues("");
                                                innerParam.setRequired(false);
                                                innerParam.setDefaultValue("");
                                                innerParam.setComplex(!CommandLineUtils.isPrimitiveType(declaredField.getType().getSimpleName()));

                                                if (innerParam.isList()) {
                                                    innerParam.setGenericType(declaredField.getGenericType().getTypeName());
                                                } else {
                                                    if (innerParam.isComplex()) {
                                                        //System.out.println("Type: " + innerParam.getType());
                                                        System.out.println(innerParam.getName() + " TypeClass: " + innerParam.getTypeClass());
                                                    }
                                                }

                                                String fieldName = variablePrefix
                                                        + declaredField.getName().toUpperCase();
                                                fieldName = normalize(fieldName);
                                                String des = getDescriptionField(fieldName);
                                                if (StringUtils.isNotEmpty(des)) {
                                                    innerParam.setDescription(des);
                                                } else {
                                                    innerParam.setDescription(
                                                            "The body web service " + declaredField.getName() + " "
                                                                    + "parameter");
                                                }
                                                bodyParams.add(innerParam);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        logger.error("Error processing: " + typeClass);
                                    }
                                }
                            }
                            parameter.setType(type);
                            parameter.setTypeClass(typeClass.endsWith(";") ? typeClass : typeClass + ";");
                            parameter.setAllowedValues(apiParam.allowableValues());
                            parameter.setRequired(apiParam.required());
                            parameter.setDefaultValue(apiParam.defaultValue());
                            parameter.setDescription(apiParam.value());
                            if (!bodyParams.isEmpty()) {
                                parameter.setData(bodyParams);
                            }
                            parameters.add(parameter);
                        }
                    }
                }
                endpoint.setParameters(parameters);
                endpoints.add(endpoint);
            }
        }

        endpoints.sort(Comparator.comparing(endpoint -> (String) endpoint.getPath()));
        category.setEndpoints(endpoints);
        return category;
    }

    @Deprecated
    private static LinkedHashMap<String, Object> getHelp(Class clazz) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        Map<String, String> fieldNames = new HashMap<>();
        map.put("name", ((Api) clazz.getAnnotation(Api.class)).value());
        map.put("path", ((Path) clazz.getAnnotation(Path.class)).value());
        //System.out.println("CATEGORY : " + map.get("name"));

        String category = String.valueOf(map.get("name")).toUpperCase() + "_";
        List<LinkedHashMap<String, Object>> endpoints = new ArrayList<>(20);
        for (Method method : clazz.getMethods()) {
            Path pathAnnotation = method.getAnnotation(Path.class);
            String httpMethod = "GET";
            if (method.getAnnotation(POST.class) != null) {
                httpMethod = "POST";
            } else {
                if (method.getAnnotation(DELETE.class) != null) {
                    httpMethod = "DELETE";
                }
            }

            ApiOperation apiOperationAnnotation = method.getAnnotation(ApiOperation.class);
            if (pathAnnotation != null && apiOperationAnnotation != null && !apiOperationAnnotation.hidden()) {
                String path = pathAnnotation.value();
                String variablePrefix = category + getMethodName(path).toUpperCase() + "_";
                LinkedHashMap<String, Object> endpoint = new LinkedHashMap<>();
                endpoint.put("path", map.get("path") + pathAnnotation.value());
                endpoint.put("method", httpMethod);
                endpoint.put("response", StringUtils.substringAfterLast(apiOperationAnnotation.response().getName().replace("Void", ""),
                        "."));

                String responseClass = apiOperationAnnotation.response().getName().replace("Void", "");
                endpoint.put("responseClass", responseClass.endsWith(";") ? responseClass : responseClass + ";");
                endpoint.put("notes", apiOperationAnnotation.notes());
                endpoint.put("description", apiOperationAnnotation.value());

                ApiImplicitParams apiImplicitParams = method.getAnnotation(ApiImplicitParams.class);
                List<LinkedHashMap<String, Object>> parameters = new ArrayList<>();
                if (apiImplicitParams != null) {
                    for (ApiImplicitParam apiImplicitParam : apiImplicitParams.value()) {
                        LinkedHashMap<String, Object> parameter = new LinkedHashMap<>();
                        parameter.put("name", apiImplicitParam.name());
                        parameter.put("param", apiImplicitParam.paramType());
                        parameter.put("type", apiImplicitParam.dataType());
                        parameter.put("typeClass", "java.lang." + StringUtils.capitalize(apiImplicitParam.dataType()));
                        parameter.put("allowedValues", apiImplicitParam.allowableValues());
                        parameter.put("required", apiImplicitParam.required());
                        parameter.put("defaultValue", apiImplicitParam.defaultValue());
                        parameter.put("description", apiImplicitParam.value());
                        parameters.add(parameter);
                    }
                }

                java.lang.reflect.Parameter[] methodParameters = method.getParameters();
                if (methodParameters != null) {
                    for (java.lang.reflect.Parameter methodParameter : methodParameters) {
                        ApiParam apiParam = methodParameter.getAnnotation(ApiParam.class);
                        if (apiParam != null && !apiParam.hidden()) {
                            List<Map<String, Object>> bodyParams = new ArrayList<>();
                            LinkedHashMap<String, Object> parameter = new LinkedHashMap<>();
                            if (methodParameter.getAnnotation(PathParam.class) != null) {
                                parameter.put("name", methodParameter.getAnnotation(PathParam.class).value());
                                parameter.put("param", "path");
                            } else {
                                if (methodParameter.getAnnotation(QueryParam.class) != null) {
                                    parameter.put("name", methodParameter.getAnnotation(QueryParam.class).value());
                                    parameter.put("param", "query");
                                } else {
                                    parameter.put("name", "body");
                                    parameter.put("param", "body");
                                }
                            }

                            // Get type in lower case except for 'body' param
                            String type = methodParameter.getType().getName();
                            String typeClass = type;
                            if (typeClass.contains(".")) {
                                String[] split = typeClass.split("\\.");
                                type = split[split.length - 1];
                                if (!parameter.get("param").equals("body")) {
                                    type = type.toLowerCase();

                                    // Complex type different from body are enums
                                    if (type.contains("$")) {
                                        type = "enum";
                                    }
                                } else {
                                    type = "object";
                                    try {
                                        Class<?> aClass = Class.forName(typeClass);
                                        for (Field declaredField : aClass.getDeclaredFields()) {
                                            int modifiers = declaredField.getModifiers();
                                            // Ignore non-private or static fields
                                            if (Modifier.isPrivate(modifiers) && !Modifier.isStatic(modifiers)) {
                                                Map<String, Object> innerParams = new LinkedHashMap<>();
                                                innerParams.put("name", declaredField.getName());
                                                innerParams.put("param", "typeClass");
                                                innerParams.put("type", declaredField.getType().getSimpleName());
                                                innerParams.put("typeClass", declaredField.getType().getName() + ";");
                                                innerParams.put("allowedValues", "");
                                                innerParams.put("required", "false");
                                                innerParams.put("defaultValue", "");
                                                String fieldName = variablePrefix
                                                        + declaredField.getName().toUpperCase();
                                                fieldName = normalize(fieldName);
                                                String des = getDescriptionField(fieldName);
                                                if (StringUtils.isNotEmpty(des)) {
                                                    innerParams.put("description", des);
                                                } else {
                                                    innerParams.put("description",
                                                            "The body web service " + declaredField.getName() + " "
                                                                    + "parameter");
                                                }
                                                bodyParams.add(innerParams);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        logger.error("Error processing: " + typeClass);
                                    }
                                }
                            }
                            parameter.put("type", type);
                            parameter.put("typeClass", typeClass.endsWith(";") ? typeClass : typeClass + ";");
                            parameter.put("allowedValues", apiParam.allowableValues());
                            parameter.put("required", apiParam.required());
                            parameter.put("defaultValue", apiParam.defaultValue());
                            parameter.put("description", apiParam.value());
                            if (!bodyParams.isEmpty()) {
                                parameter.put("data", bodyParams);
                            }
                            parameters.add(parameter);
                        }
                    }
                }
                endpoint.put("parameters", parameters);
                endpoints.add(endpoint);
            }
        }

        endpoints.sort(Comparator.comparing(endpoint -> (String) endpoint.get("path")));
        map.put("endpoints", endpoints);
        return map;
    }

    private static String normalize(String s) {
        String res = s.replaceAll(" ", "_").replaceAll("-",
                "_");
        while (res.contains("__")) {
            res = res.replaceAll("__", "_");
        }
        return res;
    }

    private static String getMethodName(String inputPath) {
        String res = "";
        String prefix = "";
        String[] array = inputPath.split("}/");
        if (array.length > 2) {
            prefix = array[1].substring(array[1].lastIndexOf("{") + 1) + "_";
        }
        if (array != null && array.length > 0) {
            String path = array[array.length - 1];
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (path != null && path.contains("/")) {
                res = prefix + path.replaceAll("/", "_");
            } else {
                res = prefix + path;
            }
        }

        return res;
    }

    public static String getDescriptionField(String fieldName) {
        String res = null;
        try {
            Field barField = org.opencb.opencga.core.api.ParamConstants.class.getDeclaredField(fieldName);
            barField.setAccessible(true);
            res = (String) barField.get(null);
        } catch (Exception e) {

        }
        return res;
    }
}
