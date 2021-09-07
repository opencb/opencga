package org.opencb.opencga.server.rest.json;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.*;

public class JSONManager {

    private static Logger logger = LoggerFactory.getLogger(JSONManager.class);

    public static List<LinkedHashMap<String, Object>> getHelp(List<Class> classes) {
        List<LinkedHashMap<String, Object>> res = new ArrayList<>();
        for (Class clazz : classes) {
            res.add(getHelp(clazz));
        }
        return res;
    }

    private static LinkedHashMap<String, Object> getHelp(Class clazz) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        map.put("name", ((Api) clazz.getAnnotation(Api.class)).value());
        map.put("path", ((Path) clazz.getAnnotation(Path.class)).value());
        System.out.println("CATEGORY : " + map.get("name"));

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
            String variablePrefix = category + getMethodName(String.valueOf(pathAnnotation.value())).toUpperCase() + "_";
            System.out.println("VARIABLE_PREFIX => " + variablePrefix);
            ApiOperation apiOperationAnnotation = method.getAnnotation(ApiOperation.class);
            if (pathAnnotation != null && apiOperationAnnotation != null && !apiOperationAnnotation.hidden()) {
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

                Parameter[] methodParameters = method.getParameters();
                if (methodParameters != null) {
                    for (Parameter methodParameter : methodParameters) {
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
                                                innerParams.put("description", "The body web service " + declaredField.getName() + " "
                                                        + "parameter");
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

    private static String getMethodName(String valueOf) {
        String res = "";
        String[] array = valueOf.split("}/");
        if (array != null && array.length > 0) {
            String path = array[array.length - 1];
            if (path.startsWith("/")) {
                path = path.substring(1, path.length());
            }
            if (path != null && path.contains("/")) {
                res = path.replaceAll("/", "_");
                res = res.replaceAll("__", "_");
            } else {
                res = path;
            }
        }
        return res;
    }
}
