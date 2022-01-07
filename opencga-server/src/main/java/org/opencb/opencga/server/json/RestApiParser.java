package org.opencb.opencga.server.json;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.json.models.RestApi;
import org.opencb.opencga.server.json.models.RestCategory;
import org.opencb.opencga.server.json.models.RestEndpoint;
import org.opencb.opencga.server.json.models.RestParameter;
import org.opencb.opencga.server.json.utils.CommandLineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.*;

public class RestApiParser {

    private static Logger logger = LoggerFactory.getLogger(RestApiParser.class);

    public static RestApi parse(Class clazz) {
        RestApi res = new RestApi();
        res.getCategories().add(getCategory(clazz));
        return res;
    }

    public static RestApi parse(List<Class> classes) {
        RestApi res = new RestApi();
        res.setCategories(getCategories(classes));
        return res;
    }

    private static List<RestCategory> getCategories(List<Class> classes) {
        List<RestCategory> res = new ArrayList<>();
        for (Class clazz : classes) {
            res.add(getCategory(clazz));
        }
        return res;
    }

    private static RestCategory getCategory(Class clazz) {
        RestCategory restCategory = new RestCategory();
        restCategory.setName(((Api) clazz.getAnnotation(Api.class)).value());
        restCategory.setPath(((Path) clazz.getAnnotation(Path.class)).value());

        String categoryName = restCategory.getName().toUpperCase() + "_";
        List<RestEndpoint> restEndpoints = new ArrayList<>();
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
                String variablePrefix = categoryName + getMethodName(path).toUpperCase() + "_";
                RestEndpoint restEndpoint = new RestEndpoint();
                restEndpoint.setMethod(httpMethod);
                restEndpoint.setPath(restCategory.getPath() + pathAnnotation.value());
                restEndpoint.setResponse(StringUtils
                        .substringAfterLast(apiOperationAnnotation.response().getName().replace("Void", ""),"."));
                String responseClass = apiOperationAnnotation.response().getName().replace("Void", "");
                restEndpoint.setResponseClass(responseClass.endsWith(";") ? responseClass : responseClass + ";");
                restEndpoint.setNotes(apiOperationAnnotation.notes());
                restEndpoint.setDescription(apiOperationAnnotation.value());

                ApiImplicitParams apiImplicitParams = method.getAnnotation(ApiImplicitParams.class);
                List<RestParameter> restParameters = new ArrayList<>();
                if (apiImplicitParams != null) {
                    for (ApiImplicitParam apiImplicitParam : apiImplicitParams.value()) {
                        RestParameter restParameter = new RestParameter();
                        restParameter.setName(apiImplicitParam.name());
                        restParameter.setParam(apiImplicitParam.paramType());
                        restParameter.setType(apiImplicitParam.dataType());
                        restParameter.setTypeClass("java.lang." + StringUtils.capitalize(apiImplicitParam.dataType()));
                        restParameter.setAllowedValues(apiImplicitParam.allowableValues());
                        restParameter.setRequired(apiImplicitParam.required());
                        restParameter.setDefaultValue(apiImplicitParam.defaultValue());
                        restParameter.setDescription(apiImplicitParam.value());
                        restParameters.add(restParameter);
                    }
                }

                Parameter[] methodParameters = method.getParameters();
                if (methodParameters != null) {
                    for (Parameter methodParameter : methodParameters) {
                        ApiParam apiParam = methodParameter.getAnnotation(ApiParam.class);
                        if (apiParam != null && !apiParam.hidden()) {
                            List<RestParameter> bodyParams = new ArrayList<>();
                            RestParameter restParameter = new RestParameter();
                            if (methodParameter.getAnnotation(PathParam.class) != null) {
                                restParameter.setName(methodParameter.getAnnotation(PathParam.class).value());
                                restParameter.setParam("path");
                            } else {
                                if (methodParameter.getAnnotation(QueryParam.class) != null) {
                                    restParameter.setName(methodParameter.getAnnotation(QueryParam.class).value());
                                    restParameter.setParam("query");
                                } else {
                                    if (methodParameter.getAnnotation(FormDataParam.class) != null) {
                                        restParameter.setName(methodParameter.getAnnotation(FormDataParam.class).value());
                                        restParameter.setParam("query");
                                    } else {
                                        restParameter.setName("body");
                                        restParameter.setParam("body");
                                    }
                                }
                            }

                            // Get type in lower case except for 'body' param
                            String type = methodParameter.getType().getName();
                            String typeClass = type;
                            if (typeClass.contains(".")) {
                                String[] split = typeClass.split("\\.");
                                type = split[split.length - 1];
                                if (!restParameter.getParam().equals("body")) {
                                    type = type.toLowerCase();

                                    // Complex type different from body are enums
                                    if (type.contains("$")) {
                                        type = "enum";
                                    }
                                } else {
                                    type = "object";
                                    try {
                                        Class<?> aClass = Class.forName(typeClass);
                                        Field[] classFields = aClass.getDeclaredFields();
                                        List<Field> declaredFields = new ArrayList<>(Arrays.asList(classFields));
                                        if (aClass.getSuperclass() != null && !"java.lang.Object".equals(aClass.getSuperclass().getName())) {
                                            Field[] parentFields = aClass.getSuperclass().getDeclaredFields();
                                            Collections.addAll(declaredFields, parentFields);
                                        }

                                        for (Field declaredField : declaredFields) {
                                            int modifiers = declaredField.getModifiers();
                                            // Ignore non-private or static fields
                                            if ((Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers))
                                                    && !Modifier.isStatic(modifiers) && !Modifier.isFinal(modifiers)) {
                                                RestParameter innerParam = getParameter(declaredField.getName(), variablePrefix, declaredField,
                                                        declaredField.getType().getName(), declaredField.getName());
                                                if (innerParam.isList()) {
                                                    innerParam.setGenericType(declaredField.getGenericType().getTypeName());
                                                } else {
                                                    if (innerParam.isComplex() && !innerParam.getTypeClass().replaceAll(";", "").contains("$")) {
                                                        String classAndPackageName = innerParam.getTypeClass().replaceAll(";", "");
                                                        Class<?> cls = Class.forName(classAndPackageName);
                                                        Field[] fields = cls.getDeclaredFields();
                                                        List<RestParameter> complexParams = new ArrayList<>();
                                                        for (Field field : fields) {
                                                            int innerModifiers = field.getModifiers();
                                                            if (CommandLineUtils.isPrimitiveType(field.getType().getSimpleName())
                                                                    && !Modifier.isStatic(innerModifiers)) {
                                                                RestParameter complexParam =
                                                                        getParameter(field.getName(), variablePrefix, field,
                                                                                field.getType().getName(), declaredField.getName());
                                                                complexParam.setGenericType(declaredField.getType().getName());
                                                                complexParam.setInnerParam(true);
                                                                complexParams.add(complexParam);
                                                            }
                                                        }
                                                        if (CollectionUtils.isNotEmpty(complexParams)) {
                                                            bodyParams.addAll(complexParams);
                                                        }
                                                    }
                                                }

                                                bodyParams.add(innerParam);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        logger.error("Error processing: " + typeClass);
                                        e.printStackTrace();
                                    }
                                }
                            }
                            restParameter.setType(type);
                            restParameter.setTypeClass(typeClass.endsWith(";") ? typeClass : typeClass + ";");
                            restParameter.setAllowedValues(apiParam.allowableValues());
                            restParameter.setRequired(apiParam.required() || "path".equals(restParameter.getParam()));
                            restParameter.setDefaultValue(apiParam.defaultValue());
                            restParameter.setDescription(apiParam.value());
                            if (!bodyParams.isEmpty()) {
                                restParameter.setData(bodyParams);
                            }
                            restParameters.add(restParameter);
                        }
                    }
                }
                restEndpoint.setParameters(restParameters);
                restEndpoints.add(restEndpoint);
            }
        }

        restEndpoints.sort(Comparator.comparing(RestEndpoint::getPath));
        restCategory.setEndpoints(restEndpoints);
        return restCategory;
    }

    private static RestParameter getParameter(String paramName, String variablePrefix, Field declaredField, String className,
                                              String variableName) {
        RestParameter innerParam = new RestParameter();
        innerParam.setName(paramName);
        innerParam.setParam("body");
        innerParam.setParentParamName(variableName);
        innerParam.setType(declaredField.getType().getSimpleName());
        innerParam.setTypeClass(className + ";");
        innerParam.setAllowedValues("");
        innerParam.setRequired(isRequired(declaredField));
        innerParam.setDefaultValue("");
        innerParam.setComplex(!CommandLineUtils.isPrimitiveType(declaredField.getType().getSimpleName()));

        String fieldName = normalize(variablePrefix + declaredField.getName().toUpperCase());
        String des = getDescriptionField(fieldName);
        if (StringUtils.isNotEmpty(des)) {
            innerParam.setDescription(des);
        } else {
            innerParam.setDescription("The body web service " + declaredField.getName() + " " + "parameter");
        }

        return innerParam;
    }

    private static boolean isRequired(Field declaredField) {
        CliParam annotation = declaredField.getAnnotation(CliParam.class);
        return annotation != null && annotation.required();
    }

    private static String normalize(String s) {
        String res = s.replaceAll(" ", "_").replaceAll("-","_");
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
        if (array.length > 0) {
            String path = array[array.length - 1];
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (path.contains("/")) {
                res = prefix + path.replaceAll("/", "_");
            } else {
                res = prefix + path;
            }
        }
        return res;
    }

    private static String getDescriptionField(String fieldName) {
        String res = null;
        try {
            Field barField = org.opencb.opencga.core.api.ParamConstants.class.getDeclaredField(fieldName);
            barField.setAccessible(true);
            res = (String) barField.get(null);
        } catch (Exception e) {
            logger.error("RestApiParser error: ", e);
        }
        return res;
    }
}
