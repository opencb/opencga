package org.opencb.opencga.server.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.models.RestParameter;
import org.opencb.opencga.server.generator.utils.CommandLineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.*;

public class RestApiParser {

    private final Logger logger;

    // This class might accept some configuration in the future
    public RestApiParser() {
        logger = LoggerFactory.getLogger(RestApiParser.class);
    }

    public RestApi parse(Class clazz) {
        return parse(Collections.singletonList(clazz));
    }

    public RestApi parse(List<Class> classes) {
        RestApi restApi = new RestApi();
        restApi.getCategories().addAll(getCategories(classes));
        return restApi;
    }

    public void parseToFile(List<Class> classes, java.nio.file.Path path) throws IOException {
        // Check if parent folder exists and is writable
        FileUtils.checkDirectory(path.getParent(), true);

        // Parse REST API
        RestApi restApi = new RestApi();
        restApi.getCategories().addAll(getCategories(classes));

        // Prepare Jackson to create JSON pretty string
        ObjectMapper objectMapper = new ObjectMapper();
        String restApiJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(restApi);

        // Write string to file
        BufferedWriter bufferedWriter = FileUtils.newBufferedWriter(path);
        bufferedWriter.write(restApiJson);
        bufferedWriter.close();
    }

    private List<RestCategory> getCategories(List<Class> classes) {
        List<RestCategory> restCategories = new ArrayList<>();
        for (Class clazz : classes) {
            restCategories.add(getCategory(clazz));
        }
        return restCategories;
    }

    private RestCategory getCategory(Class clazz) {
        RestCategory restCategory = new RestCategory();
        restCategory.setName(((Api) clazz.getAnnotation(Api.class)).value());
        restCategory.setPath(((Path) clazz.getAnnotation(Path.class)).value());

        String categoryName = restCategory.getName().toUpperCase() + "_";
        List<RestEndpoint> restEndpoints = new ArrayList<>();
        for (Method method : clazz.getMethods()) {
            Path pathAnnotation = method.getAnnotation(Path.class);
            ApiOperation apiOperationAnnotation = method.getAnnotation(ApiOperation.class);

            // Ignore method if it does not have the @Path and @ApiOperation annotations or if it is hidden
            if (pathAnnotation == null || apiOperationAnnotation == null || apiOperationAnnotation.hidden()) {
                continue;
            }

            // Method annotations are correct and method is visible
            // 1. Get HTTP method
            String httpMethod = "GET";
            if (method.getAnnotation(POST.class) != null) {
                httpMethod = "POST";
            } else {
                if (method.getAnnotation(DELETE.class) != null) {
                    httpMethod = "DELETE";
                }
            }

            // 2. Create the REST Endpoint for this REST web service method
            String path = pathAnnotation.value();
            String variablePrefix = categoryName + getMethodName(path).toUpperCase() + "_";
            RestEndpoint restEndpoint = new RestEndpoint();
            restEndpoint.setMethod(httpMethod);
            restEndpoint.setPath(restCategory.getPath() + pathAnnotation.value());
            restEndpoint.setResponse(StringUtils
                    .substringAfterLast(apiOperationAnnotation.response().getName().replace("Void", ""), "."));
            String responseClass = apiOperationAnnotation.response().getName().replace("Void", "");
            restEndpoint.setResponseClass(responseClass.endsWith(";") ? responseClass : responseClass + ";");
            restEndpoint.setNotes(apiOperationAnnotation.notes());
            restEndpoint.setDescription(apiOperationAnnotation.value());

            // Fetch all parameters for this endpoint (method), these can be ApiImplicitParams and ApiParam
            List<RestParameter> restParameters = new ArrayList<>();

            // 3. Get all @ApiImplicitParams annotations
            ApiImplicitParams apiImplicitParams = method.getAnnotation(ApiImplicitParams.class);
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

            // 4. Get all Java method parameters with @ApiParam annotation
            Parameter[] methodParameters = method.getParameters();
            if (methodParameters != null) {
                for (Parameter methodParameter : methodParameters) {
                    ApiParam apiParam = methodParameter.getAnnotation(ApiParam.class);

                    // 4.1 Ignore all method parameters without @ApiParam annotations
                    if (apiParam == null || apiParam.hidden()) {
                        continue;
                    }

                    // 4.2 Get type of parameter: path, query or body
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

                    // 4.3 Get type in lower case except for 'body' param
                    List<RestParameter> bodyParams = new ArrayList<>();
                    String type = methodParameter.getType().getName();
                    String typeClass = type;
                    if (typeClass.contains(".")) {
                        String[] split = typeClass.split("\\.");
                        type = split[split.length - 1];
                        if (!restParameter.getParam().equals("body")) {
                            // 4.3.1 Process path and query parameters
                            type = type.toLowerCase();

                            // Complex types here can only be are enums
                            if (type.contains("$")) {
                                type = "enum";
                            }
                        } else {
                            // 4.3.2 Process body parameters
                            type = "object";
                            try {
                                // Get all body fields by Java reflection
                                Class<?> aClass = Class.forName(typeClass);
                                Field[] classFields = aClass.getDeclaredFields();
                                List<Field> declaredFields = new ArrayList<>(Arrays.asList(classFields));
                                if (aClass.getSuperclass() != null
                                        && !"java.lang.Object".equals(aClass.getSuperclass().getName())) {
                                    Field[] parentFields = aClass.getSuperclass().getDeclaredFields();
                                    Collections.addAll(declaredFields, parentFields);
                                }

                                for (Field declaredField : declaredFields) {
                                    int modifiers = declaredField.getModifiers();
                                    // Ignore non-private or static fields
                                    if ((Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers))
                                            && !Modifier.isStatic(modifiers) && !Modifier.isFinal(modifiers)) {
                                        RestParameter innerParam = getParameter(declaredField.getName(),
                                                variablePrefix, declaredField,
                                                declaredField.getType().getName(), declaredField.getName());
                                        if (innerParam.isList()) {
                                            innerParam.setGenericType(declaredField.getGenericType().getTypeName());
                                        } else {
                                            if (innerParam.isComplex()
                                                    && !innerParam.getTypeClass().replaceAll(";", "").contains("$")) {
                                                String classAndPackageName = innerParam.getTypeClass().replaceAll(";", "");
                                                Class<?> cls = Class.forName(classAndPackageName);
                                                Field[] fields = cls.getDeclaredFields();
                                                List<RestParameter> complexParams = new ArrayList<>();
                                                for (Field field : fields) {
                                                    int innerModifiers = field.getModifiers();
                                                    if (CommandLineUtils.isPrimitiveType(field.getType().getSimpleName())
                                                            && !Modifier.isStatic(innerModifiers)) {
                                                        RestParameter complexParam = getParameter(field.getName(),
                                                                variablePrefix, field, field.getType().getName(),
                                                                declaredField.getName());
                                                        complexParam.setGenericType(declaredField.getType().getName());
                                                        complexParam.setInnerParam(true);
                                                        complexParams.add(complexParam);
                                                    }
                                                }
                                                if (CollectionUtils.isNotEmpty(complexParams)) {
                                                    bodyParams.addAll(complexParams);
                                                }
                                            } else {
                                                // The body param is an Enum
                                                if (declaredField.getType().isEnum()) {
                                                    String[] enumSplit = declaredField.getAnnotatedType().getType().getTypeName().split("$");
                                                    Class<?> enumClass = Class.forName(enumSplit[0]);
                                                    List<String> allowedValues = new ArrayList<>();
                                                    final Object[] enumConstants = declaredField.getType().getEnumConstants();
                                                    for (Object enumValue : enumConstants) {
                                                        allowedValues.add(String.valueOf(enumValue));
                                                    }
                                                    innerParam.setType("enum");
                                                    innerParam.setAllowedValues(StringUtils.join(allowedValues, " "));
                                                    innerParam.setDescription("Enum param allowed values: " + StringUtils.join(allowedValues, " "));
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

                    // 4.4 Set all collected vales and add REST parameter to endpoint
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
       /*     if (httpMethod.equals("POST")) {
                RestParameter restParameter = new RestParameter();
                restParameter.setType("file");
                restParameter.setTypeClass("java.lang.String;");
                restParameter.setAllowedValues("");
                restParameter.setRequired(false);
                restParameter.setDefaultValue("");
                restParameter.setDescription("Json file with all parameters");
                restParameter.setName("jsonFile");
                restParameters.add(restParameter);
            }*/

            // 5. Save all REST Parameters found: ApiImplicitParams and ApiParam
            restEndpoint.setParameters(restParameters);
            restEndpoints.add(restEndpoint);
        }

        restEndpoints.sort(Comparator.comparing(RestEndpoint::getPath));
        restCategory.setEndpoints(restEndpoints);
        return restCategory;
    }

    private RestParameter getParameter(String paramName, String variablePrefix, Field declaredField, String className,
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

    private boolean isRequired(Field declaredField) {
        CliParam annotation = declaredField.getAnnotation(CliParam.class);
        return annotation != null && annotation.required();
    }

    private String normalize(String s) {
        String res = s.replaceAll(" ", "_").replaceAll("-", "_");
        while (res.contains("__")) {
            res = res.replaceAll("__", "_");
        }
        return res;
    }

    private String getMethodName(String inputPath) {
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

    private String getDescriptionField(String fieldName) {
        String res = null;
        try {
            Field barField = org.opencb.opencga.core.api.ParamConstants.class.getDeclaredField(fieldName);
            barField.setAccessible(true);
            res = (String) barField.get(null);
        } catch (Exception e) {
            return null;
            // logger.error("RestApiParser error: field: '" + fieldName + "' not found in ParamConstants");
        }
        return res;
    }
}
