package org.opencb.opencga.server.generator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
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
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.stream.Collectors;

public class RestApiParser {

    private final Logger logger;
    private final ObjectMapper objectMapper;

    // This class might accept some configuration in the future
    public RestApiParser() {
        logger = LoggerFactory.getLogger(RestApiParser.class);
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.addMixIn(ObjectMap.class, ObjectMapMixin.class);
    }

    public interface ObjectMapMixin {
        @JsonIgnore
        boolean isEmpty();
    }

    public RestApi parse(Class<?> clazz) {
        return parse(Collections.singletonList(clazz));
    }

    public RestApi parse(List<Class<?>> classes) {
        RestApi restApi = new RestApi();
        restApi.getCategories().addAll(getCategories(classes));
        return restApi;
    }

    public void parseToFile(List<Class<?>> classes, java.nio.file.Path path) throws IOException {
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

    private List<RestCategory> getCategories(List<Class<?>> classes) {
        List<RestCategory> restCategories = new ArrayList<>();
        for (Class<?> clazz : classes) {
            restCategories.add(getCategory(clazz));
        }
        return restCategories;
    }

    private RestCategory getCategory(Class<?> clazz) {
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
                                // Get all body properties using Jackson
                                Class<?> aClass = Class.forName(typeClass);
                                List<BeanPropertyDefinition> declaredFields = getPropertyDefinitions(aClass);

                                for (BeanPropertyDefinition declaredField : declaredFields) {
                                    RestParameter innerParam = getParameter(variablePrefix, "", declaredField);
                                    if (innerParam.isComplex()
                                            && !innerParam.isList()
                                            && !innerParam.getType().equals("enum")
                                            && !innerParam.getTypeClass().contains("$")) {
                                        // FIXME: Why discarding params with "$" ?  Why discarding inner classes?
                                        // FIXME: Get more than one level here
                                        String classAndPackageName = StringUtils.removeEnd(innerParam.getTypeClass(), ";");
                                        Class<?> cls = Class.forName(classAndPackageName);
                                        List<BeanPropertyDefinition> nestedProperties = getPropertyDefinitions(cls);
                                        List<RestParameter> complexParams = new ArrayList<>();
                                        for (BeanPropertyDefinition field : nestedProperties) {
                                            RestParameter complexParam = getParameter(variablePrefix, declaredField.getName(), field);
                                            // FIXME: Why? This is wrong. It's using the genericType field to specify the parent type
                                            complexParam.setGenericType(declaredField.getRawPrimaryType().getName());
                                            complexParam.setInnerParam(true);
                                            complexParams.add(complexParam);
                                        }
                                        if (CollectionUtils.isNotEmpty(complexParams)) {
                                            bodyParams.addAll(complexParams);
                                        }
                                    }

                                    bodyParams.add(innerParam);
                                }
                            } catch (ClassNotFoundException e) {
                                logger.error("Error processing: " + typeClass, e);
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

            // 5. Save all REST Parameters found: ApiImplicitParams and ApiParam
            restEndpoint.setParameters(restParameters);
            restEndpoints.add(restEndpoint);
        }

        restEndpoints.sort(Comparator.comparing(RestEndpoint::getPath));
        restCategory.setEndpoints(restEndpoints);
        return restCategory;
    }

    private List<BeanPropertyDefinition> getPropertyDefinitions(Class<?> aClass) {
        JavaType javaType = objectMapper.constructType(aClass);
        BeanDescription beanDescription = objectMapper.getDeserializationConfig().introspect(javaType);
        return beanDescription.findProperties();
    }

    private RestParameter getParameter(String variablePrefix, String parentParamName, BeanPropertyDefinition property) {
        RestParameter innerParam = new RestParameter();
        innerParam.setName(property.getName());
        innerParam.setParam("body");
        innerParam.setParentParamName(parentParamName);
        if (property.getRawPrimaryType().isEnum()) {
            // The param is an Enum
            innerParam.setType("enum");
            innerParam.setAllowedValues(Arrays.stream(property.getRawPrimaryType().getEnumConstants())
                    .map(Object::toString)
                    .collect(Collectors.joining(" ")));
        } else {
            innerParam.setType(property.getRawPrimaryType().getSimpleName());
            innerParam.setAllowedValues("");
        }

        innerParam.setTypeClass(property.getRawPrimaryType().getName() + ";");
        innerParam.setRequired(isRequired(property));
        innerParam.setDefaultValue("");
        innerParam.setComplex(!CommandLineUtils.isPrimitiveType(property.getRawPrimaryType().getSimpleName()));


        if (innerParam.isList()) {
//            innerParam.setGenericType(property.getPrimaryType().getContentType().getRawClass().getName());
            innerParam.setGenericType(property.getPrimaryType().toCanonical());
        }

        innerParam.setDescription(getDescriptionField(variablePrefix, property));
        return innerParam;
    }

    private boolean isRequired(BeanPropertyDefinition property) {
        if (property.getField() == null || property.getField().getAnnotated() == null) {
            return false;
        } else {
            CliParam annotation = property.getField().getAnnotated().getAnnotation(CliParam.class);
            return annotation != null && annotation.required();
        }
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

    private String getDescriptionField(String variablePrefix, BeanPropertyDefinition property) {
        // Get from jackson
        if (StringUtils.isNotEmpty(property.getMetadata().getDescription())) {
            return property.getMetadata().getDescription();
        }
        // Get from custom annotation
        if (property.getField() != null) {
            DataField dataField = property.getField().getAnnotated().getAnnotation(DataField.class);
            if (dataField != null && StringUtils.isNotEmpty(dataField.description())) {
                return dataField.description();
            }
        }

        // Get from ParamConstants
        String fieldName = normalize(variablePrefix + property.getName().toUpperCase());

        try {
            Field barField = org.opencb.opencga.core.api.ParamConstants.class.getDeclaredField(fieldName);
            if (barField != null) {
                barField.setAccessible(true);
                return (String) barField.get(null);
            }
        } catch (Exception ignore) {
            // logger.error("RestApiParser error: field: '" + fieldName + "' not found in ParamConstants");
        }

        if (property.getRawPrimaryType().isEnum()) {
            return "Enum param allowed values: " + Arrays.stream(property.getRawPrimaryType().getEnumConstants())
                    .map(Object::toString)
                    .collect(Collectors.joining(", "));
        } else {
            return "The body web service " + property.getName() + " parameter";
        }
    }
}
