package org.opencb.opencga.server.generator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.commons.annotations.DataField;
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
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.stream.Collectors;

public class RestApiParser {

    private final Logger logger;
    private final ObjectMapper objectMapper;
    private final SerializerProvider serializerProvider;

    private List<RestParameter> flatternParams = new ArrayList<>();


    // This class might accept some configuration in the future
    public RestApiParser() {
        logger = LoggerFactory.getLogger(RestApiParser.class);
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        serializerProvider = objectMapper.getSerializerProviderInstance();
    }


    public RestApi parse(Class<?> clazz, boolean summary) {
        return parse(Collections.singletonList(clazz), summary);
    }


    public RestApi parse(List<Class<?>> classes, boolean summary) {
        RestApi restApi = new RestApi();
        restApi.getCategories().addAll(getCategories(classes, summary));
        return restApi;
    }

    public void parseToFile(List<Class<?>> classes, java.nio.file.Path path) throws IOException {
        // Check if parent folder exists and is writable
        FileUtils.checkDirectory(path.getParent(), true);

        // Parse REST API
        RestApi restApi = new RestApi();
        restApi.getCategories().addAll(getCategories(classes, false));

        // Prepare Jackson to create JSON pretty string
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        String restApiJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(restApi);

        // Write string to file
        BufferedWriter bufferedWriter = FileUtils.newBufferedWriter(path);
        bufferedWriter.write(restApiJson);
        bufferedWriter.close();
    }

    private List<RestCategory> getCategories(List<Class<?>> classes, boolean flatten) {
        List<RestCategory> restCategories = new ArrayList<>();
        for (Class<?> clazz : classes) {
            restCategories.add(getCategory(clazz, flatten));
        }
        return restCategories;
    }

    private RestCategory getCategory(Class<?> clazz, boolean flatten) {
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
                    restParameter.setParam(RestParamType.valueOf(apiImplicitParam.paramType().toUpperCase()));
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
                        restParameter.setParam(RestParamType.PATH);
                    } else {
                        if (methodParameter.getAnnotation(QueryParam.class) != null) {
                            restParameter.setName(methodParameter.getAnnotation(QueryParam.class).value());
                            restParameter.setParam(RestParamType.QUERY);
                        } else {
                            if (methodParameter.getAnnotation(FormDataParam.class) != null) {
                                restParameter.setName(methodParameter.getAnnotation(FormDataParam.class).value());
                                restParameter.setParam(RestParamType.QUERY);
                            } else {
                                restParameter.setName("body");
                                restParameter.setParam(RestParamType.BODY);
                            }
                        }
                    }

                    // 4.3 Get type in lower case except for 'body' param
                    Class<?> typeClass = methodParameter.getType();
                    final String type;
                    if (restParameter.getParam() != RestParamType.BODY) {
                        // 4.3.1 Process path and query parameters
                        if (typeClass.isEnum()) {
                            type = "enum";
                        } else {
                            type = typeClass.getSimpleName().toLowerCase();
                        }
                    } else {
                        // 4.3.2 Process body parameters
                        type = "object";

                        // Get all body properties using Jackson
                        List<BeanPropertyDefinition> declaredFields = getPropertyDefinitions(typeClass);
                        List<RestParameter> bodyParams = new ArrayList<>(declaredFields.size());

                        for (BeanPropertyDefinition declaredField : declaredFields) {
                            RestParameter bodyParam = getRestParameter(variablePrefix, declaredField);
                            // For CLI is necessary flatten parameters
                            if (flatten) {
                                bodyParams.addAll(flattenInnerParams(variablePrefix, bodyParam));
                            }
                            bodyParams.add(bodyParam);
                        }
                        restParameter.setData(bodyParams);
                    }


                    // 4.4 Set all collected vales and add REST parameter to endpoint
                    restParameter.setType(type);
                    restParameter.setTypeClass(typeClass.getName() + ";");
                    if (typeClass.isEnum()) {
                        // The param is an Enum
                        restParameter.setType("enum");
                        restParameter.setAllowedValues(Arrays.stream(typeClass.getEnumConstants())
                                .map(Object::toString)
                                .collect(Collectors.joining(" ")));
                    } else {
                        restParameter.setAllowedValues(apiParam.allowableValues());
                    }

                    restParameter.setRequired(apiParam.required() || restParameter.getParam() == RestParamType.PATH);
                    restParameter.setDefaultValue(apiParam.defaultValue());
                    restParameter.setDescription(apiParam.value());
                    restParameters.add(restParameter);
                }
            }
            if (clazz.getName().contains("Meta")) {
                System.out.println("Adding Meta method :::::::::::::: " + method.getName());
                System.out.println(restEndpoint);
            }
            // 5. Save all REST Parameters found: ApiImplicitParams and ApiParam
            restEndpoint.setParameters(restParameters);
            restEndpoints.add(restEndpoint);
        }

        restEndpoints.sort(Comparator.comparing(RestEndpoint::getPath));
        restCategory.setEndpoints(restEndpoints);
        return restCategory;
    }

    private List<RestParameter> flattenInnerParams(String variablePrefix, RestParameter param) {
        // FIXME: Should we remove this artificial "flattening" ? It's redundant
        if (param.isComplex()
                && !param.isList()
                && !param.getType().equals("enum")
            /*&& !param.getTypeClass().contains("$")*/) {
            // FIXME: Why discarding params with "$" ?  Why discarding inner classes?
            String classAndPackageName = StringUtils.removeEnd(param.getTypeClass(), ";");
            Class<?> cls;
            try {
                cls = Class.forName(classAndPackageName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            List<BeanPropertyDefinition> nestedProperties = getPropertyDefinitions(cls);
            List<RestParameter> innerParams = new ArrayList<>();
            for (BeanPropertyDefinition field : nestedProperties) {
                RestParameter innerParam = getRestParameter(variablePrefix, field, param.getName());
                // FIXME: Why? This is wrong. It's using the genericType field to specify the parent type
//                innerParam.setGenericType(StringUtils.removeEnd(param.getTypeClass(), ";"));
                innerParam.setInnerParam(true);
                innerParams.add(innerParam);
            }
            return innerParams;
        }
        return Collections.emptyList();
    }

    private List<BeanPropertyDefinition> getPropertyDefinitions(Class<?> aClass) {
        JavaType javaType = objectMapper.constructType(aClass);
        BeanDescription beanDescription = objectMapper.getSerializationConfig().introspect(javaType);
        List<BeanPropertyDefinition> properties = beanDescription.findProperties();

        // For some reason, the ObjectMapper is not using the rule "require getters for setters"
        // I think that this only applies for the actual serialization, not for obtaining the bean descriptors.
        properties.removeIf(property -> property.getGetter() == null || property.getSetter() == null);
        return properties;
    }

    private RestParameter getRestParameter(String variablePrefix, BeanPropertyDefinition property) {
        return getRestParameter(variablePrefix, property, "");
    }

    private RestParameter getRestParameter(String variablePrefix, BeanPropertyDefinition property, String parentParamName) {
        return getRestParameter(variablePrefix, property, parentParamName, new Stack<>());
    }

    private RestParameter getRestParameter(String variablePrefix, BeanPropertyDefinition property, String parentParamName,
                                           Stack<Class<?>> stackClasses) {
        Class<?> propertyClass = property.getRawPrimaryType();

        RestParameter param = new RestParameter();
        param.setName(property.getName());
        param.setParam(RestParamType.BODY);
        param.setParentName(parentParamName);
        param.setTypeClass(propertyClass.getName() + ";");

//        innerParam.setDefaultValue(property.getMetadata().getDefaultValue());


        param.setComplex(!CommandLineUtils.isPrimitiveType(propertyClass.getName()));

        if (property.getField() != null && property.getField().getAnnotation(DataField.class) != null) {
            param.setDefaultValue(property.getField().getAnnotation(DataField.class).defaultValue());
            param.setRequired(property.getField().getAnnotation(DataField.class).required());
            param.setDescription(property.getField().getAnnotation(DataField.class).description());
        } else {
            param.setDefaultValue("");
            param.setRequired(isRequired(property));
            param.setDescription(getDescriptionField(variablePrefix, property));
        }
        if (propertyClass.isEnum()) {
            // The param is an Enum
            param.setType("enum");
            param.setAllowedValues(Arrays.stream(propertyClass.getEnumConstants())
                    .map(Object::toString)
                    .collect(Collectors.joining(" ")));
        } else {
            param.setType(propertyClass.getSimpleName());
            param.setAllowedValues("");
//            if (CommandLineUtils.isBasicType(propertyClass.getName())) {
//                System.out.println("innerParam = " + innerParam);
//            }
            if (Collection.class.isAssignableFrom(propertyClass)) {
//                innerParam.setGenericType(property.getPrimaryType().getContentType().getRawClass().getName());
                param.setGenericType(property.getPrimaryType().toCanonical());
                JavaType contentType = property.getPrimaryType().getContentType();
                if (isBean(contentType.getRawClass())) {
                    param.setData(getInnerParams(variablePrefix, property, stackClasses, contentType.getRawClass()));
                }
            } else if (Map.class.isAssignableFrom(propertyClass)) {
//                innerParam.setGenericType(property.getPrimaryType().getContentType().getRawClass().getName());
                param.setGenericType(property.getPrimaryType().toCanonical());
                JavaType contentType = property.getPrimaryType().getContentType();
                if (isBean(contentType.getRawClass()) || Collection.class.isAssignableFrom(contentType.getRawClass())) {
                    param.setData(getInnerParams(variablePrefix, property, stackClasses, contentType.getRawClass()));
                }
            } else {
                if (isBean(propertyClass)) {
//                param.setType("object");
                    param.setData(getInnerParams(variablePrefix, property, stackClasses, propertyClass));
                }
            }
        }

        return param;
    }

    private List<RestParameter> getInnerParams(String variablePrefix, BeanPropertyDefinition property, Stack<Class<?>> stackClasses, Class<?> propertyClass) {
        List<RestParameter> data = null;
        // Fill nested "data"
        if (!stackClasses.contains(propertyClass)) {
            List<BeanPropertyDefinition> properties = getPropertyDefinitions(propertyClass);
            data = new ArrayList<>(properties.size());
            stackClasses.add(propertyClass);
            for (BeanPropertyDefinition propertyDefinition : properties) {
                data.add(getRestParameter(variablePrefix + "." + property.getName(), propertyDefinition, property.getName(), stackClasses));
            }
            stackClasses.remove(propertyClass);
        } // Else : This field was already seen
        return data;
    }

    /**
     * Check if the given class is a JavaBean depending on its serialization.
     * Example of classes that are not a JavaBean:
     * - Any primitive
     * - Arrays
     * - Collections
     * - Maps
     * - Date (it's serialized as a number)
     * - URL (it's serialized as a String)
     * - URI (it's serialized as a String)
     *
     * @param aClass Class to test
     * @return if it's a bean
     */
    private boolean isBean(Class<?> aClass) {
        try {
            JsonSerializer<Object> serializer = serializerProvider.findTypedValueSerializer(aClass, true, null);
            return serializer instanceof BeanSerializer;
        } catch (JsonMappingException e) {
            throw new UncheckedIOException(e);
        }
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
