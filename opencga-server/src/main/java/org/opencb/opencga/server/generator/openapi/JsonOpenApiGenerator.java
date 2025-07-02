package org.opencb.opencga.server.generator.openapi;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.openapi.common.SwaggerDefinitionGenerator;
import org.opencb.opencga.server.generator.openapi.models.*;

import javax.ws.rs.*;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

public class JsonOpenApiGenerator {

    private final Set<Class<?>> beansDefinitions = new LinkedHashSet<>();
    private String study;

    public Swagger generateJsonOpenApi(ApiCommons apiCommons, String token, String environment, String host, String apiVersion, String study) {

        this.study = study;
        List<Class<?>> classes = apiCommons.getApiClasses();
        Swagger swagger = new Swagger();
        Info info = new Info();
        info.setTitle("OpenCGA RESTful Web Services");
        info.setDescription("OpenCGA RESTful Web Services API");
        info.setVersion(GitRepositoryState.getInstance().getBuildVersion());
        swagger.setInfo(info);
        swagger.setHost(StringUtils.isEmpty(host) ? "test.app.zettagenomics.com" : host);
        environment = StringUtils.removeStart(environment, "/");
        environment = StringUtils.removeEnd(environment, "/");
        if (environment.isEmpty()) {
            swagger.setBasePath("/opencga/webservices/rest");
        } else {
            swagger.setBasePath("/" + environment + "/opencga/webservices/rest");
        }
        List<String> schemes = new ArrayList<>();
        schemes.add("https");
        swagger.setSchemes(schemes);
        Map<String, Map<String, Method>> paths = new HashMap<>();
        List<Tag> tags = new ArrayList<>();

        // Configuración del esquema Bearer
        Map<String, Map<String, Object>> securityDefinitions = new HashMap<>();
        Map<String, Object> bearerAuth = new HashMap<>();
        bearerAuth.put("type", "apiKey");
        bearerAuth.put("name", "Authorization");
        bearerAuth.put("in", "header");
        bearerAuth.put("description", "Use 'Bearer <token>' to authenticate");
        securityDefinitions.put("BearerAuth", bearerAuth);
        swagger.setSecurityDefinitions(securityDefinitions);
        for (Class<?> clazz : classes) {
            Api api = clazz.getAnnotation(Api.class);
            if (api == null) {
                continue;
            }
            List<String> classTags = new ArrayList<>();
            // Warning: TAG filtering is case-sensitive.
            // See https://github.com/swagger-api/swagger-ui/issues/8143
            String mainTag = api.value().toLowerCase();
            classTags.add(mainTag);
            tags.add(new Tag(mainTag, api.description()));
            if (api.tags() != null) {
                for (String tag : api.tags()) {
                    String customTag = tag.trim().toLowerCase();
                    if (!customTag.isEmpty()) {
                        tags.add(new Tag(customTag, clazz.getSimpleName() + " tag"));
                        classTags.add(customTag);
                    }
                }
            }

            // Obtener ruta base de la clase
            javax.ws.rs.Path classPathAnnotation = clazz.getAnnotation(javax.ws.rs.Path.class);
            String basePath = classPathAnnotation.value();
            if (classPathAnnotation == null) {
                continue;
            }

            // Procesar métodos
            for (java.lang.reflect.Method wsmethod : clazz.getDeclaredMethods()) {
                ApiOperation apiOperation = wsmethod.getAnnotation(ApiOperation.class);

                if (apiOperation != null && !apiOperation.hidden()) {
                    // Crear operación Swagger
                    Method method = new Method();
                    method.setSummary(apiOperation.value());
                    method.setDescription(apiOperation.notes());
                   if(apiOperation.response() != null && !apiOperation.response().equals(Void.class)) {
                       String response = apiOperation.response().getSimpleName();
                       if (StringUtils.isNotEmpty(response)) {
                           String newDescription = String.format("%s. Response Type: %s", method.getSummary(), response);
                           method.setSummary(newDescription);
                       }
                   }

                    method.setTags(classTags);
                    Map<String, Response> responses = getStringResponseMap(apiOperation);
                    method.setResponses(responses);

                    String httpMethod = extractHttpMethod(wsmethod);
                    Consumes consumes = wsmethod.getAnnotation(Consumes.class);
                    if (consumes == null) {
                        consumes = wsmethod.getDeclaringClass().getAnnotation(Consumes.class);
                    }
                    if (consumes != null){
                        method.getConsumes().addAll(Arrays.asList(consumes.value()));
                    }
                    Produces produces = wsmethod.getAnnotation(Produces.class);
                    if (produces == null) {
                        produces = wsmethod.getDeclaringClass().getAnnotation(Produces.class);
                    }
                    if (produces != null){
                        method.getProduces().addAll(Arrays.asList(produces.value()));
                    }

                    // Get parameters
                    List<Parameter> parameters = extractParameters(wsmethod);
                    method.setParameters(parameters);

                    // Full path
                    javax.ws.rs.Path methodPathAnnotation = wsmethod.getAnnotation(javax.ws.rs.Path.class);
                    String fullPath = basePath + (methodPathAnnotation != null ? methodPathAnnotation.value() : "");
                    fullPath = fullPath.replace("{apiVersion}", apiVersion);

                    // OperationID must be unique
                    String operationId = httpMethod + fullPath
                            .replace("/", "-")
                            .replace("{", "")
                            .replace("}", "");
                    method.setOperationId(operationId);

                    List<String> tokens = new ArrayList<>();
                    if(StringUtils.isNotEmpty(token)){
                        tokens.add("Bearer " + token);
                    }
                    method.setSecurity(Collections.singletonList(Collections.singletonMap("BearerAuth", tokens)));

                    paths.computeIfAbsent(fullPath, k -> new HashMap<>()).put(httpMethod, method);
                }
            }
        }
        Map<String, Definition> definitions = SwaggerDefinitionGenerator.getDefinitions(beansDefinitions);

        swagger.setTags(sortTagsByName(tags, apiCommons));

        Map<String, Map<String, Method>> orderedPaths = new LinkedHashMap<>();
        paths.entrySet().stream()
                .sorted(Map.Entry.<String, Map<String, Method>>comparingByValue(Comparator.comparing(o -> {
                            String httpMethod = o.keySet().iterator().next();
                            switch (httpMethod) {
                                case "get":
                                    return 1;
                                case "post":
                                    return 2;
                                case "put":
                                    return 3;
                                case "delete":
                                    return 4;
                                default:
                                    throw new IllegalArgumentException("Unsupported HTTP method: " + httpMethod);
                            }
                        }))
                        .thenComparing(Map.Entry.comparingByKey()))
                .forEachOrdered(entry -> orderedPaths.put(entry.getKey(), entry.getValue()));

        swagger.setPaths(orderedPaths);
        swagger.setDefinitions(definitions);
        return swagger;
    }

    public List<Tag> sortTagsByName(List<Tag> tags,ApiCommons apiCommons) {
        // Build a lookup map from tag name to Tag instance
        List<String> fixedOrder = apiCommons.getOrderCategories();
        Map<String, Tag> nameToTag = new HashMap<>();
        for (Tag tag : tags) {
            String name = tag.getName();
            if (!fixedOrder.contains(name)) {
                // Unknown name → error out immediately
                throw new IllegalArgumentException(
                        "Unsupported tag name: '" + name + "'. Allowed values are: " + fixedOrder
                );
            }
            // If there are duplicates, the last one wins; adjust if needed
            nameToTag.put(name, tag);
        }

        // Build the result list by walking FIXED_ORDER exactly once
        List<Tag> sorted = new ArrayList<>(tags.size());
        for (String expectedName : fixedOrder) {
            Tag t = nameToTag.get(expectedName);
            if (t == null) {
                // A required tag is missing → error out
                throw new IllegalArgumentException(
                        "Missing required tag: '" + expectedName + "'. All FIXED_ORDER entries must be present."
                );
            }
            sorted.add(t);
        }

        return sorted;
    }

    private Map<String, Response> getStringResponseMap(ApiOperation apiOperation) {
        Map<String,Response> responses=new HashMap<>();
        Response response = new Response();
        if (InputStream.class.isAssignableFrom(apiOperation.response())) {
            // Content  is not expected for InputStream
//            Map<String, Content> content = new HashMap<>();
//            content.put("application/octet-stream", new Content()
//                    .setSchema(new Schema().setType("string").setFormat("binary")));
//            response.setContent(content);
            response.setDescription("File successfully downloaded");
        } else {
            response.setDescription("Successful operation: " + apiOperation.response().getSimpleName());
            if (!SwaggerDefinitionGenerator.isPrimitive(apiOperation.response())) {
                if (SwaggerDefinitionGenerator.isOpencbBean(apiOperation.response())) {
                    Schema schema = new Schema();
                    schema.set$ref(SwaggerDefinitionGenerator.build$ref(apiOperation.response()));

                    response.setSchema(schema);
                    beansDefinitions.add(apiOperation.response());
                } else if (apiOperation.response() == Object.class) {
                    // TODO: This is a workaround for the Object.class case
                    response.setSchema(null);
                } else if (SwaggerDefinitionGenerator.isCollection(apiOperation.response())) {
                    response.setSchema(new Schema().setType("array"));
                } else {
                    throw new IllegalArgumentException("Unsupported response type: " + apiOperation.response());
                }
            }
        }
        responses.put("200",response);
        Response responseError = new Response();
        responseError.setDescription("Got server error, invalid parameters or missing data");
        responses.put("503",responseError);
        return responses;
    }

    private String extractHttpMethod(java.lang.reflect.Method method) {
        if (method.isAnnotationPresent(GET.class)) {
            return "get";
        } else if (method.isAnnotationPresent(POST.class)) {
            return "post";
        } else if (method.isAnnotationPresent(PUT.class)) {
            return "put";
        } else if (method.isAnnotationPresent(DELETE.class)) {
            return "delete";
        } else {
            throw new IllegalArgumentException("Unsupported HTTP method for method: " + method.getName());
        }
    }

    private List<Parameter> extractParameters(java.lang.reflect.Method method) {
        List<Parameter> parameters = new ArrayList<>();

        // Procesar parámetros definidos con @ApiImplicitParams
        ApiImplicitParams implicitParams = method.getAnnotation(ApiImplicitParams.class);
        if (implicitParams != null) {
            for (ApiImplicitParam implicitParam : implicitParams.value()) {
                Parameter parameter = new Parameter();
                parameter.setName(implicitParam.name());
                parameter.setIn(implicitParam.paramType());
                parameter.setDescription(implicitParam.value());
                parameter.setRequired(implicitParam.required());
                parameter.setType(implicitParam.dataType());
                parameter.setFormat(implicitParam.format());
                parameter.setDefault(implicitParam.defaultValue());
                if(parameter.getName() != null && parameter.getName().equals("study") && StringUtils.isNotEmpty(study)) {
                    parameter.setDefault(study);
                }
                parameters.add(parameter);
            }
        }

        // Procesar parámetros individuales del método
        for (java.lang.reflect.Parameter methodParam : method.getParameters()) {
            // Procesar ApiParam
            // 4.1 Ignore all method parameters without @ApiParam annotations
            Parameter parameter = new Parameter();
            ApiParam apiParam = methodParam.getAnnotation(ApiParam.class);
            if (apiParam == null || apiParam.hidden()) {
                continue;
            }
            if (apiParam.value().equals("body") || apiParam.name().equals("body") || isBody(methodParam)) {
                parameter.setName("body");
                parameter.setIn("body");
                parameter.setRequired(apiParam.required());
                parameter.setType(null);
                parameter.setFormat(null);
                if (Map.class.isAssignableFrom(methodParam.getType())) {
//                    parameter.setType("array");
                    parameter.setSchema(getMapSchema(methodParam));
                } else {
//                    parameter.setType(methodParam.getType().getTypeName());
                    parameter.setSchema(new Schema().set$ref(SwaggerDefinitionGenerator.build$ref(methodParam.getType())));
                    if (SwaggerDefinitionGenerator.isOpencbBean(methodParam.getType())) {
                        beansDefinitions.add(methodParam.getType());
                    }
                }
            } else {
                if (methodParam.isAnnotationPresent(PathParam.class)) {
                    PathParam pathParam = methodParam.getAnnotation(PathParam.class);
                    if (pathParam != null) {
                        parameter.setName(pathParam.value());
                        parameter.setRequired(true);
                    }
                } else if (methodParam.isAnnotationPresent(QueryParam.class)) {
                    QueryParam queryParam = methodParam.getAnnotation(QueryParam.class);
                    if (queryParam != null) {
                        parameter.setName(queryParam.value());
                        parameter.setRequired(apiParam.required());
                    }
                } else if (methodParam.isAnnotationPresent(FormDataParam.class)) {
                    FormDataParam formDataParam = methodParam.getAnnotation(FormDataParam.class);
                    if (formDataParam != null) {
                        parameter.setName(formDataParam.value());
                        parameter.setRequired(apiParam.required());
                    }
                } else {
                    throw new IllegalArgumentException("Unsupported parameter type for method: " + method.getName() + " : " + methodParam.getAnnotations());
                }
                if (SwaggerDefinitionGenerator.isPrimitive(methodParam.getType())){
                    parameter.setType(SwaggerDefinitionGenerator.mapJavaTypeToSwaggerType(methodParam.getType()));
                } else if (methodParam.getType().isEnum()) {
                    parameter.setType("string");
                    parameter.setEnum(Arrays.stream(methodParam.getType().getEnumConstants())
                            .map(Object::toString)
                            .collect(Collectors.toList()));
                } else if (InputStream.class.isAssignableFrom(methodParam.getType())) {
                    parameter.setType("file");
                } else {
                    throw new IllegalArgumentException("Unsupported parameter type for method: " + method.getName() + " : " + methodParam.getType());
                }
                parameter.setIn(getIn(methodParam));
                if (StringUtils.isNotEmpty(apiParam.defaultValue())) {
                    parameter.setDefault(apiParam.defaultValue());
                }
                parameter.setDescription(formatParameterDescription(apiParam, parameter));
            }
            if (parameter.getName() != null) {
                if(parameter.getName().equals("study") && StringUtils.isNotEmpty(study)) {
                    parameter.setDefault(study);
                }
                parameters.add(parameter);
            }
        }
        return sortParameters(parameters);
    }

    public List<Parameter> sortParameters(List<Parameter> parameters) {
        parameters.sort(Comparator
                .comparing(Parameter::isRequired).reversed() // Required=true first and alphabetical order
                .thenComparing(p -> p.getName().toLowerCase()));
        return parameters;// Alphabetically by name
    }

    public String formatParameterDescription(ApiParam apiParam, Parameter parameter) {
        String allowable = apiParam.allowableValues();
        String defaultValue = apiParam.defaultValue();
        String description = StringUtils.isEmpty(apiParam.value()) ? parameter.getName() : apiParam.value();

        StringBuilder descriptionBuilder = new StringBuilder(description);

        // Add period if the description does not already end with one
        if (!description.trim().endsWith(".")) {
            descriptionBuilder.append(".");
        }

        // Append allowable values if present, replacing commas with " | "
        if (StringUtils.isNotEmpty(allowable)) {
            String formattedAllowable = allowable.replace(",", " | ");
            descriptionBuilder.append(" Allowable values: ").append(formattedAllowable).append(".");
        }

        // Append default value if specified
        if (StringUtils.isNotEmpty(defaultValue)) {
            descriptionBuilder.append(" Default: ").append(defaultValue).append(".");
        }

        // Set the final formatted description
        return descriptionBuilder.toString();
    }

    public Schema getMapSchema(java.lang.reflect.Parameter methodParam) {
        Schema schema = new Schema();
        schema.setType("object");
        Type paramType = methodParam.getParameterizedType();
        if (paramType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) paramType;
            Type[] typeArguments = parameterizedType.getActualTypeArguments();

            if (typeArguments.length == 2) {
                Schema keySchema = getTypeSchema(typeArguments[0]);
                Schema valueSchema = getTypeSchema(typeArguments[1]);

                // En OpenAPI los mapas solo pueden tener claves tipo String
                if (!"string".equals(keySchema.getType())) {
                    throw new IllegalArgumentException("OpenAPI solo permite Map con claves de tipo String.");
                }

                schema.setAdditionalProperties(valueSchema);
            }
        } else {
            // Si no es un tipo parametrizado, asumimos Map<String, Object>
            Schema additionalPropertiesSchema = new Schema();
            additionalPropertiesSchema.setType("object");
            schema.setAdditionalProperties(additionalPropertiesSchema);
        }

        return schema;
    }

    private Schema getTypeSchema(Type type) {
        Schema schema = new Schema();

        if (type instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type;

            if (String.class.equals(clazz)) {
                schema.setType("string");
            } else if (Integer.class.equals(clazz) || int.class.equals(clazz)) {
                schema.setType("integer");
                schema.setFormat("int32");
            } else if (Long.class.equals(clazz) || long.class.equals(clazz)) {
                schema.setType("integer");
                schema.setFormat("int64");
            } else if (Boolean.class.equals(clazz) || boolean.class.equals(clazz)) {
                schema.setType("boolean");
            } else if (Double.class.equals(clazz) || double.class.equals(clazz)) {
                schema.setType("number");
                schema.setFormat("double");
            } else if (Float.class.equals(clazz) || float.class.equals(clazz)) {
                schema.setType("number");
                schema.setFormat("float");
            } else if (clazz.isArray()) {
                schema.setType("array");
                schema.setItems(getTypeSchema(clazz.getComponentType()));
            } else {
                schema.setType("object");
            }
        } else {
            schema.setType("object"); // Si es un tipo genérico desconocido, asumimos objeto
        }

        return schema;
    }

    private boolean isBody(java.lang.reflect.Parameter methodParameter) {
        return !methodParameter.isAnnotationPresent(PathParam.class) &&
                !methodParameter.isAnnotationPresent(QueryParam.class)  &&
                !methodParameter.isAnnotationPresent(FormDataParam.class) ;
    }


    private String getIn(java.lang.reflect.Parameter methodParameter) {
        if (methodParameter.isAnnotationPresent(PathParam.class)) {
            return "path";
        } else if (methodParameter.isAnnotationPresent(QueryParam.class)) {
            return "query";
        } else if (methodParameter.isAnnotationPresent(FormDataParam.class)) {
            return "formData";
        } else {
            return "body";
        }
    }

    /**
     * Determina la ubicación del parámetro (query, path, etc.).
     */
    private String determineParameterLocation(java.lang.reflect.Parameter parameter) {
        if (parameter.isAnnotationPresent(PathParam.class)) {
            return "path";
        } else if (parameter.isAnnotationPresent(QueryParam.class)) {
            return "query";
        } else if (parameter.isAnnotationPresent(ApiParam.class)) {
            return "query"; // Por defecto si no se especifica
        }
        return "query"; // Predeterminado
    }
}
