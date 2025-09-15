package org.opencb.opencga.server.generator.openapi;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.openapi.common.SwaggerDefinitionGenerator;
import org.opencb.opencga.server.generator.openapi.models.*;

import javax.ws.rs.*;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class JsonOpenApiGenerator {

    private final Set<Class<?>> beansDefinitions = new LinkedHashSet<>();
    private String study;

    /**
     * Generates a Swagger (OpenAPI v2) definition from the given API metadata.
     * <p>
     * - Initializes the Swagger Info (title, description, version) and server settings (host, basePath, HTTPS scheme).
     * - Configures Bearer token security under “BearerAuth”.
     * - Scans each API class returned by ApiCommons:
     *   • Reads @Api to collect main and custom tags (lowercased).
     *   • Reads @Path on the class and its methods, plus @ApiOperation to build Swagger Method entries.
     *   • Sets operation summary, description, response schemas via getStringResponseMap,
     *     and request/response media types from @Consumes/@Produces.
     *   • Assigns a unique operationId and applies security requirements.
     * - Orders all paths by HTTP verb (GET, POST, PUT, DELETE) then by path.
     * - Generates definitions for any discovered OpenCGA beans.
     *
     * @param apiCommons an ApiCommons implementation supplying API resource classes
     * @param token      the authentication token to include in each operation’s security
     * @param url        the base URL (host and optional path) for the Swagger host/basePath
     * @param apiVersion the API version placeholder to substitute in paths
     * @param study      the study identifier to add as a default value for the "study" parameter
     * @return a fully populated Swagger object ready to be serialized to JSON
     */

    public Swagger generateJsonOpenApi(ApiCommons apiCommons, String token, String url, String apiVersion, String study) {

        this.study = study;
        List<Class<?>> classes = apiCommons.getApiClasses();
        Swagger swagger = new Swagger();
        Info info = new Info();
        info.setTitle("OpenCGA RESTful Web Services");
        info.setDescription("OpenCGA RESTful Web Services API");
        info.setVersion(apiCommons.getVersion());
        swagger.setInfo(info);
        swagger.setHost(getHost(url));
        swagger.setBasePath(getEnvironment(url) + "/webservices/rest");
        List<String> schemes = new ArrayList<>();
        schemes.add(getScheme(url));
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
            // Warning: TAG filtering is case-sensitive.
            // See https://github.com/swagger-api/swagger-ui/issues/8143
            List<String> classTags = new ArrayList<>();
            String mainTag = api.value();
            classTags.add(mainTag);

            // Count available methods in the class
            int count = 0;
            for (java.lang.reflect.Method wsmethod : clazz.getDeclaredMethods()) {
                ApiOperation apiOperation = wsmethod.getAnnotation(ApiOperation.class);
                if (apiOperation != null && !apiOperation.hidden()) {
                    count++;
                }
            }
            Tag tag = new Tag();
            tag.setName(mainTag);
            tag.setDescription(api.description());
            tag.setCount(count);
            addTag(tags, tag);

            // Gets the path annotation from the class
            javax.ws.rs.Path classPathAnnotation = clazz.getAnnotation(javax.ws.rs.Path.class);
            String basePath = classPathAnnotation.value();
            if (classPathAnnotation == null) {
                continue;
            }
            // Process methods in the class
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

        swagger.setTags(tags);

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

    private static void addTag(List<Tag> tags,  Tag newtag) {
        // Iterate through the list by index to find and mark the existing tag for removal
        int removeIndex = -1;
        for (int i = 0; i < tags.size(); i++) {
            Tag tag = tags.get(i);
            // If we find a tag with the same name (case-insensitive)...
            if (tag.getName().equalsIgnoreCase(newtag.getName())) {
                // ...add its count to our counter...
                int newCount = tag.getCount() + newtag.getCount();
                newtag.setCount(newCount);
                // ...remember its index for removal...
                removeIndex = i;
                // ...and stop iterating after handling the first match
                break;
            }
        }

        // Remove the old tag if it was found
        if (removeIndex != -1) {
            tags.remove(removeIndex);
        }
        newtag.setDescription("<span>"+newtag.getCount()+ "</span> " + newtag.getDescription());
        // Always add (or re-add) the tag with the updated count and description
        tags.add(newtag);
    }

    /**
     * Extracts the host (everything before the first '/') from a URL string
     * that is guaranteed to come without a protocol.
     *
     * @param url the input URL, e.g. "demo.app.zettagenomics.com/trial-gtech/opencga"
     * @return the host portion, e.g. "demo.app.zettagenomics.com"
     */
    private String getScheme(String url)  {
        try {
            return new URI(url).getScheme();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts the host (everything before the first '/') from a URL string
     * that is guaranteed to come without a protocol.
     *
     * @param url the input URL, e.g. "demo.app.zettagenomics.com/trial-gtech/opencga"
     * @return the host portion, e.g. "demo.app.zettagenomics.com"
     */
    private String getHost(String url)  {
        try {
            return new URI(url).getHost();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts the environment path (everything including and after the first '/'),
     * or returns "/" if there is no path.
     *
     * @param url the input URL, e.g. "demo.app.zettagenomics.com/trial-gtech/opencga"
     * @return the environment portion, e.g. "/trial-gtech/opencga", or "" if none
     */
    private String getEnvironment(String url) {
        try {
            return new URI(url).getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds a Swagger response map for the given API operation.
     * <p>
     * - For a response type assignable from InputStream, sets a “File successfully downloaded” description.
     * - Otherwise, sets a generic success description including the response class name.
     *   • If the response is a non‐primitive OpenCB bean, creates a Schema $ref and registers the bean.
     *   • If the response is Object.class, leaves the schema null (workaround).
     *   • If the response is a collection, defines an array schema.
     *   • Throws IllegalArgumentException for unsupported response types.
     * - Always includes a default “503” error response with a server‐error description.
     *
     * @param apiOperation the API operation metadata containing the response class
     * @return a map of HTTP status codes to Swagger Response objects for “200” and “503”
     */
    private Map<String, Response> getStringResponseMap(ApiOperation apiOperation) {
        Map<String,Response> responses=new HashMap<>();
        Response response = new Response();
        if (InputStream.class.isAssignableFrom(apiOperation.response())) {
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

    /**
     * Determines the HTTP verb for a given JAX-RS resource method.
     * <p>
     * - Returns "get" if annotated with @GET.
     * - Returns "post" if annotated with @POST.
     * - Returns "put" if annotated with @PUT.
     * - Returns "delete" if annotated with @DELETE.
     * - Throws IllegalArgumentException if no supported HTTP annotation is present.
     *
     * @param method the reflected resource method to inspect
     * @return the lowercase HTTP verb corresponding to the method’s JAX-RS annotation
     * @throws IllegalArgumentException if the method has no @GET, @POST, @PUT, or @DELETE annotation
     */
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

    /**
     * Extracts and builds a list of Swagger parameters for the given JAX-RS method.
     * <p>
     * - First processes any @ApiImplicitParams on the method to add predefined parameters,
     *   including automatically setting the default “study” value if present.
     * - Then iterates over the reflected Java parameters:
     *   • Skips any without @ApiParam or those marked hidden.
     *   • For “body” parameters, sets a body schema (map or bean reference) and tracks bean definitions.
     *   • For @PathParam, @QueryParam, or @FormDataParam, sets name, required flag, and primitive/enum/file type.
     *   • Throws IllegalArgumentException for unsupported parameter types or missing annotations.
     * - Finally sorts the collected parameters before returning.
     *
     * @param method the Java method reflecting a REST operation
     * @return a sorted list of Swagger Parameter objects representing all inputs to the operation
     */

    private List<Parameter> extractParameters(java.lang.reflect.Method method) {
        List<Parameter> parameters = new ArrayList<>();

        // Process params defined by @ApiImplicitParams
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
                if(parameter.getName() != null && parameter.getName().equals(ParamConstants.STUDY_PARAM) && StringUtils.isNotEmpty(study)) {
                    parameter.setDefault(study);
                }
                parameters.add(parameter);
            }
        }

        // Process params defined by method
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
                    parameter.setSchema(getMapSchema(methodParam));
                } else {
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
                if(parameter.getName().equals(ParamConstants.STUDY_PARAM) && StringUtils.isNotEmpty(study)) {
                    parameter.setDefault(study);
                }
                parameters.add(parameter);
            }
        }
        return sortParameters(parameters);
    }

    /**
     * Orders parameters so that required ones appear first, then sorts alphabetically by name.
     *
     * @param parameters the list of Parameter objects to sort
     * @return the same list instance, now sorted with required parameters first and then by name
     */
    public List<Parameter> sortParameters(List<Parameter> parameters) {
        parameters.sort(Comparator
                .comparing(Parameter::isRequired).reversed() // Required=true first and alphabetical order
                .thenComparing(p -> p.getName().toLowerCase()));
        return parameters;// Alphabetically by name
    }

    /**
     * Builds a user-friendly parameter description from ApiParam metadata.
     * <p>
     * - Uses the @ApiParam.value() text or the parameter name if value is empty.
     * - Ensures the description ends with a period.
     * - If allowableValues is specified, appends “Allowable values: v1 | v2…” with separators.
     * - If a defaultValue is provided, appends “Default: value.”
     *
     * @param apiParam  the ApiParam annotation containing raw description, allowableValues, and defaultValue
     * @param parameter the Swagger Parameter object for which the description is formatted
     * @return the fully formatted description string, including punctuation, allowable values, and default
     */
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

    /**
     * Generates a Swagger schema for a Map parameter, assuming the key is always a String.
     * <p>
     * - If the parameter is a Map with two type arguments, uses the first as the key type and the second as the value type.
     * - If the key type is not String, throws an IllegalArgumentException.
     * - If it is not a parameterized type, defaults to Map<String, Object>.
     *
     * @param methodParam the method parameter representing the Map
     * @return a Schema object representing the Map structure
     */
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

                // Only strings are permitted as key map in openapi
                if (!"string".equals(keySchema.getType())) {
                    throw new IllegalArgumentException("OpenAPI solo permite Map con claves de tipo String.");
                }

                schema.setAdditionalProperties(valueSchema);
            }
        } else {
            // If it is not a parameterized type, we assume Map<String, Object>
            Schema additionalPropertiesSchema = new Schema();
            additionalPropertiesSchema.setType("object");
            schema.setAdditionalProperties(additionalPropertiesSchema);
        }

        return schema;
    }

    /**
     * Generates a Swagger schema for a given Java type.
     * <p>
     * - Maps primitive types to OpenAPI types (e.g., String, Integer, Boolean).
     * - For arrays, sets the type to "array" and recursively gets the component type schema.
     * - For unknown generic types, defaults to "object".
     *
     * @param type the Java type to convert into a Swagger schema
     * @return a Schema object representing the OpenAPI definition of the type
     */
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
            schema.setType("object"); //If it is an unknown generic type, we assume object
        }

        return schema;
    }

    /**
     * Determines if a method parameter is a "body" parameter, meaning it is not annotated with
     * @PathParam, @QueryParam, or @FormDataParam.
     *
     * @param methodParameter the method parameter to check
     * @return true if the parameter is a body parameter, false otherwise
     */
    private boolean isBody(java.lang.reflect.Parameter methodParameter) {
        return !methodParameter.isAnnotationPresent(PathParam.class) &&
                !methodParameter.isAnnotationPresent(QueryParam.class)  &&
                !methodParameter.isAnnotationPresent(FormDataParam.class) ;
    }

    /**
     * Determines the location of the parameter based on its annotations.
     * <p>
     * - Returns "path" for @PathParam.
     * - Returns "query" for @QueryParam.
     * - Returns "formData" for @FormDataParam.
     * - Defaults to "body" if none of the above annotations are present.
     *
     * @param methodParameter the method parameter to check
     * @return the string representing the parameter location ("path", "query", "formData", or "body")
     */
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
     * Determines the parameter location based on its annotations.
     * <p>
     * - Returns "path" for @PathParam.
     * - Returns "query" for @QueryParam.
     * - Returns "query" by default for @ApiParam.
     *
     * @param parameter the method parameter to check
     * @return the string representing the parameter location ("path", "query", or "body")
     */
    private String determineParameterLocation(java.lang.reflect.Parameter parameter) {
        if (parameter.isAnnotationPresent(PathParam.class)) {
            return "path";
        } else if (parameter.isAnnotationPresent(QueryParam.class)) {
            return "query";
        } else if (parameter.isAnnotationPresent(ApiParam.class)) {
            return "query"; // by default
        }
        return "query"; // Default
    }
}
