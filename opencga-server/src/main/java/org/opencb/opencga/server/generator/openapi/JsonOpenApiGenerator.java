package org.opencb.opencga.server.generator.openapi;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.openapi.models.*;
import org.opencb.opencga.server.generator.openapi.common.SwaggerDefinitionGenerator;

import javax.ws.rs.*;
import java.util.*;

public class JsonOpenApiGenerator {

    public Swagger generateJsonOpenApi(ApiCommons apiCommons, String token) {
        List<Class<?>> classes = apiCommons.getApiClasses();
        List<Class<?>> beansDefinitions= new ArrayList<>();
        Swagger swagger = new Swagger();
        Info info = new Info();
        info.setTitle("OpenCGA RESTful Web Services");
        info.setDescription("OpenCGA RESTful Web Services API");
        info.setVersion(GitRepositoryState.getInstance().getBuildVersion());
        swagger.setInfo(info);
        swagger.setHost("test.app.zettagenomics.com");
        swagger.setBasePath("/opencga/webservices/rest");

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
            tags.add(new Tag(api.value(), api.description()));
            // Obtener ruta base de la clase
            javax.ws.rs.Path classPathAnnotation = clazz.getAnnotation(javax.ws.rs.Path.class);
            String basePath = classPathAnnotation.value();
            if (classPathAnnotation == null) {
                continue;
            }
            // Procesar métodos
            for (java.lang.reflect.Method wsmethod : clazz.getDeclaredMethods()) {
                ApiOperation apiOperation = wsmethod.getAnnotation(ApiOperation.class);
                if (apiOperation != null) {
                    // Crear operación Swagger
                    Method method = new Method();
                    method.setSummary(apiOperation.value());
                    method.setDescription(apiOperation.notes());
                    method.setTags(Collections.singletonList(api.value()));
                    Map<String,Object> responses=new HashMap<>();
                    responses.put("type", String.valueOf(apiOperation.response()));
                    if(apiOperation.response() instanceof Class){
                        beansDefinitions.add((Class) apiOperation.response());
                    }

                    method.getResponses().put("200", responses);

                    // Obtener el método HTTP
                    String httpMethod = extractHttpMethod(wsmethod);
                    if (httpMethod == null) continue;


                    Consumes consumes = wsmethod.getAnnotation(Consumes.class);
                    // Extraer parámetros
                    List<Parameter> parameters = extractParameters(wsmethod, token);
                    method.setParameters(parameters);
                    if (consumes != null){
                        method.getConsumes().addAll(Arrays.asList(consumes.value()));
                    }
                    method.getProduces().add(String.valueOf(apiOperation.response()));
                    // Ruta completa del endpoint
                    javax.ws.rs.Path methodPathAnnotation = wsmethod.getAnnotation(javax.ws.rs.Path.class);
                    String fullPath = basePath + (methodPathAnnotation != null ? methodPathAnnotation.value() : "");
                    method.setOperationId(methodPathAnnotation != null ? methodPathAnnotation.value() : "");

                    // Crear o actualizar el Path
                    paths.put(fullPath, new HashMap<>());
                    paths.get(fullPath).put(httpMethod, method);
                }
            }
        }
        Map<String, Definition> definitions = SwaggerDefinitionGenerator.getDefinitions(beansDefinitions);
        swagger.setTags(tags);
        swagger.setPaths(paths);
        swagger.setDefinitions(definitions);
        return swagger;
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
        }
        return null;
    }

    private List<Parameter> extractParameters(java.lang.reflect.Method method, String token) {
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
                parameter.setDefaultValue(implicitParam.defaultValue());
                parameters.add(parameter);
            }
        }

        // Procesar parámetros individuales del método
        for (java.lang.reflect.Parameter methodParam : method.getParameters()) {
            // Procesar ApiParam
            ApiParam apiParam = methodParam.getAnnotation(ApiParam.class);
            if (apiParam != null) {
                Parameter parameter = new Parameter();
                parameter.setName(apiParam.value());
                parameter.setDescription(apiParam.value());
                parameter.setRequired(apiParam.required());
                parameter.setType(methodParam.getType().getSimpleName().toLowerCase(Locale.ROOT));
                parameter.setIn(determineParameterLocation(methodParam));
                parameters.add(parameter);
            }

            // Procesar PathParam
            PathParam pathParam = methodParam.getAnnotation(PathParam.class);
            if (pathParam != null) {
                Parameter parameter = new Parameter();
                parameter.setName(pathParam.value());
                parameter.setIn("path");
                parameter.setDescription("Path parameter: " + pathParam.value());
                parameter.setRequired(true);
                parameter.setType(methodParam.getType().getSimpleName().toLowerCase(Locale.ROOT));
                parameters.add(parameter);
            }

            // Procesar QueryParam
            QueryParam queryParam = methodParam.getAnnotation(QueryParam.class);
            if (queryParam != null) {
                Parameter parameter = new Parameter();
                parameter.setName(queryParam.value());
                parameter.setIn("query");
                parameter.setDescription("Query parameter: " + queryParam.value());
                parameter.setRequired(false); // Por defecto, no requerido
                parameter.setType(methodParam.getType().getSimpleName().toLowerCase(Locale.ROOT));
                parameters.add(parameter);
            }
        }

        // Añadir encabezado Authorization con el token preconfigurado
        Parameter authorizationHeader = new Parameter();
        authorizationHeader.setName("Authorization");
        authorizationHeader.setIn("header");
        authorizationHeader.setDescription("Bearer token for authorization");
        authorizationHeader.setRequired(true);
        authorizationHeader.setType("string");
        authorizationHeader.setDefaultValue("Bearer " + token);
        parameters.add(authorizationHeader);

        return parameters;
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
