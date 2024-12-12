package org.opencb.opencga.server.generator.openapi;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiImplicitParam;
import org.opencb.opencga.core.tools.annotations.ApiImplicitParams;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.models.openapi.*;
import org.opencb.opencga.server.generator.models.openapi.Path;

import javax.ws.rs.*;
import java.lang.reflect.Method;
import java.util.*;

public class JsonOpenApiGenerator {

    public Swagger generateJsonOpenApi(ApiCommons apiCommons) {
        List<Class<?>> classes = apiCommons.getApiClasses();
                Swagger swagger = new Swagger();
        Map<String, Path> paths = new HashMap<>();
        List<Tag> tags = new ArrayList<>();

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
            for (Method method : clazz.getDeclaredMethods()) {
                ApiOperation apiOperation = method.getAnnotation(ApiOperation.class);
                if (apiOperation != null) {
                    // Crear operación Swagger
                    Operation operation = new Operation();
                    operation.setSummary(apiOperation.value());
                    operation.setDescription(apiOperation.notes());
                    operation.setTags(Collections.singletonList(api.value()));
                    operation.setResponses(Collections.singletonMap("200", new Response()));

                    // Obtener el método HTTP
                    String httpMethod = extractHttpMethod(method);
                    if (httpMethod == null) continue;

                    // Extraer parámetros
                    List<Parameter> parameters = extractParameters(method);
                    operation.setParameters(parameters);

                    // Ruta completa del endpoint
                    javax.ws.rs.Path methodPathAnnotation = method.getAnnotation(javax.ws.rs.Path.class);
                    String fullPath = basePath + (methodPathAnnotation != null ? methodPathAnnotation.value() : "");


                    // Crear o actualizar el Path
                    paths.put(fullPath, new Path());
                    paths.get(fullPath).getOperations().put(httpMethod.toLowerCase(), operation);
                }
            }
        }
        swagger.setTags(tags);
        swagger.setPaths(paths);
        return swagger;
    }

    private String extractHttpMethod(Method method) {
        if (method.isAnnotationPresent(GET.class)) {
            return "GET";
        } else if (method.isAnnotationPresent(POST.class)) {
            return "POST";
        } else if (method.isAnnotationPresent(PUT.class)) {
            return "PUT";
        } else if (method.isAnnotationPresent(DELETE.class)) {
            return "DELETE";
        }
        return null;
    }

    private List<Parameter> extractParameters(Method method) {
        List<Parameter> parameters = new ArrayList<>();
        ApiImplicitParams implicitParams = method.getAnnotation(ApiImplicitParams.class);
        if (implicitParams != null) {
            for (ApiImplicitParam implicitParam : implicitParams.value()) {
                Parameter parameter = new Parameter();
                parameter.setName(implicitParam.name());
                parameter.setIn(implicitParam.paramType());
                parameter.setDescription(implicitParam.value());
                parameter.setRequired(implicitParam.required());
                parameter.setType(implicitParam.dataType());
                parameters.add(parameter);
            }
        }
        return parameters;
    }
}
