package org.opencb.opencga.server.generator.openapi;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiImplicitParam;
import org.opencb.opencga.core.tools.annotations.ApiImplicitParams;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.models.openapi.*;

import javax.ws.rs.*;
import java.util.*;

public class JsonOpenApiGenerator {

    public Swagger generateJsonOpenApi(ApiCommons apiCommons) {
        List<Class<?>> classes = apiCommons.getApiClasses();
                Swagger swagger = new Swagger();
                Info info = new Info();
                info.setTitle("OpenCGA RESTful Web Services");
                info.setDescription("OpenCGA RESTful Web Services API");
                info.setVersion(GitRepositoryState.getInstance().getBuildVersion());
                swagger.setInfo(info);
                swagger.setHost("https://test.app.zettagenomics.com/");
                swagger.setBasePath("/opencga/webservices/rest/v2/");
        Map<String, Map<String,Method>> paths = new HashMap<>();
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
                    method.getResponses().put("200", responses);

                    // Obtener el método HTTP
                    String httpMethod = extractHttpMethod(wsmethod);
                    if (httpMethod == null) continue;


                    Consumes consumes = wsmethod.getAnnotation(Consumes.class);
                    // Extraer parámetros
                    List<Parameter> parameters = extractParameters(wsmethod);
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
        swagger.setTags(tags);
        swagger.setPaths(paths);
        return swagger;
    }

    private String extractHttpMethod(java.lang.reflect.Method method) {
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

    private List<Parameter> extractParameters(java.lang.reflect.Method method) {
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
