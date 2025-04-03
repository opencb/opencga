package org.opencb.opencga.server.generator.openapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.openapi.models.Swagger;

public class SwaggerJsonSerializer {
    public static void main(String[] args) throws Exception {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        Swagger swagger = generator.generateJsonOpenApi(new ApiCommonsImpl(), "El token va aquí", "task-xxxx","test.app.zettagenomics.com");

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String swaggerJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(swagger);
        System.out.println(swaggerJson);
    }
}

