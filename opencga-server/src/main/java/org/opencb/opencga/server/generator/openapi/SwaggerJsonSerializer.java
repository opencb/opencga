package org.opencb.opencga.server.generator.openapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.models.openapi.Swagger;

public class SwaggerJsonSerializer {
    public static void main(String[] args) throws Exception {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        Swagger swagger = generator.generateJsonOpenApi(new ApiCommonsImpl());

        ObjectMapper mapper = new ObjectMapper();
        String swaggerJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(swagger);

        System.out.println(swaggerJson);
    }
}

