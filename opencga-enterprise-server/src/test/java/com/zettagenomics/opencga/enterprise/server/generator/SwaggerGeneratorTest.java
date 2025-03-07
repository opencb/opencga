package com.zettagenomics.opencga.enterprise.server.generator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zettagenomics.opencga.enterprise.server.EnterpriseRestServer;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.openapi.JsonOpenApiGenerator;
import org.opencb.opencga.server.generator.openapi.models.Swagger;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;

import static org.jasig.cas.client.util.CommonUtils.assertTrue;
import java.nio.file.Files;

public class SwaggerGeneratorTest {

    @Ignore
    @Test
    public void runServerTest() throws Exception {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        Swagger swagger = generator.generateJsonOpenApi(new EnterpriseApiCommonsImpl(), "El token va aquí", "task-xxxx");
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String swaggerJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(swagger);

       // System.out.println(swaggerJson);
        // Definir la ruta del archivo (por ejemplo, en target/)
        File outputFile = new File("target/swagger_output.json");

        // Asegurar que el directorio existe
        outputFile.getParentFile().mkdirs();

        // Escribir el contenido en el archivo
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(swaggerJson);
        }


        // Imprimir la ubicación para que puedas encontrarlo fácilmente
        System.out.println("Archivo generado en: " + outputFile.getAbsolutePath());

    }

}