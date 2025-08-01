package com.zettagenomics.opencga.enterprise.server.generator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zettagenomics.opencga.enterprise.server.EnterpriseRestServer;
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.openapi.JsonOpenApiGenerator;
import org.opencb.opencga.server.generator.openapi.models.Swagger;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.jasig.cas.client.util.CommonUtils.assertTrue;
import java.nio.file.Files;

public class SwaggerGeneratorTest {

    @Ignore
    @Test
    public void runServerTest() throws Exception {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        ApiCommons apiCommons = new EnterpriseApiCommonsImpl();
//        ApiCommons apiCommons = () -> Arrays.asList(IndividualWSServer.class);
        Swagger swagger = generator.generateJsonOpenApi(apiCommons,
                "<some_valid_token>",
                "test.app.zettagenomics.com",
                "v2", "OpencgaStudy");

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String swaggerJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(swagger);
//        System.out.println(swaggerJson);
        // Save the JSON to a file
        Path dir = Paths.get("target/test-data/swagger");
        Files.createDirectories(dir);
        FileUtils.writeStringToFile(dir.resolve("swagger.json").toFile(), swaggerJson, StandardCharsets.UTF_8);
        System.out.println("dir.toAbsolutePath() = " + dir.toAbsolutePath());

    }

}