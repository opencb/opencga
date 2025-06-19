package org.opencb.opencga.server.generator.openapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.openapi.models.Swagger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Category(ShortTests.class)
public class JsonOpenApiGeneratorTest {

    @Test
    public void testGenerateJsonOpenApi() throws Exception {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        ApiCommons apiCommons = new ApiCommonsImpl();
//        ApiCommons apiCommons = () -> Arrays.asList(IndividualWSServer.class);
        Swagger swagger = generator.generateJsonOpenApi(apiCommons,
                "<some_valid_token>",
                "reference",
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