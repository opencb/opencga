package org.opencb.opencga.server.generator.openapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.openapi.models.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.*;

import static org.junit.Assert.*;

import org.opencb.opencga.server.generator.openapi.models.*;

@Category(ShortTests.class)
public class JsonOpenApiGeneratorTest {


    private static Swagger swagger;
    private static ApiCommons apiCommons;

    @BeforeClass
    public static void initSwagger() throws Exception {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        apiCommons = new ApiCommonsImpl() {
//            @Override
//            public List<Class<?>> getApiClasses() {
//                return Arrays.asList(
//                        ExternalToolWSServer.class
//                );
//            }
        };
        swagger = generator.generateJsonOpenApi(
                apiCommons,
                "ANYVALIDTOKEN",
                "https://test.app.zettagenomics.com/task-swagger/opencga",
                "v2",
                "test@germline:platinum"
        );
        assertNotNull("Swagger should have been generated", swagger);
    }


    @Test
    public void testGenerateJsonOpenApi(){
        String basePath="/task-swagger/opencga/webservices/rest";
        String host="test.app.zettagenomics.com";
        int numCategories = apiCommons.getApiClasses().size();

        assertEquals("Expected basePath to be '" + basePath + "'",basePath, swagger.getBasePath());
        assertEquals("Expected host to be '" + host + "'",host, swagger.getHost());
        assertTrue("Expected number of categories to be " + numCategories, numCategories == swagger.getTags().size());
    }

    /**
     * Verifica que las tags generadas aparezcan en el orden esperado.
     */
    @Test
    public void tagsOrderMatchesExpected() throws Exception {
        // The order of the tags is important for the UI, so we need to ensure that they are generated in the expected order.
        List<String> actualTags = swagger.getTags().stream()
                .map(Tag::getName)
                .collect(Collectors.toList());

        List<String> expectedTags = Arrays.asList(
                "organizations",
                "Users",
                "Projects",
                "Studies",
                "Files",
                "Jobs",
                "Workflows",
                "User tools",
                "Samples",
                "Individuals",
                "Families",
                "Cohorts",
                "Disease Panels",
                "Analysis - Alignment",
                "Analysis - Variant",
                "Analysis - Clinical",
                "Operations - Variant Storage",
                "Meta",
                "Admin",
                "Ga4gh"
        );

        for (int i = 0; i < expectedTags.size(); i++) {
            String exp = expectedTags.get(i).toLowerCase();
            String act = actualTags.get(i).toLowerCase();
            String msg = "Tag '" + actualTags.get(i) + "' should be '" + expectedTags.get(i)
                    + "' ::: Tag list should match expected order";
            // JUnit4: assertEquals(message, expected, actual)
            assertEquals(msg, exp, act);
        }
    }



    /**
     * The host and basePath should be correctly extracted
     * whether or not the hostUrl contains extra segments.
     */
    @Test
    public void hostAndBasePathAreExtractedCorrectly() {
        ApiCommons noApis = new ApiCommonsTestImpl();
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();

        Swagger swaggerNoPath = generator.generateJsonOpenApi(
                noApis, "", "https://demo.server.com", "v1", ""
        );
        assertEquals("demo.server.com", swaggerNoPath.getHost());
        assertEquals("/webservices/rest", swaggerNoPath.getBasePath());

        Swagger swaggerWithPath = generator.generateJsonOpenApi(
                noApis, "", "https://demo.server.com/api", "v1", ""
        );
        assertEquals("demo.server.com", swaggerWithPath.getHost());
        assertEquals("/api/webservices/rest", swaggerWithPath.getBasePath());
    }

    @Test
     public void generateFile() throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            String swaggerJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(swagger);

            // Save the JSON to a file
            Path dir = Paths.get("target/test-data/swagger");
            Files.createDirectories(dir);
            FileUtils.writeStringToFile(dir.resolve("swagger.json").toFile(), swaggerJson, StandardCharsets.UTF_8);
            System.out.println("Created TestFile = " + dir.toAbsolutePath()+ "/swagger.json");
            assertTrue("File should exist", Files.exists(dir.resolve("swagger.json")));
    }

    @Test
    public void filesDownloadPathExists() {
        Map<String,Method> downloadPath = swagger.getPaths().get("/v2/files/{file}/download");
        assertNotNull("Path '/v2/files/{file}/download' should be defined", downloadPath);
    }

    @Test
    public void filesDownloadProducesOctetStream() {
        Map<String,Method> downloadPath = swagger.getPaths().get("/v2/files/{file}/download");
        assertNotNull("Path '/v2/files/{file}/download' should be defined", downloadPath);
        Method method = downloadPath.get("get");
        List<String> produces = method.getProduces();
        assertTrue(
                "GET /v2/files/{file}/download should produce 'application/octet-stream'",
                produces.contains("application/octet-stream")
        );
    }

    @Test
    public void filesValidateExistsToken() {
        Map<String,Method> downloadPath = swagger.getPaths().get("/v2/files/{file}/download");
        assertNotNull("Path '/v2/files/{file}/download' should be defined", downloadPath);
        Method method = downloadPath.get("get");
        List<Map<String, List<String>>> security = method.getSecurity();
        assertNotNull("GET /v2/files/{file}/download should have security defined", security);
        assertEquals("GET /v2/files/{file}/download should have one security definition", 1, security.size());
        assertNotNull("GET /v2/files/{file}/download should have BearerAuth defined",  security.get(0).get("BearerAuth"));
        assertEquals("GET /v2/files/{file}/download should have one security definition", security.get(0).get("BearerAuth").get(0), "Bearer ANYVALIDTOKEN");
    }

    public class ApiCommonsTestImpl implements ApiCommons {

        @Override
        public List<Class<?>> getApiClasses() {
            return Collections.emptyList();
        }

        @Override
        public String getVersion() {
            return "v1";
        }
    }
}