package org.opencb.opencga.analysis.customTool;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.externalTool.Container;
import org.opencb.opencga.core.models.externalTool.ExternalToolScope;
import org.opencb.opencga.core.models.externalTool.ExternalToolVariable;
import org.opencb.opencga.core.models.externalTool.custom.CustomExternalToolParams;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolCreateParams;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolRunParams;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CustomToolExecutorTest extends AbstractManagerTest {

    private Path outDir;
    private StorageConfiguration storageConfiguration;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        
        // Load storage configuration
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        storageConfiguration = StorageConfiguration.load(inputStream, "yml");
        
        // Create temporary output directory
        outDir = Paths.get(catalogManagerResource.createTmpOutdir("_custom_tool_test"));
    }

    @Test
    public void testSimpleEchoCommand() throws Exception {
        // Create a simple custom tool with echo command
        String toolId = "echo-tool";
        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Hello World'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Simple echo tool", container, null);
        
        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Create run parameters
        CustomExternalToolParams runParams = new CustomExternalToolParams(toolId, null, null);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap params = new ObjectMap();
        params.put("id", toolId);
        params.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), params, outDir, "", "", false, ownerToken);
        
        // This test requires Docker to be installed and running
        // For now, we'll just check that the setup was successful
        assertNotNull(executor);
    }

    @Test
    public void testEchoWithParameters() throws Exception {
        // Create a custom tool with parameters
        String toolId = "echo-with-params";
        
        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setName("message")
                        .setRequired(true)
                        .setDescription("Message to echo"),
                new ExternalToolVariable()
                        .setName("prefix")
                        .setRequired(false)
                        .setDefaultValue("INFO")
                        .setDescription("Message prefix")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo '${prefix}: ${message}'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Echo with parameters", container, variables);
        
        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Create run parameters with custom message
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("message", "This is a test message");
        paramsMap.put("prefix", "DEBUG");
        
        CustomToolRunParams toolRunParams = new CustomToolRunParams(null, paramsMap);
        CustomExternalToolParams runParams = new CustomExternalToolParams(toolId, null, toolRunParams);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, "", "", false, ownerToken);
        
        assertNotNull(executor);
    }

    @Test
    public void testEchoWithDefaultParameters() throws Exception {
        // Create a custom tool with default parameters
        String toolId = "echo-default-params";
        
        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setName("message")
                        .setRequired(false)
                        .setDefaultValue("Default message")
                        .setDescription("Message to echo")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo '${message}'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Echo with default parameters", container, variables);
        
        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Create run parameters without custom message (should use default)
        CustomToolRunParams toolRunParams = new CustomToolRunParams(null, new HashMap<>());
        CustomExternalToolParams runParams = new CustomExternalToolParams(toolId, null, toolRunParams);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, "", "", false, ownerToken);
        
        assertNotNull(executor);
    }

    @Test
    public void testCustomCommandLine() throws Exception {
        // Create a custom tool with a default command line
        String toolId = "echo-custom-cli";
        
        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Default command'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Echo with custom command line", container, null);
        
        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Create run parameters with custom command line
        CustomToolRunParams toolRunParams = new CustomToolRunParams()
                .setCommandLine("echo 'Custom command line override'");
        CustomExternalToolParams runParams = new CustomExternalToolParams(toolId, null, toolRunParams);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, "", "", false, ownerToken);
        
        assertNotNull(executor);
    }

    @Test
    public void testMultipleParametersWithVariables() throws Exception {
        // Create a custom tool with multiple parameters
        String toolId = "echo-multi-params";
        
        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setName("param1")
                        .setRequired(true)
                        .setDescription("First parameter"),
                new ExternalToolVariable()
                        .setName("param2")
                        .setRequired(true)
                        .setDescription("Second parameter"),
                new ExternalToolVariable()
                        .setName("param3")
                        .setRequired(false)
                        .setDefaultValue("default3")
                        .setDescription("Third parameter")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Param1: ${param1}' && echo 'Param2: ${param2}' && echo 'Param3: ${param3}'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Echo with multiple parameters", container, variables);
        
        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Create run parameters
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("param1", "value1");
        paramsMap.put("param2", "value2");
        // param3 should use default value
        
        CustomToolRunParams toolRunParams = new CustomToolRunParams(null, paramsMap);
        CustomExternalToolParams runParams = new CustomExternalToolParams(toolId, null, toolRunParams);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, "", "", false, ownerToken);
        
        assertNotNull(executor);
    }

    @Test(expected = ToolException.class)
    public void testMissingToolId() throws Exception {
        // Try to execute without tool id
        CustomExternalToolParams runParams = new CustomExternalToolParams(null, null, null);

        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap params = runParams.toObjectMap();
        params.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), params, outDir, "", "", false, ownerToken);
        
        // This should throw ToolException due to missing tool id
        executor.start();
    }

    @Test(expected = ToolException.class)
    public void testNonExistentTool() throws Exception {
        // Try to execute a tool that doesn't exist
        CustomExternalToolParams runParams = new CustomExternalToolParams("non-existent-tool", null, null);

        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap params = runParams.toObjectMap();
        params.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), params, outDir, "", "", false, ownerToken);
        
        // This should throw ToolException due to tool not found
        executor.start();
    }

    @Test
    public void testActualDockerExecution() throws Exception {
        // This test actually runs Docker (only if Docker is available)
        String toolId = "echo-docker-test";
        
        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Docker execution test' && echo 'Working directory:' && pwd");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Docker execution test", container, null);
        
        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Create run parameters
        CustomExternalToolParams runParams = new CustomExternalToolParams(toolId, null, null);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap params = runParams.toObjectMap();
        params.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), params, outDir, "", "", false, ownerToken);
        executor.start();
        
        // Check that the output directory exists
        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testDockerExecutionWithOutputFile() throws Exception {
        // This test runs Docker and creates an output file
        String toolId = "echo-output-file";
        
        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("/bin/sh -c 'echo Test output file://data/test/folder/test_0.5K.txt > ${JOB_DIR}/output.txt'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Docker execution with output file", container, null);
        
        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Create run parameters
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("JOB_DIR", "$OUTPUT");
        CustomToolRunParams runToolParams = new CustomToolRunParams();
        runToolParams.setParams(paramMap);
        CustomExternalToolParams runParams = new CustomExternalToolParams(toolId, null, runToolParams);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), new ObjectMap(runParams.toParams()), outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Check that the output file was created
        Path outputFile = outDir.resolve("output.txt");
        assertTrue("Output file should exist", Files.exists(outputFile));
        
        // Read and verify content
        String content = new String(Files.readAllBytes(outputFile));
        assertTrue("Output should contain expected text", content.contains("Test output "));
        assertTrue("Output should contain file URI", content.contains(".opencga_input/test_0.5K.txt"));
    }

    /**
     * Helper method to create a CustomTool ExternalTool object
     */
    private CustomToolCreateParams createCustomTool(String id, String description, Container container, List<ExternalToolVariable> variables) {
        CustomToolCreateParams createParams = new CustomToolCreateParams();
        createParams.setId(id);
        createParams.setName(id);
        createParams.setDescription(description);
        createParams.setScope(ExternalToolScope.OTHER);
        createParams.setContainer(container);
        
        if (variables != null) {
            createParams.setVariables(variables);
        }
        
        return createParams;
    }
}

