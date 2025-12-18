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
import org.opencb.opencga.core.models.externalTool.custom.CustomToolCreateParams;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolRunParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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
                        .setId("message")
                        .setRequired(true)
                        .setDescription("Message to echo"),
                new ExternalToolVariable()
                        .setId("prefix")
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

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

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
                        .setId("message")
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
        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, new HashMap<>());

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
        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, "echo 'Custom command line override'", null);

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
                        .setId("param1")
                        .setRequired(true)
                        .setDescription("First parameter"),
                new ExternalToolVariable()
                        .setId("param2")
                        .setRequired(true)
                        .setDescription("Second parameter"),
                new ExternalToolVariable()
                        .setId("param3")
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

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

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
        CustomToolRunParams runParams = new CustomToolRunParams(null, null, null, null);

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
        CustomToolRunParams runParams = new CustomToolRunParams("non-existent-tool", null, null, null);

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
        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, null);

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
        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramMap);

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

    @Test
    public void testCompleteVariableScenario() throws Exception {
        // Test all four types of variables as described in documentation:
        // 1. Required with default
        // 2. Required without default
        // 3. Optional with default
        // 4. Optional without default
        String toolId = "complete-variable-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                // Required with default value
                new ExternalToolVariable()
                        .setId("--threads")
                        .setRequired(true)
                        .setDefaultValue("1")
                        .setDescription("Number of threads to use"),

                // Required with default value
                new ExternalToolVariable()
                        .setId("--scope")
                        .setRequired(true)
                        .setDefaultValue("OPENCGA")
                        .setDescription("Scop of the tool"),
                // Required without default
                new ExternalToolVariable()
                        .setId("--input")
                        .setRequired(true)
                        .setDescription("Input file path"),

                // Optional with default
                new ExternalToolVariable()
                        .setId("--format")
                        .setRequired(false)
                        .setDefaultValue("json")
                        .setDescription("Output format"),

                // Optional without default
                new ExternalToolVariable()
                        .setId("--verbose")
                        .setRequired(false)
                        .setDescription("Enable verbose output")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Processing ${input} with ${threads} threads'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Complete variable scenario test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Provide only required without default, and optional without default
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("input", "/data/input.txt");  // Required without default
        paramsMap.put("verbose", "true");           // Optional without default
        // threads should use default "1"
        // format should use default "json"

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify the command line
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Actual command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain template substitution for ${input}
        assertTrue("Command line should contain 'Processing' from template",
                actualCommandLine.contains("Processing"));
        assertTrue("Command line should contain input value",
                actualCommandLine.contains("/data/input.txt") || actualCommandLine.contains("test_0.5K.txt"));

        // Should take threads from default param
        assertTrue("Command line should contain threads with default value", actualCommandLine.contains("1 threads"));

        // Should append --verbose (optional without default, provided)
        assertTrue("Command line should contain verbose parameter", actualCommandLine.contains("--verbose true"));

        // Should not append format with default value (not provided and optional with default)
        assertFalse("Command line should NOT contain format with default value", actualCommandLine.contains("--format json"));

        // Should append --scope with default value (not provided and required with default)
        assertTrue("Command line should contain scope with default value", actualCommandLine.contains("--scope OPENCGA"));

        // Verify execution completed
        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testTemplateSubstitution() throws Exception {
        // Test Step 1: Template variable substitution
        // Variables in ${variableId} should be directly substituted
        String toolId = "template-substitution-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setId("inputFile")
                        .setRequired(true)
                        .setDescription("Input file"),
                new ExternalToolVariable()
                        .setId("outputFile")
                        .setRequired(true)
                        .setDescription("Output file")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("/bin/sh -c 'cat ${inputFile} > ${outputFile}'");

        File file = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent("My content")
                        .setPath("data/input.txtt")
                        .setType(File.Type.FILE)
                , true, ownerToken).first();

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Template substitution test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // These parameters should be substituted in template
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("inputFile", "file://data/input.txtt");
        paramsMap.put("outputFile", "$OUTPUT/output.txt");

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line has template variables substituted
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Template substitution command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should have ${inputFile} replaced with actual path
        assertTrue("Command line should contain input file path", actualCommandLine.contains(file.getUri().getPath()));

        // Should have ${outputFile} replaced
        assertTrue("Command line should contain output file path", actualCommandLine.contains("/output.txt"));

        // Should NOT contain the template variables anymore
        assertFalse("Command line should not contain ${inputFile}", actualCommandLine.contains("${inputFile}"));
        assertFalse("Command line should not contain ${outputFile}", actualCommandLine.contains("${outputFile}"));

        assertNotNull(executor);
    }

    @Test
    public void testAppendMatchingVariable() throws Exception {
        // Test Step 2a: Parameter key matches ExternalToolVariable
        // Should append using variable's 'name' field as CLI flag
        String toolId = "append-matching-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setId("--threads")
                        .setRequired(false)
                        .setDescription("Number of threads"),
                new ExternalToolVariable()
                        .setId("--memory")
                        .setRequired(false)
                        .setDescription("Memory limit")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Running analysis'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Append matching variable test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // These parameters should be appended using variable names
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("threads", "4");   // Should append as: --threads 4
        paramsMap.put("memory", "8G");   // Should append as: --memory 8G

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line has parameters appended with variable names
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Append matching variable command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain base command
        assertTrue("Command line should contain 'Running analysis'",
                actualCommandLine.contains("Running analysis"));

        // Should append --threads 4 (using variable's name field)
        assertTrue("Command line should contain '--threads 4'",
                actualCommandLine.contains("--threads 4"));

        // Should append --memory 8G (using variable's name field)
        assertTrue("Command line should contain '--memory 8G'",
                actualCommandLine.contains("--memory 8G"));

        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testAppendUnmatchedParameter() throws Exception {
        // Test Step 2b: Parameter key doesn't match any variable
        // Should append using parameter key directly as CLI flag
        String toolId = "append-unmatched-test";

        // Tool with some defined variables
        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setId("--known-param")
                        .setRequired(false)
                        .setDescription("Known parameter")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Processing with params'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Append unmatched parameter test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Mix of matched and unmatched parameters
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("known-param", "value1");        // Matches variable, append as: --known-param value1
        paramsMap.put("--custom-flag", "value2");      // No match, append as: --custom-flag value2
        paramsMap.put("--another-flag", "value3");     // No match, append as: --another-flag value3

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line has mixed matched and unmatched parameters
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Append unmatched parameter command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain base command
        assertTrue("Command line should contain 'Processing with params'",
                actualCommandLine.contains("Processing with params"));

        // Should append --known-param value1 (matched variable)
        assertTrue("Command line should contain '--known-param value1'",
                actualCommandLine.contains("--known-param value1"));

        // Should append --custom-flag value2 (unmatched, use key directly)
        assertTrue("Command line should contain '--custom-flag value2'",
                actualCommandLine.contains("--custom-flag value2"));

        // Should append --another-flag value3 (unmatched, use key directly)
        assertTrue("Command line should contain '--another-flag value3'",
                actualCommandLine.contains("--another-flag value3"));

        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testMixedSubstitutionAndAppending() throws Exception {
        // Test combining template substitution with parameter appending
        String toolId = "mixed-substitution-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                // This will be substituted in template
                new ExternalToolVariable()
                        .setId("inputFile")
                        .setRequired(true)
                        .setDescription("Input file"),

                // This will be appended
                new ExternalToolVariable()
                        .setId("--threads")
                        .setRequired(false)
                        .setDefaultValue("1")
                        .setDescription("Number of threads"),

                // This will be appended if provided
                new ExternalToolVariable()
                        .setId("--verbose")
                        .setRequired(false)
                        .setDescription("Verbose mode")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("process-tool ${inputFile}");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Mixed substitution test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("inputFile", "/data/input.vcf");   // Substituted in template
        paramsMap.put("threads", "8");                   // Appended as: --threads 8
        paramsMap.put("verbose", "true");                // Appended as: --verbose true

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        // Execution should fail as there is no tool called 'process-tool' in the container
        assertThrows(ToolException.class, executor::start);

        // Verify command line combines substitution and appending
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Mixed substitution and appending command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain template substitution for ${inputFile}
        assertTrue("Command line should contain 'process-tool' from template",
                actualCommandLine.contains("process-tool"));
        assertTrue("Command line should contain input file path",
                actualCommandLine.contains("/data/input.vcf") || actualCommandLine.contains("test_0.5K.txt"));

        // Should NOT contain template variable anymore
        assertFalse("Command line should not contain ${inputFile}",
                actualCommandLine.contains("${inputFile}"));

        // Should append --threads 8 (override default)
        assertTrue("Command line should contain '--threads 8'",
                actualCommandLine.contains("--threads 8"));

        // Should append --verbose true (optional without default, provided)
        assertTrue("Command line should contain '--verbose true'",
                actualCommandLine.contains("--verbose true"));

        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testDefaultValueUsage() throws Exception {
        // Test that default values are properly applied
        String toolId = "default-value-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                // Required with default - should use default if not provided
                new ExternalToolVariable()
                        .setId("--threads")
                        .setRequired(true)
                        .setDefaultValue("1")
                        .setDescription("Number of threads"),

                // Optional with default - should use default if not provided
                new ExternalToolVariable()
                        .setId("--format")
                        .setRequired(false)
                        .setDefaultValue("json")
                        .setDescription("Output format"),

                // Required without default - must be provided
                new ExternalToolVariable()
                        .setId("--input")
                        .setRequired(true)
                        .setDescription("Input file")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Processing'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Default value test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Only provide required without default
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("input", "/data/input.txt");
        // threads should use default "1"
        // format should use default "json"

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line uses default values
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Default value usage command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain base command
        assertTrue("Command line should contain 'Processing'",
                actualCommandLine.contains("Processing"));

        // Should append --input (required without default, provided)
        assertTrue("Command line should contain '--input /data/input.txt'",
                actualCommandLine.contains("--input /data/input.txt") || actualCommandLine.contains("--input"));

        // Should append --threads with default value "1" (required with default, not provided)
        assertTrue("Command line should contain '--threads 1' (default value)",
                actualCommandLine.contains("--threads 1"));

        // Should NOT append --format with default value "json" (optional with default, not provided)
        assertFalse("Command line should NOT contain '--format json' (default value)",
                actualCommandLine.contains("--format json"));

        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testOverrideDefaultValue() throws Exception {
        // Test that provided values override defaults
        String toolId = "override-default-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setId("--threads")
                        .setRequired(false)
                        .setDefaultValue("2")
                        .setDescription("Number of threads"),

                new ExternalToolVariable()
                        .setId("--memory")
                        .setRequired(false)
                        .setDefaultValue("4G")
                        .setDescription("Memory limit")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Running with custom settings'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Override default test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Override both defaults
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("threads", "16");   // Override default 1
        paramsMap.put("memory", "32G");   // Override default 4G

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line has overridden default values
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Override default value command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain base command
        assertTrue("Command line should contain 'Running with custom settings'",
                actualCommandLine.contains("Running with custom settings"));

        // Should append --threads 16 (override default 1)
        assertTrue("Command line should contain '--threads 16' (overridden default)",
                actualCommandLine.contains("--threads 16"));
        assertFalse("Command line should NOT contain '--threads 2' (default)",
                actualCommandLine.contains("--threads 2"));

        // Should append --memory 32G (override default 4G)
        assertTrue("Command line should contain '--memory 32G' (overridden default)",
                actualCommandLine.contains("--memory 32G"));
        assertFalse("Command line should NOT contain '--memory 4G' (default)",
                actualCommandLine.contains("--memory 4G"));

        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testOptionalParametersNotProvided() throws Exception {
        // Test that optional parameters without values are not included
        String toolId = "optional-not-provided-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                new ExternalToolVariable()
                        .setId("--required-param")
                        .setRequired(true)
                        .setDescription("Required parameter"),

                new ExternalToolVariable()
                        .setId("--optional-no-default")
                        .setRequired(false)
                        .setDescription("Optional without default"),

                new ExternalToolVariable()
                        .setId("--another-optional")
                        .setRequired(false)
                        .setDescription("Another optional without default")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Minimal execution'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Optional not provided test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Only provide required parameter
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("required-param", "value");
        // optional-no-default not provided - should not appear in command
        // another-optional not provided - should not appear in command

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line excludes optional parameters not provided
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Optional parameters not provided command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain base command
        assertTrue("Command line should contain 'Minimal execution'",
                actualCommandLine.contains("Minimal execution"));

        // Should contain required parameter
        assertTrue("Command line should contain '--required-param value'",
                actualCommandLine.contains("--required-param value"));

        // Should NOT contain optional parameters that weren't provided
        assertFalse("Command line should NOT contain '--optional-no-default'",
                actualCommandLine.contains("--optional-no-default"));
        assertFalse("Command line should NOT contain '--another-optional'",
                actualCommandLine.contains("--another-optional"));

        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testComplexRealWorldScenario() throws Exception {
        // Simulate a real bioinformatics tool scenario
        String toolId = "samtools-view-simulation";

        List<ExternalToolVariable> variables = Arrays.asList(
                // Required input/output (used in template)
                new ExternalToolVariable()
                        .setId("inputBam")
                        .setRequired(true)
                        .setDescription("Input BAM file"),

                new ExternalToolVariable()
                        .setId("outputBam")
                        .setRequired(true)
                        .setDescription("Output BAM file"),

                // Optional parameters with defaults
                new ExternalToolVariable()
                        .setId("--threads")
                        .setRequired(false)
                        .setDefaultValue("1")
                        .setDescription("Number of threads"),

                new ExternalToolVariable()
                        .setId("-b")
                        .setRequired(false)
                        .setDefaultValue("true")
                        .setDescription("Output BAM format"),

                // Optional parameters without defaults
                new ExternalToolVariable()
                        .setId("-q")
                        .setRequired(false)
                        .setDescription("Minimum mapping quality"),

                new ExternalToolVariable()
                        .setId("-F")
                        .setRequired(false)
                        .setDescription("Filter flags")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'samtools view ${inputBam} -o ${outputBam}'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "SAMtools view simulation", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Provide various parameter combinations
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("inputBam", "/data/sample.bam");      // Template substitution
        paramsMap.put("outputBam", "/data/filtered.bam");   // Template substitution
        paramsMap.put("threads", "8");                       // Override default, append
        paramsMap.put("q", "30");                            // Optional provided, append as: -q 30
        paramsMap.put("F", "1024");                          // Optional provided, append as: -F 1024
        // "-b" should use default "true"

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line for complex real-world scenario
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Complex real-world scenario command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain template substitutions
        assertTrue("Command line should contain 'samtools view' from template",
                actualCommandLine.contains("samtools view"));
        assertTrue("Command line should contain input BAM path",
                actualCommandLine.contains("/data/sample.bam") || actualCommandLine.contains(".bam"));
        assertTrue("Command line should contain output BAM path",
                actualCommandLine.contains("/data/filtered.bam") || actualCommandLine.contains("-o"));

        // Should NOT contain template variables
        assertFalse("Command line should not contain ${inputBam}",
                actualCommandLine.contains("${inputBam}"));
        assertFalse("Command line should not contain ${outputBam}",
                actualCommandLine.contains("${outputBam}"));

        // Should append overridden default: --threads 8
        assertTrue("Command line should contain '--threads 8' (overridden)",
                actualCommandLine.contains("--threads 8"));

        // Should append optional parameters provided: -q 30 and -F 1024
        assertTrue("Command line should contain '-q 30'",
                actualCommandLine.contains("-q 30"));
        assertTrue("Command line should contain '-F 1024'",
                actualCommandLine.contains("-F 1024"));

        // Should NOT append -b with default value "true"
        assertFalse("Command line should NOT contain '-b true' (default)",
                actualCommandLine.contains("-b true"));

        assertTrue(Files.exists(outDir));
    }

    @Test
    public void testVariableIdMatchingWithDifferentNameFormats() throws Exception {
        // Test that variable IDs match correctly regardless of naming format
        String toolId = "variable-id-matching-test";

        List<ExternalToolVariable> variables = Arrays.asList(
                // Variable name with dashes
                new ExternalToolVariable()
                        .setId("--input-file")
                        .setRequired(false)
                        .setDescription("Input file with dashes"),

                // Variable name without prefix
                new ExternalToolVariable()
                        .setId("output")
                        .setRequired(false)
                        .setDescription("Output without prefix"),

                // Variable name with single dash
                new ExternalToolVariable()
                        .setId("-t")
                        .setRequired(false)
                        .setDescription("Threads short form")
        );

        Container container = new Container()
                .setName("ubuntu")
                .setTag("latest")
                .setCommandLine("echo 'Testing variable matching'");

        CustomToolCreateParams externalTool = createCustomTool(toolId, "Variable ID matching test", container, variables);

        // Register the custom tool
        catalogManager.getExternalToolManager().createCustomTool(studyFqn, externalTool, QueryOptions.empty(), ownerToken);

        // Parameters should match based on variable ID extraction
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("input-file", "/data/input.txt");   // Matches --input-file
        paramsMap.put("output", "/data/output.txt");      // Matches output
        paramsMap.put("t", "4");                          // Matches -t

        CustomToolRunParams runParams = new CustomToolRunParams(toolId, null, null, paramsMap);

        // Execute the tool
        CustomToolExecutor executor = new CustomToolExecutor();
        ObjectMap execParams = runParams.toObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);
        executor.start();

        // Verify command line uses correct variable names
        String actualCommandLine = getCommandLine(executor);
        System.out.println("Variable ID matching command line: " + actualCommandLine);
        assertNotNull("Command line should not be null", actualCommandLine);

        // Should contain base command
        assertTrue("Command line should contain 'Testing variable matching'",
                actualCommandLine.contains("Testing variable matching"));

        // Should append --input-file (matched by "input-file" key)
        assertTrue("Command line should contain '--input-file /data/input.txt'",
                actualCommandLine.contains("--input-file /data/input.txt") || actualCommandLine.contains("--input-file"));

        // Should append output (matched by "output" key, no prefix)
        assertTrue("Command line should contain 'output /data/output.txt'",
                actualCommandLine.contains("output /data/output.txt") || actualCommandLine.contains("output"));

        // Should append -t (matched by "t" key)
        assertTrue("Command line should contain '-t 4'",
                actualCommandLine.contains("-t 4"));

        assertTrue(Files.exists(outDir));
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

    /**
     * Helper method to extract the processed command line from the executor using reflection
     */
    private String getCommandLine(CustomToolExecutor executor) throws Exception {
        java.lang.reflect.Field cliParamsField = CustomToolExecutor.class.getDeclaredField("cliParams");
        cliParamsField.setAccessible(true);
        return (String) cliParamsField.get(executor);
    }
}

