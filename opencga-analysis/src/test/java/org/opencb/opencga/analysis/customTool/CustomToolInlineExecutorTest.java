package org.opencb.opencga.analysis.customTool;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.externalTool.Container;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolInlineParams;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test class for CustomToolInlineExecutor.
 * Contains both real Docker execution tests and mocked tests.
 */
@Category(MediumTests.class)
public class CustomToolInlineExecutorTest extends AbstractManagerTest {

    private Path outDir;
    private StorageConfiguration storageConfiguration;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // Load storage configuration
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        storageConfiguration = StorageConfiguration.load(inputStream, "yml");

        // Create temporary output directory
        outDir = Paths.get(catalogManagerResource.createTmpOutdir("_custom_tool_inline_test"));
    }

    /**
     * Test with REAL Docker execution.
     * This test requires Docker to be installed and running.
     * Tests that $OUTPUT variable is properly replaced and command executes correctly.
     */
    @Test
    public void testOutputVariableReplacementWithRealDocker() throws Exception {
        // Check if Docker is available
        if (!isDockerAvailable()) {
            System.out.println("Docker is not available. Skipping real Docker test.");
            Assume.assumeTrue("Docker is not available", false);
            return;
        }

        System.out.println("=== Running REAL Docker Test ===");

        // Create container with alpine (smaller image) and command that uses $OUTPUT
        Container container = new Container()
                .setName("alpine")
                .setTag("latest")
                .setCommandLine("sh -c 'echo -e \"gene,variant,impact\\nBRCA1,c.68_69del,high\\nTP53,c.215C>G,moderate\\n\" > $OUTPUT/test_output.csv && cat $OUTPUT/test_output.csv'");

        CustomToolInlineParams params = new CustomToolInlineParams()
                .setContainer(container)
                .setMinimumRequirements(new MinimumRequirements());

        // Execute the tool
        CustomToolInlineExecutor executor = new CustomToolInlineExecutor();

        ObjectMap execParams = new ObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);
        execParams.putAll(params.toObjectMap());

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);

        // Execute
        executor.start();

        // Verify output file was created
        Path outputFile = outDir.resolve("test_output.csv");
        assertTrue("Output file should exist at: " + outputFile, Files.exists(outputFile));

        // Read and verify content
        List<String> lines = Files.readAllLines(outputFile);
        assertNotNull("File content should not be null", lines);
        assertEquals("Should have 4 lines (header + 2 data rows + end line)", 4, lines.size());

        // Verify header
        assertEquals("Header should match", "gene,variant,impact", lines.get(0));

        // Verify data rows
        assertTrue("Should contain BRCA1 row", lines.get(1).contains("BRCA1"));
        assertTrue("Should contain c.68_69del", lines.get(1).contains("c.68_69del"));
        assertTrue("Should contain high", lines.get(1).contains("high"));

        assertTrue("Should contain TP53 row", lines.get(2).contains("TP53"));
        assertTrue("Should contain c.215C>G", lines.get(2).contains("c.215C>G"));
        assertTrue("Should contain moderate", lines.get(2).contains("moderate"));

        System.out.println("Real Docker test completed successfully!");
        System.out.println("Output file content:");
        lines.forEach(System.out::println);

        // Verify the cliParams field has $OUTPUT replaced
        String processedCliParams = getCliParams(executor);
        assertNotNull("Processed CLI params should not be null", processedCliParams);
        assertFalse("$OUTPUT should be replaced in command line", processedCliParams.contains("$OUTPUT"));
        assertTrue("Command should contain actual output directory path",
                processedCliParams.contains(outDir.toAbsolutePath().toString()));

        System.out.println("Processed command line: " + processedCliParams);
    }



    /**
     * Test that validates exact variable substitution behavior.
     * $OUTPUT and $OUTPUT/ should be replaced
     * $OUTPUTS, $OUTPUT1, $OUTPUT_DIR should NOT be replaced
     */
    @Test
    public void testExactVariableSubstitution() throws Exception {
        // Check if Docker is available
        if (!isDockerAvailable()) {
            System.out.println("Docker is not available. Skipping exact variable substitution test.");
            Assume.assumeTrue("Docker is not available", false);
            return;
        }

        System.out.println("=== Testing Exact Variable Substitution ===");

        // Create a command that uses various forms of OUTPUT variables
        // Only $OUTPUT should be substituted, not $OUTPUTS, $OUTPUT1, etc.
        String commandLine = "sh -c '" +
                "printf \"Testing OUTPUT substitution\\n\" > $OUTPUT/test1.txt && " +
                "printf \"Testing OUTPUT/ substitution\\n\" > $OUTPUT/test2.txt && " +
                "printf \"\\$OUTPUTS should not be substituted\\n\" > $OUTPUT/test3.txt && " +
                "printf \"\\$OUTPUT1 should not be substituted\\n\" > $OUTPUT/test4.txt && " +
                "printf \"\\$OUTPUT_DIR should not be substituted\\n\" > $OUTPUT/test5.txt && " +
                "cat $OUTPUT/test*.txt" +
                "'";

        Container container = new Container()
                .setName("alpine")
                .setTag("latest")
                .setCommandLine(commandLine);

        CustomToolInlineParams params = new CustomToolInlineParams()
                .setContainer(container)
                .setMinimumRequirements(new MinimumRequirements());

        // Execute the tool
        CustomToolInlineExecutor executor = new CustomToolInlineExecutor();

        ObjectMap execParams = new ObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);
        execParams.putAll(params.toObjectMap());

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);

        // Execute
        executor.start();

        // Verify output files were created
        assertTrue("test1.txt should exist", Files.exists(outDir.resolve("test1.txt")));
        assertTrue("test2.txt should exist", Files.exists(outDir.resolve("test2.txt")));
        assertTrue("test3.txt should exist", Files.exists(outDir.resolve("test3.txt")));
        assertTrue("test4.txt should exist", Files.exists(outDir.resolve("test4.txt")));
        assertTrue("test5.txt should exist", Files.exists(outDir.resolve("test5.txt")));

        // Read and verify content - this is the real test of variable substitution
        String content1 = new String(Files.readAllBytes(outDir.resolve("test1.txt"))).trim();
        assertEquals("File should contain expected content", "Testing OUTPUT substitution", content1);

        String content2 = new String(Files.readAllBytes(outDir.resolve("test2.txt"))).trim();
        assertEquals("File should contain expected content", "Testing OUTPUT/ substitution", content2);

        String content3 = new String(Files.readAllBytes(outDir.resolve("test3.txt"))).trim();
        assertTrue("Content should contain literal $OUTPUTS", content3.contains("$OUTPUTS"));
        assertEquals("File should contain expected content", "$OUTPUTS should not be substituted", content3);

        String content4 = new String(Files.readAllBytes(outDir.resolve("test4.txt"))).trim();
        assertTrue("Content should contain literal $OUTPUT1", content4.contains("$OUTPUT1"));
        assertEquals("File should contain expected content", "$OUTPUT1 should not be substituted", content4);

        String content5 = new String(Files.readAllBytes(outDir.resolve("test5.txt"))).trim();
        assertTrue("Content should contain literal $OUTPUT_DIR", content5.contains("$OUTPUT_DIR"));
        assertEquals("File should contain expected content", "$OUTPUT_DIR should not be substituted", content5);

        System.out.println("Exact variable substitution test completed successfully!");
        System.out.println("✓ $OUTPUT was correctly replaced");
        System.out.println("✓ $OUTPUTS was NOT replaced (remained literal)");
        System.out.println("✓ $OUTPUT1 was NOT replaced (remained literal)");
        System.out.println("✓ $OUTPUT_DIR was NOT replaced (remained literal)");
    }

    /**
     * Test $JOB_OUTPUT variable substitution.
     * Both $OUTPUT and $JOB_OUTPUT should be replaced, but not $JOB_OUTPUTS or $JOB_OUTPUT1
     */
    @Test
    public void testJobOutputVariableSubstitution() throws Exception {
        // Check if Docker is available
        if (!isDockerAvailable()) {
            System.out.println("Docker is not available. Skipping JOB_OUTPUT test.");
            Assume.assumeTrue("Docker is not available", false);
            return;
        }

        System.out.println("=== Testing $JOB_OUTPUT Variable Substitution ===");

        String commandLine = "sh -c '" +
                "printf \"Testing JOB_OUTPUT substitution\\n\" > $JOB_OUTPUT/job_test1.txt && " +
                "printf \"\\$JOB_OUTPUTS should not be replaced\\n\" > $JOB_OUTPUT/job_test2.txt && " +
                "printf \"\\$JOB_OUTPUT1 should not be replaced\\n\" > $JOB_OUTPUT/job_test3.txt && " +
                "cat $JOB_OUTPUT/job_test*.txt" +
                "'";

        Container container = new Container()
                .setName("alpine")
                .setTag("latest")
                .setCommandLine(commandLine);

        CustomToolInlineParams params = new CustomToolInlineParams()
                .setContainer(container)
                .setMinimumRequirements(new MinimumRequirements());

        // Execute the tool
        CustomToolInlineExecutor executor = new CustomToolInlineExecutor();

        ObjectMap execParams = new ObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);
        execParams.putAll(params.toObjectMap());

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);

        // Execute
        executor.start();

        // Verify output files were created
        assertTrue("job_test1.txt should exist", Files.exists(outDir.resolve("job_test1.txt")));
        assertTrue("job_test2.txt should exist", Files.exists(outDir.resolve("job_test2.txt")));
        assertTrue("job_test3.txt should exist", Files.exists(outDir.resolve("job_test3.txt")));

        // Read and verify content - this is the real test
        String content1 = new String(Files.readAllBytes(outDir.resolve("job_test1.txt"))).trim();
        assertEquals("File should contain expected content", "Testing JOB_OUTPUT substitution", content1);

        String content2 = new String(Files.readAllBytes(outDir.resolve("job_test2.txt"))).trim();
        assertTrue("Content should contain literal $JOB_OUTPUTS", content2.contains("$JOB_OUTPUTS"));
        assertEquals("File should contain expected content", "$JOB_OUTPUTS should not be replaced", content2);

        String content3 = new String(Files.readAllBytes(outDir.resolve("job_test3.txt"))).trim();
        assertTrue("Content should contain literal $JOB_OUTPUT1", content3.contains("$JOB_OUTPUT1"));
        assertEquals("File should contain expected content", "$JOB_OUTPUT1 should not be replaced", content3);

        System.out.println("$JOB_OUTPUT variable substitution test completed successfully!");
        System.out.println("✓ $JOB_OUTPUT was correctly replaced");
        System.out.println("✓ $JOB_OUTPUTS was NOT replaced (remained literal)");
        System.out.println("✓ $JOB_OUTPUT1 was NOT replaced (remained literal)");
    }

    /**
     * Test edge cases for variable substitution.
     * Tests $OUTPUT at end of string, followed by special characters, etc.
     */
    @Test
    public void testVariableSubstitutionEdgeCases() throws Exception {
        // Check if Docker is available
        if (!isDockerAvailable()) {
            System.out.println("Docker is not available. Skipping edge cases test.");
            Assume.assumeTrue("Docker is not available", false);
            return;
        }

        System.out.println("=== Testing Variable Substitution Edge Cases ===");

        String commandLine = "sh -c '" +
                "mkdir -p $OUTPUT/subdir && " +
                "printf \"End of line: $OUTPUT\\n\" > $OUTPUT/edge1.txt && " +
                "printf \"With slash: $OUTPUT/\\n\" > $OUTPUT/edge2.txt && " +
                "printf \"With comma: $OUTPUT,\\n\" > $OUTPUT/edge3.txt && " +
                "printf \"With semicolon: $OUTPUT;\\n\" > $OUTPUT/edge4.txt && " +
                "printf \"With space: $OUTPUT \\n\" > $OUTPUT/edge5.txt && " +
                "cat $OUTPUT/edge*.txt" +
                "'";

        Container container = new Container()
                .setName("alpine")
                .setTag("latest")
                .setCommandLine(commandLine);

        CustomToolInlineParams params = new CustomToolInlineParams()
                .setContainer(container)
                .setMinimumRequirements(new MinimumRequirements());

        // Execute the tool
        CustomToolInlineExecutor executor = new CustomToolInlineExecutor();

        ObjectMap execParams = new ObjectMap();
        execParams.put(ParamConstants.STUDY_PARAM, studyFqn);
        execParams.putAll(params.toObjectMap());

        executor.setUp(catalogManagerResource.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(storageConfiguration), execParams, outDir, studyFqn, "", false, ownerToken);

        // Execute
        executor.start();

        // Verify output files were created
        assertTrue("edge1.txt should exist", Files.exists(outDir.resolve("edge1.txt")));
        assertTrue("edge2.txt should exist", Files.exists(outDir.resolve("edge2.txt")));
        assertTrue("edge3.txt should exist", Files.exists(outDir.resolve("edge3.txt")));
        assertTrue("edge4.txt should exist", Files.exists(outDir.resolve("edge4.txt")));
        assertTrue("edge5.txt should exist", Files.exists(outDir.resolve("edge5.txt")));

        // Verify subdirectory was created
        assertTrue("subdir should exist", Files.exists(outDir.resolve("subdir")));

        System.out.println("Edge cases test completed successfully!");
        System.out.println("✓ All edge case files created successfully");
    }

    /**
     * Helper method to check if Docker is available on the system.
     */
    private boolean isDockerAvailable() {
        try {
            ProcessBuilder pb = new ProcessBuilder("docker", "--version");
            Process process = pb.start();
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                // Also check if Docker daemon is running
                pb = new ProcessBuilder("docker", "ps");
                process = pb.start();
                exitCode = process.waitFor();
                return exitCode == 0;
            }
            return false;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    /**
     * Helper method to extract the processed command line from the executor using reflection.
     */
    private String getCliParams(CustomToolInlineExecutor executor) throws Exception {
        java.lang.reflect.Field cliParamsField = CustomToolInlineExecutor.class.getDeclaredField("cliParams");
        cliParamsField.setAccessible(true);
        return (String) cliParamsField.get(executor);
    }

}

