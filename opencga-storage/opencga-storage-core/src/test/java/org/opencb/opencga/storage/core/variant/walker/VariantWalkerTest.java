package org.opencb.opencga.storage.core.variant.walker;

import org.junit.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.storage.core.StorageEngineTest;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.WALKER_DOCKER_MAX_BYTES_PER_MAP;

/**
 * Abstract test for LocalVariantWalker. Subclasses provide the storage engine.
 */
@Ignore
@StorageEngineTest
public abstract class VariantWalkerTest extends VariantStorageBaseTest {

    protected static boolean loaded = false;
    private static String dockerImage;

    @BeforeClass
    public static void beforeClass() throws Exception {
        dockerImage = buildDocker();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        pruneDocker(dockerImage);
        dockerImage = null;
    }

    @Before
    public void before() throws Exception {
        if (!loaded) {
            clearDB(DB_NAME);
            URI inputUri = VariantStorageBaseTest.getResourceUri("variant-test-file.vcf.gz");
            StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
            VariantStorageBaseTest.runDefaultETL(inputUri, getVariantStorageEngine(), studyMetadata,
                    new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true)
                            .append(VariantStorageOptions.STATS_CALCULATE.key(), false));


//            inputUri = VariantStorageBaseTest.getResourceUri("variant-test-unusual-contigs.vcf");
//            VariantStorageBaseTest.runDefaultETL(inputUri, getVariantStorageEngine(), studyMetadata,
//                    new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
//                            .append(VariantStorageOptions.ANNOTATE.key(), false)
//                            .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
//            );
            loaded = true;
        }
    }


    private static String buildDocker() throws IOException {
        String dockerImage = "local/variant-walker-test:latest";
        Path dockerFile = Paths.get(getResourceUri("variantWalker/Dockerfile").getPath());
//        Path pythonDir = Paths.get("../../opencga-storage-core/src/main/python").toAbsolutePath();
        Path dir = Paths.get("").toAbsolutePath();
        while (!dir.getFileName().toString().equals("opencga-storage")) {
            dir = dir.getParent();
        }
        Path pythonDir = dir.resolve("opencga-storage-core/src/main/python").toAbsolutePath();
        Command dockerBuild = new Command(new String[]{"docker", "build", "-t", dockerImage, "-f", dockerFile.toString(), pythonDir.toString()}, Collections.emptyMap());
        dockerBuild.run();
        assertEquals(0, dockerBuild.getExitValue());
        return dockerImage;
    }

    private static void pruneDocker(String dockerImage) throws IOException {
        if (dockerImage != null) {
            Command dockerPrune = new Command(new String[]{"docker", "rmi", dockerImage}, Collections.emptyMap());
            dockerPrune.run();
            assertEquals(0, dockerPrune.getExitValue());
        }
    }

    @Test
    public void testWalkCommandJson() throws Exception {
        URI outdir = newOutputUri();

        String cmd = "while read -r line ; do echo \"$line\" | jq -c .id ; done";

        List<URI> uris = variantStorageEngine.walkData(
                outdir.resolve("walker_json.txt.gz"),
                VariantWriterFactory.VariantOutputFormat.JSON,
                new Query(), new QueryOptions(), cmd);

        assertNotNull(uris);
        assertTrue("Expected at least 1 output file", uris.size() >= 1);
        for (URI uri : uris) {
            assertTrue(uri + " not found!", Paths.get(uri).toFile().exists());
        }

        // Read stdout output and verify it has content
        List<String> lines = new StringDataReader(Paths.get(uris.get(0))).stream().collect(Collectors.toList());
        assertFalse("Output should not be empty", lines.isEmpty());
    }

    @Test
    public void testWalkCommandVcf() throws Exception {
        URI outdir = newOutputUri();

        String cmd = "cat";

        List<URI> uris = variantStorageEngine.walkData(
                outdir.resolve("walker_vcf.txt.gz"),
                VariantWriterFactory.VariantOutputFormat.VCF,
                new Query(), new QueryOptions(), cmd);

        assertNotNull(uris);
        assertTrue("Expected at least 1 output file", uris.size() >= 1);
        for (URI uri : uris) {
            assertTrue(uri + " not found!", Paths.get(uri).toFile().exists());
        }

        List<String> lines = new StringDataReader(Paths.get(uris.get(0))).stream().collect(Collectors.toList());
        assertFalse("Output should not be empty", lines.isEmpty());

        // VCF output should start with header lines
        assertTrue("Expected VCF header", lines.get(0).startsWith("#"));

        // Count data lines (non-header)
        long dataLines = lines.stream().filter(l -> !l.startsWith("#")).count();
        assertTrue("Expected variant data lines", dataLines > 0);
    }

    @Test
    public void testWalkMultipleRestarts() throws Exception {
        URI outdir = newOutputUri();

        String cmd = "cat";

        // Force multiple restarts with a very low byte limit
        variantStorageEngine.getOptions().put(WALKER_DOCKER_MAX_BYTES_PER_MAP.key(), 2 * 1024);
        try {
            List<URI> result = variantStorageEngine.walkData(
                    outdir.resolve("walker_restart.txt.gz"),
                    VariantWriterFactory.VariantOutputFormat.VCF,
                    new Query(), new QueryOptions(), cmd);

            assertNotNull(result);
            assertTrue("Expected at least 1 output file", result.size() >= 1);

            // Check that output has only one set of header lines (header deduplication)
            boolean inHeader = true;
            List<String> lines = new StringDataReader(Paths.get(result.get(0))).stream().collect(Collectors.toList());
            for (String line : lines) {
                if (line.startsWith("#")) {
                    assertTrue("Found header line after body started: " + line, inHeader);
                } else if (!line.trim().isEmpty()) {
                    inHeader = false;
                }
            }
        } finally {
            variantStorageEngine.getOptions().remove(WALKER_DOCKER_MAX_BYTES_PER_MAP.key());
        }
    }

    @Test
    public void testWalkProcessError() throws Exception {
        URI outdir = newOutputUri();

        String cmd = "exit 1";

        try {
            variantStorageEngine.walkData(
                    outdir.resolve("walker_error.txt.gz"),
                    VariantWriterFactory.VariantOutputFormat.JSON,
                    new Query(), new QueryOptions(), cmd);
            fail("Expected StorageEngineException for failing process");
        } catch (Exception e) {
            // Expected — StorageEngineException from local walker, RuntimeException from Hadoop MR
            String fullMessage = e.toString();
            for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
                fullMessage += " " + t.toString();
            }
            for (Throwable suppressed : e.getSuppressed()) {
                fullMessage += " " + suppressed.toString();
            }
            assertTrue("Expected process failure info in exception: " + fullMessage,
                    fullMessage.toLowerCase().contains("exit") || fullMessage.contains("Stream closed"));
        }
    }


    @Test
    public void exportCommand() throws Exception {
        URI outdir = newOutputUri();

        List<String> cmdList = Arrays.asList(
                "export NUM_VARIANTS=0 ;",
                "function setup() {",
                "    echo \"#SETUP\" ;",
                "    echo '## Something in single quotes' ; ",
                "} ;",
                "function map() {",
//                "    echo \"[$NUM_VARIANTS] $1\" 1>&2 ;",
                "    echo \"[$NUM_VARIANTS] \" 1>&2 ;",
                "    echo \"$1\" | jq .id ;",
                "    NUM_VARIANTS=$((NUM_VARIANTS+1)) ;",
                "};",
                "function cleanup() {",
                "    echo \"CLEANUP\" ;",
                "    echo \"NumVariants = $NUM_VARIANTS\" ;",
                "};",
                "setup;",
                "while read -r i ; do ",
                "    map \"$i\" ; ",
                "done; ",
                "cleanup;");

        //        String cmd = "bash -c '" + String.join("\n", cmdList) + "'";
        String cmd = String.join("\n", cmdList);

//        variantStorageEngine.walkData(outdir.resolve("variant3.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmdDocker);
//        variantStorageEngine.walkData(outdir.resolve("variant2.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmdBash);
        List<URI> uris = variantStorageEngine.walkData(outdir.resolve("variant1.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmd);
//        variantStorageEngine.walkData(outdir.resolve("variant5.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmdPython1);
//        variantStorageEngine.walkData(outdir.resolve("variant8.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmdPython2);
//        variantStorageEngine.walkData(outdir.resolve("variant6.txt.gz"), VariantWriterFactory.VariantOutputFormat.VCF, new Query(), new QueryOptions(), cmdPython);
//        variantStorageEngine.walkData(outdir.resolve("variant4.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), "opencb/opencga-base", cmd);
//        variantStorageEngine.walkData(outdir.resolve("variant4.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), "opencb/opencga-base", cmdPython1);
        assertEquals(3, uris.size());
        for (URI uri : uris) {
            // Ensure uri exists
            assertTrue(uri + " not found!", Paths.get(uri).toFile().exists());
        }
    }

    @Test
    public void exportDocker() throws Exception {
        URI outdir = newOutputUri();

        String cmdPython1 = "python variant_walker.py walker_example Echo --length 30";
        List<URI> uris = variantStorageEngine.walkData(outdir.resolve("variant4.txt.gz"), VariantWriterFactory.VariantOutputFormat.VCF, new Query(), new QueryOptions(), dockerImage, cmdPython1);
        assertEquals(3, uris.size());
        for (URI uri : uris) {
            // Ensure uri exists
            assertTrue(uri + " not found!", Paths.get(uri).toFile().exists());
        }

        // Ensure that the docker image is not pruned
        Command dockerImages = new Command(new String[]{"docker", "images", "--filter", "label=opencga_scope=test"}, Collections.emptyMap());
        dockerImages.run();
        assertEquals(0, dockerImages.getExitValue());
        assertEquals(2, dockerImages.getOutput().split("\n").length);
    }

    @Test
    public void exportDockerMultipleRestarts() throws Exception {
        URI outdir = newOutputUri();

        String cmdPython1 = "python variant_walker.py walker_example Echo";
        // Force multiple restarts
        variantStorageEngine.getOptions().put(WALKER_DOCKER_MAX_BYTES_PER_MAP.key(), 2*1024);
        List<URI> result = variantStorageEngine.walkData(outdir.resolve("variant4.txt.gz"), VariantWriterFactory.VariantOutputFormat.VCF, new Query(), new QueryOptions(), dockerImage, cmdPython1);

        // Check that output has only one header
        boolean inHeader = true;
        for (String line : new StringDataReader(Paths.get(result.get(0)))) {
            if (line.startsWith("#")) {
                assertTrue(inHeader);
            } else {
                inHeader = false;
            }
        }

        // Ensure that the docker image is not pruned
        Command dockerImages = new Command(new String[]{"docker", "images", "--filter", "label=opencga_scope=test"}, Collections.emptyMap());
        dockerImages.run();
        assertEquals(0, dockerImages.getExitValue());
        assertEquals(2, dockerImages.getOutput().split("\n").length);
    }

}
