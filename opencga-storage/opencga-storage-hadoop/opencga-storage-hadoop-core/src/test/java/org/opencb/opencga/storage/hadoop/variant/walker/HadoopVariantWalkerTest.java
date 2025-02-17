package org.opencb.opencga.storage.hadoop.variant.walker;


import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.mr.StreamVariantMapper;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(LongTests.class)
public class HadoopVariantWalkerTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private static String dockerImage;

    @Before
    public void before() throws Exception {
        // Do not clear DB for each test
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = externalResource.getVariantStorageEngine();
        externalResource.clearDB(variantStorageManager.getDBName());

//        URI inputUri = VariantStorageBaseTest.getResourceUri("sample1.genome.vcf");
//        URI inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        URI inputUri = VariantStorageBaseTest.getResourceUri("variant-test-file.vcf.gz");

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageManager, studyMetadata,
                new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        VariantHbaseTestUtils.printVariants(variantStorageManager.getDBAdaptor(), newOutputUri());

        dockerImage = buildDocker();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        pruneDocker(dockerImage);
        dockerImage = null;
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
        variantStorageEngine.walkData(outdir.resolve("variant1.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmd);
//        variantStorageEngine.walkData(outdir.resolve("variant5.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmdPython1);
//        variantStorageEngine.walkData(outdir.resolve("variant8.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), cmdPython2);
//        variantStorageEngine.walkData(outdir.resolve("variant6.txt.gz"), VariantWriterFactory.VariantOutputFormat.VCF, new Query(), new QueryOptions(), cmdPython);
//        variantStorageEngine.walkData(outdir.resolve("variant4.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), "opencb/opencga-base", cmd);
//        variantStorageEngine.walkData(outdir.resolve("variant4.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), "opencb/opencga-base", cmdPython1);
    }

    @Test
    public void exportDocker() throws Exception {
        URI outdir = newOutputUri();

        String cmdPython1 = "python variant_walker.py walker_example Cut --length 30";
        variantStorageEngine.getOptions().put(StreamVariantMapper.DOCKER_PRUNE_OPTS, " --filter label!=opencga_scope='test'");
        variantStorageEngine.walkData(outdir.resolve("variant4.txt.gz"), VariantWriterFactory.VariantOutputFormat.JSON, new Query(), new QueryOptions(), dockerImage, cmdPython1);

        // Ensure that the docker image is not pruned
        Command dockerImages = new Command(new String[]{"docker", "images", "--filter", "label=opencga_scope=test"}, Collections.emptyMap());
        dockerImages.run();
        assertEquals(0, dockerImages.getExitValue());
        assertEquals(2, dockerImages.getOutput().split("\n").length);
    }

    private static String buildDocker() throws IOException {
        String dockerImage = "local/variant-walker-test:latest";
        Path dockerFile = Paths.get(getResourceUri("variantWalker/Dockerfile").getPath());
//        Path pythonDir = Paths.get("../../opencga-storage-core/src/main/python").toAbsolutePath();
        Path pythonDir = Paths.get("src/main/python").toAbsolutePath();
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
}
