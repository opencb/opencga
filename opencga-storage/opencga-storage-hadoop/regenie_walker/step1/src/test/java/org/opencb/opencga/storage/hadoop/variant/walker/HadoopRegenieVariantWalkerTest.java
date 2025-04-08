package org.opencb.opencga.storage.hadoop.variant.walker;


import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFReader;
import org.apache.hadoop.util.ToolRunner;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.mr.StreamVariantMapper;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(LongTests.class)
public class HadoopRegenieVariantWalkerTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private static String dockerImage;
    private static URI outdir;

    @Before
    public void before() throws Exception {
        // Do not clear DB for each test
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = externalResource.getVariantStorageEngine();
        externalResource.clearDB(variantStorageManager.getDBName());

        outdir = newOutputUri();
        dockerImage = buildDocker();
        System.exit(1);

        URI inputUri = VariantStorageBaseTest.getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageManager, studyMetadata,
                new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                        .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "GT,FT")
        );

        VariantHbaseTestUtils.printVariants(variantStorageManager.getDBAdaptor(), newOutputUri());
    }

    @AfterClass
    public static void afterClass() throws Exception {
//        pruneDocker(dockerImage);
//        dockerImage = null;
    }

    @Test
    public void tesRegenieWalker() throws Exception {


        String cmdPython1 = "python3 variant_walker.py regenie_walker Regenie";
        variantStorageEngine.getOptions().put(StreamVariantMapper.DOCKER_PRUNE_OPTS, " --filter label!=opencga_scope='test'");
        VariantQuery variantQuery = new VariantQuery()
                .includeSampleAll()
                .includeSampleData("GT")
                .unknownGenotype("./.");

        URI regenieResults = outdir.resolve("regenie_results.txt");
        variantStorageEngine.walkData(regenieResults, VariantWriterFactory.VariantOutputFormat.VCF, variantQuery, new QueryOptions(),
                dockerImage, cmdPython1);

        // Ensure that the docker image is not pruned
        Command dockerImages = new Command(new String[]{"docker", "images", "--filter", "label=opencga_scope=test"}, Collections.emptyMap());
        dockerImages.run();
        assertEquals(0, dockerImages.getExitValue());
//        assertEquals(2, dockerImages.getOutput().split("\n").length);
        System.out.println("outdir = " + outdir);

        assertTrue(Files.exists(Paths.get(regenieResults.getPath())));
    }

    private static String buildDocker() throws IOException {
        String dockerImage = "local/regine-walker:latest";


//        python3 regenie-docker-build.py
//        --image-name regenie-walker
//        --step1-path ~/data/opencga/walker/regenie/test4/step1
//        --python-path /home/jtarraga/appl/opencga/opencga-storage/opencga-storage-hadoop/opencga-storage-hadoop-core/src/main/python/
//        --pheno-file ~/data/opencga/walker/regenie/test4/phenotype.txt
//        --output-dockerfile /tmp/test-regenie-walker/Dockerfile


        Path pythonScript = Paths.get("../../../opencga-app/app/cloud/docker/opencga-regenie/regenie-docker-build.py");
        Path step1Path = Paths.get("");
        Path pythonPath = Paths.get("");
        Path phenoPath = Paths.get("");
        Path dockerfilePath = Paths.get("");
        Command dockerBuild = new Command(new String[]{"python3", pythonScript.toAbsolutePath().toString(),
                                                        "--image-name", dockerImage,
                                                        "--step1-path", step1Path.toAbsolutePath().toString(),
                                                        "--python-path", pythonPath.toAbsolutePath().toString(),
                                                        "--pheno-file", phenoPath.toAbsolutePath().toString(),
                                                        "--output-dockerfile", dockerfilePath.toAbsolutePath().toString()
                                                },
                Collections.emptyMap());
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
