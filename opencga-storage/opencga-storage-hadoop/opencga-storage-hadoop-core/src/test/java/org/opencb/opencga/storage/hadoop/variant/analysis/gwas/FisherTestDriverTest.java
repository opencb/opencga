package org.opencb.opencga.storage.hadoop.variant.analysis.gwas;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.*;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Category(LongTests.class)
public class FisherTestDriverTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();
    private URI localOut;

    @Before
    public void setUp() throws Exception {
        localOut = newOutputUri(getClass().getSimpleName());
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), localOut);
    }

    @Test
    public void testFisher() throws Exception {

        StudyMetadata studyMetadata = newStudyMetadata();

        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        URI input = getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

//        URI input = smallInputUri;
//        IntStream controlCohort = IntStream.of(1, 2);
//        IntStream caseCohort = IntStream.of(3, 4);

        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(input, variantStorageEngine, studyMetadata, params);
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        List<String> controlCohort = metadataManager.getIndexedSamples(studyMetadata.getId())
                .subList(0, 250)
                .stream()
                .map(s -> metadataManager.getSampleName(studyMetadata.getId(), s))
                .collect(Collectors.toList());
        List<String> caseCohort = metadataManager.getIndexedSamples(studyMetadata.getId())
                .subList(250, 500)
                .stream()
                .map(s -> metadataManager.getSampleName(studyMetadata.getId(), s))
                .collect(Collectors.toList());

        ObjectMap objectMap = new ObjectMap()
                .append(FisherTestDriver.CONTROL_COHORT, controlCohort)
                .append(FisherTestDriver.CASE_COHORT, caseCohort)
                .append(FisherTestDriver.OUTPUT, "fisher_result");
        getMrExecutor().run(FisherTestDriver.class, FisherTestDriver.buildArgs(
                dbAdaptor.getArchiveTableName(1),
                dbAdaptor.getVariantTable(),
                1,
                Collections.emptySet(), objectMap), "");

        URI local1 = copyToLocal("fisher_result");

        URI local2 = localOut.resolve("fisher_result2.tsv");
        objectMap.append(FisherTestDriver.OUTPUT, local2)
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "lof,missense_variant")
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding");
        getMrExecutor().run(FisherTestDriver.class, FisherTestDriver.buildArgs(
                dbAdaptor.getArchiveTableName(1),
                dbAdaptor.getVariantTable(),
                1,
                Collections.emptySet(), objectMap), "");

//        URI local2 = copyToLocal("fisher_result2");

        variantStorageEngine.loadVariantScore(local1, studyMetadata.getName(), "fisher1", "ALL", null, new VariantScoreFormatDescriptor(1, 16, 15), new ObjectMap());
        variantStorageEngine.loadVariantScore(local2, studyMetadata.getName(), "fisher2", "ALL", null, new VariantScoreFormatDescriptor(1, 16, 15), new ObjectMap());

        FileSystem fs = FileSystem.get(configuration.get());
        Set<String> lines1 = new HashSet<>();
        int lines2 = 0;
        try (BufferedReader is = new BufferedReader(new InputStreamReader(fs.open(new Path("fisher_result/part-r-00000"))))) {
            String x = is.readLine();
            while (StringUtils.isNotEmpty(x)) {
//                System.out.println(x);
                if (!x.startsWith("#")) {
                    lines1.add(x);
                }
                x = is.readLine();
            }
        }
        try (BufferedReader is = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(local2.getPath()))))) {
            String x = is.readLine();
            while (StringUtils.isNotEmpty(x)) {
//                System.out.println(x);
                if (!x.startsWith("#")) {
                    Assert.assertTrue(lines1.contains(x));
                    lines2++;
                }
                x = is.readLine();
            }
        }

        Assert.assertThat(lines2, VariantMatchers.lt(lines1.size()));
        Assert.assertThat(lines2, VariantMatchers.gt(0));
    }

    private URI copyToLocal(String s) throws IOException {
        FileSystem fs = FileSystem.get(configuration.get());
        URI local = localOut.resolve(s + ".tsv");
        fs.copyToLocalFile(new Path(s + "/part-r-00000"), new Path(local));
        return local;
    }

}