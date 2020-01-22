package org.opencb.opencga.storage.hadoop.variant;

import org.junit.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

public class HadoopVariantStorageEngineSplitDataTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    public static final List<String> SAMPLES = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
    }

    @Test
    public void testMultiChromosomeSplitData() throws Exception {

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr20.variant-test-file.vcf.gz")),
                outputUri, true, true, true);

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        int studyId = mm.getStudyId(STUDY_NAME);
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            Assert.assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            Assert.assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            Assert.assertEquals(TaskMetadata.Status.NONE, SampleIndexDBAdaptor.getSampleIndexStatus(sampleMetadata));
        }

        variantStorageEngine.annotate(new Query(), new QueryOptions(DefaultVariantAnnotationManager.OUT_DIR, outputUri));
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            Assert.assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            Assert.assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
            Assert.assertEquals(TaskMetadata.Status.READY, SampleIndexDBAdaptor.getSampleIndexStatus(sampleMetadata));
        }

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), true);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")),
                outputUri, true, true, true);

        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            Assert.assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            Assert.assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            Assert.assertEquals(TaskMetadata.Status.NONE, SampleIndexDBAdaptor.getSampleIndexStatus(sampleMetadata));
        }
        variantStorageEngine.annotate(new Query(), new QueryOptions(DefaultVariantAnnotationManager.OUT_DIR, outputUri));
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            Assert.assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            Assert.assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
            Assert.assertEquals(TaskMetadata.Status.READY, SampleIndexDBAdaptor.getSampleIndexStatus(sampleMetadata));
        }


        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), true);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22.variant-test-file.vcf.gz")),
                outputUri, true, true, true);

        for (Variant variant : variantStorageEngine) {
            String expectedFile = "chr" + variant.getChromosome() + ".variant-test-file.vcf.gz";
            Assert.assertEquals(1, variant.getStudies().get(0).getFiles().size());
            Assert.assertEquals(expectedFile, variant.getStudies().get(0).getFiles().get(0).getFileId());
            if (variant.getChromosome().equals("20") || variant.getChromosome().equals("21")) {
                Assert.assertNotNull(variant.getAnnotation().getConsequenceTypes());
                Assert.assertFalse(variant.getAnnotation().getConsequenceTypes().isEmpty());
            } else {
                Assert.assertTrue(variant.getAnnotation() == null || variant.getAnnotation().getConsequenceTypes().isEmpty());
            }
        }

        variantStorageEngine.forEach(new Query(VariantQueryParam.FILE.key(), "chr21.variant-test-file.vcf.gz"), variant -> {
            Assert.assertEquals("21", variant.getChromosome());
            String expectedFile = "chr" + variant.getChromosome() + ".variant-test-file.vcf.gz";
            Assert.assertEquals(1, variant.getStudies().get(0).getFiles().size());
            Assert.assertEquals(expectedFile, variant.getStudies().get(0).getFiles().get(0).getFileId());
        }, QueryOptions.empty());
    }

    @Test
    public void testMultiChromosomeFail() throws Exception {
        HadoopVariantStorageEngine engine = getVariantStorageEngine();
        URI outDir = newOutputUri();

        engine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        engine.index(Collections.singletonList(getResourceUri("by_chr/chr20.variant-test-file.vcf.gz")),
                outDir, true, true, true);

        engine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        engine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), false);

        StorageEngineException expected = StorageEngineException.alreadyLoadedSamples("chr21.variant-test-file.vcf.gz", SAMPLES);
//        thrown.expectMessage(expected.getMessage());
        thrown.expectCause(isA(expected.getClass()));
        thrown.expectCause(hasMessage(is(expected.getMessage())));
        engine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")),
                outDir, true, true, true);
    }
}
