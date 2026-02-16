package org.opencb.opencga.storage.core.variant;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageEngineTest;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.NATIVE;

@StorageEngineTest
@Ignore
public abstract class VariantStorageEngineSplitDataTest extends VariantStorageBaseTest {
    public static final List<String> SAMPLES = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");
    public static final String MOCKED_EXCEPTION = "Mocked exception";

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
    }

    @Test
    public void testMultiChromosomeSplitDataConcurrentFail() throws Exception {
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        failAtLoadingFile("by_chr/", "chr20.variant-test-file.vcf.gz", outputUri);
        // Will fail if LOAD_SPLIT_DATA is not set
        thrown.expect(StoragePipelineException.class);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")), outputUri);
    }

    @Test
    public void testMultiChromosomeSplitDataConcurrentFailOneIndexOther() throws Exception {
        // Test goal:
        //  - Index chr20 and chr21 concurrently with shared samples and LOAD_SPLIT_DATA=CHROMOSOME
        // Test steps:
        //  - Fail at loading chr20 (left the file in status "RUNNING")
        //  - Index chr21 correctly with LOAD_SPLIT_DATA=CHROMOSOME

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        failAtLoadingFile("by_chr/", "chr20.variant-test-file.vcf.gz", outputUri);

        // Won't fail if LOAD_SPLIT_DATA is set correctly
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")), outputUri);
    }

    @Test
    public void testMultiChromosomeSplitDataConcurrentFail3() throws Exception {
        // Test goal:
        //  - Ensure file can be loaded after deleting a file with shared samples without any LOAD_SPLIT_DATA.
        // Test steps:
        //  - Fail at loading chr20 (left the file in status "RUNNING")
        //  - Force remove chr20
        //  - Index chr21 correctly

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        failAtLoadingFile("by_chr/", "chr20.variant-test-file.vcf.gz", outputUri);

        variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
        variantStorageEngine.removeFile(STUDY_NAME, "chr20.variant-test-file.vcf.gz", outputUri);
        variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), false);

        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")), outputUri);
    }

    @Test
    public void testMultiChromosomeSplitDataConcurrentFailDelete() throws Exception {
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        failAtLoadingFile("by_chr/", "chr20.variant-test-file.vcf.gz", outputUri);

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")), outputUri);

        try {
            // FORCE=true is needed, as the file is not correctly indexed
            // Set FORCE=false to assert exception
            variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), false);
            variantStorageEngine.removeFile(STUDY_NAME, "chr20.variant-test-file.vcf.gz", outputUri);
            fail();
        } catch (StorageEngineException e) {
            assertEquals("Unable to remove non indexed file: chr20.variant-test-file.vcf.gz", e.getMessage());
        }

        // FORCE=true is needed, as the file is not correctly indexed
        failAtDeletingFile("chr20.variant-test-file.vcf.gz", outputUri, 1, new ObjectMap(VariantStorageOptions.FORCE.key(), true));

        try {
            // FORCE=true is needed, as the file is not correctly indexed
            variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
            // RESUME=true is needed, as the delete task is in status "RUNNING" from the previous failAtDeletingFile
            // Set RESUME=false to assert exception
            variantStorageEngine.getOptions().put(VariantStorageOptions.RESUME.key(), false);
            variantStorageEngine.removeFile(STUDY_NAME, "chr20.variant-test-file.vcf.gz", outputUri);
            fail();
        } catch (StorageEngineException e) {
            assertEquals("Operation \"remove\" for files [\"chr20.variant-test-file.vcf.gz\" (id=1)] in status \"RUNNING\". Relaunch with resume=true to finish the operation.", e.getMessage());
        }


        // FORCE=true is needed, as the file is not correctly indexed
        variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
        // RESUME=true is needed, as the delete task is in status "RUNNING"
        variantStorageEngine.getOptions().put(VariantStorageOptions.RESUME.key(), true);
        variantStorageEngine.removeFile(STUDY_NAME, "chr20.variant-test-file.vcf.gz", outputUri);
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
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }

        variantStorageEngine.annotate(outputUri, new QueryOptions());
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")),
                outputUri, true, true, true);

        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }
        variantStorageEngine.annotate(outputUri, new QueryOptions());
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }


        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22.variant-test-file.vcf.gz")),
                outputUri);

        for (Variant variant : variantStorageEngine.iterable(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL), null)) {
            String expectedFile = "chr" + variant.getChromosome() + ".variant-test-file.vcf.gz";
            assertEquals(1, variant.getStudies().get(0).getFiles().size());
            assertEquals(expectedFile, variant.getStudies().get(0).getFiles().get(0).getFileId());
            if (variant.getChromosome().equals("20") || variant.getChromosome().equals("21")) {
                Assert.assertNotNull(variant.getAnnotation().getConsequenceTypes());
                Assert.assertFalse(variant.getAnnotation().getConsequenceTypes().isEmpty());
            } else {
                assertTrue(variant.getAnnotation() == null || variant.getAnnotation().getConsequenceTypes().isEmpty());
            }
        }

        variantStorageEngine.forEach(new Query(VariantQueryParam.FILE.key(), "chr21.variant-test-file.vcf.gz"), variant -> {
            assertEquals("21", variant.getChromosome());
            String expectedFile = "chr" + variant.getChromosome() + ".variant-test-file.vcf.gz";
            assertEquals(1, variant.getStudies().get(0).getFiles().size());
            assertEquals(expectedFile, variant.getStudies().get(0).getFiles().get(0).getFileId());
        }, QueryOptions.empty());
    }


    @Test
    public void testMultiChromosomeSplitDataVirtualFile() throws Exception {
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_VIRTUAL_FILE.key(), "virtual-variant-test-file.vcf");

        int studyId = variantStorageEngine.getMetadataManager().createStudy(STUDY_NAME).getId();
        int sampleIdMock = variantStorageEngine.getMetadataManager().registerSamples(studyId, Collections.singletonList("NA19660")).get(0);
        // Mark one random sample as having unknown largest variant size
        // Ensure that the largest variant size is not updated
        variantStorageEngine.getMetadataManager().updateSampleMetadata(studyId, sampleIdMock, sampleMetadata -> {
            sampleMetadata.getAttributes().put(SampleIndexSchema.UNKNOWN_LARGEST_VARIANT_LENGTH, true);
        });

        StoragePipelineResult result = variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr20.variant-test-file.vcf.gz")),
                outputUri, true, true, true).get(0);

        // All samples expected to be updated
        assertEquals(4, result.getLoadStats().getInt("updatedSampleMetadata"));
        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(1));
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(1));
            if (sampleIdMock == sampleMetadata.getId()) {
                assertFalse(sample, sampleMetadata.getAttributes().containsKey(SampleIndexSchema.LARGEST_VARIANT_LENGTH));
                assertTrue(sample, sampleMetadata.getAttributes().getBoolean(SampleIndexSchema.UNKNOWN_LARGEST_VARIANT_LENGTH));
            } else {
                assertNotEquals(sample, -1, sampleMetadata.getAttributes().getInt(SampleIndexSchema.LARGEST_VARIANT_LENGTH, -1));
                assertFalse(sample, sampleMetadata.getAttributes().getBoolean(SampleIndexSchema.UNKNOWN_LARGEST_VARIANT_LENGTH));
            }
        }

        variantStorageEngine.annotate(outputUri, new QueryOptions());
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(1));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_VIRTUAL_FILE.key(), "virtual-variant-test-file.vcf");
        result = variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")),
                outputUri, true, true, true).get(0);

        // No sample expected to be updated
        assertEquals(0, result.getLoadStats().getInt("updatedSampleMetadata"));

        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(1));
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }
        variantStorageEngine.annotate(outputUri, new QueryOptions());
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(1));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }

        // Revert the unknown largest variant size
        // Ensure that the largest variant size is now updated
        variantStorageEngine.getMetadataManager().updateSampleMetadata(studyId, sampleIdMock, sampleMetadata -> {
            sampleMetadata.getAttributes().put(SampleIndexSchema.UNKNOWN_LARGEST_VARIANT_LENGTH, false);
        });

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_VIRTUAL_FILE.key(), "virtual-variant-test-file.vcf");
        result = variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22.variant-test-file.vcf.gz")),
                outputUri).get(0);
        // One sample (sampleIdMock) expected to be updated
        assertEquals(1, result.getLoadStats().getInt("updatedSampleMetadata"));

        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(1));
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(1));

            assertNotEquals(sample, -1, sampleMetadata.getAttributes().getInt(SampleIndexSchema.LARGEST_VARIANT_LENGTH, -1));
            assertFalse(sample, sampleMetadata.getAttributes().getBoolean(SampleIndexSchema.UNKNOWN_LARGEST_VARIANT_LENGTH));
        }

        for (Variant variant : variantStorageEngine.iterable(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL), null)) {
            String expectedFile = "virtual-variant-test-file.vcf";
            assertEquals(1, variant.getStudies().get(0).getFiles().size());
            assertEquals(expectedFile, variant.getStudies().get(0).getFiles().get(0).getFileId());
            if (variant.getChromosome().equals("20") || variant.getChromosome().equals("21")) {
                Assert.assertNotNull(variant.getAnnotation().getConsequenceTypes());
                Assert.assertFalse(variant.getAnnotation().getConsequenceTypes().isEmpty());
            } else {
                assertTrue(variant.getAnnotation() == null || variant.getAnnotation().getConsequenceTypes().isEmpty());
            }
        }
    }

    @Test
    public void testMultiChromosomeFail() throws Exception {
        URI outDir = newOutputUri();

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr20.variant-test-file.vcf.gz")),
                outDir, true, true, true);

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), null);

        StorageEngineException expected = StorageEngineException.alreadyLoadedSamples("chr21.variant-test-file.vcf.gz", SAMPLES);
//        thrown.expectMessage(expected.getMessage());
        thrown.expectCause(isA(expected.getClass()));
        thrown.expectCause(hasMessage(is(expected.getMessage())));
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")),
                outDir, true, true, true);
    }

    @Test
    public void testDuplicatedVariantsFail() throws Exception {
        URI outDir = newOutputUri();

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.REGION);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr20.variant-test-file.vcf.gz")),
                outDir, true, true, true);

        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr21.variant-test-file.vcf.gz")),
                outputUri, true, true, true);


        thrown.expect(StoragePipelineException.class);
        thrown.expect(hasCause(isA(StorageEngineException.class)));
        thrown.expect(hasCause(hasCause(isA(ExecutionException.class))));
        thrown.expect(hasCause(hasCause(hasCause(isA(IllegalArgumentException.class)))));
        thrown.expect(hasCause(hasCause(hasCause(hasMessage(containsString("Already loaded variant 20:238441:T:C"))))));
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr20-21.variant-test-file.vcf.gz")),
                outDir, true, true, true);
    }

    @Test
    public void testMergeWithWithoutFT_TASK_1935() throws Exception {
        URI outDir = newOutputUri();

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.FAMILY.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-2-DUP2.variant-test-file.vcf.gz")), outDir);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz")), outDir);


        Variant v = variantStorageEngine.get(new Query()
                .append(VariantQueryParam.ID.key(), "1:100000:G:A")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL)
                .append(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), true), new QueryOptions()).first();
        System.out.println("VARIANT " + v.toString() + " = " + v.toJson());
    }

    @Test
    public void testDuplicatedVariantsAccepted1() throws Exception {
        // Make sure loading order doesn't matter for this test
        testDuplicatedVariantsAccepted(
                "by_chr/chr22.variant-test-file.vcf.gz",
                "by_chr/chr22_1-1.variant-test-file.vcf.gz",
                "by_chr/chr22_1-2.variant-test-file.vcf.gz",
                "by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz",
                "by_chr/chr22_1-2-DUP2.variant-test-file.vcf.gz");
    }

    @Test
    public void testDuplicatedVariantsAccepted2() throws Exception {
        // Make sure loading order doesn't matter for this test
        testDuplicatedVariantsAccepted(
                "by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz",
                "by_chr/chr22.variant-test-file.vcf.gz",
                "by_chr/chr22_1-2-DUP2.variant-test-file.vcf.gz",
                "by_chr/chr22_1-1.variant-test-file.vcf.gz",
                "by_chr/chr22_1-2.variant-test-file.vcf.gz");
    }

    public void testDuplicatedVariantsAccepted(String... files) throws Exception {
        URI outDir = newOutputUri();

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.FAMILY.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);

        for (String file : files) {
            variantStorageEngine.index(Collections.singletonList(getResourceUri(file)), outDir);
        }

        variantStorageEngine.removeFile(STUDY_NAME, Paths.get(files[0]).getFileName().toString(), outputUri);
        variantStorageEngine.removeFile(STUDY_NAME, Paths.get(files[2]).getFileName().toString(), outputUri);
        variantStorageEngine.removeFile(STUDY_NAME, Paths.get(files[3]).getFileName().toString(), outputUri);
        variantStorageEngine.index(Collections.singletonList(getResourceUri(files[2])), outDir);
        variantStorageEngine.index(Collections.singletonList(getResourceUri(files[3])), outDir);
        variantStorageEngine.index(Collections.singletonList(getResourceUri(files[0])), outDir);

        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        int studyId = metadataManager.getStudyId(STUDY_NAME);

        Variant v = variantStorageEngine.get(new Query()
                .append(VariantQueryParam.ID.key(), "22:44681612:A:G")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL)
                .append(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), true), new QueryOptions()).first();
        System.out.println("VARIANT " + v.toString() + " = " + v.toJson());
        checkIssueEntries_22_44681612_A_G(v);

        v = variantStorageEngine.getSampleData("22:44681612:A:G", STUDY_NAME, new QueryOptions()).first();
        System.out.println("VARIANT " + v.toString() + " = " + v.toJson());
        checkIssueEntries_22_44681612_A_G(v);

        Iterator<FileMetadata> it = metadataManager.fileMetadataIterator(studyId);
        while (it.hasNext()) {
            FileMetadata fileMetadata = it.next();
            for (Boolean nativeQuery : Arrays.asList(true, false)) {
                String name = fileMetadata.getName();
                Query query = new Query(VariantQueryParam.FILE.key(), name);
                QueryOptions options = new QueryOptions(NATIVE, nativeQuery);
                System.out.println("-----------------------");
                System.out.println("FILE-QUERY = " + query.toJson() + " " + options.toJson());
                for (Variant variant : variantStorageEngine.get(query, new QueryOptions(options)).getResults()) {
                    StudyEntry studyEntry = variant.getStudies().get(0);
                    assertEquals(0, studyEntry.getIssues().size());
                    assertEquals(name, studyEntry.getFiles().get(0).getFileId());
                }
                for (Integer sample : fileMetadata.getSamples()) {
                    String sampleName = metadataManager.getSampleName(studyId, sample);
                    query = new Query(VariantQueryParam.FILE.key(), name).append(VariantQueryParam.SAMPLE.key(), sampleName);
                    System.out.println("SAMPLE-QUERY = " + query.toJson() + " " + options.toJson());
                    for (Variant variant : variantStorageEngine.get(query, new QueryOptions(options)).getResults()) {
                        StudyEntry studyEntry = variant.getStudies().get(0);
                        assertEquals(0, studyEntry.getIssues().size());
                        List<FileEntry> fileEntries = studyEntry.getFiles();
                        if (fileEntries.size() > 1) {
                            fileEntries = new LinkedList<>(fileEntries);
                            // Some extra file entries might be returned containing only an original call
                            fileEntries.removeIf(fe -> fe.getData().isEmpty());
                        }
                        assertEquals(1, fileEntries.size());
                        assertEquals(name, fileEntries.get(0).getFileId());
                        assertEquals(Collections.singleton(sampleName), studyEntry.getSamplesName());
                    }
                }
            }
        }
    }

    private void checkIssueEntries_22_44681612_A_G(Variant v) {
        assertEquals("22:44681612:A:G", v.getId());

        StudyEntry study = v.getStudies().get(0);
        FileEntry file = study.getFile(study.getSample("NA19600").getFileIndex());
        assertEquals("chr22_1-2-DUP.variant-test-file.vcf.gz", file.getFileId());
        Set<String> uniq = new HashSet<>();
        for (SampleEntry sample : study.getSamples()) {
            assertNotNull(sample.getSampleId());
            assertNotNull(sample.getFileIndex());
            assertTrue(uniq + " + " + sample.getSampleId() + "_" + sample.getFileIndex(),
                    uniq.add(sample.getSampleId() + "_" + sample.getFileIndex()));
            String FILE = sample.getData().get(study.getSampleDataKeyPosition("FILE"));
            if (StringUtils.isNotEmpty(FILE) && !FILE.equals(VCFConstants.MISSING_VALUE_v4)) {
                assertEquals(study.getFile(sample.getFileIndex()).getFileId(), FILE + ".variant-test-file.vcf.gz");
            }
        }
        for (IssueEntry issue : study.getIssues()) {
            SampleEntry sample = issue.getSample();
            assertNotNull(sample.getSampleId());
            assertNotNull(sample.getFileIndex());
            assertTrue(uniq + " + " + sample.getSampleId() + "_" + sample.getFileIndex(),
                    uniq.add(sample.getSampleId() + "_" + sample.getFileIndex()));
            String FILE = sample.getData().get(study.getSampleDataKeyPosition("FILE"));
            if (StringUtils.isNotEmpty(FILE) && !FILE.equals(VCFConstants.MISSING_VALUE_v4)) {
                assertEquals(study.getFile(sample.getFileIndex()).getFileId(), FILE + ".variant-test-file.vcf.gz");
            }
        }
        assertNotEquals(0, study.getIssues().size());
    }


    @Test
    public void testLoadMultiFileDataConcurrency() throws Exception {

        URI outDir = newOutputUri();
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.FAMILY.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);

        String resourceDir = "by_chr/";
        String file1 = "chr22.variant-test-file.vcf.gz";
        String file2 = "chr22_1-2-DUP.variant-test-file.vcf.gz";

        failAtLoadingFile(resourceDir, file1, outDir);

        try {
            variantStorageEngine.index(Collections.singletonList(getResourceUri(resourceDir + file2)), outDir);
        } catch (StoragePipelineException e) {
            MatcherAssert.assertThat(e.getCause().getMessage(), startsWith("Can not \"Load\" files"));
            MatcherAssert.assertThat(e.getCause().getMessage(), CoreMatchers.containsString(file2));
            MatcherAssert.assertThat(e.getCause().getMessage(), CoreMatchers.containsString(file1));
        }

        variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
        variantStorageEngine.removeFile(STUDY_NAME, file1, outDir);
        variantStorageEngine.index(Collections.singletonList(getResourceUri(resourceDir + file2)), outDir);

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true);
//        variantStorageEngine.getOptions().put(VariantStorageOptions.RESUME.key(), true);
        variantStorageEngine.index(Collections.singletonList(getResourceUri(resourceDir + file1)), outDir);
    }


    @Test
    public void testLoadMultiFileDataConcurrencyDeleteMany() throws Exception {

        URI outDir = newOutputUri();
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.FAMILY.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);

        String resourceDir = "platinum/";
        String file1 = "1K.end.platinum-genomes-vcf-NA12877_S1.vcf.gz";
        String file2 = "1K.end.platinum-genomes-vcf-NA12878_S1.vcf.gz";

        failAtLoadingFile(resourceDir, file1, outDir);
        failAtLoadingFile(resourceDir, file2, outDir, 2);
//        try {
//            getMockedStorageEngine().index(Collections.singletonList(getResourceUri(resourceDir + file1)), outDir);
//            fail("Should have thrown an exception");
//        } catch (StoragePipelineException e) {
//            assertEquals(MOCKED_EXCEPTION, e.getCause().getMessage());
//        }
//        try {
//            getMockedStorageEngine().index(Collections.singletonList(getResourceUri(resourceDir + file2)), outDir);
//            fail("Should have thrown an exception");
//        } catch (StoragePipelineException e) {
//            assertEquals(MOCKED_EXCEPTION, e.getCause().getMessage());
//        }


        variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
        try {
            variantStorageEngine.removeFile(STUDY_NAME, file1, outDir);
            fail();
        } catch (StorageEngineException e) {
            MatcherAssert.assertThat(e.getMessage(), startsWith("Can not \"remove\" files"));
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString(file1));
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString(file2));
        }
        try {
            variantStorageEngine.removeFile(STUDY_NAME, file2, outDir);
            fail();
        } catch (StorageEngineException e) {
            MatcherAssert.assertThat(e.getMessage(), startsWith("Can not \"remove\" files"));
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString(file1));
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString(file2));
        }
        variantStorageEngine.removeFiles(STUDY_NAME, Arrays.asList(file1, file2), outDir);

        variantStorageEngine.index(Collections.singletonList(getResourceUri(resourceDir + file2)), outDir);
        variantStorageEngine.index(Collections.singletonList(getResourceUri(resourceDir + file1)), outDir);
    }

    @Test
    public void testLoadMultiFileDataConcurrencyFail() throws Exception {

        URI outDir = newOutputUri();
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.FAMILY.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);

        String resourceDir = "by_chr/";
        String file1 = "chr22.variant-test-file.vcf.gz";
        String file2 = "chr22_1-2-DUP.variant-test-file.vcf.gz";

        failAtLoadingFile(resourceDir, file1, outDir);

        try {
            variantStorageEngine.index(Collections.singletonList(getResourceUri(resourceDir + file2)), outDir);
        } catch (StoragePipelineException e) {
            MatcherAssert.assertThat(e.getCause().getMessage(), startsWith("Can not \"Load\" files"));
            MatcherAssert.assertThat(e.getCause().getMessage(), CoreMatchers.containsString(file2));
            MatcherAssert.assertThat(e.getCause().getMessage(), CoreMatchers.containsString(file1));
        }

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true);
        variantStorageEngine.getOptions().put(VariantStorageOptions.RESUME.key(), true);
        variantStorageEngine.index(Collections.singletonList(getResourceUri(resourceDir + file1)), outDir);

    }

    @Test
    public void testDeleteErrorFiles() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        URI file = variantStorageEngine.index(Collections.singletonList(getPlatinumFile(1)), outDir).get(0).getInput();
        String fileName = UriUtils.fileName(file);

        int studyId = mm.getStudyId(STUDY_NAME);
        int fileId = mm.getFileId(studyId, fileName);
        FileMetadata fileMetadata = mm.updateFileMetadata(studyId, fileId, fm -> {
            fm.setIndexStatus(TaskMetadata.Status.INVALID);
        });
        assertFalse(mm.isFileIndexed(studyId, fileId));
        assertFalse(fileMetadata.isIndexed());
        LinkedHashSet<Integer> samples = fileMetadata.getSamples();

        for (Integer sample : samples) {
            mm.updateSampleMetadata(studyId, sample, sampleMetadata -> {
                sampleMetadata.setIndexStatus(TaskMetadata.Status.INVALID);
            });
        }

        try {
            variantStorageEngine.get(new VariantQuery().file(fileName), new QueryOptions());
            fail();
        } catch (VariantQueryException e) {
            String expected = VariantQueryException.fileNotIndexed(fileName, STUDY_NAME).getMessage();
            assertEquals(expected, e.getMessage());
        }

        try {
            variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
            variantStorageEngine.index(Collections.singletonList(getPlatinumFile(1)), outDir);
            fail();
        } catch (StorageEngineException e) {
            try {
                String expected = StorageEngineException.invalidFileStatus(fileId, fileName).getMessage();
                assertEquals(expected, e.getCause().getMessage());
            } catch (AssertionError error) {
                e.printStackTrace();
                throw error;
            }
        }

        try {
            variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), false);
            variantStorageEngine.index(Collections.singletonList(getPlatinumFile(1)), outDir);
            fail();
        } catch (StorageEngineException e) {
            try {
                String expected = StorageEngineException.invalidFileStatus(fileId, fileName).getMessage();
                assertEquals(expected, e.getCause().getMessage());
            } catch (AssertionError error) {
                e.printStackTrace();
                throw error;
            }
        }

        variantStorageEngine.removeFile(STUDY_NAME, fileName, outDir);

        fileMetadata = mm.getFileMetadata(studyId, fileId);
        assertEquals(TaskMetadata.Status.NONE, fileMetadata.getIndexStatus());
        for (Integer sample : samples) {
            assertEquals(TaskMetadata.Status.NONE, mm.getSampleMetadata(studyId, sample).getIndexStatus());
        }

        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(1)), outDir);

        fileMetadata = mm.getFileMetadata(studyId, fileId);
        assertEquals(TaskMetadata.Status.READY, fileMetadata.getIndexStatus());
        for (Integer sample : samples) {
            assertEquals(TaskMetadata.Status.READY, mm.getSampleMetadata(studyId, sample).getIndexStatus());
        }
    }

    @Test
    public void testLoadByRegion() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME + "_split");
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz")),
                outDir, true, true, true);

        int studyId_split = mm.getStudyId(STUDY_NAME + "_split");

//        variantStorageEngine.annotate(outputUri, new ObjectMap());
//        for (String sample : SAMPLES) {
//            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId_split, mm.getSampleId(studyId_split, sample));
//            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
//            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
//            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
//                assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(v));
//                assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexAnnotationStatus(v));
//            }
//        }

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.REGION);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz")),
                outputUri, true, true, true);


        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId_split, mm.getSampleId(studyId_split, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), null);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22.variant-test-file.vcf.gz")),
                outputUri, true, true, true);

        int studyId_normal = mm.getStudyId(STUDY_NAME);
        for (String sample : SAMPLES) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId_normal, mm.getSampleId(studyId_normal, sample));
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(1));
        }

        checkVariantsData(studyId_split, studyId_normal, new VariantQuery().includeSample(ParamConstants.ALL), new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_FILES),
                v -> v.getStudies().get(0).getFiles().forEach(file -> file.setFileId("")));
        checkSampleIndex(studyId_split, studyId_normal);
    }

    @Test
    public void testLoadByRegionAndRemove() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        String studyActual = "study_actual_split";

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), studyActual);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.REGION);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz")), outDir);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz")), outDir);
        variantStorageEngine.removeFiles(studyActual, Collections.singletonList("chr22_1-2.variant-test-file.vcf.gz"), outDir);


        String studyExpected = "study_expected";
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), studyExpected);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), null);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz")), outDir);

        int studyId_actual = mm.getStudyId(studyActual);
        int studyId_expected = mm.getStudyId(studyExpected);

        checkVariantsData(studyId_actual, studyId_expected, new Query(VariantQueryParam.FILE.key(), "chr22_1-1.variant-test-file.vcf.gz"),
                new QueryOptions());
        checkSampleIndex(studyId_actual, studyId_expected);
    }

    @Test
    public void testLoadAndRemoveSamples() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        String study_actual = "study_actual";
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study_actual);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_HOM_REF.key(), true);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22.variant-test-file.vcf.gz")), outDir);

        List<String> samplesToRemove = Arrays.asList("NA19600", "NA19660");
        variantStorageEngine.removeSamples(study_actual, samplesToRemove, outputUri);

        int study_actualId = variantStorageEngine.getMetadataManager().getStudyId(study_actual);
        for (String sample : samplesToRemove) {
            SampleMetadata sampleMetadata = variantStorageEngine.getMetadataManager().getSampleMetadata(study_actualId, sample);
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSecondaryAnnotationIndexStatus());
            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexStatus(v));
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(v));
            }
        }

        for (Variant variant : variantStorageEngine.iterable(new VariantQuery()
                .includeSample(ParamConstants.ALL)
                .includeSampleId(true), new QueryOptions())) {
            List<String> samplesName = variant.getStudies().get(0).getOrderedSamplesName();
            assertEquals(samplesName, Arrays.asList("NA19661", "NA19685"));
        }

        int studyId_actual = mm.getStudyId(study_actual);
        assertEquals(2, mm.getIndexedSamples(studyId_actual).size());

        String study_expected = "study_expected";
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study_expected);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_HOM_REF.key(), true);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22.variant-test-file.s3s4.vcf.gz")), outDir);

        int studyId_expected = mm.getStudyId(study_expected);

        checkVariantsData(studyId_actual, studyId_expected,
                new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA19661,NA19685"), new QueryOptions(),
                v -> v.getStudies().get(0).getFiles().get(0).setFileId("")
        );
        checkSampleIndex(studyId_actual, studyId_expected);
    }

    @Test
    public void testLoadMultiFile() throws Exception {
        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), CellBaseRestVariantAnnotator.class.getName())
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        params.append(VariantStorageOptions.STUDY.key(), "s1");
        runETL(getVariantStorageEngine(), getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri, params, true, true, true);

        params.append(VariantStorageOptions.STUDY.key(), "s2");
        runETL(getVariantStorageEngine(), getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri, params, true, true, true);

        params.append(VariantStorageOptions.STUDY.key(), "multi");
        params.append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI);
        runETL(getVariantStorageEngine(), getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(getVariantStorageEngine(), getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri, params, true, true, true);

//        VariantQueryResult<Variant> s1 = variantStorageEngine.get(new Query(VariantQueryParam.STUDY.key(), "s1").append(VariantQueryParam.GENOTYPE.key(), "NA19600:1|0"));


    }

    /**
     * Load two overlapping VCF files for the same four samples using SplitData.MULTI.
     * Verify that:
     * <ul>
     *   <li>All samples are registered as MULTI split-data</li>
     *   <li>Variants that appear in only one file have no issue entries</li>
     *   <li>Variants shared by both files produce IssueEntry(DISCREPANCY) entries for the secondary file</li>
     * </ul>
     */
    @Test
    public void testLoadMultiFileSameStudy() throws Exception {
        runETL(variantStorageEngine,
                getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri,
                new ObjectMap()
                        .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                        .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI),
                true, true, true);

        runETL(variantStorageEngine,
                getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri,
                new ObjectMap()
                        .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                        .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI),
                true, true, true);

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        int studyId = mm.getStudyId(STUDY_NAME);

        // All samples in both files should be registered as MULTI split-data
        List<String> samples = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");
        for (String sample : samples) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, mm.getSampleId(studyId, sample));
            assertEquals("Sample " + sample + " should have MULTI split-data",
                    VariantStorageEngine.SplitData.MULTI, sampleMetadata.getSplitData());
        }

        // Query all variants with all samples
        Query query = new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL)
                .append(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), true);

        int variantsWithIssues = 0;
        int variantsWithoutIssues = 0;

        for (Variant variant : variantStorageEngine.iterable(query, new QueryOptions())) {
            StudyEntry study = variant.getStudies().get(0);
            List<IssueEntry> issues = study.getIssues();

            if (issues != null && !issues.isEmpty()) {
                variantsWithIssues++;
                // Every sample in the issues list should have a sampleId and fileIndex
                for (IssueEntry issue : issues) {
                    SampleEntry issueSample = issue.getSample();
                    assertNotNull("Issue sample should not be null for variant " + variant, issueSample);
                    assertNotNull("Issue sample sampleId should not be null for variant " + variant, issueSample.getSampleId());
                    assertNotNull("Issue sample fileIndex should not be null for variant " + variant, issueSample.getFileIndex());
                }
                // Verify no duplicate (sampleId, fileIndex) pairs across primary samples and issues
                Set<String> seen = new HashSet<>();
                for (SampleEntry sample : study.getSamples()) {
                    if (sample.getSampleId() != null && sample.getFileIndex() != null) {
                        assertTrue("Duplicate sample+fileIndex in primary entries",
                                seen.add(sample.getSampleId() + "_" + sample.getFileIndex()));
                    }
                }
                for (IssueEntry issue : issues) {
                    SampleEntry issueSample = issue.getSample();
                    if (issueSample.getSampleId() != null && issueSample.getFileIndex() != null) {
                        assertTrue("Duplicate sample+fileIndex across primary+issue entries for " + variant,
                                seen.add(issueSample.getSampleId() + "_" + issueSample.getFileIndex()));
                    }
                }
            } else {
                variantsWithoutIssues++;
            }
        }

        // The two VCF files (chr22_1-2 and chr22_1-2-DUP) have overlapping variants,
        // so there should be some variants with issues
        assertTrue("Expected some variants with issue entries (shared between files), got 0", variantsWithIssues > 0);
        assertTrue("Expected some variants without issue entries (unique to one file), got 0", variantsWithoutIssues > 0);
    }

    /**
     * Verify that querying by file returns results with no issue entries (per-file view).
     */
    @Test
    public void testQueryByFileNoIssues() throws Exception {
        String file1 = "chr22_1-2.variant-test-file.vcf.gz";
        String file2 = "chr22_1-2-DUP.variant-test-file.vcf.gz";

        runETL(variantStorageEngine,
                getResourceUri("by_chr/" + file1), outputUri,
                new ObjectMap()
                        .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                        .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI),
                true, true, true);
        runETL(variantStorageEngine,
                getResourceUri("by_chr/" + file2), outputUri,
                new ObjectMap()
                        .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                        .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI),
                true, true, true);

        for (String file : Arrays.asList(file1, file2)) {
            Query query = new Query(VariantQueryParam.FILE.key(), file);
            for (Variant variant : variantStorageEngine.iterable(query, new QueryOptions())) {
                StudyEntry study = variant.getStudies().get(0);
                List<IssueEntry> issues = study.getIssues();
                assertTrue("Querying by file '" + file + "' should produce no issue entries for variant " + variant,
                        issues == null || issues.isEmpty());
            }
        }
    }


    protected void failAtLoadingFile(String x, String file1, URI outputUri) throws Exception {
        failAtLoadingFile(x, file1, outputUri, 1);
    }

    protected void failAtLoadingFile(String x, String file1, URI outputUri, int expectedRunningTasks) throws Exception {
        try {
            VariantStorageEngine engine = getMockedStorageEngine(new ObjectMap(VariantStorageOptions.STUDY.key(), STUDY_NAME));
            engine.index(Collections.singletonList(getResourceUri(x + file1)), outputUri);
            fail("Should have thrown an exception");
        } catch (StoragePipelineException e) {
            try {
                assertEquals(MOCKED_EXCEPTION, e.getCause().getMessage());
                int studyId = metadataManager.getStudyId(STUDY_NAME);
                FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, file1);
                assertEquals(TaskMetadata.Status.NONE, fileMetadata.getIndexStatus());
                List<TaskMetadata> runningTasks = new ArrayList<>();
                metadataManager.getRunningTasks(studyId).forEach(runningTasks::add);
                assertEquals(expectedRunningTasks, runningTasks.size());
                TaskMetadata taskMetadata = runningTasks.get(runningTasks.size() - 1);
                assertEquals(TaskMetadata.Type.LOAD, taskMetadata.getType());
                assertEquals(TaskMetadata.Status.RUNNING, taskMetadata.currentStatus());
                assertEquals(Arrays.asList(fileMetadata.getId()), taskMetadata.getFileIds());
            } catch (AssertionError error) {
                error.addSuppressed(e);
                e.printStackTrace();
                throw error;
            }
        }
    }

    protected void failAtDeletingFile(String file, URI outputUri, int expectedRunningTasks, ObjectMap options) throws Exception {
        int studyId = metadataManager.getStudyId(STUDY_NAME);
        FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, file);
        try {
            getMockedStorageEngine(options).removeFile(STUDY_NAME, file, outputUri);
            fail("Should have thrown an exception");
        } catch (StorageEngineException e) {
            try {
                assertEquals(MOCKED_EXCEPTION, e.getMessage());
                assertEquals(TaskMetadata.Status.NONE, fileMetadata.getIndexStatus());
                List<TaskMetadata> runningTasks = new ArrayList<>();
                metadataManager.getRunningTasks(studyId).forEach(runningTasks::add);
                assertEquals(expectedRunningTasks, runningTasks.size());
                Optional<TaskMetadata> optional = runningTasks.stream()
                        .filter(t -> t.getType() == TaskMetadata.Type.REMOVE && Arrays.asList(fileMetadata.getId()).equals(t.getFileIds()))
                        .findFirst();
                assertTrue(optional.isPresent());
                assertEquals(TaskMetadata.Type.REMOVE, optional.get().getType());
                assertEquals(TaskMetadata.Status.RUNNING, optional.get().currentStatus());
                assertEquals(Arrays.asList(fileMetadata.getId()), optional.get().getFileIds());
            } catch (AssertionError error) {
                e.printStackTrace();
                throw error;
            }
        }
    }

    protected VariantStorageEngine getMockedStorageEngine() throws Exception {
        return getMockedStorageEngine(new ObjectMap());
    }

    protected VariantStorageEngine getMockedStorageEngine(ObjectMap options) throws Exception {
        VariantStorageEngine mockedStorageEngine = Mockito.spy(getVariantStorageEngine());
        mockedStorageEngine.getOptions().putAll(options);
        mockedStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        VariantStoragePipeline mockedPipeline = Mockito.spy(variantStorageEngine.newStoragePipeline(true));

        Mockito.doReturn(mockedPipeline).when(mockedStorageEngine).newStoragePipeline(Mockito.anyBoolean());
//        Mockito.doThrow(new StoragePipelineException(MOCKED_EXCEPTION, Collections.emptyList())).when(mockedPipeline).load(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            // Throw StorageEngineException when calling load
            System.out.printf("MOCKED load(%s, %s)%n", invocation.getArgument(0), invocation.getArgument(1));
            System.out.println("MOCKED load throw StorageEngineException");
            throw new StoragePipelineException(MOCKED_EXCEPTION, Collections.emptyList());
        }).when(mockedPipeline).load(Mockito.any(), Mockito.any());

        Mockito.doAnswer(invocation -> {
            // Call real method when calling preRemove, then throw StorageEngineException
            System.out.printf("MOCKED preRemove(%s, %s, %s)%n", invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
            System.out.println("MOCKED preRemove callRealMethod");
            invocation.callRealMethod();
            System.out.println("MOCKED preRemove callRealMethod DONE");
            System.out.println("MOCKED preRemove throw StorageEngineException");
            throw new StorageEngineException(MOCKED_EXCEPTION);
        }).when(mockedStorageEngine).preRemove(Mockito.any(), Mockito.any(), Mockito.any());
        return mockedStorageEngine;
    }

    public void checkVariantsData(int studyIdActual, int studyIdExpected, Query query, QueryOptions options) throws Exception {
        checkVariantsData(studyIdActual, studyIdExpected, query, options, v -> {});
    }

    public void checkVariantsData(int studyIdActual, int studyIdExpected, Query query, QueryOptions options, Consumer<Variant> mapper)
            throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        VariantDBIterator itAct = dbAdaptor.iterator(new Query(query).append(VariantQueryParam.STUDY.key(), studyIdActual), options);
        VariantDBIterator itExp = dbAdaptor.iterator(new Query(query).append(VariantQueryParam.STUDY.key(), studyIdExpected), options);
        while (itExp.hasNext()) {
            assertTrue(itAct.hasNext());
            Variant exp = itExp.next();
            mapper.accept(exp);
            exp.getStudies().get(0).setStudyId("");
            Variant act = itAct.next();
            mapper.accept(act);
            act.getStudies().get(0).setStudyId("");
            assertEquals(exp.toJson(), act.toJson());
        }
        assertFalse(itAct.hasNext());

    }

    public void checkSampleIndex(int studyIdActual, int studyIdExpected) throws Exception {
        SampleIndexDBAdaptor sampleIndexDBAdaptor = variantStorageEngine.getSampleIndexDBAdaptor();
        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        for (Integer sampleIdExpected : mm.getIndexedSamples(studyIdExpected)) {
            String sampleName = mm.getSampleName(studyIdExpected, sampleIdExpected);
            Iterator<SampleIndexEntry> itExp = sampleIndexDBAdaptor.indexEntryIterator(studyIdExpected, sampleIdExpected);
            Iterator<SampleIndexEntry> itAct = sampleIndexDBAdaptor.indexEntryIterator(studyIdActual, mm.getSampleId(studyIdActual, sampleName));

            while (itExp.hasNext()) {
                assertTrue(itAct.hasNext());
                SampleIndexEntry exp = itExp.next();
                SampleIndexEntry act = itAct.next();
                exp.setSampleId(0); // sample id might be different
                act.setSampleId(0); // sample id might be different
                assertEquals(exp, act);
                for (String gt : exp.getGts().keySet()) {
                    assertEquals(gt, exp.getGtEntry(gt), act.getGtEntry(gt));
                }
            }
            assertFalse(itAct.hasNext());
        }
    }
}
