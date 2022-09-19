package org.opencb.opencga.storage.hadoop.variant;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor.NATIVE;

public class HadoopVariantStorageEngineSplitDataTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    public static final List<String> SAMPLES = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
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
                System.out.println("-----------------------");
                System.out.println("FILE-QUERY = " + query.toJson());
                for (Variant variant : variantStorageEngine.get(query, new QueryOptions(NATIVE, nativeQuery)).getResults()) {
                    StudyEntry studyEntry = variant.getStudies().get(0);
                    assertEquals(0, studyEntry.getIssues().size());
                    assertEquals(name, studyEntry.getFiles().get(0).getFileId());
                }
                for (Integer sample : fileMetadata.getSamples()) {
                    String sampleName = metadataManager.getSampleName(studyId, sample);
                    query = new Query(VariantQueryParam.FILE.key(), name).append(VariantQueryParam.SAMPLE.key(), sampleName);
                    System.out.println("SAMPLE-QUERY = " + query.toJson());
                    for (Variant variant : variantStorageEngine.get(query, new QueryOptions(NATIVE, nativeQuery)).getResults()) {
                        StudyEntry studyEntry = variant.getStudies().get(0);
                        assertEquals(0, studyEntry.getIssues().size());
                        assertEquals(name, studyEntry.getFiles().get(0).getFileId());
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
    public void testLoadByRegion() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME + "_split");
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz")),
                outDir, true, true, true);

        int studyId_split = mm.getStudyId(STUDY_NAME + "_split");

//        variantStorageEngine.annotate(new Query(), new QueryOptions(DefaultVariantAnnotationManager.OUT_DIR, outputUri));
//        for (String sample : SAMPLES) {
//            SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId_split, mm.getSampleId(studyId_split, sample));
//            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
//            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getAnnotationStatus());
//            assertEquals(TaskMetadata.Status.READY, SampleIndexDBAdaptor.getSampleIndexStatus(sampleMetadata));
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

        checkVariantsTable(studyId_split, studyId_normal, new VariantQuery().includeSample(ParamConstants.ALL), new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_FILES));
        checkSampleIndex(studyId_split, studyId_normal);
    }

    @Test
    public void testLoadByRegionAndRemove() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME + "_split");
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz")), outDir);

        int studyId_actual = mm.getStudyId(STUDY_NAME + "_split");

        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.REGION);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz")), outputUri);

        variantStorageEngine.removeFiles(STUDY_NAME + "_split", Collections.singletonList("chr22_1-2.variant-test-file.vcf.gz"), outputUri);


        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), null);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        variantStorageEngine.index(Collections.singletonList(getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz")), outDir);

        int studyId_expected = mm.getStudyId(STUDY_NAME);

        checkVariantsTable(studyId_actual, studyId_expected, new Query(VariantQueryParam.FILE.key(), "chr22_1-1.variant-test-file.vcf.gz"),
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

        variantStorageEngine.removeSamples(study_actual, Arrays.asList("NA19600", "NA19660"), outputUri);

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

        checkVariantsTable(studyId_actual, studyId_expected,
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

    public void checkVariantsTable(int studyIdActual, int studyIdExpected, Query query, QueryOptions options) throws Exception {
        checkVariantsTable(studyIdActual, studyIdExpected, query, options, v -> {});
    }

    public void checkVariantsTable(int studyIdActual, int studyIdExpected, Query query, QueryOptions options, Consumer<Variant> mapper)
            throws Exception {
        VariantHadoopDBAdaptor dbAdaptor = (VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor();

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
        VariantHadoopDBAdaptor dbAdaptor = (VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor();
        SampleIndexDBAdaptor sampleIndexDBAdaptor = new SampleIndexDBAdaptor(
                dbAdaptor.getHBaseManager(),
                dbAdaptor.getTableNameGenerator(),
                dbAdaptor.getMetadataManager());
        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        for (Integer sampleIdExpected : mm.getIndexedSamples(studyIdExpected)) {
            String sampleName = mm.getSampleName(studyIdExpected, sampleIdExpected);
            Iterator<SampleIndexEntry> itExp = sampleIndexDBAdaptor.rawIterator(studyIdExpected, sampleIdExpected);
            Iterator<SampleIndexEntry> itAct = sampleIndexDBAdaptor.rawIterator(studyIdActual, mm.getSampleId(studyIdActual, sampleName));

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
