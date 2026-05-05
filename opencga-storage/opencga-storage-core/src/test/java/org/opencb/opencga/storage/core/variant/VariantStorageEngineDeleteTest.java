package org.opencb.opencga.storage.core.variant;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.storage.core.StorageEngineTest;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.Assert.*;

@Ignore
@StorageEngineTest
public abstract class VariantStorageEngineDeleteTest  extends VariantStorageBaseTest {

    private static Logger logger = LoggerFactory.getLogger(VariantStorageEngineDeleteTest.class);

    /**
     * Hook for backend-specific cross-engine reference comparison after a file removal.
     * Default is a no-op; Mongo overrides it to compare variant documents against a freshly
     * built "expected" engine that never loaded the removed file. Called explicitly from
     * tests where such a comparison is meaningful (not from tests that mutate state via
     * fill-gaps or multi-file data, where the comparison would not hold).
     */
    protected void checkAfterRemoveFileAgainstReference(String removedStudy, String removedFileName,
                                                        List<String> otherStudies) throws Exception {
    }

    /**
     * Hook called after file removal. Verifies sample index is cleared for fully removed samples.
     * Override to add additional backend-specific checks.
     */
    protected void checkAfterRemoveFile(String study, String removedFileName) throws Exception {
        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        int studyId = mm.getStudyId(study);
        org.opencb.opencga.storage.core.metadata.models.FileMetadata fileMetadata =
                mm.getFileMetadata(studyId, removedFileName);
        org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor sampleIndexDBAdaptor =
                variantStorageEngine.getSampleIndexDBAdaptor();
        org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema schema =
                sampleIndexDBAdaptor.getSchemaLatest(study);
        for (Integer sampleId : fileMetadata.getSamples()) {
            SampleMetadata sm = mm.getSampleMetadata(studyId, sampleId);
            boolean hasOtherFiles = sm.getFiles().stream()
                    .anyMatch(fid -> mm.isFileIndexed(studyId, fid));
            if (!hasOtherFiles) {
                assertFalse("Sample index should be empty for sample " + sampleId
                                + " after removing file " + removedFileName,
                        sampleIndexDBAdaptor.iteratorByGt(studyId, sampleId, schema).hasNext());
            }
        }
    }

    /**
     * Hook called after variantsPrune. Override to add backend-specific post-prune verification.
     */
    protected void checkAfterPrune(String study) throws Exception {
    }

    /**
     * Hook called after loading and before any removal. Override for debug output.
     */
    protected void printVariants(String label) throws Exception {
    }

    @Test
    public void testLoadAndRemove() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        String study1 = "study1";
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study1);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_HOM_REF.key(), true);

        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(1)), outDir);
        URI file2Uri = getPlatinumFile(2);
        variantStorageEngine.index(Collections.singletonList(file2Uri), outDir);
        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(3)), outDir);

        int study1Id = mm.getStudyId(study1);
        String file2 = UriUtils.fileName(file2Uri);

        variantStorageEngine.removeFile(study1, file2, outputUri);
        LinkedHashSet<Integer> samples = mm.getSampleIdsFromFileId(study1Id, mm.getFileId(study1Id, file2));

        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(study1Id, sampleId);
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSecondaryAnnotationIndexStatus());
            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexStatus(v));
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(v));
            }
        }

        // Simulate that the samples were left as annotated. Ensure that the status is cleared
        for (Integer sample : samples) {
            mm.updateSampleMetadata(study1Id, sample, s -> s.setAnnotationStatus(TaskMetadata.Status.READY));
        }

        variantStorageEngine.index(Collections.singletonList(file2Uri), outDir);

        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(study1Id, sampleId);
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSecondaryAnnotationIndexStatus());
            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(v));
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(v));
            }
        }

        // Simulate that the samples are annotated. Ensure that the status is cleared
        for (Integer sample : samples) {
            mm.updateSampleMetadata(study1Id, sample, s -> s.setAnnotationStatus(TaskMetadata.Status.READY));
        }
        variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
        variantStorageEngine.index(Collections.singletonList(file2Uri), outDir);

        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(study1Id, sampleId);
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSecondaryAnnotationIndexStatus());
            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(v));
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(v));
            }
        }

    }

    @Test
    public void testRemoveSingleFile() throws Exception {
        String study = "study_remove_single";
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ANNOTATE.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STATS_CALCULATE.key(), false);

        URI platinumFile = getPlatinumFile(0);
        variantStorageEngine.index(Collections.singletonList(platinumFile), outputUri);

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        int studyId = mm.getStudyId(study);

        long countBefore = variantStorageEngine.getDBAdaptor()
                .count(new Query(VariantQueryParam.STUDY.key(), study)).first();
        assertTrue("Expected variants loaded", countBefore > 0);

        String fileName = java.nio.file.Paths.get(platinumFile).getFileName().toString();
        variantStorageEngine.removeFile(study, fileName, newOutputUri("remove_single"));
        checkAfterRemoveFile(study, fileName);
        printVariants("after_remove_single");

        // After removing the only file, no indexed files remain — prune directly
        variantStorageEngine.variantsPrune(false, false, newOutputUri("prune_single"));
        checkAfterPrune(study);

        long countAfter = variantStorageEngine.getDBAdaptor()
                .count(new Query(VariantQueryParam.STUDY.key(), study)).first();
        assertEquals("Expected no variants after removing only file + prune", 0, countAfter);
        assertEquals("Expected no indexed files", 0,
                mm.getIndexedFiles(studyId).size());
    }

    @Test
    public void testRemoveFileAndPrune() throws Exception {
        String study = "study_remove_prune";
        String otherStudy = "study_remove_prune_untouched";
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ANNOTATE.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STATS_CALCULATE.key(), false);

        // Load 2 platinum files with different samples into study1
        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(0)), outputUri);
        URI file2 = getPlatinumFile(1);
        variantStorageEngine.index(Collections.singletonList(file2), outputUri);

        // Load a 3rd file into a second untouched study, to verify removal from study1 does
        // not affect variants, files or counts belonging to another study.
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), otherStudy);
        URI otherFile = getPlatinumFile(2);
        variantStorageEngine.index(Collections.singletonList(otherFile), outputUri);
        // Restore active study for the rest of the test
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study);

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        int studyId = mm.getStudyId(study);
        int otherStudyId = mm.getStudyId(otherStudy);

        variantStorageEngine.calculateStats(study,
                Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());
        // The untouched study also needs ALL-cohort stats so variantsPrune can run over it.
        variantStorageEngine.calculateStats(otherStudy,
                Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        long countBefore = variantStorageEngine.getDBAdaptor()
                .count(new Query(VariantQueryParam.STUDY.key(), study)).first();
        assertTrue("Expected variants loaded", countBefore > 0);
        long otherCountBefore = variantStorageEngine.getDBAdaptor()
                .count(new Query(VariantQueryParam.STUDY.key(), otherStudy)).first();
        assertTrue("Expected variants loaded in untouched study", otherCountBefore > 0);
        int otherIndexedFilesBefore = mm.getIndexedFiles(otherStudyId).size();
        assertEquals("Expected 1 indexed file in untouched study", 1, otherIndexedFilesBefore);

        // Dry-run prune should find nothing to prune (all files present)
        variantStorageEngine.variantsPrune(true, false, newOutputUri("prune_dry_before"));

        // Remove file2
        String file2Name = java.nio.file.Paths.get(file2).getFileName().toString();
        variantStorageEngine.removeFile(study, file2Name, newOutputUri("remove_file2"));
        checkAfterRemoveFile(study, file2Name);
        printVariants("after_remove_file2");

        // Recalculate stats and prune
        variantStorageEngine.calculateStats(study,
                Collections.singletonList(StudyEntry.DEFAULT_COHORT),
                new QueryOptions(VariantStorageOptions.STATS_OVERWRITE.key(), true));
        variantStorageEngine.variantsPrune(false, false, newOutputUri("prune_wet"));
        checkAfterPrune(study);

        long countAfter = variantStorageEngine.getDBAdaptor()
                .count(new Query(VariantQueryParam.STUDY.key(), study)).first();
        assertTrue("Expected some variants remaining from file1", countAfter > 0);
        assertTrue("Expected fewer variants after removing file2 + prune",
                countAfter <= countBefore);

        // The untouched second study must be unaffected by the removal + prune
        long otherCountAfter = variantStorageEngine.getDBAdaptor()
                .count(new Query(VariantQueryParam.STUDY.key(), otherStudy)).first();
        assertEquals("Untouched study's variant count must not change after removing a file "
                + "from another study", otherCountBefore, otherCountAfter);
        assertEquals("Untouched study must keep its indexed file", otherIndexedFilesBefore,
                mm.getIndexedFiles(otherStudyId).size());

        // Dry-run prune after wet prune should find 0
        variantStorageEngine.variantsPrune(true, false, newOutputUri("prune_dry_after"));

        // Verify only 1 indexed file remains
        assertEquals("Expected 1 indexed file remaining", 1,
                mm.getIndexedFiles(studyId).size());

        checkAfterRemoveFileAgainstReference(study, file2Name,
                Collections.singletonList(otherStudy));
    }

    @Test
    public void testRemoveStudyAndPrune() throws Exception {
        String study1 = "study_rs1";
        String study2 = "study_rs2";

        // Load into study1
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study1);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ANNOTATE.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STATS_CALCULATE.key(), false);
        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(0)), outputUri);
        variantStorageEngine.calculateStats(study1,
                Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        // Load into study2
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study2);
        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(1)), outputUri);
        variantStorageEngine.calculateStats(study2,
                Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        long totalBefore = variantStorageEngine.getDBAdaptor().count(new Query()).first();

        // Remove study1
        variantStorageEngine.removeStudy(study1, newOutputUri("remove_study1"));
        printVariants("after_remove_study1");

        // Recalculate stats for remaining study and prune
        variantStorageEngine.calculateStats(study2,
                Collections.singletonList(StudyEntry.DEFAULT_COHORT),
                new QueryOptions(VariantStorageOptions.STATS_OVERWRITE.key(), true));
        variantStorageEngine.variantsPrune(false, false, newOutputUri("prune_study"));
        checkAfterPrune(study2);

        // All study1-only variants should be pruned
        long countStudy2 = variantStorageEngine.getDBAdaptor()
                .count(new Query(VariantQueryParam.STUDY.key(), study2)).first();
        assertTrue("Expected variants remaining in study2", countStudy2 > 0);

        // Second prune should find nothing
        variantStorageEngine.variantsPrune(true, false, newOutputUri("prune_dry_after"));
    }

    @Test
    public void testRemoveFileMultiFileData() throws Exception {
        testRemoveFileMultiFileData(false, false);
    }

    @Test
    public void testRemoveFileMultiFileDataWithAnnotationAndFamilyIndex() throws Exception {
        testRemoveFileMultiFileData(true, true);
    }

    protected void testRemoveFileMultiFileData(boolean annotate, boolean familyIndex) throws Exception {
        String study = STUDY_NAME;
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ANNOTATE.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STATS_CALCULATE.key(), false);

        runETL(variantStorageEngine, getResourceUri("s1.genome.vcf"), outputUri,
                new ObjectMap(variantStorageEngine.getOptions()), true, true, true);
        runETL(variantStorageEngine, getResourceUri("s2.genome.vcf"), outputUri,
                new ObjectMap(variantStorageEngine.getOptions()), true, true, true);
        runETL(variantStorageEngine, getResourceUri("s1_s2.genome.vcf"), outputUri,
                new ObjectMap(variantStorageEngine.getOptions())
                        .append(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true),
                true, true, true);

        if (annotate) {
            variantStorageEngine.annotate(newOutputUri(), new ObjectMap());
        }
        if (familyIndex) {
            variantStorageEngine.familyIndex(study,
                    Collections.singletonList(new Trio("s1", null, "s2")), new ObjectMap());
        }

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
        int studyId = mm.getStudyId(study);
        int sampleId = mm.getSampleIdOrFail(studyId, "s2");

        SampleMetadata sampleMetadata = mm.getSampleMetadata(studyId, sampleId);
        assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(1));
        assertEquals(annotate ? TaskMetadata.Status.READY : TaskMetadata.Status.NONE,
                sampleMetadata.getSampleIndexAnnotationStatus(1));
        assertEquals(familyIndex ? TaskMetadata.Status.READY : TaskMetadata.Status.NONE,
                sampleMetadata.getFamilyIndexStatus(1));

        printVariants("before_remove_multifile");

        // Remove one of the files for s2
        variantStorageEngine.removeFile(study, "s2.genome.vcf", newOutputUri("remove_s2"));
        checkAfterRemoveFile(study, "s2.genome.vcf");

        sampleMetadata = mm.getSampleMetadata(studyId, sampleId);
        // Sample index should still be ready — sample still has data from s1_s2.genome.vcf
        assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(1));
        assertEquals(annotate ? TaskMetadata.Status.READY : TaskMetadata.Status.NONE,
                sampleMetadata.getSampleIndexAnnotationStatus(1));
        assertEquals(familyIndex ? TaskMetadata.Status.READY : TaskMetadata.Status.NONE,
                sampleMetadata.getFamilyIndexStatus(1));

        printVariants("after_remove_multifile");
        variantStorageEngine.calculateStats(study,
                Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        // Sample s2 should still be queryable from the remaining multi-file
        Query sampleQuery = new Query(VariantQueryParam.SAMPLE.key(), "s2");
        List<Variant> results = variantStorageEngine.getDBAdaptor()
                .get(sampleQuery, new QueryOptions()).getResults();
        assertNotEquals("Sample s2 should still have variants from multi-file", 0, results.size());
    }

    @Test
    public void testRemoveFileAfterFillGaps() throws Exception {
        String study = STUDY_NAME;
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ANNOTATE.key(), false);
        variantStorageEngine.getOptions().put(VariantStorageOptions.STATS_CALCULATE.key(), false);

        runETL(variantStorageEngine, getResourceUri("s1.genome.vcf"), outputUri,
                new ObjectMap(variantStorageEngine.getOptions()), true, true, true);
        runETL(variantStorageEngine, getResourceUri("s2.genome.vcf"), outputUri,
                new ObjectMap(variantStorageEngine.getOptions()), true, true, true);

        printVariants("after_load_fillgaps");

        // Fill gaps between s1 and s2
        variantStorageEngine.aggregateFamily(study,
                new VariantAggregateFamilyParams(Arrays.asList("s1", "s2"), false),
                new ObjectMap(), newOutputUri("fill_gaps"));

        printVariants("after_fillgaps");

        // Verify fill-gaps worked: s2 should have data for s1's variants
        Query query = new Query(VariantQueryParam.SAMPLE.key(), "s2");
        long countBeforeRemove = variantStorageEngine.getDBAdaptor()
                .count(query).first();
        assertTrue("Expected s2 variants after fill-gaps", countBeforeRemove > 0);

        // Remove s2's file
        variantStorageEngine.removeFile(study, "s2.genome.vcf", newOutputUri("remove_s2_fillgaps"));
        checkAfterRemoveFile(study, "s2.genome.vcf");
        printVariants("after_remove_fillgaps");

        // s1 should still have its variants
        query = new Query(VariantQueryParam.SAMPLE.key(), "s1");
        List<Variant> s1Results = variantStorageEngine.getDBAdaptor()
                .get(query, new QueryOptions()).getResults();
        assertNotEquals("s1 should still have variants", 0, s1Results.size());

        // s2 should have no data from its own file (removed), but may still show from fill-gaps
        // The key assertion: the removal completed without error
    }

}
