package org.opencb.opencga.storage.hadoop.variant.prune;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class VariantPruneManagerTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    public static final String STUDY_NAME_3 = "study_3";
    public static final String STUDY_NAME_4 = "study_4";
    public static final String STUDY_NAME_5 = "study_5";

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private VariantHadoopDBAdaptor dbAdaptor;
    private boolean loaded;
    private HadoopVariantStorageEngine engine;

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Before
    public void before() throws Exception {
        engine = getVariantStorageEngine();
        dbAdaptor = engine.getDBAdaptor();
        clearDB(DB_NAME);
//        if (!loaded) {
//            load();
//            loaded = true;
//        }
    }

    public void load() throws Exception {
        clearDB(DB_NAME);

        // Study 1 - single file
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, smallInputUri, outputUri, params, true, true, true);
        engine.calculateStats(STUDY_NAME, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        // Study 2 - multi files
        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_2)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI);

        runETL(engine, getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        engine.calculateStats(STUDY_NAME_2, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());


        // Study 3 - platinum
        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_3)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getPlatinumFile(0), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(1), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(2), outputUri, params, true, true, true);
        engine.calculateStats(STUDY_NAME_3, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        // Study 4 - platinum_2
        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_4)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getPlatinumFile(3), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(4), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(5), outputUri, params, true, true, true);
        engine.calculateStats(STUDY_NAME_4, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        // Study 5, dense
        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_4)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getResourceUri("variant-test-dense.vcf.gz"), outputUri, params, true, true, true);
        engine.calculateStats(STUDY_NAME_4, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());


        // ---------------- Annotate
        this.variantStorageEngine.annotate(outputUri, new QueryOptions());

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
    }

    @Test
    public void testVariantPruneSingleStudy() throws Exception {
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_3)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getPlatinumFile(0), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(1), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(2), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(3), outputUri, params, true, true, true);

        checkStatsUpdateRequiredForVariantPrune();
        engine.calculateStats(STUDY_NAME_3, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        variantPrune("1_prune_dry", true, 0);

        int studyId = engine.getMetadataManager().getStudyId(STUDY_NAME_3);
        List<Integer> samples = engine.getMetadataManager().getIndexedSamples(studyId);
        String sampleName = engine.getMetadataManager().getSampleName(studyId, samples.get(0));
        engine.removeSamples(STUDY_NAME_3, Collections.singletonList(sampleName), newOutputUri("2_remove_sample_" + sampleName));

        checkStatsUpdateRequiredForVariantPrune();
        engine.calculateStats(STUDY_NAME_3, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        int variantsToPrune = variantPrune("3_prune_dry", true);
        variantPrune("4_prune_wet", false, variantsToPrune);
        variantPrune("5_prune_dry", true, 0);
    }

    @Test
    public void testVariantPruneSingleStudyMultiFile() throws Exception {
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_2)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI);

        runETL(engine, getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri, params, true, true, true);

        checkStatsUpdateRequiredForVariantPrune();
        engine.calculateStats(STUDY_NAME_2, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());
        runETL(engine, getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri, params, true, true, true);


        checkStatsUpdateRequiredForVariantPrune();
        engine.calculateStats(STUDY_NAME_2, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        variantPrune("1_prune_dry", true, 0);

        engine.removeFile(STUDY_NAME_2, "chr22_1-1.variant-test-file.vcf.gz", newOutputUri("2_remove_sample_chr22_1-1.variant-test-file"));

        checkStatsUpdateRequiredForVariantPrune();
        engine.calculateStats(STUDY_NAME_2, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        int variantsToPrune = variantPrune("3_prune_dry", true);
        variantPrune("4_prune_wet", false, variantsToPrune);
        variantPrune("5_prune_dry", true, 0);
    }


    @Test
    public void testVariantPruneMultiStudy() throws Exception {
        load();

        variantPrune("1_prune_dry", true, 0);

        engine.removeFile(STUDY_NAME_3, "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz", outputUri);
        checkStatsUpdateRequiredForVariantPrune();
        engine.calculateStats(STUDY_NAME_3, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        engine.removeFile(STUDY_NAME_2, "chr22_1-1.variant-test-file.vcf.gz", newOutputUri("2_remove_sample_chr22_1-1.variant-test-file"));
        checkStatsUpdateRequiredForVariantPrune();
        engine.calculateStats(STUDY_NAME_2, Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        int variantsToPrune = variantPrune("3_prune_dry", true);
        variantPrune("4_prune_wet", false, variantsToPrune);
        variantPrune("5_prune_dry", true, 0);
    }

    private int variantPrune(String testName, boolean dryMode) throws Exception {
        return variantPrune(testName, dryMode, null);
    }

    private int variantPrune(String testName, boolean dryMode, Integer expectedPrunedVariants) throws Exception {
        URI outdir = newOutputUri(testName);
        getVariantStorageEngine().variantsPrune(dryMode, false, outdir);

        Path report = Files.list(Paths.get(outdir)).filter(p -> p.getFileName().toString().contains("variant_prune_report")).findFirst()
                .orElse(null);
        int reportedVariants;
        if (report == null) {
            reportedVariants = 0;
        } else {
            reportedVariants = (int) Files.lines(report).count();
        }
        if (expectedPrunedVariants == null) {
            MatcherAssert.assertThat(reportedVariants, VariantMatchers.gt(0));
        } else {
            assertEquals(expectedPrunedVariants.intValue(), reportedVariants);
        }
        return reportedVariants;
    }

    private void checkStatsUpdateRequiredForVariantPrune() throws Exception {
        try {
            getVariantStorageEngine().variantsPrune(true, false, outputUri);
            fail("Should fail, as the variant stats are not valid");
        } catch (StorageEngineException e) {
            System.out.println(e.getClass() + " = " + e.getMessage());
            assertTrue(e.getMessage().startsWith("Unable to run variant prune operation. Please, run variant stats index on cohort"));
        }
    }

}