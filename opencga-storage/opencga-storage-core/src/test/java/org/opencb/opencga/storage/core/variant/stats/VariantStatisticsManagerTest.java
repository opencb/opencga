package org.opencb.opencga.storage.core.variant.stats;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by hpccoll1 on 01/06/15.
 */
@Ignore
public abstract class VariantStatisticsManagerTest extends VariantStorageManagerTestUtils {

    public static final String VCF_TEST_FILE_NAME = "variant-test-file.vcf.gz";
    private StudyConfiguration studyConfiguration;
    private VariantDBAdaptor dbAdaptor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws IOException {
        Path rootDir = getTmpRootDir();
        Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAME);
        Files.copy(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME), inputPath, StandardCopyOption.REPLACE_EXISTING);
        inputUri = inputPath.toUri();
    }

    @Override
    @Before
    public void before() throws Exception {
        studyConfiguration = newStudyConfiguration();
        clearDB(DB_NAME);
        runDefaultETL(inputUri, getVariantStorageManager(), studyConfiguration, new ObjectMap(VariantStorageManager.Options.ANNOTATE.key(), false));
        dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
    }

    @Test
    public void calculateStatsMultiCohortsTest() throws Exception {
        //Calculate stats for 2 cohorts at one time
        VariantStatisticsManager vsm = new VariantStatisticsManager();

        checkCohorts(dbAdaptor, studyConfiguration);

        Integer fileId = studyConfiguration.getFileIds().get(Paths.get(inputUri).getFileName().toString());
        QueryOptions options = new QueryOptions(VariantStorageManager.Options.FILE_ID.key(), fileId);
        options.put(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 100);
        Iterator<String> iterator = studyConfiguration.getSampleIds().keySet().iterator();

        /** Create cohorts **/
        HashSet<String> cohort1 = new HashSet<>();
        cohort1.add(iterator.next());
        cohort1.add(iterator.next());

        HashSet<String> cohort2 = new HashSet<>();
        cohort2.add(iterator.next());
        cohort2.add(iterator.next());

        Map<String, Set<String>> cohorts = new HashMap<>();
        Map<String, Integer> cohortIds = new HashMap<>();
        cohorts.put("cohort1", cohort1);
        cohorts.put("cohort2", cohort2);
        cohortIds.put("cohort1", 10);
        cohortIds.put("cohort2", 11);

        //Calculate stats
        URI stats = vsm.createStats(dbAdaptor, outputUri.resolve("cohort1.cohort2.stats"), cohorts, cohortIds, studyConfiguration, options);
        vsm.loadStats(dbAdaptor, stats, studyConfiguration, options);


        checkCohorts(dbAdaptor, studyConfiguration);
    }

    @Test
    public void calculateStatsSeparatedCohortsTest() throws Exception {
        //Calculate stats for 2 cohorts separately
        VariantStatisticsManager vsm = new VariantStatisticsManager();

        int studyId = studyConfiguration.getStudyId();
        String studyName = studyConfiguration.getStudyName();
        Integer fileId = studyConfiguration.getFileIds().get(Paths.get(inputUri).getFileName().toString());
        QueryOptions options = new QueryOptions(VariantStorageManager.Options.FILE_ID.key(), fileId);
        options.put(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 100);
        Iterator<String> iterator = studyConfiguration.getSampleIds().keySet().iterator();
        StudyConfiguration studyConfiguration;

        /** Create first cohort **/
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, null).first();
        HashSet<String> cohort1 = new HashSet<>();
        cohort1.add(iterator.next());
        cohort1.add(iterator.next());

        Map<String, Set<String>> cohorts;
        Map<String, Integer> cohortIds;

        cohorts = new HashMap<>();
        cohortIds = new HashMap<>();
        cohorts.put("cohort1", cohort1);
        cohortIds.put("cohort1", 10);

        //Calculate stats for cohort1
        URI stats = vsm.createStats(dbAdaptor, outputUri.resolve("cohort1.stats"), cohorts, cohortIds, studyConfiguration, options);
        vsm.loadStats(dbAdaptor, stats, studyConfiguration, options);

        assertTrue(studyConfiguration.getCalculatedStats().contains(10));
        checkCohorts(dbAdaptor, studyConfiguration);

        /** Create second cohort **/
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, null).first();
        HashSet<String> cohort2 = new HashSet<>();
        cohort2.add(iterator.next());
        cohort2.add(iterator.next());

        cohorts = new HashMap<>();
        cohortIds = new HashMap<>();
        cohorts.put("cohort2", cohort2);
        cohortIds.put("cohort2", 11);

        //Calculate stats for cohort2
        stats = vsm.createStats(dbAdaptor, outputUri.resolve("cohort2.stats"), cohorts, cohortIds, studyConfiguration, options);
        vsm.loadStats(dbAdaptor, stats, studyConfiguration, options);

        assertTrue(studyConfiguration.getCalculatedStats().contains(10));
        assertTrue(studyConfiguration.getCalculatedStats().contains(11));
        checkCohorts(dbAdaptor, studyConfiguration);

        //Try to recalculate stats for cohort2. Will fail
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, null).first();
        thrown.expect(IOException.class);
        stats = vsm.createStats(dbAdaptor, outputUri.resolve("cohort2.stats"), cohorts, cohortIds, studyConfiguration, options);
        vsm.loadStats(dbAdaptor, stats, studyConfiguration, options);

    }

    private static void checkCohorts(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration) {
        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                Map<String, VariantStats> cohortStats = sourceEntry.getStats();
                String calculatedCohorts = cohortStats.keySet().toString();
                for (Map.Entry<String, Integer> entry : studyConfiguration.getCohortIds().entrySet()) {
                    assertTrue("CohortStats should contain stats for cohort " + entry.getKey() + ". Only contains stats for " + calculatedCohorts,
                            cohortStats.containsKey(entry.getKey()));    //Check stats are calculated

                    assertEquals("Stats have less genotypes than expected.",
                            studyConfiguration.getCohorts().get(entry.getValue()).size(),  //Check numGenotypes are correct (equals to the number of samples)
                            cohortStats.get(entry.getKey()).getGenotypesCount().values().stream().reduce(0, (a, b) -> a + b).intValue());
                }
            }
        }
    }

}
