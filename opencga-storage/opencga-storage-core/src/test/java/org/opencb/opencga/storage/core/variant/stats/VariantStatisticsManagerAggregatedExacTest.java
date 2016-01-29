package org.opencb.opencga.storage.core.variant.stats;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by hpccoll1 on 01/06/15.
 */
@Ignore
public abstract class VariantStatisticsManagerAggregatedExacTest extends VariantStatisticsManagerAggregatedTest {

    public static final String VCF_TEST_FILE_NAME = "exachead.vcf.gz";
    public static final String MAPPING_FILE = "exac-tag-mapping.properties";
    public static Properties tagMap = new Properties();

    private StudyConfiguration studyConfiguration;
    private VariantDBAdaptor dbAdaptor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws IOException {
        Path rootDir = getTmpRootDir();
        Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAME);
        Files.copy(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME), inputPath,
                StandardCopyOption.REPLACE_EXISTING);
        inputUri = inputPath.toUri();
        tagMap.load(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(MAPPING_FILE));
    }

    @Override
    @Before
    public void before() throws Exception {
        studyConfiguration = newStudyConfiguration();
        studyConfiguration.setAggregation(VariantSource.Aggregation.EXAC);
        clearDB(DB_NAME);
        runDefaultETL(inputUri, getVariantStorageManager(), studyConfiguration,
                new ObjectMap(VariantStorageManager.Options.ANNOTATE.key(), false)
                        .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false));
        dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
    }


    @Test
    public void calculateAggregatedStatsTest() throws Exception {
        //Calculate stats for 2 cohorts at one time
        VariantStatisticsManager vsm = new VariantStatisticsManager();

        checkAggregatedCohorts(dbAdaptor, studyConfiguration);

        Integer fileId = studyConfiguration.getFileIds().get(Paths.get(inputUri).getFileName().toString());
        QueryOptions options = new QueryOptions(VariantStorageManager.Options.FILE_ID.key(), fileId)
                .append(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), tagMap);
        options.put(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 100);


        //Calculate stats
        URI stats = vsm.createStats(dbAdaptor, outputUri.resolve("aggregated.stats"), null, Collections.emptyMap(), studyConfiguration,
                options);
        vsm.loadStats(dbAdaptor, stats, studyConfiguration, options);


        checkAggregatedCohorts(dbAdaptor, studyConfiguration);
    }

    private static void checkAggregatedCohorts(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration) {
        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                Map<String, VariantStats> cohortStats = sourceEntry.getStats();
                String calculatedCohorts = cohortStats.keySet().toString();
                for (Map.Entry<String, Integer> entry : studyConfiguration.getCohortIds().entrySet()) {
                    assertTrue("CohortStats should contain stats for cohort " + entry.getKey()
                                    + ". Only contains stats for " + calculatedCohorts,
                            cohortStats.containsKey(entry.getKey()));    //Check stats are calculated

                    assertNotEquals("Stats seem with no valid values, for instance (chr=" + variant.getChromosome()
                                    + ", start=" + variant.getStart() + ", ref=" + variant.getReference() + ", alt="
                                    + variant.getAlternate() + "), gtc=" + cohortStats.get(entry.getKey()).getGenotypesCount().toString(),
                            0,
                            cohortStats.get(entry.getKey()).getGenotypesCount().size());
                }
            }
        }
    }
}
