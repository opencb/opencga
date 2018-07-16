package org.opencb.opencga.storage.core.variant.stats;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.net.URI;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerTest.checkCohorts;
import static org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerTest.stats;

/**
 * Created on 14/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStatisticsManagerMultiFilesTest extends VariantStorageBaseTest {

    protected StudyConfiguration studyConfiguration;
    protected VariantDBAdaptor dbAdaptor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws Exception {
        clearDB(DB_NAME);
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantStorageEngine storageEngine = getVariantStorageEngine();
        List<URI> inputFiles = new ArrayList<>();

       // 12877, 12889 and 12890 have an overlapping at M:16184:C:A , M:16184:C:- , M:16184:CC:-
//        for (int i : new int[]{0, 12, 13}) {

        for (int fileId = 12877; fileId < 12877 + 17; fileId++) {
            String fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            inputFiles.add(getResourceUri("platinum/" + fileName));
        }
        storageEngine.getOptions().put(VariantStorageEngine.Options.STUDY.key(), STUDY_NAME);
        storageEngine.getOptions().put(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);
//        storageEngine.index(inputFiles.subList(0, inputFiles.size()/2), outputUri, true, true, true);
//        storageEngine.index(inputFiles.subList(inputFiles.size()/2, inputFiles.size()), outputUri, true, true, true);
        storageEngine.index(inputFiles, outputUri, true, true, true);
        studyConfiguration = storageEngine.getStudyConfigurationManager().getStudyConfiguration(STUDY_ID, null).first();
    }

    @Test
    public void calculateStatsMultiCohortsTest() throws Exception {
        //Calculate stats for 2 cohorts at one time
        VariantStatisticsManager vsm = variantStorageEngine.newVariantStatisticsManager();

        checkCohorts(dbAdaptor, studyConfiguration);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageEngine.Options.LOAD_BATCH_SIZE.key(), 100);

        List<String> samples = new ArrayList<>(studyConfiguration.getSampleIds().keySet());

        /** Create cohorts **/
        HashSet<String> cohort1 = new HashSet<>(samples.subList(0, samples.size() / 2));
        HashSet<String> cohort2 = new HashSet<>(samples.subList(samples.size() / 2, samples.size()));

        Map<String, Set<String>> cohorts = new HashMap<>();
        Map<String, Integer> cohortIds = new HashMap<>();
        cohorts.put("ALL", studyConfiguration.getSampleIds().keySet());
        cohorts.put("cohort1", cohort1);
        cohorts.put("cohort2", cohort2);
        cohortIds.put("ALL", 1);
        cohortIds.put("cohort1", 10);
        cohortIds.put("cohort2", 11);

        //Calculate stats
        stats(vsm, options, studyConfiguration, cohorts, cohortIds, dbAdaptor, outputUri.resolve("cohort1.cohort2.stats"));

        checkCohorts(dbAdaptor, studyConfiguration);
    }

}
