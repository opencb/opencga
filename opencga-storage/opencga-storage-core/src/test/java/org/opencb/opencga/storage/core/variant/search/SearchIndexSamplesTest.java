package org.opencb.opencga.storage.core.variant.search;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSolrIterator;
import org.opencb.opencga.storage.core.variant.solr.SolrExternalResource;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created on 20/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class SearchIndexSamplesTest extends VariantStorageBaseTest {

    @ClassRule
    public static SolrExternalResource solr = new SolrExternalResource();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static boolean loaded = false;
    private static List<String> samples1;
    private static List<String> samples2;

    @Before
    public void before() throws Exception {
        solr.configure(variantStorageEngine);
        if (!loaded) {
            load();
            loaded = true;
        }
    }

    protected void load() throws Exception {
        clearDB(DB_NAME);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();

        StudyConfiguration studyConfiguration = newStudyConfiguration();
        runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration);

        Iterator<String> it = studyConfiguration.getSampleIds().keySet().iterator();

        samples1 = Arrays.asList(it.next(), it.next());
        variantStorageEngine.searchIndexSamples(studyConfiguration.getStudyName(), samples1);
        samples2 = Arrays.asList(it.next(), it.next());
        variantStorageEngine.searchIndexSamples(studyConfiguration.getStudyName(), samples2);
    }

    @Test
    public void testFailReindex() throws Exception {
        thrown.expectMessage("already in search index");
        variantStorageEngine.searchIndexSamples(STUDY_NAME, samples1);
    }

    @Test
    public void testFailReindexMix() throws Exception {
        thrown.expectMessage("already in search index");
        variantStorageEngine.searchIndexSamples(STUDY_NAME, Arrays.asList(samples1.get(0), samples2.get(0)));
    }

    @Test
    public void testQuery() throws Exception {
        checkLoadedData("opencga_variants_test_1_1", samples1);
        checkLoadedData("opencga_variants_test_1_2", samples2);
    }

    protected void checkLoadedData(String collection, List<String> samples)
            throws StorageEngineException, IOException, VariantSearchException {
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();

        Query query = new Query(VariantQueryParam.SAMPLE.key(), samples);
        int expectedCount = variantStorageEngine.getDBAdaptor().count(query).first().intValue();
        assertEquals(expectedCount, variantSearchManager.query(collection, new Query(), new QueryOptions()).getNumTotalResults());


        VariantSolrIterator solrIterator = variantSearchManager.iterator(collection, new Query(), new QueryOptions(QueryOptions.SORT, true));

        int i = 0;
        while (solrIterator.hasNext()) {
            Variant actual = solrIterator.next();
            Variant expected = variantStorageEngine.getDBAdaptor().get(query.append(VariantQueryParam.ID.key(), actual.toString()), null).first();
//            System.out.println(collection + "[" + i + "] " + actual);
            i++;

            assertNotNull(expected);
            assertEquals(expected.toString(), actual.toString());
            StudyEntry expectedStudy = expected.getStudies().get(0);
            StudyEntry actualStudy = actual.getStudies().get(0);

            Map<String, VariantStats> expectedStudyStats = expectedStudy.getStats();
            expectedStudy.setStats(Collections.emptyMap());
            Map<String, VariantStats> actualStudyStats = actualStudy.getStats();
            actualStudy.setStats(Collections.emptyMap());

            assertEquals(expected.toString(), expectedStudy, actualStudy);
            // FIXME
//            assertEquals(expected.toString(), expectedStudyStats, actualStudyStats);
        }

        assertFalse(solrIterator.hasNext());

    }
}
