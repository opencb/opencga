package org.opencb.opencga.storage.core.variant;

import org.apache.solr.client.solrj.SolrClient;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.solr.SolrExternalResource;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 04/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageSearchIntersectTest extends VariantStorageBaseTest {

    @ClassRule
    public static SolrExternalResource solr = new SolrExternalResource();

    private VariantDBAdaptor dbAdaptor;
    private StudyConfiguration studyConfiguration;
    private static VariantQueryResult<Variant> allVariants;
    private VariantSource source;
    private static boolean loaded = false;
    private SolrClient solrClient;

    @Before
    public void before() throws Exception {
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        if (!loaded) {
            loadFile();
            loaded = true;
        }
        solrClient = spy(solr.getSolrClient());
        variantStorageEngine.getVariantSearchManager().setSolrClient(solrClient);
    }

    public void loadFile() throws Exception {
        studyConfiguration = newStudyConfiguration();

        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.STUDY_TYPE.key(), VariantStudy.StudyType.FAMILY)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "DS,GL")
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);

        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration, params);
        source = variantStorageEngine.getVariantReaderUtils().readVariantSource(Paths.get(etlResult.getTransformResult().getPath()).toUri());
        Integer indexedFileId = studyConfiguration.getIndexedFiles().iterator().next();

        //Calculate stats
        if (params.getBoolean(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)) {
            QueryOptions options = new QueryOptions(VariantStorageEngine.Options.STUDY_ID.key(), STUDY_ID)
                    .append(VariantStorageEngine.Options.LOAD_BATCH_SIZE.key(), 100)
                    .append(DefaultVariantStatisticsManager.OUTPUT, outputUri)
                    .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, "cohort1.cohort2.stats");
            Iterator<Integer> iterator = studyConfiguration.getSamplesInFiles().get(indexedFileId).iterator();

            /** Create cohorts **/
            HashSet<Integer> cohort1 = new HashSet<>();
            cohort1.add(iterator.next());
            cohort1.add(iterator.next());

            HashSet<Integer> cohort2 = new HashSet<>();
            cohort2.add(iterator.next());
            cohort2.add(iterator.next());

            Map<String, Integer> cohortIds = new HashMap<>();
            cohortIds.put("cohort1", 10);
            cohortIds.put("cohort2", 11);

            studyConfiguration.getCohortIds().putAll(cohortIds);
            studyConfiguration.getCohorts().put(10, cohort1);
            studyConfiguration.getCohorts().put(11, cohort2);

            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, QueryOptions.empty());

            variantStorageEngine.calculateStats(studyConfiguration.getStudyName(),
                    new ArrayList<>(cohortIds.keySet()), options);

        }
        if (params.getBoolean(VariantStorageEngine.Options.ANNOTATE.key())) {
            for (int i = 0; i < 30  ; i++) {
                allVariants = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.SORT, true));
                Long annotated = dbAdaptor.count(new Query(ANNOTATION_EXISTS.key(), true)).first();
                Long all = dbAdaptor.count(new Query()).first();

                System.out.println("count annotated = " + annotated);
                System.out.println("count           = " + all);
                System.out.println("get             = " + allVariants.getNumResults());

                List<Variant> nonAnnotatedVariants = allVariants.getResult()
                        .stream()
                        .filter(variant -> variant.getAnnotation() == null)
                        .collect(Collectors.toList());
                if (!nonAnnotatedVariants.isEmpty()) {
                    System.out.println(nonAnnotatedVariants.size() + " variants not annotated:");
                    System.out.println("Variants not annotated: " + nonAnnotatedVariants);
                }
                if (Objects.equals(annotated, all)) {
                    break;
                }
            }
            assertEquals(dbAdaptor.count(new Query(ANNOTATION_EXISTS.key(), true)).first(), dbAdaptor.count(new Query()).first());
        } else {
            allVariants = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.SORT, true));
        }

        solr.configure(variantStorageEngine);
        variantStorageEngine.searchIndex();
    }

    @Test
    public void testGetSummary() throws Exception {

        VariantQueryResult<Variant> result = variantStorageEngine.get(new Query(),
                new QueryOptions(VariantSearchManager.SUMMARY, true)
                        .append(QueryOptions.LIMIT, 2000));
        assertEquals(allVariants.getResult().size(), result.getResult().size());

    }

    @Test
    public void testGetFromSearch() throws Exception {
        Query query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), 1631)
                .append(ANNOT_CONSERVATION.key(), "gerp>1")
                .append(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.01");
        VariantDBIterator iterator = variantStorageEngine.iterator(query, new QueryOptions());
        QueryResult<Variant> queryResult = iterator.toQueryResult();
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        for (Variant variant : queryResult.getResult()) {
            System.out.println("variant = " + variant);
        }

        verify(solrClient, atLeastOnce()).query(anyString(), any());
    }

    @Test
    public void testGetNotFromSearch() throws Exception {
        Query query = new Query(ANNOT_SIFT.key(), ">0.1");
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());

        verify(solrClient, never()).query(anyString(), any());
        assertThat(result, everyResult(allVariants, hasAnnotation(hasSift(hasItem(gt(0.1))))));
    }

    @Test
    public void testSkipLimit() throws Exception {
        int expectedNumResults = allVariants.getNumResults();
        int numResults = 0;
        int batchSize = 100;

        int numQueries = (int) Math.ceil(expectedNumResults / (float) batchSize);
        for (int i = 0; i < numQueries; i++) {
            QueryOptions options = new QueryOptions()
                    .append(QueryOptions.SKIP, i * batchSize)
                    .append(QueryOptions.LIMIT, batchSize)
                    .append(VariantSearchManager.SUMMARY, true);
            VariantQueryResult<Variant> result = variantStorageEngine.get(new Query(), options);
            assertNotEquals(0, result.getNumResults());
            numResults += result.getNumResults();
        }
        verify(solrClient, atLeast(numQueries)).query(anyString(), any());
        assertEquals(expectedNumResults, numResults);
    }

    @Test
    public void testSkipLimit_extraFields() throws Exception {
        int expectedNumResults = allVariants.getNumResults();
        int numResults = 0;
        int batchSize = 100;

        int numQueries = (int) Math.ceil(expectedNumResults / (float) batchSize);
        for (int i = 0; i < numQueries; i++) {
            QueryOptions options = new QueryOptions()
                    .append(QueryOptions.SKIP, i * batchSize)
                    .append(QueryOptions.LIMIT, batchSize)
                    .append("forceSearch", true);
            VariantQueryResult<Variant> result = variantStorageEngine.get(new Query(), options);
            assertNotEquals(0, result.getNumResults());
            numResults += result.getNumResults();
        }
        verify(solrClient, atLeast(numQueries)).query(anyString(), any());
        assertEquals(expectedNumResults, numResults);
    }

    @Test
    public void testSkipLimit_extraQueries() throws Exception {
        Query query = new Query(SAMPLES.key(), "NA19660").append(ANNOT_CONSERVATION.key(), "gerp>1");
        int expectedNumResults = dbAdaptor.count(query).first().intValue();
        int numResults = 0;
        int batchSize = 50;
        int numQueries = (int) Math.ceil(expectedNumResults / (float) batchSize);
        for (int i = 0; i < numQueries; i++) {
            QueryOptions options = new QueryOptions()
                    .append(QueryOptions.SKIP, i * batchSize)
                    .append(QueryOptions.LIMIT, batchSize)
                    .append("forceSearch", true);
            VariantQueryResult<Variant> result = variantStorageEngine.get(query, options);
            numResults += result.getNumResults();
            assertNotEquals(0, result.getNumResults());
        }
        verify(solrClient, atLeast(numQueries)).query(anyString(), any());
        assertEquals(expectedNumResults, numResults);
    }

    @Test
    @Ignore
    public void testQueryWithIds() throws Exception {
        // TODO
    }

}
