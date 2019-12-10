/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.SearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager.USE_SEARCH_INDEX;

/**
 * Created on 04/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageSearchIntersectTest extends VariantStorageBaseTest {

    @ClassRule
    public static VariantSolrExternalResource solr = new VariantSolrExternalResource();

    protected VariantDBAdaptor dbAdaptor;
    private StudyMetadata studyMetadata;
    private static VariantQueryResult<Variant> allVariants;
    private VariantFileMetadata fileMetadata;
    protected static boolean loaded = false;
    private SolrClient solrClient;
    private SearchIndexVariantQueryExecutor variantQueryExecutor;

    @Before
    public void before() throws Exception {
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        if (!loaded) {
            loadFile();
            loaded = true;
        }
        solrClient = spy(solr.getSolrClient());
//        doAnswer(invocation -> {
//            new Exception().printStackTrace();
//            return invocation.callRealMethod();
//        }).when(solrClient).query(anyString(), any());
        solr.configure(variantStorageEngine);
        variantStorageEngine.getVariantSearchManager().setSolrClient(solrClient);
        for (VariantQueryExecutor variantQueryExecutor : getVariantStorageEngine().getVariantQueryExecutors()) {
            if (variantQueryExecutor instanceof SearchIndexVariantQueryExecutor) {
                this.variantQueryExecutor = ((SearchIndexVariantQueryExecutor) variantQueryExecutor);
            }
        }
    }

    public void loadFile() throws Exception {
        studyMetadata = newStudyMetadata();

        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "DS,GL")
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, params);
        fileMetadata = this.variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(Paths.get(etlResult.getTransformResult().getPath()).toUri());

        Integer indexedFileId = metadataManager.getIndexedFiles(studyMetadata.getId()).iterator().next();

        //Calculate stats
        if (params.getBoolean(VariantStorageOptions.STATS_CALCULATE.key(), true)) {
            QueryOptions options = new QueryOptions(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                    .append(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100)
                    .append(DefaultVariantStatisticsManager.OUTPUT, outputUri)
                    .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, "cohort1.cohort2.stats");
            Iterator<Integer> iterator = metadataManager.getFileMetadata(studyMetadata.getId(), indexedFileId).getSamples().iterator();

            /** Create cohorts **/
            HashSet<String> cohort1 = new HashSet<>();
            cohort1.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));
            cohort1.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));

            HashSet<String> cohort2 = new HashSet<>();
            cohort2.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));
            cohort2.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));

            Map<String, Set<String>> cohorts = new HashMap<>();
            cohorts.put("cohort1", cohort1);
            cohorts.put("cohort2", cohort2);
            metadataManager.registerCohorts(studyMetadata.getName(), cohorts);


            this.variantStorageEngine.calculateStats(studyMetadata.getName(),
                    new ArrayList<>(cohorts.keySet()), options);

        }
        if (params.getBoolean(VariantStorageOptions.ANNOTATE.key())) {
            for (int i = 0; i < 30  ; i++) {
                allVariants = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.SORT, true));
                Long annotated = dbAdaptor.count(new Query(ANNOTATION_EXISTS.key(), true)).first();
                Long all = dbAdaptor.count(new Query()).first();

                System.out.println("count annotated = " + annotated);
                System.out.println("count           = " + all);
                System.out.println("get             = " + allVariants.getNumResults());

                List<Variant> nonAnnotatedVariants = allVariants.getResults()
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

        solr.configure(this.variantStorageEngine);
        this.variantStorageEngine.secondaryIndex();
    }

    @Test
    public void testGetSummary() throws Exception {

        VariantQueryResult<Variant> result = variantStorageEngine.get(new Query(),
                new QueryOptions(VariantField.SUMMARY, true)
                        .append(QueryOptions.LIMIT, 2000));
        assertEquals(allVariants.getResults().size(), result.getResults().size());

    }

    @Test
    public void testGetFromSearch() throws Exception {
        Query query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), 1631)
                .append(ANNOT_CONSERVATION.key(), "gerp>1")
                .append(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.01")
                .append(UNKNOWN_GENOTYPE.key(), "./.");
        DataResult<Variant> queryResult = variantStorageEngine.get(query, new QueryOptions());
//        for (Variant variant : queryResult.getResults()) {
//            System.out.println("variant = " + variant);
//        }
        assertEquals(queryResult.getNumResults(), queryResult.getNumTotalResults());
        assertNotEquals(0, queryResult.getNumResults());

        verify(solrClient, times(1)).query(anyString(), any());
    }

    @Test
    public void testGetNotFromSearch() throws Exception {
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.1");
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());

        verify(solrClient, never()).query(anyString(), any());
        assertThat(result, everyResult(allVariants, hasAnnotation(hasSift(hasItem(gt(0.1))))));
    }

    @Test
    public void testSkipLimit() throws Exception {
        skipLimit(new Query(), new QueryOptions(VariantField.SUMMARY, true), 100, false);
    }

    @Test
    public void testSkipLimit_extraFields() throws Exception {
        skipLimit(new Query(), new QueryOptions(USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.YES), 100, false);
    }

    @Test
    public void testSkipLimit_extraQueries() throws Exception {
        Query query = new Query(SAMPLE.key(), "NA19660")
                .append(ANNOT_CONSERVATION.key(), "gerp>1");
        QueryOptions options = new QueryOptions(USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.YES);
        skipLimit(query, options, 250, true);
    }

    private void skipLimit(Query query, QueryOptions options, int batchSize, boolean serverSideSkip) throws StorageEngineException, SolrServerException, IOException {
        Set<String> expectedResults = dbAdaptor.get(query, null).getResults().stream().map(Variant::toString).collect(Collectors.toSet());
        Set<String> results = new HashSet<>();
        int numQueries = (int) Math.ceil(expectedResults.size() / (float) batchSize);
        for (int i = 0; i < numQueries; i++) {
            QueryOptions thisOptions = new QueryOptions(options)
                    .append(QueryOptions.SKIP, i * batchSize)
                    .append(QueryOptions.LIMIT, batchSize);
            VariantQueryResult<Variant> result = variantStorageEngine.get(query, thisOptions);
            for (Variant variant : result.getResults()) {
                assertTrue(results.add(variant.toString()));
            }
            assertNotEquals(0, result.getNumResults());
        }
        assertEquals(expectedResults, results);
        if (serverSideSkip) {
            long count = mockingDetails(solrClient).getInvocations()
                    .stream()
                    .filter(invocation -> invocation.getMethod().getName().equals("query"))
                    .count();
            System.out.println(SolrClient.class.getName() + ".query(...) invocations : " + count);
            verify(solrClient, atLeast(numQueries)).query(anyString(), any());
        } else {
            verify(solrClient, times(numQueries)).query(anyString(), any());
        }
    }

    @Test
    public void testQueryWithIds() throws Exception {
        List<String> variantIds = allVariants.getResults().stream()
                .filter(v -> EnumSet.of(VariantType.SNV, VariantType.SNP).contains(v.getType()))
                .map(Variant::toString)
                .limit(400)
                .collect(Collectors.toList());
        Query query = new Query(SAMPLE.key(), "NA19660")
                .append(ANNOT_CONSERVATION.key(), "gerp>0.2")
                .append(ID.key(), variantIds);
        QueryOptions options = new QueryOptions(USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.YES);
        skipLimit(query, options, 50, true);
    }

    @Test
    public void testApproxCount() throws Exception {
        Query query = new Query(SAMPLE.key(), "NA19660")
                .append(ANNOT_CONSERVATION.key(), "gerp>0.1");
        long realCount = dbAdaptor.count(query).first();
        VariantQueryResult<Long> result = variantQueryExecutor
                .approximateCount(query, new QueryOptions(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), realCount * 0.1));
        long approxCount = result.first();
        System.out.println("approxCount = " + approxCount);
        System.out.println("realCount = " + realCount);
        assertTrue(result.getApproximateCount());
        assertThat(approxCount, lte(realCount * 1.25));
        assertThat(approxCount, gte(realCount * 0.75));
    }

    @Test
    public void testExactApproxCount() throws Exception {
        Query query = new Query(SAMPLE.key(), "NA19660")
                .append(ANNOT_CONSERVATION.key(), "gerp>0.1");
        long realCount = dbAdaptor.count(query).first();
        VariantQueryResult<Long> result = variantQueryExecutor
                .approximateCount(query, new QueryOptions(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), allVariants.getNumResults()));
        long approxCount = result.first();
        System.out.println("approxCount = " + approxCount);
        System.out.println("realCount = " + realCount);
        assertFalse(result.getApproximateCount());
        assertEquals(approxCount, realCount);
    }

    @Test
    public void testExactApproxCountToSearch() throws Exception {
        Query query = new Query(ANNOT_CONSERVATION.key(), "gerp>0.1");
        long realCount = dbAdaptor.count(query).first();
        VariantQueryResult<Long> result = variantQueryExecutor
                .approximateCount(query, new QueryOptions(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), 2));
        long approxCount = result.first();
        System.out.println("approxCount = " + approxCount);
        System.out.println("realCount = " + realCount);
        assertFalse(result.getApproximateCount());
        assertEquals(approxCount, realCount);
    }

    @Test
    public void testUseSearchIndex() throws StorageEngineException {
        assertFalse(variantQueryExecutor.doIntersectWithSearch(new Query(), new QueryOptions()));
        assertTrue(variantQueryExecutor.doIntersectWithSearch(new Query(), new QueryOptions(USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.YES)));
        assertTrue(variantQueryExecutor.doIntersectWithSearch(new Query(ANNOT_TRAIT.key(), "myTrait"), new QueryOptions()));
    }

    @Test
    public void testFailTraitWithoutSearch() throws StorageEngineException {
        VariantQueryException exception = VariantQueryException.unsupportedVariantQueryFilter(VariantQueryParam.ANNOT_TRAIT, variantStorageEngine.getStorageEngineId());
        thrown.expect(exception.getClass());
        thrown.expectMessage(exception.getMessage());
        variantQueryExecutor.doIntersectWithSearch(new Query(ANNOT_TRAIT.key(), "myTrait"), new QueryOptions(USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.NO));
    }

    @Test
    public void testFailSearchNotAvailable() throws StorageEngineException {
        VariantQueryException exception = new VariantQueryException("Unable to use search index. SearchEngine is not available");
        thrown.expect(exception.getClass());
        thrown.expectMessage(exception.getMessage());
        variantStorageEngine.getConfiguration().getSearch().setActive(false);
        variantQueryExecutor.doIntersectWithSearch(new Query(ANNOT_TRAIT.key(), "myTrait"), new QueryOptions(USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.YES));
    }

    @Test
    public void testDoQuerySearchManager() throws Exception {
        VariantStorageEngine engine = getVariantStorageEngine();
        assertFalse(variantQueryExecutor.doQuerySearchManager(new Query(), new QueryOptions()));
        assertTrue(variantQueryExecutor.doQuerySearchManager(new Query(), new QueryOptions(VariantField.SUMMARY, true)));
        assertTrue(variantQueryExecutor.doQuerySearchManager(new Query(STUDY.key(), 3), new QueryOptions(VariantField.SUMMARY, true)));
        assertTrue(variantQueryExecutor.doQuerySearchManager(new Query(STUDY.key(), 3), new QueryOptions(INCLUDE, VariantField.ANNOTATION)));
    }
}
