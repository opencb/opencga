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

package org.opencb.opencga.storage.core.variant.adaptors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsAnything;
import org.junit.*;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

/**
 * Tests that all the VariantDBAdaptor filters and methods work correctly.
 *
 * Do not check that all the values are loaded correctly
 * Do not check that variant annotation is correct
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantDBAdaptorTest extends VariantStorageBaseTest {

    private static final int QUERIES_LIM = 50;
    //    private static final String GENOMES_PHASE_3 = "1000GENOMES_phase_3";
    //    private static final String ESP_6500 = "ESP_6500";
    private static final String GENOMES_PHASE_3 = "1kG_phase3";
    private static final String ESP_6500 = "ESP6500";
    protected static int NUM_VARIANTS = 998;
    protected static Set<String> FORMAT;
    protected static boolean fileIndexed;
    protected static VariantFileMetadata fileMetadata;
    protected static StudyMetadata studyMetadata;
    protected VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected DataResult<Variant> queryResult;
    protected DataResult<Variant> allVariants;
    private static Logger logger = LoggerFactory.getLogger(VariantDBAdaptorTest.class);
    private String homAlt;
    private String homRef;
    private String het;
    private String het1;
    private String het2;
    protected int fileId = 1;
    protected static int na19600;
    protected static int na19660;
    protected static int na19661;
    protected static int na19685;

    protected List<String> sampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");
    protected Set<String> cohorts = new HashSet<>(Arrays.asList("ALL", "cohort1", "cohort2"));



    @BeforeClass
    public static void beforeClass() throws IOException {
        fileIndexed = false;
    }

    @Override
    @Before
    public void before() throws Exception {

        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        if (!fileIndexed) {
            studyMetadata = newStudyMetadata();
//            variantSource = new VariantSource(smallInputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
            clearDB(DB_NAME);
            ObjectMap params = new ObjectMap()
                    .append(VariantStorageOptions.ANNOTATE.key(), true)
                    .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "DS,GL")
                    .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), CellBaseRestVariantAnnotator.class.getName())
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), true);
            params.putAll(getOtherParams());
            FORMAT = new HashSet<>();
            if (!params.getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
                    VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue())) {
                FORMAT.add("GT");
            }
            FORMAT.addAll(params.getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key()));

            StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyMetadata, params);
            fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(Paths.get(etlResult.getTransformResult().getPath()).toUri());
            NUM_VARIANTS = getExpectedNumLoadedVariants(fileMetadata);
            fileIndexed = true;
            Integer indexedFileId = metadataManager.getIndexedFiles(studyMetadata.getId()).iterator().next();

            na19600 = metadataManager.getSampleId(studyMetadata.getId(), "NA19600");
            na19660 = metadataManager.getSampleId(studyMetadata.getId(), "NA19660");
            na19661 = metadataManager.getSampleId(studyMetadata.getId(), "NA19661");
            na19685 = metadataManager.getSampleId(studyMetadata.getId(), "NA19685");

            //Calculate stats
            if (getOtherParams().getBoolean(VariantStorageOptions.STATS_CALCULATE.key(), true)) {
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

                variantStorageEngine.calculateStats(studyMetadata.getName(),
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
                assertEquals(dbAdaptor.count(new Query(ANNOTATION_EXISTS.key(), true)).getNumMatches(), dbAdaptor.count().getNumMatches());
            }
        }
        allVariants = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.SORT, true));
        options = new QueryOptions();

        homAlt = getHomAltGT();
        homRef = getHomRefGT();
        het = getHetGT();
        String[] hetGts = het.split(",");
        het1 = hetGts[0];
        het2 = hetGts[hetGts.length - 1];
    }

    @After
    public void after() throws IOException {
        dbAdaptor.close();
    }

    public VariantQueryResult<Variant> query(Query query, QueryOptions options) {
        query = preProcessQuery(query, options);
        return dbAdaptor.get(query, options);
    }

    public VariantDBIterator iterator(Query query, QueryOptions options) {
        query = preProcessQuery(query, options);
        return dbAdaptor.iterator(query, options);
    }

    protected Query preProcessQuery(Query query, QueryOptions options) {
        return variantStorageEngine.preProcessQuery(query, options);
    }

    public Long count(Query query) {
        query = preProcessQuery(query, null);
        return dbAdaptor.count(query).first();
    }

    public DataResult groupBy(Query query, String field, QueryOptions options) {
        return dbAdaptor.groupBy(query, field, options);
    }

    public DataResult rank(int limit, Query query, String field, boolean asc) {
        return dbAdaptor.rank(query, field, limit, asc);
    }

    protected String getHetGT() {
        return "0|1,1|0";
    }

    protected String getHomRefGT() {
        return "0/0";
    }

    protected String getHomAltGT() {
        return "1|1";
    }

    protected ObjectMap getOtherParams() {
        return new ObjectMap();
    }

    @Test
    public void multiIterator() throws Exception {
        List<String> variantsToQuery = allVariants.getResults()
                .stream()
                .filter(v -> !v.isSymbolic())
                .map(Variant::toString)
                .limit(allVariants.getResults().size() / 2)
                .collect(Collectors.toList());

        VariantDBIterator iterator = dbAdaptor.iterator(variantsToQuery.iterator(), new Query(), new QueryOptions());

        DataResult<Variant> queryResult = iterator.toDataResult();
        assertEquals(variantsToQuery.size(), queryResult.getResults().size());
    }

    @Test
    public void testGetAllVariants() {
        long numResults = count(null);
        assertEquals(NUM_VARIANTS, numResults);
    }

    @Test
    public void testGetAllVariants_limit_skip() {
        limitSkip(new Query(), new QueryOptions());
    }

    @Test
    public void testGetAllVariants_limit_skip_sorted() {
        limitSkip(new Query(), new QueryOptions(QueryOptions.SORT, true));
    }

    @Test
    public void testGetAllVariants_limit_skip_filters() {
        limitSkip(new Query(ANNOT_POLYPHEN.key(), "<0.5"), new QueryOptions());
    }

    @Test
    public void testGetAllVariants_limit_skip_sorted_filters() {
        limitSkip(new Query(ANNOT_POLYPHEN.key(), "<0.5"), new QueryOptions(QueryOptions.SORT, true));
    }

    public void limitSkip(Query query, QueryOptions options) {
        VariantQueryResult<Variant> expected = query(query, options);
        int numVariants = expected.getNumResults();
//        expected.getResults().forEach(v -> logger.info("expected variant: == " + v));
        for (int batchSize : new int[]{50, 100, 1000}) {
            List<Variant> variants = new ArrayList<>();
            Set<String> variantStr = new HashSet<>();
            for (int i = 0; i < numVariants / batchSize + 1; i++) {
                DataResult<Variant> result = query(query, new QueryOptions(options)
                        .append(QueryOptions.LIMIT, batchSize)
                        .append(QueryOptions.SKIP, i * batchSize));
                logger.info("Got " + result.getNumResults() + " results");
                variants.addAll(result.getResults());
                for (Variant variant : result.getResults()) {
                    boolean repeated = !variantStr.add(variant.toString());
                    assertFalse("Repeated variant! : " + variant.toString(), repeated);
                }
            }
            assertEquals(numVariants, variants.size());
            assertEquals(numVariants, variantStr.size());
            assertEquals(expected.getResults().stream().map(Object::toString).collect(Collectors.toSet()), variantStr);
        }
        assertEquals(0, query(query, new QueryOptions(options).append(QueryOptions.LIMIT, 0)).getNumResults());
    }

    @Test
    public void testGetVariantsByType() {
        Set<Variant> snv = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.SNV), new QueryOptions()).getResults());
        System.out.println("SNV = " + snv.size());
        snv.forEach(variant -> assertThat(EnumSet.of(VariantType.SNV, VariantType.SNP), hasItem(variant.getType())));

        Set<Variant> not_snv = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), "!" + VariantType.SNV), new QueryOptions()).getResults());
        System.out.println("!SNV = " + not_snv.size());
        not_snv.forEach(variant -> assertFalse(EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType())));

        Set<Variant> snv_snp = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.SNV + "," + VariantContext.Type.SNP), new QueryOptions()).getResults());
        System.out.println("SNV_SNP = " + snv_snp.size());
        assertEquals(snv_snp, snv);

        Set<Variant> snp = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.SNP), new QueryOptions()).getResults());
        snp.forEach(variant -> assertEquals(VariantType.SNP, variant.getType()));
        snp.forEach(variant -> assertThat(snv, hasItem(variant)));
        System.out.println("SNP = " + snp.size());

        Set<Variant> indels = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL), new QueryOptions()).getResults());
        indels.forEach(variant -> assertEquals(VariantType.INDEL, variant.getType()));
        System.out.println("INDEL = " + indels.size());

        Set<Variant> indels_snp = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL + "," + VariantType.SNP), new QueryOptions()).getResults());
        indels_snp.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP), hasItem(variant.getType())));
        indels_snp.forEach(variant -> assertTrue(indels.contains(variant) || snp.contains(variant)));
        System.out.println("INDEL_SNP = " + indels_snp.size());

        Set<Variant> indels_snv = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL + "," + VariantType.SNV), new QueryOptions()).getResults());
        indels_snv.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP, VariantType.SNV), hasItem(variant.getType())));
        indels_snv.forEach(variant -> assertTrue(indels.contains(variant) || snv.contains(variant)));
        System.out.println("INDEL_SNV = " + indels_snv.size());
    }

    @Test
    public void testGetAllVariants_populationFrequencyRef() {
        Query query;
        query = new Query()
                .append(ANNOT_POPULATION_REFERENCE_FREQUENCY.key(), GENOMES_PHASE_3 + ":AFR<=0.05001");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopRefFreq(GENOMES_PHASE_3, "AFR", lte(0.05001)))));
    }

    @Test
    public void testGetAllVariants_populationFrequency() {
        Query query;

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), ESP_6500 + ":AA>0.05001");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopAltFreq(ESP_6500, "AA", gt(0.05001)))));

//        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && map.get(ESP_6500 + ":AA").getAltAlleleFreq() > 0.05001), filter);

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), GENOMES_PHASE_3 + ":AFR<=0.05001");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopAltFreq(GENOMES_PHASE_3, "AFR", lte(0.05001)))));

//        filterPopulation(map -> (!map.containsKey(GENOMES_PHASE_3 + ":AFR") || map.get(GENOMES_PHASE_3 + ":AFR").getAltAlleleFreq() <= 0.05001), filter);

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), ESP_6500 + ":AA>0.05001;" + GENOMES_PHASE_3 + ":AFR<=0.05001");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(allOf(
                        hasPopAltFreq(ESP_6500, "AA", gt(0.05001)),
                        hasPopAltFreq(GENOMES_PHASE_3, "AFR", lte(0.05001))))));

//        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && map.get(ESP_6500 + ":AA").getAltAlleleFreq() > 0.05001
//                        && (!map.containsKey(GENOMES_PHASE_3 + ":AFR") || map.get(GENOMES_PHASE_3 + ":AFR").getAltAlleleFreq() <= 0.05001)), filter);

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), ESP_6500 + ":AA>0.05001," + GENOMES_PHASE_3 + ":AFR<=0.05001");
        queryResult = query(query, options);

        assertThat(queryResult, everyResult(allVariants, hasAnnotation(anyOf(
                hasPopAltFreq(ESP_6500, "AA", gt(0.05001)),
                hasPopAltFreq(GENOMES_PHASE_3, "AFR", lte(0.05001))))));

//        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && map.get(ESP_6500 + ":AA").getAltAlleleFreq() > 0.05001
//                        || (!map.containsKey(GENOMES_PHASE_3 + ":AFR") || map.get(GENOMES_PHASE_3 + ":AFR").getAltAlleleFreq() <= 0.05001)), filter);

    }

    @Test
    public void testGetAllVariants_population_maf() {
        Query baseQuery = new Query();

        Query query = new Query(baseQuery).append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                GENOMES_PHASE_3 + ":AFR<=0.0501");
        queryResult = query(query, options);
//        filterPopulation(map -> (Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getRefAlleleFreq(),
//                map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopMaf(GENOMES_PHASE_3, "AFR", lte(0.05001)))));

        query = new Query(baseQuery).append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                ESP_6500 + ":AA>0.0501");
        queryResult = query(query, options);
//        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && Math.min(map.get(ESP_6500 + ":AA").getRefAlleleFreq(),
//                map.get(ESP_6500 + ":AA").getAltAlleleFreq()) > 0.0501));
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopMaf(ESP_6500, "AA", gt(0.05001)))));

        query = new Query(baseQuery).append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                GENOMES_PHASE_3 + ":ALL<=0.0501");
        queryResult = query(query, options);
//        filterPopulation(map -> (Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":ALL", defaultPopulation).getRefAlleleFreq(),
//                map.getOrDefault(GENOMES_PHASE_3 + ":ALL", defaultPopulation).getAltAlleleFreq()) <= 0.0501));
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopMaf(GENOMES_PHASE_3, "ALL", lt(0.05001)))));

        query = new Query(baseQuery).append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                ESP_6500 + ":AA>0.0501" + AND + GENOMES_PHASE_3 + ":AFR<=0.0501");
        queryResult = query(query, options);
//        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && Math.min(map.get(ESP_6500 + ":AA").getRefAlleleFreq(),
//                map.get(ESP_6500 + ":AA").getAltAlleleFreq()) > 0.0501
//                && Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getRefAlleleFreq(),
//                map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(allOf(
                hasPopMaf(ESP_6500, "AA", gt(0.0501)),
                hasPopMaf(GENOMES_PHASE_3, "AFR", lte(0.0501))))));

        query = new Query(baseQuery).append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                ESP_6500 + ":AA>0.0501" + OR + GENOMES_PHASE_3 + ":AFR<=0.0501");
        queryResult = query(query, options);
//        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && Math.min(map.get(ESP_6500 + ":AA").getRefAlleleFreq(),
//                map.get(ESP_6500 + ":AA").getAltAlleleFreq()) > 0.0501
//                || Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getRefAlleleFreq(),
//                map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(anyOf(
                hasPopMaf(ESP_6500, "AA", gt(0.0501)),
                hasPopMaf(GENOMES_PHASE_3, "AFR", lte(0.0501))))));

    }

    public long filterPopulation(Predicate<Map<String, PopulationFrequency>> predicate) {
        return filterPopulation(queryResult, v -> true, predicate);
    }

    public long filterPopulation(DataResult<Variant> queryResult, Predicate<Variant> filterVariants, Predicate<Map<String, PopulationFrequency>> predicate) {
        queryResult.getResults().forEach(variant -> {
            assertNotNull(variant);
            assertNotNull("In " + variant, variant.getAnnotation());
//            assertNotNull("In " + variant, variant.getAnnotation().getPopulationFrequencies());
        });
        Set<String> expectedVariants = allVariants.getResults()
                .stream()
                .filter(filterVariants.and(variant -> variant.getAnnotation() != null))
                .filter(variant -> {
                    Map<String, PopulationFrequency> map;
                    if (variant.getAnnotation().getPopulationFrequencies() == null) {
                        map = Collections.emptyMap();
                    } else {
                        map = new HashMap<>();
                        for (PopulationFrequency p : variant.getAnnotation().getPopulationFrequencies()) {
                            map.put(p.getStudy() + ":" + p.getPopulation(), p);
                        }
                    }
                    return predicate.test(map);
                })
                .map(Variant::toString)
                .collect(Collectors.toSet());

        assertTrue("Expect to get at least one result", expectedVariants.size() > 0);

        for (String variant : expectedVariants) {
            Set<String> result = queryResult.getResults().stream().map(Variant::toString).collect(Collectors.toSet());
            if (!result.contains(variant)) {
                System.out.println("variant missing = " + variant);
            }
        }
        for (Variant variant : queryResult.getResults()) {
            if (!expectedVariants.contains(variant.toString())) {
                System.out.println("variant not suppose to be = " + variant);
            }
        }

        assertEquals(expectedVariants.size(), queryResult.getNumResults());
        long count = queryResult.getResults().stream()
                .map(variant -> {
                    Map<String, PopulationFrequency> map;
                    if (variant.getAnnotation().getPopulationFrequencies() == null) {
                        map = Collections.emptyMap();
                    } else {
                        map = new HashMap<>();
                        for (PopulationFrequency p : variant.getAnnotation().getPopulationFrequencies()) {
                            map.put(p.getStudy() + ":" + p.getPopulation(), p);
                        }
                    }
                    return map;
                })
                .filter(predicate.negate())
                .count();
        assertEquals(0, count);
        return count;
    }

    @Test
    public void testGetAllVariants_id() {
        testGetAllVariants_rs(ID.key());
    }

    @Test
    public void testGetAllVariants_variantId() {
        int i = 0;
        List<Variant> variants = new ArrayList<>();
        for (Variant variant : allVariants.getResults()) {
            if (i++ % 10 == 0) {
                if (!variant.isSymbolic()) {
                    variants.add(variant);
                }
            }
        }
        List<Variant> result = query(new Query(ID.key(), variants), new QueryOptions()).getResults();

        assertTrue(variants.size() > 0);
        List<String> expectedList = variants.stream().map(Object::toString).sorted().collect(Collectors.toList());
        List<String> actualList = result.stream().map(Object::toString).sorted().collect(Collectors.toList());
        for (String expected : expectedList) {
            if (!actualList.contains(expected)) {
                System.out.println("missing expected = " + expected);
            }
        }
        for (String actual : actualList) {
            if (!expectedList.contains(actual)) {
                System.out.println("extra actual = " + actual);
            }
        }
        assertEquals(expectedList, actualList);
    }

    @Test
    public void testGetAllVariants_xref() {
        Query query = new Query(ANNOT_XREF.key(), "3:108634973:C:A,rs2032582,HP:0001250,VAR_048225,Q9BY64,ENSG00000250026,TMPRSS11B,COSM1421316");
        queryResult = query(query, null);
        assertThat(queryResult, everyResult(allVariants, anyOf(
                hasAnnotation(at("3:108634973:C:A")),
                with("id", Variant::getId, is("rs2032582")),
                hasAnnotation(with("GeneTraitAssociation", VariantAnnotation::getGeneTraitAssociation,
                        hasItem(with("HPO", GeneTraitAssociation::getHpo, is("HP:0001250"))))),
                hasAnnotation(with("ConsequenceType", VariantAnnotation::getConsequenceTypes,
                        hasItem(with("ProteinVariantAnnotation", ConsequenceType::getProteinVariantAnnotation,
                                with("UniprotVariantId", ProteinVariantAnnotation::getUniprotVariantId, is("VAR_048225")))))),
                hasAnnotation(with("ConsequenceType", VariantAnnotation::getConsequenceTypes,
                        hasItem(with("ProteinVariantAnnotation", ConsequenceType::getProteinVariantAnnotation,
                                with("UniprotName", ProteinVariantAnnotation::getUniprotAccession, is("Q9BY64")))))),
                hasAnnotation(with("ConsequenceType", VariantAnnotation::getConsequenceTypes,
                        hasItem(with("EnsemblGene", ConsequenceType::getEnsemblGeneId, is("ENSG00000250026"))))),
                hasAnnotation(with("ConsequenceType", VariantAnnotation::getConsequenceTypes,
                        hasItem(with("GeneName", ConsequenceType::getGeneName, is("TMPRSS11B"))))),
                hasAnnotation(with("VariantTraitAssociation", VariantAnnotation::getVariantTraitAssociation,
                        with("Cosmic", VariantTraitAssociation::getCosmic,
                                hasItem(with("MutationId", Cosmic::getMutationId, is("COSM1421316"))))))
        )));

    }

    @Test
    public void testGetAllVariants_xref_rs() {
        testGetAllVariants_rs(ANNOT_XREF.key());
    }

    public void testGetAllVariants_rs(String key) {
        // This test queries a single ID with no more options
        Query query = new Query(key, "rs1137005");
        queryResult = query(query, null);
        Variant variant = queryResult.first();
        assertEquals(1, queryResult.getNumResults());
        assertEquals(variant.getStart(), Integer.valueOf(1650807));
        assertThat(variant.getIds(), hasItem("rs1137005"));

        query = new Query(key, "rs1137005,rs150535390");
        queryResult = query(query, this.options);
        assertEquals(2, queryResult.getNumResults());
        queryResult.getResults().forEach(v -> assertThat(v.getIds(), anyOf(hasItem("rs1137005"), hasItem("rs150535390"))));
    }

    @Test
    public void testGetAllVariants_ct() {
        Query query;

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566");
        queryResult = query(query, null);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(hasItem("SO:0001566")))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(911, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "1566");
        queryResult = query(query, null);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(hasItem("SO:0001566")))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(911, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566,SO:0001583");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(anyOf(hasItem("SO:0001566"), hasItem("SO:0001583"))))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(947, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), ConsequenceTypeMappings.accessionToTerm.get(1566) + ",SO:0001583");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(anyOf(hasItem("SO:0001566"), hasItem("SO:0001583"))))));
        assertThat(queryResult, numResults(gt(0)));

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "1566,SO:0001583");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(anyOf(hasItem("SO:0001566"), hasItem("SO:0001583"))))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(947, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566;SO:0001583");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(allOf(hasItem("SO:0001566"), hasItem("SO:0001583"))))));
        assertThat(queryResult, numResults(gt(0)));

//        assertEquals(396, queryResult.getNumResults());
    }

    /*
       # Get variants grouped by transcript
       zcat opencga_variants_test.*.annot.json.gz |
            jq '
                def unwind(key): (key | .[]) as $value | . + ({} | (key = $value));
                unwind(.consequenceTypes) | {
                    so:.consequenceTypes.sequenceOntologyTerms,
                    gene:.consequenceTypes.geneName,
                    transcript:.consequenceTypes.ensemblTranscriptId,
                    biotype:.consequenceTypes.biotype,
                    flags:.consequenceTypes.transcriptAnnotationFlags,
                    var: (.chromosome+":"+ (.start  | tostring ) +":"+.reference+":"+.alternate),
                    id:.id
                }' -c | gzip > annotation_by_transcript.json.gz &
     */

    @Test
    public void testCombineGeneBtSoFlag() {
        // Expected match at PLCH2
        queryCombined(
                Arrays.asList(),
                Arrays.asList("PLCH2", "ERMAP", "SH2D5"),
                "protein_coding",
                "downstream_gene_variant",
                "basic");

        // Expected match at PLCH2 and 1:150970577:G:T
        queryCombined(
                Arrays.asList("1:150970577:G:T", "1:92445257:C:G", "1:104116413:T:C"),
                Arrays.asList("PLCH2", "ERMAP", "SH2D5"),
                "protein_coding",
                "downstream_gene_variant",
                "basic");
    }

    @Test
    public void testCombineGeneBtSo() {
        queryCombined(
                Arrays.asList(),
                Arrays.asList("ERMAP", "SH2D5"),
                "protein_coding",
                "downstream_gene_variant");

        // Expected match at ERMAP and 1:92445257:C:G
        queryCombined(
                Arrays.asList("1:92445257:C:G", "1:104116413:T:C"),
                Arrays.asList("ERMAP", "SH2D5"),
                "protein_coding",
                "downstream_gene_variant");
    }
    @Test
    public void testCombineGeneBt() {
        queryCombined(
                Arrays.asList(),
                Arrays.asList("ERMAP", "MIR431"),
                "protein_coding",
                null);

        // Expected match at ERMAP and 1:92445257:C:G.
        // Other two are miRNA
        queryCombined(
                Arrays.asList("1:92445257:C:G", "14:101350721:T:C"),
                Arrays.asList("ERMAP", "MIR431"),
                "protein_coding",
                null);

    }

    @Test
    public void testCombineBtSo() {
        queryCombined(
                Collections.emptyList(),
                Collections.emptyList(),
                "protein_coding",
                "downstream_gene_variant");

        // Expected match at 1:92445257:C:G
        queryCombined(
                Arrays.asList("1:92445257:C:G", "1:104116413:T:C"),
                Arrays.asList(),
                "protein_coding",
                "downstream_gene_variant");
    }

    @Test
    public void testCombineBtSoFlag() {
        // May have extra values!

        // Combine bt+so+flag
        queryCombined(
                Collections.emptyList(),
                Collections.emptyList(),
                "protein_coding",
                "downstream_gene_variant",
                "basic");
    }

    @Test
    public void testCombineGeneSo() {
        queryGeneCT("BIRC6", "SO:0001566");  // Should return 0 results
        queryGeneCT("BIRC6", "SO:0001583");
        queryGeneCT("DNAJC6", "SO:0001819");
        queryGeneCT("SH2D5", "SO:0001632");
        queryGeneCT("ERMAP,SH2D5", "SO:0001632");
    }

    @Test
    public void testCombineGeneSoVariants() {
        queryCombined(
                Arrays.asList("7:100807230:G:T"),
                Arrays.asList("ERMAP", "SH2D5"),
                null,
                "downstream_gene_variant");

        queryGeneCT("ERMAP,SH2D5", "SO:0001632", new Query()
                .append(GENE.key(), "ERMAP")
                .append(ANNOT_XREF.key(), "SH2D5,rs12345")
                .append(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001632"),
                with("id", VariantAnnotation::getId, is("rs1171830")));

        queryGeneCT("ERMAP,SH2D5", "SO:0001632", new Query()
                        .append(ANNOT_XREF.key(), "ERMAP,rs1171830,SH2D5,RCV000036856,7:100807230:G:T,COSM3760638")
                        .append(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001632"),
                anyOf(
                        with("id", VariantAnnotation::getId, is("rs1171830")),
                        at("7:100807230:G:T")));

        assertThat(query(new Query(ANNOT_XREF.key(), "rs1171830").append(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566"), null),
                everyResult(allVariants, allOf(
                        with("id", Variant::getId, is("rs1171830")),
                        hasAnnotation(hasSO(hasItem(is("SO:0001566")))))));
    }

    private void queryGeneCT(String gene, String so) {
        queryGeneCT(gene, so, new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), so).append(GENE.key(), gene), not(new IsAnything<>()));
    }

    private void queryGeneCT(String gene, String so, Query query, Matcher<VariantAnnotation> regionMatcher) {
        logger.info(query.toJson());
        queryResult = query(query, null);
        logger.info(" -> numResults " + queryResult.getNumResults());

        Matcher<String> geneMatcher;
        List<String> genes = Arrays.asList(gene.split(","));
        if (gene.contains(",")) {
            geneMatcher = anyOf(genes.stream().map(CoreMatchers::is).collect(Collectors.toList()));
        } else {
            geneMatcher = is(gene);
        }
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(
                anyOf(
                        allOf(
                                hasAnyGeneOf(genes),
                                withAny("consequence type", VariantAnnotation::getConsequenceTypes, allOf(
                                        with("gene", ConsequenceType::getGeneName, geneMatcher),
                                        withAny("SO", ConsequenceType::getSequenceOntologyTerms,
                                                with("accession", SequenceOntologyTerm::getAccession, is(so))))))
                        ,
                        allOf(
                                regionMatcher,
//                                not(hasAnyGeneOf(genes)),
                                hasSO(hasItem(so))
                )))));
    }

    private void queryCombined(List<String> variants,
                               List<String> genes,
                               String biotype,
                               String so) {
        queryCombined(variants, genes, biotype, so, null);
    }

    private void queryCombined(List<String> variants,
                               List<String> genes,
                               String biotype,
                               String so, String flag) {
        List<String> xrefs = new ArrayList<>();
        if (variants != null) {
            xrefs.addAll(variants);
        }
        if (genes != null) {
            xrefs.addAll(genes);
        }
        Query query = new Query()
//                .append(REGION.key(), region)
                .append(ANNOT_XREF.key(), xrefs)
                .append(ANNOT_BIOTYPE.key(), biotype)
                .append(ANNOT_CONSEQUENCE_TYPE.key(), so)
                .append(ANNOT_TRANSCRIPT_FLAG.key(), flag);
        logger.info(query.toJson());
        queryResult = query(query, null);
        logger.info(" -> numResults " + queryResult.getNumResults());

        Matcher<VariantAnnotation> regionMatcher;
        Matcher<String> geneMatcher;
        Matcher<String> biotypeMatcher;
        Matcher<String> soMatcher;
        Matcher<ConsequenceType> flagMatcher;

        if (CollectionUtils.isEmpty(variants)) {
            if (CollectionUtils.isEmpty(genes)) {
                // No gene or region filter, accept anything as region
                regionMatcher = any(VariantAnnotation.class);
            } else {
                regionMatcher = not(any(VariantAnnotation.class));
            }
        } else {
            regionMatcher = anyOf(variants.stream().map(VariantMatchers::at).collect(Collectors.toList()));
        }

        if (CollectionUtils.isEmpty(genes)) {
            geneMatcher = not(any(String.class));
        } else {
            geneMatcher = anyOf(genes.stream().map(CoreMatchers::is).collect(Collectors.toList()));
        }

        soMatcher = StringUtils.isEmpty(so) ? any(String.class) : is(so);
        if (StringUtils.isEmpty(flag)) {
            flagMatcher = any(ConsequenceType.class);
        } else {
            flagMatcher = withAny("flag", ConsequenceType::getTranscriptAnnotationFlags, is(flag));
        }

        if (StringUtils.isEmpty(biotype)) {
            biotypeMatcher = any(String.class);
        } else {
            biotypeMatcher = is(biotype);
        }

        assertThat(queryResult, everyResult(allVariants, hasAnnotation(
                anyOf(
                        allOf(
                                hasAnyGeneOf(genes),
                                withAny("consequence type", VariantAnnotation::getConsequenceTypes, allOf(
                                        with("gene", ConsequenceType::getGeneName, geneMatcher),
                                        withAny("SO", ConsequenceType::getSequenceOntologyTerms,
                                                anyOf(
                                                        with("soAccession", SequenceOntologyTerm::getAccession, soMatcher),
                                                        with("soName", SequenceOntologyTerm::getName, soMatcher)
                                                )
                                        ),
                                        with("biotype", ConsequenceType::getBiotype, biotypeMatcher),
                                        flagMatcher)))
                        ,
                        allOf(
                                regionMatcher,
//                                not(hasAnyGeneOf(genes)),
                                withAny("consequence type", VariantAnnotation::getConsequenceTypes, allOf(
                                        withAny("SO", ConsequenceType::getSequenceOntologyTerms,
                                                anyOf(
                                                        with("soAccession", SequenceOntologyTerm::getAccession, soMatcher),
                                                        with("soName", SequenceOntologyTerm::getName, soMatcher)
                                                )
                                        ),
                                        with("biotype", ConsequenceType::getBiotype, biotypeMatcher),
                                        flagMatcher
                                )))
                ))
        ));
    }

    @Test
    public void testGetAllVariants_transcriptionAnnotationFlags() {
        //ANNOT_TRANSCRIPTION_FLAGS
        Query query;
        Multiset<String> flags = HashMultiset.create();
        Set<String> flagsInVariant = new HashSet<>();
        for (Variant variant : allVariants.getResults()) {
            if (variant.getAnnotation().getConsequenceTypes() != null) {
                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                    if (consequenceType.getTranscriptAnnotationFlags() != null) {
                        flagsInVariant.addAll(consequenceType.getTranscriptAnnotationFlags());
                    }
                }
            }
            flags.addAll(flagsInVariant);
            flagsInVariant.clear();
        }

        System.out.println(flags);
        assertThat(flags, hasItem("basic"));
        assertThat(flags, hasItem("CCDS"));
        assertThat(flags, hasItem("mRNA_start_NF"));
        assertThat(flags, hasItem("mRNA_end_NF"));
        assertThat(flags, hasItem("cds_start_NF"));
        assertThat(flags, hasItem("cds_end_NF"));

        for (String flag : flags.elementSet()) {
            System.out.println(flag + ", " + flags.count(flag));
            query = new Query(ANNOT_TRANSCRIPT_FLAG.key(), flag);
            queryResult = query(query, null);
            assertEquals(flags.count(flag), queryResult.getNumResults());
        }

    }

    @Test
    public void testGetAllVariants_geneTrait() {
        //ANNOT_GENE_TRAIT_ID
        //ANNOT_GENE_TRAIT_NAME
        Query query;
        Map<String, Integer> idsMap = new HashMap<>();
        Map<String, Integer> namesMap = new HashMap<>();
        Map<String, Integer> hposMap = new HashMap<>();
        for (Variant variant : allVariants.getResults()) {
            Set<String> ids = new HashSet<>();
            Set<String> names = new HashSet<>();
            Set<String> hpos = new HashSet<>();
            if (variant.getAnnotation().getGeneTraitAssociation() != null) {
                for (GeneTraitAssociation geneTrait : variant.getAnnotation().getGeneTraitAssociation()) {
                    ids.add(geneTrait.getId());
                    names.add(geneTrait.getName());
                    if (StringUtils.isNotEmpty(geneTrait.getHpo())) {
                        hpos.add(geneTrait.getHpo());
                    }
                }
            }
            for (String id : ids) {
                idsMap.put(id, idsMap.getOrDefault(id, 0) + 1);
            }
            for (String name : names) {
                namesMap.put(name, namesMap.getOrDefault(name, 0) + 1);
            }
            for (String hpo : hpos) {
                hposMap.put(hpo, hposMap.getOrDefault(hpo, 0) + 1);
            }
        }

        System.out.println(idsMap.size());
        System.out.println(namesMap.size());
        System.out.println(hposMap.size());

        namesMap.entrySet().stream().limit(QUERIES_LIM).forEach(entry -> {
            Query q = new Query(ANNOT_GENE_TRAIT_NAME.key(), '"' + entry.getKey() + '"');
            queryResult = query(q, null);
            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
        });

        idsMap.entrySet().stream().limit(QUERIES_LIM).forEach(entry -> {
            Query q = new Query(ANNOT_GENE_TRAIT_ID.key(), entry.getKey());
            queryResult = query(q, null);
            assertEquals(entry.getValue().intValue(), queryResult.getNumResults());
        });

        hposMap.entrySet().stream().limit(QUERIES_LIM).forEach(entry -> {
            Query q = new Query(ANNOT_HPO.key(), entry.getKey());
            queryResult = query(q, null);
            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
        });

        List<String> ids = idsMap.keySet().stream().limit(10).collect(Collectors.toList());
        query = new Query(ANNOT_GENE_TRAIT_ID.key(), String.join(OR, ids));
        queryResult = query(query, null);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants,
                hasAnnotation(with("GeneTraitAssociation", VariantAnnotation::getGeneTraitAssociation,
                        hasItem(with("GeneTraitId", GeneTraitAssociation::getId, is(anyOf(ids.stream().map(CoreMatchers::is).collect(Collectors.toList())))))))));

        List<String> hpos = hposMap.keySet().stream().limit(10).collect(Collectors.toList());
        query = new Query(ANNOT_GENE_TRAIT_ID.key(), String.join(OR, ids) + OR + String.join(OR, hpos));
        queryResult = query(query, null);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants,
                hasAnnotation(with("GeneTraitAssociation", VariantAnnotation::getGeneTraitAssociation, anyOf(
                                hasItem(with("GeneTraitId", GeneTraitAssociation::getId, is(anyOf(ids.stream().map(CoreMatchers::is).collect(Collectors.toList()))))),
                                hasItem(with("HPO", GeneTraitAssociation::getHpo, is(anyOf(hpos.stream().map(CoreMatchers::is).collect(Collectors.toList())))))
                        )))));

        ids = Arrays.asList("umls:C0007131", "umls:C0000786");
        query = new Query(ANNOT_GENE_TRAIT_ID.key(), String.join(AND, ids));
        queryResult = query(query, null);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants,
                hasAnnotation(with("GeneTraitAssociation", VariantAnnotation::getGeneTraitAssociation,
                        allOf(ids.stream().map(id -> hasItem(with("GeneTraitId", GeneTraitAssociation::getId, is(id)))).collect(Collectors.toList()))))));

        ids = Arrays.asList("umls:C0007131", "umls:C0000786", "HP:0002483");
        query = new Query(ANNOT_GENE_TRAIT_ID.key(), String.join(AND, ids));
        queryResult = query(query, null);
        System.out.println("queryResult.getNumResults() = " + queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants,
                hasAnnotation(with("GeneTraitAssociation", VariantAnnotation::getGeneTraitAssociation,
                        allOf(
                                hasItem(with("GeneTraitId", GeneTraitAssociation::getId, is("umls:C0007131"))),
                                hasItem(with("GeneTraitId", GeneTraitAssociation::getId, is("umls:C0000786"))),
                                hasItem(with("HPO", GeneTraitAssociation::getHpo, is("HP:0002483")))
                        )))));
    }

    @Test
    public void testGoQuery() throws StorageEngineException {

        // MMP26 -> GO:0004222,GO:0005578,GO:0006508
        // CEBPA -> GO:0000050

        int totalResults = 0;
        Collection<String> genes;
        Query query;
        DataResult<Variant> result;

        query = new Query(ANNOT_GO.key(), "GO:XXXXXXX");
        result = variantStorageEngine.get(query, null);
        assertEquals(0, result.getNumResults());

        query = new Query(ANNOT_GO.key(), "GO:0006508");
        result = variantStorageEngine.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResults()) {
            System.out.println(variant);
        }
        assertNotEquals(0, result.getNumResults());
        CellBaseUtils cellBaseUtils = variantStorageEngine.getCellBaseUtils();
        genes = cellBaseUtils.getGenesByGo(query.getAsStringList(ANNOT_GO.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        totalResults = result.getNumResults();

        genes = Arrays.asList("MMP11", "KLK15", "HPR", "GZMA", "METAP1D", "MMP23B");
        // Last 3 genes does not match with that GO term
        List<String> extraGenes = Arrays.asList("MMP11", "KLK15", "HPR", "GZMA", "METAP1D", "MMP23B", "MIB2", "ADSL", "BRCA2");
        query = new Query(ANNOT_GO.key(), "GO:0006508").append(GENE.key(), extraGenes);
        result = variantStorageEngine.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResults()) {
            System.out.println(variant);
        }
        assertNotEquals(0, result.getNumResults());
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));

        query = new Query(ANNOT_GO.key(), "GO:0000050");
        result = variantStorageEngine.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResults()) {
            System.out.println(variant);
        }
        genes = cellBaseUtils.getGenesByGo(query.getAsStringList(ANNOT_GO.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        assertNotEquals(0, result.getNumResults());
        totalResults += result.getNumResults();

        query = new Query(ANNOT_GO.key(), "GO:0006508,GO:0000050");
        result = variantStorageEngine.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResults()) {
            System.out.println(variant);
        }
        genes = cellBaseUtils.getGenesByGo(query.getAsStringList(ANNOT_GO.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        assertNotEquals(0, result.getNumResults());
        assertEquals(result.getNumResults(), totalResults);
    }


    @Test
    public void testExpressionQuery() throws StorageEngineException {
        Collection<String> genes;
        Query query = new Query(ANNOT_EXPRESSION.key(), "non_existing_tissue");
        DataResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        for (String tissue : Arrays.asList("umbilical cord", "midbrain")) {

            query = new Query(ANNOT_EXPRESSION.key(), tissue);
            result = variantStorageEngine.get(query, null);
            System.out.println("result.getNumResults() = " + result.getNumResults());
            assertNotEquals(0, result.getNumResults());
            assertNotEquals(allVariants.getNumResults(), result.getNumResults());
            genes = variantStorageEngine.getCellBaseUtils()
                    .getGenesByExpression(query.getAsStringList(ANNOT_EXPRESSION.key()));
            assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        }
    }


    @Test
    public void testGetAllVariants_proteinKeywords() {
        //ANNOT_PROTEIN_KEYWORDS
        Query query;
        Map<String, Integer> keywords = new HashMap<>();
        int combinedKeywordsOr = 0;
        int combinedKeywordsAnd = 0;
        int combinedKeywordsAndNot = 0;
        for (Variant variant : allVariants.getResults()) {
            Set<String> keywordsInVariant = new HashSet<>();
            if (variant.getAnnotation().getConsequenceTypes() != null) {
                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                    if (consequenceType.getProteinVariantAnnotation() != null && consequenceType.getProteinVariantAnnotation().getKeywords() != null) {
                        keywordsInVariant.addAll(consequenceType.getProteinVariantAnnotation().getKeywords());
                    }
                }
            }
            for (String flag : keywordsInVariant) {
                keywords.put(flag, keywords.getOrDefault(flag, 0) + 1);
            }
            if (keywordsInVariant.contains("Complete proteome") || keywordsInVariant.contains("Transmembrane helix")) {
                combinedKeywordsOr++;
            }
            if (keywordsInVariant.contains("Complete proteome") && keywordsInVariant.contains("Transmembrane helix")) {
                combinedKeywordsAnd++;
            }
            if (keywordsInVariant.contains("Complete proteome") && !keywordsInVariant.contains("Transmembrane helix")) {
                combinedKeywordsAndNot++;
            }
        }

        assertTrue(combinedKeywordsOr > 0);
        assertTrue(combinedKeywordsAnd > 0);
        assertTrue(combinedKeywordsAndNot > 0);

        query = new Query(ANNOT_PROTEIN_KEYWORD.key(), "Complete proteome,Transmembrane helix");
        assertEquals(combinedKeywordsOr, count(query).intValue());
        query = new Query(ANNOT_PROTEIN_KEYWORD.key(), "Complete proteome;Transmembrane helix");
        assertEquals(combinedKeywordsAnd, count(query).intValue());
        query = new Query(ANNOT_PROTEIN_KEYWORD.key(), "Complete proteome;!Transmembrane helix");
        assertEquals(combinedKeywordsAndNot, count(query).intValue());

        int i = 0;
        for (Map.Entry<String, Integer> entry : keywords.entrySet()) {
            System.out.println(entry);
            query = new Query(ANNOT_PROTEIN_KEYWORD.key(), entry.getKey());
            queryResult = query(query, null);
            assertEquals(entry.getValue().intValue(), queryResult.getNumResults());
            if (++i > QUERIES_LIM) {
                break;
            }
        }

    }

    @Test
    public void testGetAllVariants_drugs() {
        //ANNOT_DRUG
        Query query;
        Map<String, Integer> drugs = new HashMap<>();
        for (Variant variant : allVariants.getResults()) {
            if (variant.getAnnotation().getGeneDrugInteraction() != null) {
                Set<String> drugsInVariant = new HashSet<>();
                for (GeneDrugInteraction drugInteraction : variant.getAnnotation().getGeneDrugInteraction()) {
                    drugsInVariant.add(drugInteraction.getDrugName());
                }
                for (String flag : drugsInVariant) {
                    drugs.put(flag, drugs.getOrDefault(flag, 0) + 1);
                }
            }
        }

        int i = 0;
        for (Map.Entry<String, Integer> entry : drugs.entrySet()) {
            if (entry.getKey().contains(",")) {
                continue;
            }
            query = new Query(ANNOT_DRUG.key(), entry.getKey());
            queryResult = query(query, null);
            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
            if (++i > QUERIES_LIM) {
                break;
            }
        }

    }

    @Test
    public void testGetAllVariants_polyphenSift() {
        //POLYPHEN
        //SIFT

        Map<String, Matcher<Double>> queries = new HashMap<>();
        queries.put("<0.101", lt(0.101));
        queries.put("<0.201", lt(0.201));
        queries.put("<0.501", lt(0.501));
        queries.put("<0.901", lt(0.901));

        queries.put(">0.101", gt(0.101));
        queries.put(">0.201", gt(0.201));
        queries.put(">0.501", gt(0.501));
        queries.put(">0.901", gt(0.901));

        for (Map.Entry<String, Matcher<Double>> entry : queries.entrySet()) {
            String q = entry.getKey();
            Matcher<Double> m = entry.getValue();

            System.out.println("q = " + q + " -> " + m);
            queryResult = query(new Query(ANNOT_SIFT.key(), q), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnySift(m))));

            queryResult = query(new Query(ANNOT_POLYPHEN.key(), q), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnyPolyphen(m))));

            queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen" + q), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnyPolyphen(m))));

            queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift" + q), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnySift(m))));

            // Duplicate operator
            q = q.charAt(0) + q;
            System.out.println("q = " + q);

            queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen" + q), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPolyphen(anyOf(hasItem(m), isEmpty())))));

            queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift" + q), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSift(anyOf(hasItem(m), isEmpty())))));
        }

//        for (Map.Entry<Double, Integer> entry : polyphen.entrySet()) {
//            query = new Query(VariantDBAdaptor.VariantQueryParams.SIFT.key(), entry.getKey());
//            queryResult = dbAdaptor.get(query, null);
//            assertEquals(entry.getKey(), entry.getValue(), queryResult.getNumResults());
//        }

    }

    @Test
    public void testGetAllVariants_polyphenSiftMalformed() {
        Query query = new Query(ANNOT_POLYPHEN.key(), "sift>0.5");
        thrown.expect(VariantQueryException.class);
        query(query, null);
    }

    @Test
    public void testGetAlVariants_polyphenSiftDescription() {
        for (String p : Arrays.asList("benign", "possibly damaging", "probably damaging", "unknown")) {
            queryResult = query(new Query(ANNOT_POLYPHEN.key(), p), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnyPolyphenDesc(equalTo(p)))));
            queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen=" + p), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnyPolyphenDesc(equalTo(p)))));
        }

        for (String s : Arrays.asList("deleterious", "tolerated")) {
            queryResult = query(new Query(ANNOT_SIFT.key(), s), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnySiftDesc(equalTo(s)))));
            queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=" + s), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnySiftDesc(equalTo(s)))));
        }
    }

    @Test
    public void testGetAllVariants_functionalScore() {
        //ANNOT_FUNCTIONAL_SCORE

        assertTrue(countFunctionalScore("cadd_scaled", allVariants, s -> s > 5.0) > 0);
        System.out.println("countFunctionalScore(\"cadd_scaled\", allVariants, s -> s > 5.0) = " + countFunctionalScore("cadd_scaled", allVariants, s -> s > 5.0));

        checkFunctionalScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_scaled>5"), s -> s > 5.0, "cadd_scaled");

        checkFunctionalScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_raw<0.5"), s1 -> s1 < 0.5, "cadd_raw");

        checkFunctionalScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_scaled<=0.5"), s -> s <= 0.5, "cadd_scaled");

//        checkFunctionalScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_scaled<<0.5"), s -> s < 0.5, "cadd_scaled");

        checkScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_scaled<<0.5"),
                ((Predicate<List<Score>>) scores -> scores.stream()
                        .anyMatch(s -> s.getSource().equalsIgnoreCase("cadd_scaled") && s.getScore() < 0.5))
                        .or(List::isEmpty),
                VariantAnnotation::getFunctionalScore);

        checkScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_scaled<<=0.5"),
                ((Predicate<List<Score>>) scores -> scores.stream()
                        .anyMatch(s -> s.getSource().equalsIgnoreCase("cadd_scaled") && s.getScore() <= 0.5))
                        .or(List::isEmpty),
                VariantAnnotation::getFunctionalScore);

        checkScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_raw>>0.5"),
                ((Predicate<List<Score>>) scores -> scores.stream()
                        .anyMatch(s -> s.getSource().equalsIgnoreCase("cadd_raw") && s.getScore() > 0.5))
                        .or(List::isEmpty),
                VariantAnnotation::getFunctionalScore);

        checkScore(new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_raw>>=0.5"),
                ((Predicate<List<Score>>) scores -> scores.stream()
                        .anyMatch(s -> s.getSource().equalsIgnoreCase("cadd_raw") && s.getScore() >= 0.5))
                        .or(List::isEmpty),
                VariantAnnotation::getFunctionalScore);

    }

    @Test
    public void testGetAllVariants_functionalScore_wrongSource() {
        String value = "cad<=0.5";
        VariantQueryException expected = VariantQueryException.malformedParam(ANNOT_FUNCTIONAL_SCORE, value);
        thrown.expect(expected.getClass());
        thrown.expectMessage(expected.getMessage());
        query(new Query(ANNOT_FUNCTIONAL_SCORE.key(), value), null);
    }

    @Test
    public void testGetAllVariants_functionalScore_wrongValue() {
        String value = "cadd_scaled<=A";
        VariantQueryException expected = VariantQueryException.malformedParam(ANNOT_FUNCTIONAL_SCORE, value);
        thrown.expect(expected.getClass());
        thrown.expectMessage(expected.getMessage());
        query(new Query(ANNOT_FUNCTIONAL_SCORE.key(), value), null);
    }

    @Test
    public void testGetAllVariants_conservationScore() {
        //ANNOT_CONSERVATION

        long phastCons = countConservationScore("phastCons", allVariants, s -> s > 0.5);
        assertTrue(phastCons > 0);

        checkConservationScore(new Query(ANNOT_CONSERVATION.key(), "phylop>0.5"), s -> s > 0.5, "phylop");

        checkConservationScore(new Query(ANNOT_CONSERVATION.key(), "phastCons<0.5"), s1 -> s1 < 0.5, "phastCons");

        checkConservationScore(new Query(ANNOT_CONSERVATION.key(), "gerp<=0.5"), s -> s <= 0.5, "gerp");
        checkScore(new Query(ANNOT_CONSERVATION.key(), "gerp<=0.5,phastCons<0.5"),
                ((Predicate<List<Score>>) scores -> scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("gerp") && s.getScore() <= 0.5))
                        .or(scores -> scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("phastCons") && s.getScore() < 0.5)), VariantAnnotation::getConservation);

        checkScore(new Query(ANNOT_CONSERVATION.key(), "gerp<=0.5;phastCons<0.5"),
                ((Predicate<List<Score>>) scores -> scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("gerp") && s.getScore() <= 0.5))
                        .and(scores -> scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("phastCons") && s.getScore() < 0.5)),
                VariantAnnotation::getConservation);

        checkScore(new Query(ANNOT_CONSERVATION.key(), "gerp<<0.5"),
                scores ->
                        scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("gerp") && s.getScore() < 0.5)
                                || scores.stream().noneMatch(s -> s.getSource().equalsIgnoreCase("gerp")),
                VariantAnnotation::getConservation);

        checkScore(new Query(ANNOT_CONSERVATION.key(), "phastCons>>=0.5"),
                scores ->
                        scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("phastCons") && s.getScore() >= 0.5)
                                || scores.stream().noneMatch(s -> s.getSource().equalsIgnoreCase("phastCons")),
                VariantAnnotation::getConservation);

        checkScore(new Query(ANNOT_CONSERVATION.key(), "gerp<<=0.5;phastCons<0.5"),
                scores -> (scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("gerp") && s.getScore() <= 0.5)
                        || scores.stream().noneMatch(s -> s.getSource().equalsIgnoreCase("gerp")))
                        && scores.stream().anyMatch(s -> s.getSource().equalsIgnoreCase("phastCons") && s.getScore() < 0.5),
                VariantAnnotation::getConservation);

    }

    @Test
    public void testGetAllVariants_conservationScoreWrongSource() {
        VariantQueryException e = VariantQueryException.malformedParam(ANNOT_CONSERVATION, "phast<0.5");
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        query(new Query(ANNOT_CONSERVATION.key(), "phast<0.5"), null);
    }

    @Test
    public void testGetAllVariants_conservationScoreWrongValue() {
        VariantQueryException e = VariantQueryException.malformedParam(ANNOT_CONSERVATION, "phastCons<a");
        thrown.expect(e.getClass());
        thrown.expectMessage(e.getMessage());
        query(new Query(ANNOT_CONSERVATION.key(), "phastCons<a"), null);
    }

    public void checkConservationScore(Query query, Predicate<Double> doublePredicate, String source) {
        checkScore(query, doublePredicate, source, VariantAnnotation::getConservation);
    }

    public void checkFunctionalScore(Query query, Predicate<Double> doublePredicate, String source) {
        checkScore(query, doublePredicate, source, VariantAnnotation::getFunctionalScore);
    }

    public void checkScore(Query query, Predicate<Double> doublePredicate, String source, Function<VariantAnnotation, List<Score>> mapper) {
        checkScore(query, scores -> scores.stream().anyMatch(score -> score.getSource().equalsIgnoreCase(source) && doublePredicate.test(score.getScore())), mapper);
    }

    public void checkScore(Query query, Predicate<List<Score>> scorePredicate, Function<VariantAnnotation, List<Score>> mapper) {
        DataResult<Variant> result = query(query, null);
        Collection<Variant> expected = filterByScore(allVariants, scorePredicate, mapper);
        Collection<Variant> filteredResult = filterByScore(result, scorePredicate, mapper);
        TreeSet<Variant> actual = new TreeSet<>(Comparator.comparing(Variant::getChromosome).thenComparing(Variant::getStart).thenComparing(Variant::toString));
        actual.addAll(result.getResults());
        if (expected.size()!=actual.size()) {
            System.out.println("expected = " + expected);
            System.out.println("actual   = " + actual);
        }
        assertTrue("Expecting a query returning some value.", expected.size() > 0);
        assertEquals(expected.size(), result.getNumResults());
        assertEquals(expected.size(), actual.size());
        assertEquals(expected.size(), filteredResult.size());
    }

    private long countConservationScore(String source, DataResult<Variant> variantQueryResult, Predicate<Double> doublePredicate) {
        return countScore(source, variantQueryResult, doublePredicate, VariantAnnotation::getConservation);
    }

    private long countFunctionalScore(String source, DataResult<Variant> variantQueryResult, Predicate<Double> doublePredicate) {
        return countScore(source, variantQueryResult, doublePredicate, VariantAnnotation::getFunctionalScore);
    }

    private long countScore(String source, DataResult<Variant> variantQueryResult, Predicate<Double> doublePredicate, Function<VariantAnnotation, List<Score>> mapper) {
        return countScore(variantQueryResult, scores -> scores.stream().anyMatch(score -> score.getSource().equalsIgnoreCase(source) && doublePredicate.test(score.getScore())), mapper);
    }

    private long countScore(DataResult<Variant> variantQueryResult, Predicate<List<Score>> predicate, Function<VariantAnnotation, List<Score>> mapper) {
        return filterByScore(variantQueryResult, predicate, mapper).size();
    }

    private Collection<Variant> filterByScore(DataResult<Variant> variantQueryResult, Predicate<List<Score>> predicate, Function<VariantAnnotation, List<Score>> mapper) {
        TreeSet<Variant> variants = new TreeSet<>(Comparator.comparing(Variant::getChromosome).thenComparing(Variant::getStart).thenComparing(Variant::toString));
        for (Variant variant : variantQueryResult.getResults()) {
            List<Score> list = mapper.apply(variant.getAnnotation());
            if (list == null) {
                list = Collections.emptyList();
            }
            if (predicate.test(list)) {
                variants.add(variant);
            }
        }
        return variants;
    }

    @Test
    public void testGetSortedVariantsDefault() {
        QueryOptions options = new QueryOptions(QueryOptions.SORT, true);
        VariantDBIterator iterator = iterator(null, options);
        Variant next, prev;
        prev = iterator.next();
        while (iterator.hasNext()) {
            next = iterator.next();
            if (next.getChromosome().equals(prev.getChromosome())) {
                assertTrue(prev + " <= " + next, prev.getStart() <= next.getStart());
            }
            prev = next;
        }
    }

    @Test
    public void testGetSortedVariantsAscending() {
        QueryOptions options = new QueryOptions(QueryOptions.SORT, true).append(QueryOptions.ORDER, QueryOptions.ASCENDING);
        VariantDBIterator iterator = iterator(null, options);
        Variant next, prev;
        prev = iterator.next();
        while (iterator.hasNext()) {
            next = iterator.next();
            if (next.getChromosome().equals(prev.getChromosome())) {
                assertTrue(prev + " <= " + next, prev.getStart() <= next.getStart());
            }
            prev = next;
        }
    }

    @Test
    public void testGetSortedVariantsReverse() {
        QueryOptions options = new QueryOptions(QueryOptions.SORT, true).append(QueryOptions.ORDER, QueryOptions.DESCENDING);
        VariantDBIterator iterator = iterator(null, options);
        Variant next, prev;
        prev = iterator.next();
        while (iterator.hasNext()) {
            next = iterator.next();
            if (next.getChromosome().equals(prev.getChromosome())) {
                assertTrue(prev + " >= " + next, prev.getStart() >= next.getStart());
            }
            prev = next;
        }
    }

    @Test
    public void testGetAllVariants_region() {
        Query query = new Query(REGION.key(), "1:13910417-13910417,1:165389129-165389129");
        queryResult = query(query, options);
        assertEquals(2, queryResult.getNumResults());

        query = new Query(REGION.key(), Arrays.asList("1:13910417-13910417", "1:165389129-165389129"));
        queryResult = query(query, options);
        assertEquals(2, queryResult.getNumResults());

        query = new Query(REGION.key(),
                Arrays.asList(Region.parseRegion("1:13910417-13910417"), Region.parseRegion("1:165389129-165389129")));
        queryResult = query(query, options);
        assertEquals(2, queryResult.getNumResults());

        options.put(QueryOptions.SORT, true);
        query = new Query(REGION.key(), "1:14000000-160000000");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, overlaps(new Region("1:14000000-160000000"))));

        int lastStart = 0;
        for (Variant variant : queryResult.getResults()) {
            assertEquals("1", variant.getChromosome());
            assertTrue(lastStart <= variant.getStart());
            lastStart = variant.getStart();
        }

        // Basic queries
        checkRegion(new Region("1:1000000-2000000"));
        checkRegion(new Region("1:10000000-20000000"));
        checkRegion(new Region("1:14000000-160000000"));
        checkRegion(new Region("1"), new Region("1"));
        checkRegion(new Region("ch1"), new Region("1"));
        checkRegion(new Region("chr1"), new Region("1"));
        checkRegion(new Region("chrm1"), new Region("1"));
        checkRegion(new Region("chrom1"), new Region("1"));
        checkRegion(new Region("2"));
        checkRegion(new Region("X"));
        checkRegion(new Region("30"));
        checkRegion(new Region("3:1-200000000"));
        checkRegion(new Region("X:1-200000000"));

        // Exactly in the limits
        checkRegion(new Region("20:238441-7980390"));

        // Just inside the limits
        checkRegion(new Region("20:238440-7980391"));

        // Just outside the limits
        checkRegion(new Region("20:238441-7980389"));
        checkRegion(new Region("20:238442-7980390"));
        checkRegion(new Region("20:238442-7980389"));

        query = new Query(REGION.key(), "chr2");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, overlaps(new Region("2"))));
    }

    public void checkRegion(Region region) {
        checkRegion(region, region);
    }

    public void checkRegion(Region queryRegion, Region overlappingRegion) {
        queryResult = query(new Query(REGION.key(), queryRegion), null);
        assertThat(queryResult, everyResult(allVariants, overlaps(overlappingRegion)));
    }

    @Test
    public void testGetAllVariants_genes() {
        Query query = new Query(GENE.key(), "FLG-AS1");
        DataResult<Variant> result = query(query, new QueryOptions());

        assertThat(result, everyResult(allVariants, hasAnnotation(hasGenes(Collections.singletonList("FLG-AS1")))));

        for (Variant variant : result.getResults()) {
            System.out.println("variant = " + variant);
        }

        query = new Query(GENE.key(), "WRONG_GENE");

        VariantQueryException exception = VariantQueryException.geneNotFound("WRONG_GENE");
        thrown.expect(exception.getClass());
        thrown.expectMessage(exception.getMessage());
        result = query(query, new QueryOptions());
    }

    @Test
    public void testGetAllVariants_studies() {

        Query query = new Query(STUDY.key(), studyMetadata.getName());
        long numResults = count(query);
        assertEquals(allVariants.getNumResults(), numResults);

        query = new Query(STUDY.key(), studyMetadata.getId());
        numResults = count(query);
        assertEquals(allVariants.getNumResults(), numResults);

    }

    @Test
    public void testGetAllVariants_Negatedstudies() {
//        query = new Query(STUDY.key(), NOT + studyConfiguration.getStudyId());
//        numResults = count(query);
//        assertEquals(0, numResults);

        Query query = new Query(STUDY.key(), NOT + studyMetadata.getName());
        long numResults = count(query);
        assertEquals(0, numResults);

    }

    @Test
    public void testGetAllVariants_files() {

        Query query = new Query(FILE.key(), fileId);
        long numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query = new Query(FILE.key(), fileId).append(STUDY.key(), studyMetadata.getId());
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query = new Query().append(STUDY.key(), studyMetadata.getId());
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

    }

    @Test
    public void testGetAllVariants_fileNotFound() {
//        VariantQueryException e = VariantQueryException.missingStudyForFile("-1", Collections.singletonList(studyMetadata.getName()));
        VariantQueryException e = VariantQueryException.fileNotFound("-1", studyMetadata.getName());
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        count(new Query(FILE.key(), -1));
//        assertEquals("There is no file with ID -1", 0, numResults);
    }

    @Test
    public void testGetAllVariants_filterNoFile() {
        // FILTER
        Query query = new Query(FILTER.key(), "PASS");
        long numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, count(query).longValue());

        // FILTER+STUDY
        query = new Query(STUDY.key(), studyMetadata.getId()).append(FILTER.key(), "PASS");
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, count(query).longValue());

    }

    @Test
    public void testGetAllVariants_filter() {
        Query query;
        long numResults;

        // FILTER+FILE
        query = new Query(FILE.key(), fileId).append(FILTER.key(), "PASS");
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, count(query).longValue());

        // FILTER+FILE+STUDY
        query = new Query(FILE.key(), fileId).append(STUDY.key(), studyMetadata.getId()).append(FILTER.key(), "PASS");
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, count(query).longValue());
    }

    @Test
    public void testGetAllVariants_include_samples() {
        checkSamplesData("NA19600");
        checkSamplesData("NA19660");
        checkSamplesData("NA19661");
        checkSamplesData("NA19685");
        checkSamplesData("NA19600,NA19685");
        checkSamplesData("NA19685,NA19600");
        checkSamplesData("NA19660,NA19661,NA19600");
        checkSamplesData(null);
        checkSamplesData(VariantQueryUtils.ALL);
        checkSamplesData(VariantQueryUtils.NONE);
    }

    public void checkSamplesData(String returnedSamples) {
        Query query = new Query(SAMPLE_METADATA.key(), true);
        QueryOptions options = new QueryOptions(QueryOptions.SORT, true); //no limit;

        query.put(INCLUDE_SAMPLE.key(), returnedSamples);
        VariantQueryResult<Variant> queryResult = query(query, options);
        List<String> samplesName;
        if (returnedSamples == null || returnedSamples.equals(VariantQueryUtils.ALL)) {
            samplesName = this.sampleNames;
        } else if (returnedSamples.equals(VariantQueryUtils.NONE)) {
            samplesName = Collections.emptyList();
        } else {
            samplesName = query.getAsStringList(VariantQueryParam.INCLUDE_SAMPLE.key());
        }
        Map<String, List<String>> expectedSamples = Collections.singletonMap(studyMetadata.getName(), samplesName);

        Iterator<Variant> it_1 = allVariants.getResults().iterator();
        Iterator<Variant> it_2 = queryResult.getResults().iterator();

        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());

        LinkedHashMap<String, Integer> samplesPosition1 = null;
        LinkedHashMap<String, Integer> samplesPosition2 = null;
        for (int i = 0; i < queryResult.getNumResults(); i++) {
            Variant variant1 = it_1.next();
            Variant variant2 = it_2.next();

            assertEquals(variant1.toString(), variant2.toString());
            assertEquals(expectedSamples, queryResult.getSamples());

            LinkedHashMap<String, Integer> thisSamplesPosition1 = variant1.getStudy(studyMetadata.getName()).getSamplesPosition();
            LinkedHashMap<String, Integer> thisSamplesPosition2 = variant2.getStudy(studyMetadata.getName()).getSamplesPosition();
            if (samplesPosition1 == null) {
                samplesPosition1 = thisSamplesPosition1;
            }
            if (samplesPosition2 == null) {
                samplesPosition2 = thisSamplesPosition2;
                assertEquals(samplesName, new ArrayList<>(samplesPosition2.keySet()));
            }
            assertEquals(samplesPosition1, thisSamplesPosition1);
            assertEquals(samplesPosition2, thisSamplesPosition2);

            assertEquals(System.identityHashCode(samplesPosition1), System.identityHashCode(thisSamplesPosition1));
            assertEquals(System.identityHashCode(samplesPosition2), System.identityHashCode(thisSamplesPosition2));

            assertSame(samplesPosition1, thisSamplesPosition1);
            assertSame(samplesPosition2, thisSamplesPosition2);
            for (String sampleName : samplesName) {
                String gt1 = variant1.getStudy(studyMetadata.getName()).getSampleData(sampleName, "GT");
                String gt2 = variant2.getStudy(studyMetadata.getName()).getSampleData(sampleName, "GT");
                assertEquals(sampleName + " " + variant1.getChromosome() + ":" + variant1.getStart(), gt1, gt2);
            }
        }
    }

    @Test
    public void testIterator() {
        int numVariants = 0;
        Query query = new Query();
        for (VariantDBIterator iterator = iterator(query, new QueryOptions()); iterator.hasNext(); ) {
            Variant variant = iterator.next();
            numVariants++;
            StudyEntry entry = variant.getStudiesMap().entrySet().iterator().next().getValue();
//            assertEquals("6", entry.getFileId());
            assertEquals(studyMetadata.getName(), entry.getStudyId());
            assertEquals(sampleNames, new ArrayList<>(entry.getSamplesName()));
        }
        assertEquals(NUM_VARIANTS, numVariants);
    }

    @Test
    public void testGetAllVariants_genotypes() {

        Query query = new Query(GENOTYPE.key(), na19600 + IS + homAlt);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResults().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), STUDY_NAME + ":NA19600" + IS + homAlt);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResults().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), STUDY_NAME + ":NA19600" + IS + GenotypeClass.HOM_ALT);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResults().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), "NA19600" + IS + homAlt)
                .append(STUDY.key(), STUDY_NAME);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResults().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), "NA19600" + IS + homAlt);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResults().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        //get for each genotype. Should return all variants
        query = new Query(GENOTYPE.key(), na19600 + IS + homRef + OR + het + OR + homAlt + OR + "./.");
        long numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        //get for each genotype. Should return all variants
        query = new Query(GENOTYPE.key(), na19600 + IS + GenotypeClass.HOM_REF + OR + GenotypeClass.HET + OR + GenotypeClass.HOM_ALT + OR + GenotypeClass.MISS);
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        //Get all missing genotypes for sample na19600
        query = new Query(GENOTYPE.key(), na19600 + IS + "./.");
        queryResult = query(query, new QueryOptions());
        assertEquals(9, queryResult.getNumResults());
        queryResult.getResults().forEach(v -> v.getStudiesMap().forEach((s, vse) -> {
            assertThat(Arrays.asList(".", "./."), hasItem(vse.getSampleData("NA19600", "GT")));
        }));

        //Get all variants with 1|1 for na19600 and 0|0 or 1|0 for na19685
        query = new Query(GENOTYPE.key(), na19600 + IS + homAlt + AND + na19685 + IS + homRef + OR + het);
        queryResult = query(query, new QueryOptions());
        assertEquals(40, queryResult.getNumResults());
        Set<String> refHet = new HashSet<>();
        refHet.add(homRef);
        refHet.addAll(Arrays.asList(het.split(OR)));
        queryResult.getResults().forEach(v -> v.getStudiesMap().forEach((s, vse) -> {
            assertEquals(homAlt, vse.getSampleData("NA19600", "GT"));
            assertTrue(refHet.contains(vse.getSampleData("NA19685", "GT")));
        }));
    }

    @Test
    public void testGetAllVariants_negatedGenotypes() {
        Query query;

        DataResult<Variant> allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19600"), new QueryOptions());
        //Get all variants with not 1|1 for na19600
        query = new Query(GENOTYPE.key(), na19600 + IS + NOT + homAlt);
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19600", "GT", not(is(homAlt))))));

        //Get all variants with not 0/0 for na19600
        query = new Query(GENOTYPE.key(), na19600 + IS + NOT + homRef);
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19600", "GT", not(is(homRef))))));

        //Get all variants with not 0/0 or 0|1 for na19600
        query = new Query(GENOTYPE.key(), na19600 + IS + NOT + homRef + OR + NOT + het1);
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19600", "GT", allOf(not(is(homRef)), not(is(het1)))))));

        allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19600,NA19685"), new QueryOptions());
        //Get all variants with 1|1 for na19600 and 0|0 or 1|0 for na19685
        query = new Query(GENOTYPE.key(), na19600 + IS + homAlt + AND + na19685 + IS + NOT + homRef + OR + NOT + het2);
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, allOf(
                withSampleData("NA19600", "GT", is(homAlt)),
                withSampleData("NA19685", "GT", allOf(not(is(homRef)), not(is(het2))))))));

    }

    @Test
    public void testGetAllVariants_negatedGenotypesMixed() {
        Query query;

        query = new Query(GENOTYPE.key(), na19600 + IS + NOT + homRef + OR + het1)
                .append(INCLUDE_SAMPLE.key(), ALL);
        thrown.expect(VariantQueryException.class);
        queryResult = query(query, new QueryOptions());
//        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19600", "GT", is(het1)))));
    }

    @Test
    public void testGetAllVariants_samples() {
        Query query;

        DataResult<Variant> allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19600"), new QueryOptions());
        query = new Query(SAMPLE.key(), "NA19600");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19600", "GT", containsString("1")))));

        allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19685"), new QueryOptions());
        query = new Query(SAMPLE.key(), "NA19685");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19685", "GT", containsString("1")))));

        query = new Query(STUDY.key(), studyMetadata.getName()).append(SAMPLE.key(), "NA19685");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19685", "GT", containsString("1")))));

        allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19600,NA19685"), new QueryOptions());
        query = new Query(SAMPLE.key(), "NA19600,NA19685");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, anyOf(
                withSampleData("NA19600", "GT", containsString("1")),
                withSampleData("NA19685", "GT", containsString("1"))))));

        query = new Query(SAMPLE.key(), "NA19600;NA19685");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, allOf(
                withSampleData("NA19600", "GT", containsString("1")),
                withSampleData("NA19685", "GT", containsString("1"))))));
    }

    @Test
    public void testGetAllVariants_samples_gt() {
        Query query = new Query(SAMPLE.key(), "NA19600").append(GENOTYPE.key(), "NA19685" + IS + homRef).append(INCLUDE_SAMPLE.key(), ALL);
        thrown.expect(VariantQueryException.class);
        thrown.expectMessage("Unsupported combination of params \"genotype\", \"sample\".");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, allOf(
                withSampleData("NA19600", "GT", containsString("1")),
                withSampleData("NA19685", "GT", is(homRef))))));
    }

    @Test
    public void testGetAllVariants_genotypes_wrong_values() {
        Query query = new Query(GENOTYPE.key(), "WRONG_SAMPLE:1|1");
        thrown.expect(VariantQueryException.class);
        queryResult = query(query, new QueryOptions());
    }

    @Test
    public void testGetAllVariants_clinicalSignificance() {
        for (ClinicalSignificance clinicalSignificance : ClinicalSignificance.values()) {
            Query query = new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), clinicalSignificance);
            queryResult = query(query, new QueryOptions());
            System.out.println(clinicalSignificance + " --> " + queryResult.getNumResults());
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(withClinicalSignificance(hasItem(clinicalSignificance)))));

            if (clinicalSignificance != ClinicalSignificance.pathogenic) {
                query = new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), clinicalSignificance + OR + ClinicalSignificance.pathogenic);
                queryResult = query(query, new QueryOptions());
                System.out.println(query.toJson() + " --> " + queryResult.getNumResults());
                assertThat(queryResult, everyResult(allVariants,
                        hasAnnotation(
                                withClinicalSignificance(
                                        anyOf(
                                                hasItem(clinicalSignificance),
                                                hasItem(ClinicalSignificance.pathogenic)
                                        )
                                )
                        )
                ));

                query = new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), clinicalSignificance + AND + ClinicalSignificance.pathogenic);
                queryResult = query(query, new QueryOptions());
                System.out.println(query.toJson() + " --> " + queryResult.getNumResults());
                assertThat(queryResult, everyResult(allVariants,
                        hasAnnotation(
                                withClinicalSignificance(
                                        allOf(
                                                hasItem(clinicalSignificance),
                                                hasItem(ClinicalSignificance.pathogenic)
                                        )
                                )
                        )
                ));
            }
        }
    }

    @Test
    public void groupBy_gene_limit_0() throws Exception {
        DataResult queryResult = groupBy(new Query(), "gene", new QueryOptions("limit", 0).append("count", true));
        assertTrue(queryResult.getNumResults() > 0);
    }

    @Test
    public void groupBy_gene() throws Exception {
        int limit = 10;
        DataResult<Map<String, Object>> queryResult_count = groupBy(new Query(), "gene", new QueryOptions("limit", limit)
                .append("count", true));
        Map<String, Long> counts = queryResult_count.getResults().stream().collect(Collectors.toMap(o -> ((Map<String, Object>) o).get
                ("id").toString(), o -> Long.parseLong(((Map<String, Object>) o).get("count").toString())));
        DataResult<Map<String, Object>> queryResult_group = groupBy(new Query(), "gene", new QueryOptions("limit", limit));

//        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_group));
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_count));

        assertEquals(limit, queryResult_count.getNumResults());
        assertEquals(limit, queryResult_group.getNumResults());
        for (Map<String, Object> resultMap : queryResult_group.getResults()) {
            System.out.println("resultMap = " + resultMap);
            String id = resultMap.get("id").toString();
            assertTrue("Should contain key " + id, counts.containsKey(id));
            assertEquals("Size and count for id (" + id + ")are different", ((List) resultMap.get("values")).size(), counts.get(id)
                    .intValue());

            QueryOptions queryOptions = new QueryOptions("limit", 1).append("skipCount", false);
            DataResult<Variant> queryResult3 = query(new Query(GENE.key(), id), queryOptions);
            assertEquals("Count for ID " + id, counts.get(id).longValue(), queryResult3.getNumTotalResults());
            assertEquals(1, queryResult3.getNumResults());
        }
    }

    @Test
    public void rank_gene() throws Exception {
        DataResult<Map<String, Object>> queryResult_rank = rank(40, new Query(), "gene", false);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_rank));

        assertEquals(40, queryResult_rank.getNumResults());
        for (Map<String, Object> map : queryResult_rank.getResults()) {
            Long variantQueryResult = count(new Query(GENE.key(), map.get("id")));
            assertEquals((variantQueryResult).intValue(), ((Number) map.get("count")).intValue());
        }
    }

    @Test
    public void rank_ct() throws Exception {
        int limit = 20;
        DataResult<Map<String, Object>> queryResult_rank = rank(limit, new Query(), "ct", false);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_rank));

        assertEquals(limit, queryResult_rank.getNumResults());
        for (Map<String, Object> map : queryResult_rank.getResults()) {
            Long variantQueryResult = count(new Query(ANNOT_CONSEQUENCE_TYPE.key(), map.get("id")));
            assertEquals((variantQueryResult).intValue(), ((Number) map.get("count")).intValue());
        }
    }

    @Test
    public void testGetAllVariants_Freqs() throws Exception {
//        STATS_REF
//        STATS_ALT

        DataResult<Variant> queryResult;
        long numResults = 0;
        long expectedNumResults = 0;


        queryResult = query(new Query(STATS_ALT.key(), STUDY_NAME + ":" + StudyEntry.DEFAULT_COHORT + "<0.3"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withStats(StudyEntry.DEFAULT_COHORT, with("af",
                VariantStats::getAltAlleleFreq, lt(0.3))))));

        numResults += queryResult.getNumResults();
        numResults += query(new Query(STATS_REF.key(), STUDY_NAME + ":" + StudyEntry.DEFAULT_COHORT + "<0.3"), null).getNumResults();
        expectedNumResults = query(new Query(STATS_MAF.key(), STUDY_NAME + ":" + StudyEntry.DEFAULT_COHORT + "<0.3"), null).getNumResults();
        assertEquals(expectedNumResults, numResults);

    }

    @Test
    public void testGetAllVariants_maf() throws Exception {

        DataResult<Variant> queryResult;
        long numResults;
//        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), ">0.2")).first();
//        System.out.println("queryResult.getNumTotalResults() = " + numResults);

        queryResult = query(new Query(STATS_MAF.key(), STUDY_NAME + ":" + StudyEntry.DEFAULT_COHORT + ">0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withStats(StudyEntry.DEFAULT_COHORT, withMaf(gt(0.2))))));

        int expectedCount = (int) VariantMatchers.count(allVariants.getResults(), withStudy(STUDY_NAME, withStats("cohort1", withMaf(gt(0.2)))));
        numResults = count(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort1>0.2"));
        assertEquals(expectedCount, numResults);
        numResults = count(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort1>0.2"));
        assertEquals(expectedCount, numResults);
        queryResult = query(new Query(STATS_MAF.key(), "1:cohort1>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        queryResult = query(new Query(STUDY.key(), STUDY_NAME).append(STATS_MAF.key(), "cohort1>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withStats("cohort1", withMaf(gt(0.2))))));

        queryResult = query(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort2>0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withStats("cohort2", withMaf(gt(0.2))))));

        queryResult = query(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort2>0.2," + STUDY_NAME + ":cohort2<=0.2"), null);
        assertThat(queryResult, numResults(is(allVariants.getNumResults())));

        queryResult = query(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort2>0.2;" + STUDY_NAME + ":cohort2<=0.2"), null);
        assertThat(queryResult, numResults(is(0)));

        queryResult = query(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort2>0.2;" + STUDY_NAME + ":cohort1<0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, allOf(
                withStats("cohort2", withMaf(gt(0.2))),
                withStats("cohort1", withMaf(lt(0.2)))))));

        queryResult = query(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort2>0.2," + STUDY_NAME + ":cohort1<0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, anyOf(
                withStats("cohort2", withMaf(gt(0.2))),
                withStats("cohort1", withMaf(lt(0.2)))))));
    }

    @Test
    public void testGetAllVariants_maf_cohortNotFound() throws Exception {
        VariantQueryException exception = VariantQueryException.cohortNotFound("cohort3", studyMetadata.getId(), cohorts);
        thrown.expect(instanceOf(exception.getClass()));
        thrown.expectCause(is(exception.getCause()));
        query(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort3>0.2"), null);
    }

    @Test
    public void testGetAllVariants_mgf() throws Exception {
        queryResult = query(new Query(STATS_MGF.key(), STUDY_NAME + ":ALL>0.2"), null);
        System.out.println(queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withStats("ALL", withMgf(gt(0.2))))));

        queryResult = query(new Query(STATS_MGF.key(), STUDY_NAME + ":ALL<0.2"), null);
        System.out.println(queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withStats("ALL", withMgf(lt(0.2))))));
    }

    @Test
    public void testGetAllVariants_cohorts() throws Exception {

        queryResult = query(new Query(COHORT.key(), STUDY_NAME + ":cohort2"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = query(new Query(COHORT.key(), STUDY_NAME + ":cohort1"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = query(new Query(STUDY.key(), STUDY_NAME)
                .append(COHORT.key(), "cohort1"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = query(new Query(STUDY.key(), 1)
                .append(COHORT.key(), "cohort1"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = query(new Query(STUDY.key(), 1)
                .append(COHORT.key(), "!cohort1"), null);
        assertEquals(0, queryResult.getNumResults());

    }

    @Test
    public void testGetAllVariants_cohorts_fail1() throws Exception {
        VariantQueryException expected = VariantQueryException.cohortNotFound("cohort5_dont_exists", 1, cohorts);
        thrown.expect(expected.getClass());
        thrown.expectMessage(expected.getMessage());
        queryResult = query(new Query(STUDY.key(), 1)
                .append(COHORT.key(), "!cohort5_dont_exists"), null);
    }

    @Test
    public void testGetAllVariants_missingAllele() throws Exception {

        queryResult = query(new Query(MISSING_ALLELES.key(), STUDY_NAME + ":" + StudyEntry.DEFAULT_COHORT + ">4"), null);
        assertEquals(9, queryResult.getNumResults());
        queryResult.getResults().stream().map(variant -> variant.getStudiesMap().get(STUDY_NAME).getStats(StudyEntry.DEFAULT_COHORT))
                .forEach(vs -> assertTrue(vs.getMissingAlleleCount() > 4));

    }

    @Test
    public void testIncludeAll() {
        for (Variant variant : allVariants.getResults()) {
            assertThat(variant.getStudies(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getStats(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getFiles(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getSamples(), not(is(Collections.emptyList())));
            assertNotNull(variant.getAnnotation());
        }
    }

    @Test
    public void testExcludeChromosome() {

        queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "chromosome"));
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertNotNull(variant.getChromosome());
        }
    }

    @Test
    public void testExcludeStudies() {
        queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "studies"));
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testReturnNoneStudies() {
        queryResult = query(new Query(INCLUDE_STUDY.key(), VariantQueryUtils.NONE), new QueryOptions());
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testExcludeStats() {
        for (String exclude : Arrays.asList("studies.stats", "stats")) {
            queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
            for (Variant variant : queryResult.getResults()) {
                assertThat(variant.getStudies().get(0).getStats(), is(Collections.emptyList()));
            }
        }

    }

    @Test
    public void testExcludeFiles() {
        for (String exclude : Arrays.asList("studies.files", "files")) {
            queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
            for (Variant variant : queryResult.getResults()) {
                assertThat(variant.getStudies().get(0).getFiles(), is(Collections.emptyList()));
                assertThat(new HashSet<>(variant.getStudies().get(0).getSampleDataKeys()), is(FORMAT));
            }
        }
    }

    @Test
    public void testReturnNoneFiles() {
        queryResult = query(new Query(INCLUDE_FILE.key(), VariantQueryUtils.NONE), new QueryOptions());
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getStudies().get(0).getFiles(), is(Collections.emptyList()));
            assertThat(new HashSet<>(variant.getStudies().get(0).getSampleDataKeys()), is(FORMAT));
        }
    }

    @Test
    public void testExcludeSamples() {
        for (String exclude : Arrays.asList("studies.samplesData", "samplesData", "samples")) {
            queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
            for (Variant variant : queryResult.getResults()) {
                assertThat(variant.getStudies().get(0).getSamples(), is(Collections.emptyList()));
            }
        }
    }

    @Test
    public void testReturnNoneSamples() {
        queryResult = query(new Query(INCLUDE_SAMPLE.key(), VariantQueryUtils.NONE), new QueryOptions());
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getStudies().get(0).getSamples(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testSummary() {
        queryResult = query(new Query(), new QueryOptions(VariantField.SUMMARY, true).append(QueryOptions.LIMIT, 1000));
        System.out.println("queryResult = " + ((VariantQueryResult) queryResult).getSource());
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getStudies().get(0).getSamples(), is(Collections.emptyList()));
            assertThat(variant.getStudies().get(0).getFiles(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testExcludeAnnotation() {
        queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "annotation"));
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        VariantAnnotation defaultAnnotation = new VariantAnnotation();
        defaultAnnotation.setConsequenceTypes(Collections.emptyList());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getAnnotation(), anyOf(is((VariantAnnotation) null), is(defaultAnnotation)));
        }

    }

    @Test
    public void testExcludeAnnotationParts() {
        List<Variant> allVariants = query(new Query(), new QueryOptions(QueryOptions.SORT, true)).getResults();
        queryResult = query(new Query(), new QueryOptions(QueryOptions.SORT, true).append(QueryOptions.EXCLUDE, VariantField.ANNOTATION_XREFS));
        assertEquals(allVariants.size(), queryResult.getResults().size());

        List<Variant> result = queryResult.getResults();
        for (int i = 0; i < result.size(); i++) {
            Variant expectedVariant = allVariants.get(i);
            Variant variant = result.get(i);
            assertEquals(expectedVariant.toString(), variant.toString());

            assertNotNull(expectedVariant.getAnnotation());
            assertNotNull(variant.getAnnotation());
            VariantAnnotation expectedAnnotation = expectedVariant.getAnnotation();
            VariantAnnotation annotation = variant.getAnnotation();

            expectedAnnotation.setXrefs(null);
//            expectedAnnotation.setId(null);
            assertEquals("\n" + expectedAnnotation + "\n" + annotation, expectedAnnotation, annotation);
        }
    }

    @Test
    public void testInclude() {
        queryResult = query(new Query(), new QueryOptions(QueryOptions.INCLUDE, "studies"));
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getStudies(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getStats(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getFiles(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getSamples(), not(is(Collections.emptyList())));
            assertNull(variant.getAnnotation());
        }

        queryResult = query(new Query(), new QueryOptions(QueryOptions.INCLUDE, "annotation"));
        assertEquals(allVariants.getResults().size(), queryResult.getResults().size());
        for (Variant variant : queryResult.getResults()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
            assertNotNull(variant.getAnnotation());
        }

    }

    @Test
    public void testIncludeFormat() {
        Variant variant = query(new Query(INCLUDE_SAMPLE_DATA.key(), "GT"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GT", variant.getStudies().get(0).getSampleDataKeysAsString());

        variant = query(new Query(INCLUDE_SAMPLE_DATA.key(), "GL"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GL", variant.getStudies().get(0).getSampleDataKeysAsString());

        variant = query(new Query(INCLUDE_SAMPLE_DATA.key(), "GT,GL,DS"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GT:GL:DS", variant.getStudies().get(0).getSampleDataKeysAsString());

        variant = query(new Query(INCLUDE_SAMPLE_DATA.key(), "GT,XX,GL"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GT:XX:GL", variant.getStudies().get(0).getSampleDataKeysAsString());

        variant = query(new Query(INCLUDE_SAMPLE_DATA.key(), "GT,SAMPLE_ID"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        assertEquals("GT:SAMPLE_ID", variant.getStudies().get(0).getSampleDataKeysAsString());

        variant = query(new Query(INCLUDE_SAMPLE_DATA.key(), "all"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        assertEquals("GT:DS:GL", variant.getStudies().get(0).getSampleDataKeysAsString());

        variant = query(new Query(INCLUDE_SAMPLE_DATA.key(), "none"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        assertEquals("", variant.getStudies().get(0).getSampleDataKeysAsString());
    }

/*
    @Test
    public void testGetAllVariants() {
        QueryOptions options = new QueryOptions();
        options.put("id", "rs1137005,rs150535390");
        options.put("region", "1:13910417-13910417,1:165389129-165389129");
        options.put("gene", "RCC2,HRNR");
        options.put("mgf", "<=0.5");

        DataResult queryResult = vqb.getAllVariants(options);
        assertEquals(5, queryResult.getNumResults());
//        System.out.println(queryResult);
    }

    @Test
    public void testGetVariantById() {
        DataResult queryResult;

        // This test queries a single ID with no more options
        queryResult = vqb.getVariantById("rs1137005", null);
        Variant object = (Variant) queryResult.getResults().get(0);
        assertEquals(object.getStart(), 1650807);

        // This test adds a few other options. Options related with genomic coordinates must be
        // added as a logical OR while others as and logical AND.
        QueryOptions options = new QueryOptions("type", "SNV");
        options.put("id", "rs150535390");
        options.put("region", "1:13910417-13910417,1:165389129-165389129");
        options.put("gene", "RCC2,HRNR");
        options.put("mgf", "<=0.5");

        queryResult = vqb.getVariantById("rs1137005", options);
        assertEquals(5, queryResult.getNumResults());
//        System.out.println("queryResult = " + queryResult);
    }

    @Test
    public void testGetAllVariantsByRegion() {
        DataResult queryResult;

        // Basic queries
        queryResult = vqb.getAllVariantsByRegion(new Region("1:1000000-2000000"), null);
        assertEquals(3, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("1:10000000-20000000"), null);
        assertEquals(11, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("3:1-200000000"), null);
        assertEquals(50, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("X:1-200000000"), null);
        assertEquals(11, queryResult.getNumResults());

        // Exactly in the limits
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238441-7980390"), null);
        assertEquals(5, queryResult.getNumResults());

        // Just inside the limits
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238440-7980391"), null);
        assertEquals(5, queryResult.getNumResults());

        // Just outside the limits
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238441-7980389"), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238442-7980390"), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238442-7980389"), null);
        assertEquals(3, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariantFrequencyByRegion() {
        DataResult queryResult;

        QueryOptions options = new QueryOptions("interval", 100000);
        options.put("mgf", "<=0.5");
        // Basic queries
        queryResult = vqb.getVariantFrequencyByRegion(new Region("1:10000000-20000000"), options);
        System.out.println("queryResult = " + queryResult);
//        assertEquals(3, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariantsByRegionAndStudy() {
        DataResult queryResult;

        // Basic queries
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("1:1000000-2000000"), Arrays.asList(study.getStudyId()), null);
        System.out.println("queryResult = " + queryResult);
        assertEquals(3, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("1:10000000-20000000"), Arrays.asList(study.getStudyId()), null);
        assertEquals(11, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("3:1-200000000"), Arrays.asList(study.getStudyId()), null);
        assertEquals(50, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("X:1-200000000"), Arrays.asList(study.getStudyId()), null);
        assertEquals(11, queryResult.getNumResults());

        // Exactly in the limits
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238441-7980390"), Arrays.asList(study.getStudyId()), null);
        assertEquals(5, queryResult.getNumResults());

        // Just inside the limits
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238440-7980391"), Arrays.asList(study.getStudyId()), null);
        assertEquals(5, queryResult.getNumResults());

        // Just outside the limits
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238441-7980389"), Arrays.asList(study.getStudyId()), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238442-7980390"), Arrays.asList(study.getStudyId()), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238442-7980389"), Arrays.asList(study.getStudyId()), null);
        assertEquals(3, queryResult.getNumResults());

        // Non-existing study
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("1:1000000-2000000"), Arrays.asList("FalseStudy"), null);
        assertEquals(0, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariantsByGene() {
        DataResult queryResult;

        // Gene present in the dataset
        queryResult = vqb.getAllVariantsByGene("MIB2", null);
        assertNotEquals(0, queryResult.getNumResults());
        List<Variant> variantsInGene = queryResult.getResults();

        for (Variant v : variantsInGene) {
            assertEquals("1", v.getChromosome());
        }

        // Gene not present in the dataset
        queryResult = vqb.getAllVariantsByGene("NonExistingGene", null);
        assertEquals(0, queryResult.getNumResults());
    }

    @Test
    public void testGetMostAffectedGenes() {
//        DataResult queryResult = vqb.getMostAffectedGenes(10, null);

        DataResult queryResult = vqb.groupBy("gene", null);
        assertEquals(10, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResults().toArray()));

        System.out.println("queryResult = " + queryResult);

        List<DBObject> result = queryResult.getResults();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) >= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetLeastAffectedGenes() {
        DataResult queryResult = vqb.getLeastAffectedGenes(10, null);
        assertEquals(10, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResults().toArray()));

        List<DBObject> result = queryResult.getResults();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) <= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetTopConsequenceTypes() {
        DataResult queryResult = vqb.getTopConsequenceTypes(5, null);
        assertEquals(5, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResults().toArray()));

        List<DBObject> result = queryResult.getResults();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) >= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetBottomConsequenceTypes() {
        DataResult queryResult = vqb.getBottomConsequenceTypes(5, null);
        assertEquals(5, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResults().toArray()));

        List<DBObject> result = queryResult.getResults();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) <= ((int) object.get("count")));
        }
    }



//    @Test
//    public void testGetRecords() throws Exception {
//
//        Map<String, String> opts = new HashMap<>();
//        opts.put("studyId", "aaleman_-_XOidGTJMUq1Cr1J");
////        opts.put("region_list", "6:1-15021068");
////        opts.put("sampleGT_D801[]", "1/1,0/1");
////        opts.put("sampleGT_muestra_B[]", "0/1");
////        opts.put("conseq_type[]", "non_synonymous_codon,intron_variant");
////        opts.put("mend_error", "1");
////        opts.put("option_mend_error", ">=");
////        opts.put("maf", "0.1");
////        opts.put("option_maf", "<=");
//
//        MutableInt count = new MutableInt(-1);
//
//        DataResult<VariantInfo> records = ((VariantMongoDBAdaptor) vqb).getRecordsMongo(1, 0, 25, count, opts);
////
//        System.out.println(records.getResults().get(0).getSampleGenotypes());
//    }
//
//    @Test
//    public void testAnalysisInfo() throws Exception {
//
//        DataResult<VariantAnalysisInfo> res = ((VariantMongoDBAdaptor) vqb).getAnalysisInfo("aaleman_-_XOidGTJMUq1Cr1J");
//        VariantAnalysisInfo vi = res.getResults().get(0);
//
//        System.out.println("vi.getSamples() = " + vi.getSamples());
//        System.out.println("vi.getConsequenceTypes() = " + vi.getConsequenceTypes());
//        System.out.println("vi.getGlobalStats() = " + vi.getGlobalStats());
//
//
//    }
*/


}
