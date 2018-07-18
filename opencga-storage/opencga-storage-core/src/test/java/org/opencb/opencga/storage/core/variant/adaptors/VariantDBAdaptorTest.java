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
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsAnything;
import org.junit.*;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
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
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

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
    protected static StudyConfiguration studyConfiguration;
    protected VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected QueryResult<Variant> allVariants;
    private static Logger logger = LoggerFactory.getLogger(VariantDBAdaptorTest.class);
    private String homAlt;
    private String homRef;
    private String het;
    private String het1;
    private String het2;
    protected int fileId = 1;

    @BeforeClass
    public static void beforeClass() throws IOException {
        fileIndexed = false;
    }

    @Override
    @Before
    public void before() throws Exception {

        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        if (!fileIndexed) {
            studyConfiguration = newStudyConfiguration();
//            variantSource = new VariantSource(smallInputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
            clearDB(DB_NAME);
            ObjectMap params = new ObjectMap(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.FAMILY)
                    .append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                    .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "DS,GL")
                    .append(VariantAnnotationManager.VARIANT_ANNOTATOR_CLASSNAME, CellBaseRestVariantAnnotator.class.getName())
                    .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
            params.putAll(getOtherParams());
            FORMAT = new HashSet<>();
            if (!params.getBoolean(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(),
                    VariantStorageEngine.Options.EXCLUDE_GENOTYPES.defaultValue())) {
                FORMAT.add("GT");
            }
            FORMAT.addAll(params.getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key()));

            StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration, params);
            fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(Paths.get(etlResult.getTransformResult().getPath()).toUri());
            NUM_VARIANTS = getExpectedNumLoadedVariants(fileMetadata);
            fileIndexed = true;
            Integer indexedFileId = studyConfiguration.getIndexedFiles().iterator().next();


            //Calculate stats
            if (getOtherParams().getBoolean(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)) {
                QueryOptions options = new QueryOptions(VariantStorageEngine.Options.STUDY.key(), STUDY_NAME)
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
                    Long annotated = count(new Query(ANNOTATION_EXISTS.key(), true));
                    Long all = count(new Query());

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
                assertEquals(count(new Query(ANNOTATION_EXISTS.key(), true)), count(new Query()));
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
        return dbAdaptor.get(query, options);
    }

    public VariantDBIterator iterator(Query query, QueryOptions options) {
        return dbAdaptor.iterator(query, options);
    }

    public Long count(Query query) {
        return dbAdaptor.count(query).first();
    }

    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        return dbAdaptor.groupBy(query, field, options);
    }

    public QueryResult rank(int limit, Query query, String field, boolean asc) {
        return dbAdaptor.rank(query, field, limit, asc);
    }

    protected String getHetGT() {
        return "0|1,1|0";
    }

    protected String getHomRefGT() {
        return "0|0";
    }

    protected String getHomAltGT() {
        return "1|1";
    }

    protected ObjectMap getOtherParams() {
        return new ObjectMap();
    }

    @Test
    public void multiIterator() throws Exception {
        List<String> variantsToQuery = allVariants.getResult()
                .stream()
                .filter(v -> !v.isSymbolic())
                .map(Variant::toString)
                .limit(allVariants.getResult().size() / 2)
                .collect(Collectors.toList());

        VariantDBIterator iterator = dbAdaptor.iterator(variantsToQuery.iterator(), new Query(), new QueryOptions());

        QueryResult<Variant> queryResult = iterator.toQueryResult();
        assertEquals(variantsToQuery.size(), queryResult.getResult().size());
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
//        expected.getResult().forEach(v -> logger.info("expected variant: == " + v));
        for (int batchSize : new int[]{50, 100, 1000}) {
            List<Variant> variants = new ArrayList<>();
            Set<String> variantStr = new HashSet<>();
            for (int i = 0; i < numVariants / batchSize + 1; i++) {
                QueryResult<Variant> result = query(query, new QueryOptions(options)
                        .append(QueryOptions.LIMIT, batchSize)
                        .append(QueryOptions.SKIP, i * batchSize));
                logger.info("Got " + result.getNumResults() + " results");
                variants.addAll(result.getResult());
                for (Variant variant : result.getResult()) {
                    boolean repeated = !variantStr.add(variant.toString());
                    assertFalse("Repeated variant! : " + variant.toString(), repeated);
                }
            }
            assertEquals(numVariants, variants.size());
            assertEquals(numVariants, variantStr.size());
            assertEquals(expected.getResult().stream().map(Object::toString).collect(Collectors.toSet()), variantStr);
        }
    }

    @Test
    public void testGetVariantsByType() {
        Set<Variant> snv = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("SNV = " + snv.size());
        snv.forEach(variant -> assertThat(EnumSet.of(VariantType.SNV, VariantType.SNP), hasItem(variant.getType())));

        Set<Variant> not_snv = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), "!" + VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("!SNV = " + not_snv.size());
        not_snv.forEach(variant -> assertFalse(EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType())));

        Set<Variant> snv_snp = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.SNV + "," + VariantContext.Type.SNP), new QueryOptions()).getResult());
        System.out.println("SNV_SNP = " + snv_snp.size());
        assertEquals(snv_snp, snv);

        Set<Variant> snp = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.SNP), new QueryOptions()).getResult());
        snp.forEach(variant -> assertEquals(VariantType.SNP, variant.getType()));
        snp.forEach(variant -> assertThat(snv, hasItem(variant)));
        System.out.println("SNP = " + snp.size());

        Set<Variant> indels = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL), new QueryOptions()).getResult());
        indels.forEach(variant -> assertEquals(VariantType.INDEL, variant.getType()));
        System.out.println("INDEL = " + indels.size());

        Set<Variant> indels_snp = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL + "," + VariantType.SNP), new QueryOptions()).getResult());
        indels_snp.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP), hasItem(variant.getType())));
        indels_snp.forEach(variant -> assertTrue(indels.contains(variant) || snp.contains(variant)));
        System.out.println("INDEL_SNP = " + indels_snp.size());

        Set<Variant> indels_snv = new HashSet<>(query(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL + "," + VariantType.SNV), new QueryOptions()).getResult());
        indels_snv.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP, VariantType.SNV), hasItem(variant.getType())));
        indels_snv.forEach(variant -> assertTrue(indels.contains(variant) || snv.contains(variant)));
        System.out.println("INDEL_SNV = " + indels_snv.size());
    }

    @Test
    public void testGetAllVariants_populationFrequencyRef() {
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
        Query query;
        query = new Query()
                .append(ANNOT_POPULATION_REFERENCE_FREQUENCY.key(), GENOMES_PHASE_3 + ":AFR<=0.05001");
        queryResult = query(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopRefFreq(GENOMES_PHASE_3, "AFR", lte(0.05001)))));
    }

    @Test
    public void testGetAllVariants_populationFrequency() {
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
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
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
        Query baseQuery = new Query();

        Query query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), GENOMES_PHASE_3 + ":AFR<=0.0501");
        queryResult = query(query, options);
        filterPopulation(map -> (Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), ESP_6500 + ":AA>0.0501");
        queryResult = query(query, options);
        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && Math.min(map.get(ESP_6500 + ":AA").getRefAlleleFreq(),
                map.get(ESP_6500 + ":AA").getAltAlleleFreq()) > 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),GENOMES_PHASE_3 + ":AFR<=0.0501");
        queryResult = query(query, options);
        filterPopulation(map -> (Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),GENOMES_PHASE_3 + ":ALL<=0.0501");
        queryResult = query(query, options);
        filterPopulation(map -> (Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":ALL", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault(GENOMES_PHASE_3 + ":ALL", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), ESP_6500 + ":AA>0.0501;" + GENOMES_PHASE_3 + ":AFR<=0.0501");
        queryResult = query(query, options);
        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && Math.min(map.get(ESP_6500 + ":AA").getRefAlleleFreq(),
                map.get(ESP_6500 + ":AA").getAltAlleleFreq()) > 0.0501
                && Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), ESP_6500 + ":AA>0.0501," + GENOMES_PHASE_3 + ":AFR<=0.0501");
        queryResult = query(query, options);
        filterPopulation(map -> (map.containsKey(ESP_6500 + ":AA") && Math.min(map.get(ESP_6500 + ":AA").getRefAlleleFreq(),
                map.get(ESP_6500 + ":AA").getAltAlleleFreq()) > 0.0501
                || Math.min(map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault(GENOMES_PHASE_3 + ":AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

    }

    public long filterPopulation(Predicate<Map<String, PopulationFrequency>> predicate) {
        return filterPopulation(queryResult, v -> true, predicate);
    }

    public long filterPopulation(QueryResult<Variant> queryResult, Predicate<Variant> filterVariants, Predicate<Map<String, PopulationFrequency>> predicate) {
        queryResult.getResult().forEach(variant -> {
            assertNotNull(variant);
            assertNotNull("In " + variant, variant.getAnnotation());
//            assertNotNull("In " + variant, variant.getAnnotation().getPopulationFrequencies());
        });
        Set<String> expectedVariants = allVariants.getResult()
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
            Set<String> result = queryResult.getResult().stream().map(Variant::toString).collect(Collectors.toSet());
            if (!result.contains(variant)) {
                System.out.println("variant missing = " + variant);
            }
        }
        for (Variant variant : queryResult.getResult()) {
            if (!expectedVariants.contains(variant.toString())) {
                System.out.println("variant not suppose to be = " + variant);
            }
        }

        assertEquals(expectedVariants.size(), queryResult.getNumResults());
        long count = queryResult.getResult().stream()
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
        for (Variant variant : allVariants.getResult()) {
            if (i++ % 10 == 0) {
                if (!variant.isSymbolic()) {
                    variants.add(variant);
                }
            }
        }
        List<Variant> result = query(new Query(ID.key(), variants), new QueryOptions()).getResult();

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
        queryResult.getResult().forEach(v -> assertThat(v.getIds(), anyOf(hasItem("rs1137005"), hasItem("rs150535390"))));
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

    @Test
    public void testGetAllVariants_ct_gene() {
        queryGeneCT("BIRC6", "SO:0001566");  // Should return 0 results
        queryGeneCT("BIRC6", "SO:0001583");
        queryGeneCT("DNAJC6", "SO:0001819");
        queryGeneCT("SH2D5", "SO:0001632");
        queryGeneCT("ERMAP,SH2D5", "SO:0001632");

        queryGeneCT("ERMAP,SH2D5", "SO:0001632", new Query()
                        .append(ANNOT_XREF.key(), "ERMAP,SH2D5,7:100807230:G:T")
                        .append(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001632"),
                at("7:100807230:G:T"));

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

    @Test
    public void testGetAllVariants_transcriptionAnnotationFlags() {
        //ANNOT_TRANSCRIPTION_FLAGS
        Query query;
        Multiset<String> flags = HashMultiset.create();
        Set<String> flagsInVariant = new HashSet<>();
        for (Variant variant : allVariants.getResult()) {
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
            query = new Query(ANNOT_TRANSCRIPTION_FLAG.key(), flag);
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
        for (Variant variant : allVariants.getResult()) {
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
        QueryResult<Variant> result;

        query = new Query(ANNOT_GO.key(), "GO:XXXXXXX");
        result = variantStorageEngine.get(query, null);
        assertEquals(0, result.getNumResults());

        query = new Query(ANNOT_GO.key(), "GO:0006508");
        result = variantStorageEngine.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResult()) {
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
        for (Variant variant : result.getResult()) {
            System.out.println(variant);
        }
        assertNotEquals(0, result.getNumResults());
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));

        query = new Query(ANNOT_GO.key(), "GO:0000050");
        result = variantStorageEngine.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResult()) {
            System.out.println(variant);
        }
        genes = cellBaseUtils.getGenesByGo(query.getAsStringList(ANNOT_GO.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        assertNotEquals(0, result.getNumResults());
        totalResults += result.getNumResults();

        query = new Query(ANNOT_GO.key(), "GO:0006508,GO:0000050");
        result = variantStorageEngine.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResult()) {
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
        QueryResult<Variant> result = variantStorageEngine.get(query, null);
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
        for (Variant variant : allVariants.getResult()) {
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
        for (Variant variant : allVariants.getResult()) {
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
        }

        Query query = new Query(ANNOT_POLYPHEN.key(), "sift>0.5");
        thrown.expect(VariantQueryException.class);
        query(query, null);
//        for (Map.Entry<Double, Integer> entry : polyphen.entrySet()) {
//            query = new Query(VariantDBAdaptor.VariantQueryParams.SIFT.key(), entry.getKey());
//            queryResult = dbAdaptor.get(query, null);
//            assertEquals(entry.getKey(), entry.getValue(), queryResult.getNumResults());
//        }

    }

    @Test
    public void testGetAlVariants_polyphenSiftDescription() {
        for (String p : Arrays.asList("benign", "possibly damaging", "probably damaging", "unknown")) {
            queryResult = query(new Query(ANNOT_POLYPHEN.key(), p), null);
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasAnyPolyphenDesc(equalTo(p)))));
        }

        for (String s : Arrays.asList("deleterious", "tolerated")) {
            queryResult = query(new Query(ANNOT_SIFT.key(), s), null);
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
        QueryResult<Variant> result = query(query, null);
        long expected = countScore(allVariants, scorePredicate, mapper);
        long actual = countScore(result, scorePredicate, mapper);
        assertTrue("Expecting a query returning some value.", expected > 0);
        assertEquals(expected, result.getNumResults());
        assertEquals(expected, actual);
    }

    private long countConservationScore(String source, QueryResult<Variant> variantQueryResult, Predicate<Double> doublePredicate) {
        return countScore(source, variantQueryResult, doublePredicate, VariantAnnotation::getConservation);
    }

    private long countFunctionalScore(String source, QueryResult<Variant> variantQueryResult, Predicate<Double> doublePredicate) {
        return countScore(source, variantQueryResult, doublePredicate, VariantAnnotation::getFunctionalScore);
    }

    private long countScore(String source, QueryResult<Variant> variantQueryResult, Predicate<Double> doublePredicate, Function<VariantAnnotation, List<Score>> mapper) {
        return countScore(variantQueryResult, scores -> scores.stream().anyMatch(score -> score.getSource().equalsIgnoreCase(source) && doublePredicate.test(score.getScore())), mapper);
    }

    private long countScore(QueryResult<Variant> variantQueryResult, Predicate<List<Score>> predicate, Function<VariantAnnotation, List<Score>> mapper) {
        long c = 0;
        for (Variant variant : variantQueryResult.getResult()) {
            List<Score> list = mapper.apply(variant.getAnnotation());
            if (list != null) {
                if (predicate.test(list)) {
                    c++;
                }
            }
        }
        return c;
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
        for (Variant variant : queryResult.getResult()) {
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
        QueryResult<Variant> result = query(query, new QueryOptions());

        assertThat(result, everyResult(allVariants, hasAnnotation(hasGenes(Collections.singletonList("FLG-AS1")))));

        for (Variant variant : result.getResult()) {
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

        Query query = new Query(STUDY.key(), studyConfiguration.getStudyName());
        long numResults = count(query);
        assertEquals(allVariants.getNumResults(), numResults);

        query = new Query(STUDY.key(), studyConfiguration.getStudyId());
        numResults = count(query);
        assertEquals(allVariants.getNumResults(), numResults);

        query = new Query(STUDY.key(), NOT + studyConfiguration.getStudyId());
        numResults = count(query);
        assertEquals(0, numResults);

        query = new Query(STUDY.key(), NOT + studyConfiguration.getStudyName());
        numResults = count(query);
        assertEquals(0, numResults);

    }

    @Test
    public void testGetAllVariants_files() {

        Query query = new Query(FILE.key(), fileId);
        long numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query = new Query(FILE.key(), fileId).append(STUDY.key(), studyConfiguration.getStudyId());
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        query = new Query().append(STUDY.key(), studyConfiguration.getStudyId());
        numResults = count(query);
        assertEquals(NUM_VARIANTS, numResults);

        VariantQueryException e = VariantQueryException.missingStudyForFile("-1", Collections.singletonList(studyConfiguration.getStudyName()));
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        query = new Query(FILE.key(), -1);
        count(query);
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
        query = new Query(STUDY.key(), studyConfiguration.getStudyId()).append(FILTER.key(), "PASS");
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
        query = new Query(FILE.key(), fileId).append(STUDY.key(), studyConfiguration.getStudyId()).append(FILTER.key(), "PASS");
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
            samplesName = new ArrayList<>(StudyConfiguration.getSortedIndexedSamplesPosition(studyConfiguration).keySet());
        } else if (returnedSamples.equals(VariantQueryUtils.NONE)) {
            samplesName = Collections.emptyList();
        } else {
            samplesName = query.getAsStringList(VariantQueryParam.INCLUDE_SAMPLE.key());
        }
        Map<String, List<String>> expectedSamples = Collections.singletonMap(studyConfiguration.getStudyName(), samplesName);

        Iterator<Variant> it_1 = allVariants.getResult().iterator();
        Iterator<Variant> it_2 = queryResult.getResult().iterator();

        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());

        LinkedHashMap<String, Integer> samplesPosition1 = null;
        LinkedHashMap<String, Integer> samplesPosition2 = null;
        for (int i = 0; i < queryResult.getNumResults(); i++) {
            Variant variant1 = it_1.next();
            Variant variant2 = it_2.next();

            assertEquals(variant1.toString(), variant2.toString());
            assertEquals(expectedSamples, queryResult.getSamples());

            if (samplesPosition1 == null) {
                samplesPosition1 = variant1.getStudy(studyConfiguration.getStudyName()).getSamplesPosition();
            }
            if (samplesPosition2 == null) {
                samplesPosition2 = variant2.getStudy(studyConfiguration.getStudyName()).getSamplesPosition();
                assertEquals(samplesName, new ArrayList<>(samplesPosition2.keySet()));
            }
            assertSame(samplesPosition1, variant1.getStudy(studyConfiguration.getStudyName()).getSamplesPosition());
            assertSame(samplesPosition2, variant2.getStudy(studyConfiguration.getStudyName()).getSamplesPosition());
            for (String sampleName : samplesName) {
                String gt1 = variant1.getStudy(studyConfiguration.getStudyName()).getSampleData(sampleName, "GT");
                String gt2 = variant2.getStudy(studyConfiguration.getStudyName()).getSampleData(sampleName, "GT");
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
            assertEquals(studyConfiguration.getStudyName(), entry.getStudyId());
            assertEquals(studyConfiguration.getSampleIds().keySet(), entry.getSamplesName());
        }
        assertEquals(NUM_VARIANTS, numVariants);
    }

    @Test
    public void testGetAllVariants_genotypes() {
        Integer na19600 = studyConfiguration.getSampleIds().get("NA19600");
        Integer na19685 = studyConfiguration.getSampleIds().get("NA19685");

        Query query = new Query(GENOTYPE.key(), na19600 + IS + homAlt);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), STUDY_NAME + ":NA19600" + IS + homAlt);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), STUDY_NAME + ":NA19600" + IS + GenotypeClass.HOM_ALT);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), "NA19600" + IS + homAlt)
                .append(STUDY.key(), STUDY_NAME);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), "NA19600" + IS + homAlt);
        queryResult = query(query, new QueryOptions());
        assertEquals(282, queryResult.getNumResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals(homAlt, vse.getSampleData("NA19600", "GT")
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
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> {
            assertThat(Arrays.asList(".", "./."), hasItem(vse.getSampleData("NA19600", "GT")));
        }));

        //Get all variants with 1|1 for na19600 and 0|0 or 1|0 for na19685
        query = new Query(GENOTYPE.key(), na19600 + IS + homAlt + AND + na19685 + IS + homRef + OR + het);
        queryResult = query(query, new QueryOptions());
        assertEquals(40, queryResult.getNumResults());
        Set<String> refHet = new HashSet<>();
        refHet.add(homRef);
        refHet.addAll(Arrays.asList(het.split(OR)));
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> {
            assertEquals(homAlt, vse.getSampleData("NA19600", "GT"));
            assertTrue(refHet.contains(vse.getSampleData("NA19685", "GT")));
        }));
    }

    @Test
    public void testGetAllVariants_negatedGenotypes() {
        Query query;
        Integer na19600 = studyConfiguration.getSampleIds().get("NA19600");
        Integer na19685 = studyConfiguration.getSampleIds().get("NA19685");

        QueryResult<Variant> allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19600"), new QueryOptions());
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
        Integer na19600 = studyConfiguration.getSampleIds().get("NA19600");

        query = new Query(GENOTYPE.key(), na19600 + IS + NOT + homRef + OR + het1)
                .append(INCLUDE_SAMPLE.key(), ALL);
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19600", "GT", is(het1)))));
    }

    @Test
    public void testGetAllVariants_samples() {
        Query query;

        QueryResult<Variant> allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19600"), new QueryOptions());
        query = new Query(SAMPLE.key(), "NA19600");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19600", "GT", containsString("1")))));

        allVariants = query(new Query(INCLUDE_SAMPLE.key(), "NA19685"), new QueryOptions());
        query = new Query(SAMPLE.key(), "NA19685");
        queryResult = query(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData("NA19685", "GT", containsString("1")))));

        query = new Query(STUDY.key(), studyConfiguration.getStudyName()).append(SAMPLE.key(), "NA19685");
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
            if (ClinicalSignificance.uncertain_significance.equals(clinicalSignificance)) {
                continue;
            }
            Query query = new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), clinicalSignificance);
            queryResult = query(query, new QueryOptions());
            assertThat(queryResult, everyResult(allVariants, hasAnnotation(with("clinicalSignificance",
                    va -> va == null || va.getTraitAssociation() == null
                            ? Collections.emptyList()
                            : va.getTraitAssociation()
                            .stream()
                            .map(EvidenceEntry::getVariantClassification)
                            .filter(Objects::nonNull)
                            .map(VariantClassification::getClinicalSignificance)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList()),
                    hasItem(clinicalSignificance)))));
        }
    }

    @Test
    public void groupBy_gene_limit_0() throws Exception {
        QueryResult queryResult = groupBy(new Query(), "gene", new QueryOptions("limit", 0).append("count", true));
        assertTrue(queryResult.getNumResults() > 0);
    }

    @Test
    public void groupBy_gene() throws Exception {
        int limit = 10;
        QueryResult<Map<String, Object>> queryResult_count = groupBy(new Query(), "gene", new QueryOptions("limit", limit)
                .append("count", true));
        Map<String, Long> counts = queryResult_count.getResult().stream().collect(Collectors.toMap(o -> ((Map<String, Object>) o).get
                ("id").toString(), o -> Long.parseLong(((Map<String, Object>) o).get("count").toString())));
        QueryResult<Map<String, Object>> queryResult_group = groupBy(new Query(), "gene", new QueryOptions("limit", limit));

//        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_group));
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_count));

        assertEquals(limit, queryResult_count.getNumResults());
        assertEquals(limit, queryResult_group.getNumResults());
        for (Map<String, Object> resultMap : queryResult_group.getResult()) {
            System.out.println("resultMap = " + resultMap);
            String id = resultMap.get("id").toString();
            assertTrue("Should contain key " + id, counts.containsKey(id));
            assertEquals("Size and count for id (" + id + ")are different", ((List) resultMap.get("values")).size(), counts.get(id)
                    .intValue());

            QueryOptions queryOptions = new QueryOptions("limit", 1).append("skipCount", false);
            QueryResult<Variant> queryResult3 = query(new Query(GENE.key(), id), queryOptions);
            assertEquals("Count for ID " + id, counts.get(id).longValue(), queryResult3.getNumTotalResults());
            assertEquals(1, queryResult3.getNumResults());
        }
    }

    @Test
    public void rank_gene() throws Exception {
        QueryResult<Map<String, Object>> queryResult_rank = rank(40, new Query(), "gene", false);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_rank));

        assertEquals(40, queryResult_rank.getNumResults());
        for (Map<String, Object> map : queryResult_rank.getResult()) {
            Long variantQueryResult = count(new Query(GENE.key(), map.get("id")));
            assertEquals((variantQueryResult).intValue(), ((Number) map.get("count")).intValue());
        }
    }

    @Test
    public void rank_ct() throws Exception {
        int limit = 20;
        QueryResult<Map<String, Object>> queryResult_rank = rank(limit, new Query(), "ct", false);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_rank));

        assertEquals(limit, queryResult_rank.getNumResults());
        for (Map<String, Object> map : queryResult_rank.getResult()) {
            Long variantQueryResult = count(new Query(ANNOT_CONSEQUENCE_TYPE.key(), map.get("id")));
            assertEquals((variantQueryResult).intValue(), ((Number) map.get("count")).intValue());
        }
    }

    @Test
    public void testGetAllVariants_maf() throws Exception {

        QueryResult<Variant> queryResult;
        long numResults;
//        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), ">0.2")).first();
//        System.out.println("queryResult.getNumTotalResults() = " + numResults);

        queryResult = query(new Query(STATS_MAF.key(), STUDY_NAME + ":" + StudyEntry.DEFAULT_COHORT + ">0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withStats(StudyEntry.DEFAULT_COHORT, withMaf(gt(0.2))))));

        int expectedCount = (int) VariantMatchers.count(allVariants.getResult(), withStudy(STUDY_NAME, withStats("cohort1", withMaf(gt(0.2)))));
        numResults = count(new Query(STATS_MAF.key(), STUDY_NAME + ":cohort1>0.2"));
        assertEquals(expectedCount, numResults);
        numResults = count(new Query(STATS_MAF.key(), "1:10>0.2"));
        assertEquals(expectedCount, numResults);
        numResults = count(new Query(STATS_MAF.key(), STUDY_NAME + ":10>0.2"));
        assertEquals(expectedCount, numResults);
        queryResult = query(new Query(STATS_MAF.key(), "1:cohort1>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        queryResult = query(new Query(STUDY.key(), STUDY_NAME).append(STATS_MAF.key(), "cohort1>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        queryResult = query(new Query(STUDY.key(), STUDY_NAME).append(STATS_MAF.key(), "10>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        queryResult = query(new Query(STUDY.key(), 1).append(STATS_MAF.key(), "10>0.2"), null);
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
        VariantQueryException exception = VariantQueryException.cohortNotFound("cohort3", studyConfiguration.getStudyId(), studyConfiguration.getCohortIds().keySet());
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
                .append(COHORT.key(), 10), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = query(new Query(STUDY.key(), 1)
                .append(COHORT.key(), "!cohort1"), null);
        assertEquals(0, queryResult.getNumResults());

    }

    @Test
    public void testGetAllVariants_cohorts_fail1() throws Exception {
        VariantQueryException expected = VariantQueryException.cohortNotFound("cohort5_dont_exists", 1, studyConfiguration.getCohortIds().keySet());
        thrown.expect(expected.getClass());
        thrown.expectMessage(expected.getMessage());
        queryResult = query(new Query(STUDY.key(), 1)
                .append(COHORT.key(), "!cohort5_dont_exists"), null);
    }

    @Test
    public void testGetAllVariants_missingAllele() throws Exception {

        queryResult = query(new Query(MISSING_ALLELES.key(), STUDY_NAME + ":" + StudyEntry.DEFAULT_COHORT + ">4"), null);
        assertEquals(9, queryResult.getNumResults());
        queryResult.getResult().stream().map(variant -> variant.getStudiesMap().get(STUDY_NAME).getStats())
                .forEach(map -> assertTrue(map.get(StudyEntry.DEFAULT_COHORT).getMissingAlleles() > 4));

    }

    @Test
    public void testIncludeAll() {
        for (Variant variant : allVariants.getResult()) {
            assertThat(variant.getStudies(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getStats(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getFiles(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getSamplesData(), not(is(Collections.emptyList())));
            assertNotNull(variant.getAnnotation());
        }
    }

    @Test
    public void testExcludeChromosome() {

        queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "chromosome"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertNotNull(variant.getChromosome());
        }
    }

    @Test
    public void testExcludeStudies() {
        queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "studies"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testReturnNoneStudies() {
        queryResult = query(new Query(INCLUDE_STUDY.key(), VariantQueryUtils.NONE), new QueryOptions());
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testExcludeStats() {
        for (String exclude : Arrays.asList("studies.stats", "stats")) {
            queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
            for (Variant variant : queryResult.getResult()) {
                assertThat(variant.getStudies().get(0).getStats(), not(is(Collections.emptyList())));
            }
        }

    }

    @Test
    public void testExcludeFiles() {
        for (String exclude : Arrays.asList("studies.files", "files")) {
            queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
            for (Variant variant : queryResult.getResult()) {
                assertThat(variant.getStudies().get(0).getFiles(), is(Collections.emptyList()));
                assertThat(new HashSet<>(variant.getStudies().get(0).getFormat()), is(FORMAT));
            }
        }
    }

    @Test
    public void testReturnNoneFiles() {
        queryResult = query(new Query(INCLUDE_FILE.key(), VariantQueryUtils.NONE), new QueryOptions());
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies().get(0).getFiles(), is(Collections.emptyList()));
            assertThat(new HashSet<>(variant.getStudies().get(0).getFormat()), is(FORMAT));
        }
    }

    @Test
    public void testExcludeSamples() {
        for (String exclude : Arrays.asList("studies.samplesData", "samplesData", "samples")) {
            queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
            for (Variant variant : queryResult.getResult()) {
                assertThat(variant.getStudies().get(0).getSamplesData(), is(Collections.emptyList()));
            }
        }
    }

    @Test
    public void testReturnNoneSamples() {
        queryResult = query(new Query(INCLUDE_SAMPLE.key(), VariantQueryUtils.NONE), new QueryOptions());
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies().get(0).getSamplesData(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testExcludeAnnotation() {
        queryResult = query(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "annotation"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        VariantAnnotation defaultAnnotation = new VariantAnnotation();
        defaultAnnotation.setConsequenceTypes(Collections.emptyList());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getAnnotation(), anyOf(is((VariantAnnotation) null), is(defaultAnnotation)));
        }

    }

    @Test
    public void testExcludeAnnotationParts() {
        List<Variant> allVariants = query(new Query(), new QueryOptions(QueryOptions.SORT, true)).getResult();
        queryResult = query(new Query(), new QueryOptions(QueryOptions.SORT, true).append(QueryOptions.EXCLUDE, VariantField.ANNOTATION_XREFS));
        assertEquals(allVariants.size(), queryResult.getResult().size());

        List<Variant> result = queryResult.getResult();
        for (int i = 0; i < result.size(); i++) {
            Variant expectedVariant = allVariants.get(i);
            Variant variant = result.get(i);
            assertEquals(expectedVariant.toString(), variant.toString());

            assertNotNull(expectedVariant.getAnnotation());
            assertNotNull(variant.getAnnotation());
            VariantAnnotation expectedAnnotation = expectedVariant.getAnnotation();
            VariantAnnotation annotation = variant.getAnnotation();

            expectedAnnotation.setXrefs(null);
            expectedAnnotation.setId(null);
            assertEquals(expectedAnnotation, annotation);
        }
    }

    @Test
    public void testInclude() {
        queryResult = query(new Query(), new QueryOptions(QueryOptions.INCLUDE, "studies"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getStats(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getFiles(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getSamplesData(), not(is(Collections.emptyList())));
            assertNull(variant.getAnnotation());
        }

        queryResult = query(new Query(), new QueryOptions(QueryOptions.INCLUDE, "annotation"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
            assertNotNull(variant.getAnnotation());
        }

    }

    @Test
    public void testIncludeFormat() {
        Variant variant = query(new Query(INCLUDE_FORMAT.key(), "GT"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GT", variant.getStudies().get(0).getFormatAsString());

        variant = query(new Query(INCLUDE_FORMAT.key(), "GL"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GL", variant.getStudies().get(0).getFormatAsString());

        variant = query(new Query(INCLUDE_FORMAT.key(), "GT,GL,DS"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GT:GL:DS", variant.getStudies().get(0).getFormatAsString());

        variant = query(new Query(INCLUDE_FORMAT.key(), "GT,XX,GL"), new QueryOptions(QueryOptions.LIMIT, 1)).first();
        System.out.println("variant.toJson() = " + variant.toJson());
        assertEquals("GT:XX:GL", variant.getStudies().get(0).getFormatAsString());
    }

/*
    @Test
    public void testGetAllVariants() {
        QueryOptions options = new QueryOptions();
        options.put("id", "rs1137005,rs150535390");
        options.put("region", "1:13910417-13910417,1:165389129-165389129");
        options.put("gene", "RCC2,HRNR");
        options.put("mgf", "<=0.5");

        QueryResult queryResult = vqb.getAllVariants(options);
        assertEquals(5, queryResult.getNumResults());
//        System.out.println(queryResult);
    }

    @Test
    public void testGetVariantById() {
        QueryResult queryResult;

        // This test queries a single ID with no more options
        queryResult = vqb.getVariantById("rs1137005", null);
        Variant object = (Variant) queryResult.getResult().get(0);
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
        QueryResult queryResult;

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
        QueryResult queryResult;

        QueryOptions options = new QueryOptions("interval", 100000);
        options.put("mgf", "<=0.5");
        // Basic queries
        queryResult = vqb.getVariantFrequencyByRegion(new Region("1:10000000-20000000"), options);
        System.out.println("queryResult = " + queryResult);
//        assertEquals(3, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariantsByRegionAndStudy() {
        QueryResult queryResult;

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
        QueryResult queryResult;

        // Gene present in the dataset
        queryResult = vqb.getAllVariantsByGene("MIB2", null);
        assertNotEquals(0, queryResult.getNumResults());
        List<Variant> variantsInGene = queryResult.getResult();

        for (Variant v : variantsInGene) {
            assertEquals("1", v.getChromosome());
        }

        // Gene not present in the dataset
        queryResult = vqb.getAllVariantsByGene("NonExistingGene", null);
        assertEquals(0, queryResult.getNumResults());
    }

    @Test
    public void testGetMostAffectedGenes() {
//        QueryResult queryResult = vqb.getMostAffectedGenes(10, null);

        QueryResult queryResult = vqb.groupBy("gene", null);
        assertEquals(10, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        System.out.println("queryResult = " + queryResult);

        List<DBObject> result = queryResult.getResult();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) >= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetLeastAffectedGenes() {
        QueryResult queryResult = vqb.getLeastAffectedGenes(10, null);
        assertEquals(10, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        List<DBObject> result = queryResult.getResult();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) <= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetTopConsequenceTypes() {
        QueryResult queryResult = vqb.getTopConsequenceTypes(5, null);
        assertEquals(5, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        List<DBObject> result = queryResult.getResult();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) >= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetBottomConsequenceTypes() {
        QueryResult queryResult = vqb.getBottomConsequenceTypes(5, null);
        assertEquals(5, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        List<DBObject> result = queryResult.getResult();
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
//        QueryResult<VariantInfo> records = ((VariantMongoDBAdaptor) vqb).getRecordsMongo(1, 0, 25, count, opts);
////
//        System.out.println(records.getResult().get(0).getSampleGenotypes());
//    }
//
//    @Test
//    public void testAnalysisInfo() throws Exception {
//
//        QueryResult<VariantAnalysisInfo> res = ((VariantMongoDBAdaptor) vqb).getAnalysisInfo("aaleman_-_XOidGTJMUq1Cr1J");
//        VariantAnalysisInfo vi = res.getResult().get(0);
//
//        System.out.println("vi.getSamples() = " + vi.getSamples());
//        System.out.println("vi.getConsequenceTypes() = " + vi.getConsequenceTypes());
//        System.out.println("vi.getGlobalStats() = " + vi.getGlobalStats());
//
//
//    }
*/


}
