/*
 * Copyright 2015 OpenCB
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
import org.junit.*;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.annotation.CellBaseVariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;

/**
 * Tests that all the VariantDBAdaptor filters and methods work correctly.
 *
 * Do not check that all the values are loaded correctly
 * Do not check that variant annotation is correct
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantDBAdaptorTest extends VariantStorageManagerTestUtils {

    protected static int NUM_VARIANTS = 998;
    protected static boolean fileIndexed;
    protected static VariantSource source;
    protected static StudyConfiguration studyConfiguration;
    protected VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected QueryResult<Variant> allVariants;

    @BeforeClass
    public static void beforeClass() throws IOException {
        fileIndexed = false;
    }

    @Override
    @Before
    public void before() throws Exception {

        dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
        if (!fileIndexed) {
            studyConfiguration = newStudyConfiguration();
//            variantSource = new VariantSource(smallInputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
            clearDB(DB_NAME);
            ObjectMap params = new ObjectMap(VariantStorageManager.Options.STUDY_TYPE.key(), VariantStudy.StudyType.FAMILY)
                    .append(VariantStorageManager.Options.ANNOTATE.key(), true)
                    .append(VariantAnnotationManager.VARIANT_ANNOTATOR_CLASSNAME, CellBaseVariantAnnotator.class.getName())
                    .append(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "json")
                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
            params.putAll(getOtherParams());
            StorageETLResult etlResult = runDefaultETL(smallInputUri, getVariantStorageManager(), studyConfiguration, params);
            source = variantStorageManager.getVariantReaderUtils().readVariantSource(Paths.get(etlResult.getTransformResult().getPath()).toUri());
            NUM_VARIANTS = getExpectedNumLoadedVariants(source);
            fileIndexed = true;
            Integer indexedFileId = studyConfiguration.getIndexedFiles().iterator().next();

            VariantStatisticsManager vsm = new VariantStatisticsManager();

            QueryOptions options = new QueryOptions(VariantStorageManager.Options.STUDY_ID.key(), STUDY_ID)
                .append(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 100);
            Iterator<Integer> iterator = studyConfiguration.getSamplesInFiles().get(indexedFileId).iterator();

            /** Create cohorts **/
            HashSet<String> cohort1 = new HashSet<>();
            cohort1.add(studyConfiguration.getSampleIds().inverse().get(iterator.next()));
            cohort1.add(studyConfiguration.getSampleIds().inverse().get(iterator.next()));

            HashSet<String> cohort2 = new HashSet<>();
            cohort2.add(studyConfiguration.getSampleIds().inverse().get(iterator.next()));
            cohort2.add(studyConfiguration.getSampleIds().inverse().get(iterator.next()));

            Map<String, Set<String>> cohorts = new HashMap<>();
            Map<String, Integer> cohortIds = new HashMap<>();
            cohorts.put("cohort1", cohort1);
            cohorts.put("cohort2", cohort2);
            cohortIds.put("cohort1", 10);
            cohortIds.put("cohort2", 11);

            //Calculate stats
            if (getOtherParams().getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key(), true)) {
                URI stats = vsm.createStats(dbAdaptor, outputUri.resolve("cohort1.cohort2.stats"), cohorts, cohortIds, studyConfiguration,
                        options);
                vsm.loadStats(dbAdaptor, stats, studyConfiguration, options);
            }

            assertEquals(dbAdaptor.count(new Query(ANNOTATION_EXISTS.key(), true)).first(), dbAdaptor.count(new Query()).first());
        }
        allVariants = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.SORT, true));
        options = new QueryOptions();
    }

    @After
    public void after() throws IOException {
        dbAdaptor.close();
    }

    protected ObjectMap getOtherParams() {
        return new ObjectMap();
    }

    @Test
    public void testGetAllVariants() {
        long numResults = dbAdaptor.count(null).first();
        assertEquals(NUM_VARIANTS, numResults);
    }

    @Test
    public void testGetVariantsByType() {
        Set<Variant> snv = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("SNV = " + snv.size());
        snv.forEach(variant -> assertThat(EnumSet.of(VariantType.SNV, VariantType.SNP), hasItem(variant.getType())));

        Set<Variant> not_snv = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), "!" + VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("!SNV = " + not_snv.size());
        not_snv.forEach(variant -> assertFalse(EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType())));

        Set<Variant> snv_snp = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.SNV + "," + VariantContext.Type.SNP), new QueryOptions()).getResult());
        System.out.println("SNV_SNP = " + snv_snp.size());
        assertEquals(snv_snp, snv);

        Set<Variant> snp = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.SNP), new QueryOptions()).getResult());
        snp.forEach(variant -> assertEquals(VariantType.SNP, variant.getType()));
        snp.forEach(variant -> assertThat(snv, hasItem(variant)));
        System.out.println("SNP = " + snp.size());

        Set<Variant> indels = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.INDEL), new QueryOptions()).getResult());
        indels.forEach(variant -> assertEquals(VariantType.INDEL, variant.getType()));
        System.out.println("INDEL = " + indels.size());

        Set<Variant> indels_snp = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.INDEL + "," + VariantType.SNP), new QueryOptions()).getResult());
        indels_snp.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP), hasItem(variant.getType())));
        indels_snp.forEach(variant -> assertTrue(indels.contains(variant) || snp.contains(variant)));
        System.out.println("INDEL_SNP = " + indels_snp.size());

        Set<Variant> indels_snv = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.INDEL + "," + VariantType.SNV), new QueryOptions()).getResult());
        indels_snv.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP, VariantType.SNV), hasItem(variant.getType())));
        indels_snv.forEach(variant -> assertTrue(indels.contains(variant) || snv.contains(variant)));
        System.out.println("INDEL_SNV = " + indels_snv.size());
    }

    @Test
    public void testGetAllVariants_populationFrequencyRef() {
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
        Query query;
        query = new Query()
                .append(ANNOT_POPULATION_REFERENCE_FREQUENCY.key(), "1000GENOMES_phase_3:AFR<=0.05001");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopRefFreq("1000GENOMES_phase_3", "AFR", lte(0.05001)))));
    }

    @Test
    public void testGetAllVariants_populationFrequency() {
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
        Query query;

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:AA>0.05001");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopAltFreq("ESP_6500", "AA", gt(0.05001)))));

//        filterPopulation(map -> (map.containsKey("ESP_6500:AA") && map.get("ESP_6500:AA").getAltAlleleFreq() > 0.05001), filter);

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000GENOMES_phase_3:AFR<=0.05001");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasPopAltFreq("1000GENOMES_phase_3", "AFR", lte(0.05001)))));

//        filterPopulation(map -> (!map.containsKey("1000GENOMES_phase_3:AFR") || map.get("1000GENOMES_phase_3:AFR").getAltAlleleFreq() <= 0.05001), filter);

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:AA>0.05001;1000GENOMES_phase_3:AFR<=0.05001");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(allOf(
                        hasPopAltFreq("ESP_6500", "AA", gt(0.05001)),
                        hasPopAltFreq("1000GENOMES_phase_3", "AFR", lte(0.05001))))));

//        filterPopulation(map -> (map.containsKey("ESP_6500:AA") && map.get("ESP_6500:AA").getAltAlleleFreq() > 0.05001
//                        && (!map.containsKey("1000GENOMES_phase_3:AFR") || map.get("1000GENOMES_phase_3:AFR").getAltAlleleFreq() <= 0.05001)), filter);

        query = new Query()
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:AA>0.05001,1000GENOMES_phase_3:AFR<=0.05001");
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants, hasAnnotation(anyOf(
                hasPopAltFreq("ESP_6500", "AA", gt(0.05001)),
                hasPopAltFreq("1000GENOMES_phase_3", "AFR", lte(0.05001))))));

//        filterPopulation(map -> (map.containsKey("ESP_6500:AA") && map.get("ESP_6500:AA").getAltAlleleFreq() > 0.05001
//                        || (!map.containsKey("1000GENOMES_phase_3:AFR") || map.get("1000GENOMES_phase_3:AFR").getAltAlleleFreq() <= 0.05001)), filter);

    }

    @Test
    public void testGetAllVariants_population_maf() {
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
        Query baseQuery = new Query();

        Query query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1000GENOMES_phase_3:AFR<=0.0501");
        queryResult = dbAdaptor.get(query, options);
        filterPopulation(map -> (Math.min(map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"ESP_6500:AA>0.0501");
        queryResult = dbAdaptor.get(query, options);
        filterPopulation(map -> (map.containsKey("ESP_6500:AA") && Math.min(map.get("ESP_6500:AA").getRefAlleleFreq(),
                map.get("ESP_6500:AA").getAltAlleleFreq()) > 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"1000GENOMES_phase_3:AFR<=0.0501");
        queryResult = dbAdaptor.get(query, options);
        filterPopulation(map -> (Math.min(map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"ESP_6500:AA>0.0501;1000GENOMES_phase_3:AFR<=0.0501");
        queryResult = dbAdaptor.get(query, options);
        filterPopulation(map -> (map.containsKey("ESP_6500:AA") && Math.min(map.get("ESP_6500:AA").getRefAlleleFreq(),
                map.get("ESP_6500:AA").getAltAlleleFreq()) > 0.0501
                && Math.min(map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

        query = new Query(baseQuery)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"ESP_6500:AA>0.0501,1000GENOMES_phase_3:AFR<=0.0501");
        queryResult = dbAdaptor.get(query, options);
        filterPopulation(map -> (map.containsKey("ESP_6500:AA") && Math.min(map.get("ESP_6500:AA").getRefAlleleFreq(),
                map.get("ESP_6500:AA").getAltAlleleFreq()) > 0.0501
                || Math.min(map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_3:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.0501));

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
                variants.add(variant);
            }
        }
        List<Variant> result = dbAdaptor.get(new Query(ID.key(), variants), new QueryOptions()).getResult();

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
        testGetAllVariants_rs(ANNOT_XREF.key());
    }

    public void testGetAllVariants_rs(String key) {
        // This test queries a single ID with no more options
        Query query = new Query(key, "rs1137005");
        queryResult = dbAdaptor.get(query, null);
        Variant variant = queryResult.first();
        assertEquals(1, queryResult.getNumResults());
        assertEquals(variant.getStart(), Integer.valueOf(1650807));
        assertThat(variant.getIds(), hasItem("rs1137005"));

        query = new Query(key, "rs1137005,rs150535390");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(2, queryResult.getNumResults());
        queryResult.getResult().forEach(v -> assertThat(v.getIds(), anyOf(hasItem("rs1137005"), hasItem("rs150535390"))));
    }

    @Test
    public void testGetAllVariants_ct() {
        Query query;

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566");
        queryResult = dbAdaptor.get(query, null);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(hasItem("SO:0001566")))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(911, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "1566");
        queryResult = dbAdaptor.get(query, null);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(hasItem("SO:0001566")))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(911, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566,SO:0001583");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(anyOf(hasItem("SO:0001566"), hasItem("SO:0001583"))))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(947, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "1566,SO:0001583");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(anyOf(hasItem("SO:0001566"), hasItem("SO:0001583"))))));
        assertThat(queryResult, numResults(gt(0)));
//        assertEquals(947, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566;SO:0001583");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, hasAnnotation(hasSO(allOf(hasItem("SO:0001566"), hasItem("SO:0001583"))))));
        assertThat(queryResult, numResults(gt(0)));

//        assertEquals(396, queryResult.getNumResults());
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
            query = new Query(ANNOT_TRANSCRIPTION_FLAGS.key(), flag);
            queryResult = dbAdaptor.get(query, null);
            assertEquals(flags.count(flag), queryResult.getNumResults());
        }

    }

    @Test
    public void testGetAllVariants_geneTraits() {
        //ANNOT_GENE_TRAITS_ID
        //ANNOT_GENE_TRAITS_NAME
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
//        for (Map.Entry<String, Integer> entry : namesMap.entrySet()) {
//            query = new Query(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_NAME.key(), "~="+entry.getKey());
//            queryResult = dbAdaptor.get(query, null);
//            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
//        }

        int i = 0;
        for (Map.Entry<String, Integer> entry : idsMap.entrySet()) {
            query = new Query(ANNOT_GENE_TRAITS_ID.key(), entry.getKey());
            queryResult = dbAdaptor.get(query, null);
            assertEquals(entry.getValue().intValue(), queryResult.getNumResults());
            if (i++ == 400) {
                break;
            }
        }

        i = 0;
        for (Map.Entry<String, Integer> entry : hposMap.entrySet()) {
            query = new Query(ANNOT_HPO.key(), entry.getKey());
            queryResult = dbAdaptor.get(query, null);
            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
            if (i++ == 400) {
                break;
            }
        }
    }

    @Test
    public void testGoQuery() {

        // MMP26 -> GO:0004222,GO:0005578,GO:0006508
        // CEBPA -> GO:0000050

        int totalResults = 0;
        Collection<String> genes;
        Query query;
        QueryResult<Variant> result;

        query = new Query(ANNOT_GO.key(), "GO:XXXXXXX");
        result = dbAdaptor.get(query, null);
        assertEquals(0, result.getNumResults());

        query = new Query(ANNOT_GO.key(), "GO:0006508");
        result = dbAdaptor.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResult()) {
            System.out.println(variant);
        }
        assertNotEquals(0, result.getNumResults());
        genes = dbAdaptor.getDBAdaptorUtils().getGenesByGo(query.getAsStringList(ANNOT_GO.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        totalResults = result.getNumResults();

        query = new Query(ANNOT_GO.key(), "GO:0000050");
        result = dbAdaptor.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResult()) {
            System.out.println(variant);
        }
        genes = dbAdaptor.getDBAdaptorUtils().getGenesByGo(query.getAsStringList(ANNOT_GO.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        assertNotEquals(0, result.getNumResults());
        totalResults += result.getNumResults();

        query = new Query(ANNOT_GO.key(), "GO:0006508,GO:0000050");
        result = dbAdaptor.get(query, null);
        System.out.println("numResults: " + result.getNumResults());
        for (Variant variant : result.getResult()) {
            System.out.println(variant);
        }
        genes = dbAdaptor.getDBAdaptorUtils().getGenesByGo(query.getAsStringList(ANNOT_GO.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));
        assertNotEquals(0, result.getNumResults());
        assertEquals(result.getNumResults(), totalResults);
    }


    @Test
    public void testExpressionQuery() {
        Collection<String> genes;
        Query query = new Query(ANNOT_EXPRESSION.key(), "non_existing_tissue");
        QueryResult<Variant> result = dbAdaptor.get(query, null);
        assertEquals(0, result.getNumResults());


        query = new Query(ANNOT_EXPRESSION.key(), "skin");
        result = dbAdaptor.get(query, null);
        System.out.println("result.getNumResults() = " + result.getNumResults());
        assertNotEquals(0, result.getNumResults());
        assertNotEquals(allVariants.getNumResults(), result.getNumResults());
        genes = dbAdaptor.getDBAdaptorUtils().getGenesByExpression(query.getAsStringList(ANNOT_EXPRESSION.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));

        query = new Query(ANNOT_EXPRESSION.key(), "brain");
        result = dbAdaptor.get(query, null);
        System.out.println("result.getNumResults() = " + result.getNumResults());
        assertNotEquals(0, result.getNumResults());
        assertNotEquals(allVariants.getNumResults(), result.getNumResults());
        genes = dbAdaptor.getDBAdaptorUtils().getGenesByExpression(query.getAsStringList(ANNOT_EXPRESSION.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));

        query = new Query(ANNOT_EXPRESSION.key(), "tongue");
        result = dbAdaptor.get(query, null);
        System.out.println("result.getNumResults() = " + result.getNumResults());
        assertNotEquals(0, result.getNumResults());
        assertNotEquals(allVariants.getNumResults(), result.getNumResults());
        genes = dbAdaptor.getDBAdaptorUtils().getGenesByExpression(query.getAsStringList(ANNOT_EXPRESSION.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));

        query = new Query(ANNOT_EXPRESSION.key(), "pancreas");
        result = dbAdaptor.get(query, null);
        System.out.println("result.getNumResults() = " + result.getNumResults());
        assertNotEquals(0, result.getNumResults());
        assertNotEquals(allVariants.getNumResults(), result.getNumResults());
        genes = dbAdaptor.getDBAdaptorUtils().getGenesByExpression(query.getAsStringList(ANNOT_EXPRESSION.key()));
        assertThat(result, everyResult(hasAnnotation(hasAnyGeneOf(genes))));

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

        query = new Query(ANNOT_PROTEIN_KEYWORDS.key(), "Complete proteome,Transmembrane helix");
        assertEquals(combinedKeywordsOr, dbAdaptor.count(query).first().intValue());
        query = new Query(ANNOT_PROTEIN_KEYWORDS.key(), "Complete proteome;Transmembrane helix");
        assertEquals(combinedKeywordsAnd, dbAdaptor.count(query).first().intValue());
        query = new Query(ANNOT_PROTEIN_KEYWORDS.key(), "Complete proteome;!Transmembrane helix");
        assertEquals(combinedKeywordsAndNot, dbAdaptor.count(query).first().intValue());

        for (Map.Entry<String, Integer> entry : keywords.entrySet()) {
            System.out.println(entry);
            query = new Query(ANNOT_PROTEIN_KEYWORDS.key(), entry.getKey());
            queryResult = dbAdaptor.get(query, null);
            assertEquals(entry.getValue().intValue(), queryResult.getNumResults());
        }

    }

    @Test
    public void testGetAllVariants_drugs() {
        //ANNOT_DRUG
        Query query;
        Map<String, Integer> drugs = new HashMap<>();
        for (Variant variant : allVariants.getResult()) {
            Set<String> drugsInVariant = new HashSet<>();
            for (GeneDrugInteraction drugInteraction : variant.getAnnotation().getGeneDrugInteraction()) {
                drugsInVariant.add(drugInteraction.getDrugName());
            }
            for (String flag : drugsInVariant) {
                drugs.put(flag, drugs.getOrDefault(flag, 0) + 1);
            }
        }

        for (Map.Entry<String, Integer> entry : drugs.entrySet()) {
            if (entry.getKey().contains(",")) {
                continue;
            }
            query = new Query(ANNOT_DRUG.key(), entry.getKey());
            queryResult = dbAdaptor.get(query, null);
            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
        }

    }

    @Test
    public void testGetAllVariants_polyphenSift() {
        //POLYPHEN
        //SIFT
        Query query;
        Map<Double, Integer> sift = new HashMap<>();
        Map<String, Integer> siftDesc = new HashMap<>();
        Map<Double, Integer> polyphen = new HashMap<>();
        Map<Double, Integer> maxPolyphen = new HashMap<>();
        Map<String, Integer> polyphenDesc = new HashMap<>();
        for (Variant variant : allVariants.getResult()) {
            Set<Double> siftInVariant = new HashSet<>();
            Set<Double> polyphenInVariant = new HashSet<>();
            Set<String> siftDescInVariant = new HashSet<>();
            Set<String> polyphenDescInVariant = new HashSet<>();
            if (variant.getAnnotation().getConsequenceTypes() != null) {
                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                    if (consequenceType.getProteinVariantAnnotation() != null) {
                        if (consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                            for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                                if (score.getSource().equals("sift")) {
                                    siftInVariant.add(score.getScore());
                                    siftDescInVariant.add(score.getDescription());
                                } else if (score.getSource().equals("polyphen")) {
                                    polyphenInVariant.add(score.getScore());
                                    polyphenDescInVariant.add(score.getDescription());
                                }
                            }
                        }
                    }
                }
            }
            for (Double value : siftInVariant) {
                sift.put(value, sift.getOrDefault(value, 0) + 1);
            }
            for (String value : siftDescInVariant) {
                siftDesc.put(value, siftDesc.getOrDefault(value, 0) + 1);
            }
            for (Double value : polyphenInVariant) {
                polyphen.put(value, polyphen.getOrDefault(value, 0) + 1);
            }
            Optional<Double> max = polyphenInVariant.stream().max(Double::compareTo);
            if (max.isPresent()) {
                maxPolyphen.put(max.get(), maxPolyphen.getOrDefault(max.get(), 0) + 1);
            }
            for (String value : polyphenDescInVariant) {
                polyphenDesc.put(value, polyphenDesc.getOrDefault(value, 0) + 1);
            }
        }

        for (Map.Entry<String, Integer> entry : siftDesc.entrySet()) {
            query = new Query(ANNOT_SIFT.key(), entry.getKey());
            queryResult = dbAdaptor.get(query, null);
            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
            System.out.println("queryResult.getDbTime() = " + queryResult.getDbTime());
        }
        for (Map.Entry<String, Integer> entry : polyphenDesc.entrySet()) {
            query = new Query(ANNOT_POLYPHEN.key(), entry.getKey());
            queryResult = dbAdaptor.get(query, null);
            assertEquals(entry.getKey(), entry.getValue().intValue(), queryResult.getNumResults());
            System.out.println("queryResult.getDbTime() = " + queryResult.getDbTime());
        }
        query = new Query(ANNOT_POLYPHEN.key(), ">0.5");
        queryResult = dbAdaptor.get(query, null);
        Integer expected = maxPolyphen.entrySet()
                .stream()
                .filter(entry -> entry.getKey() > 0.5)
                .map(Map.Entry::getValue)
                .reduce((i, j) -> i + j).orElse(0);
        assertEquals(expected.intValue(), queryResult.getNumResults());


        query = new Query(ANNOT_POLYPHEN.key(), "sift>0.5");
        thrown.expect(VariantQueryException.class);
        dbAdaptor.get(query, null);
//        for (Map.Entry<Double, Integer> entry : polyphen.entrySet()) {
//            query = new Query(VariantDBAdaptor.VariantQueryParams.SIFT.key(), entry.getKey());
//            queryResult = dbAdaptor.get(query, null);
//            assertEquals(entry.getKey(), entry.getValue(), queryResult.getNumResults());
//        }

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
    public void testGetAllVariants_conservationScore() {
        //ANNOT_CONSERVATION

        assertTrue(countConservationScore("phastCons", allVariants, s -> s > 0.5) > 0);
        System.out.println("countFunctionalScore(\"phastCons\", allVariants, s -> s > 0.5) = " + countConservationScore("phastCons", allVariants, s -> s > 0.5));

        checkConservationScore(new Query(ANNOT_CONSERVATION.key(), "phylop>0.5"), s -> s > 0.5, "phylop");

        checkConservationScore(new Query(ANNOT_CONSERVATION.key(), "phastCons<0.5"), s1 -> s1 < 0.5, "phastCons");

        checkConservationScore(new Query(ANNOT_CONSERVATION.key(), "gerp<=0.5"), s -> s <= 0.5, "gerp");
    }

    public void checkConservationScore(Query query, Predicate<Double> doublePredicate, String source) {
        checkScore(query, doublePredicate, source, VariantAnnotation::getConservation);
    }

    public void checkFunctionalScore(Query query, Predicate<Double> doublePredicate, String source) {
        checkScore(query, doublePredicate, source, VariantAnnotation::getFunctionalScore);
    }

    public void checkScore(Query query, Predicate<Double> doublePredicate, String source, Function<VariantAnnotation, List<Score>> mapper) {
        QueryResult<Variant> result = dbAdaptor.get(query, null);
        long expected = countScore(source, allVariants, doublePredicate, mapper);
        long actual = countScore(source, result, doublePredicate, mapper);
        assertTrue(expected > 0);
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
        long c = 0;
        for (Variant variant : variantQueryResult.getResult()) {
            List<Score> list = mapper.apply(variant.getAnnotation());
            if (list != null) {
                for (Score score : list) {
                    if (score.getSource().equalsIgnoreCase(source)) {
                        if (doublePredicate.test(score.getScore())) {
                            c++;
                        }
                    }
                }
            }
        }
        return c;
    }

    @Test
    public void testGetSortedVariantsDefault() {
        QueryOptions options = new QueryOptions(QueryOptions.SORT, true);
        VariantDBIterator iterator = dbAdaptor.iterator(null, options);
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
        VariantDBIterator iterator = dbAdaptor.iterator(null, options);
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
        VariantDBIterator iterator = dbAdaptor.iterator(null, options);
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
        queryResult = dbAdaptor.get(query, options);
        assertEquals(2, queryResult.getNumResults());

        query = new Query(REGION.key(), Arrays.asList("1:13910417-13910417", "1:165389129-165389129"));
        queryResult = dbAdaptor.get(query, options);
        assertEquals(2, queryResult.getNumResults());

        query = new Query(REGION.key(),
                Arrays.asList(Region.parseRegion("1:13910417-13910417"), Region.parseRegion("1:165389129-165389129")));
        queryResult = dbAdaptor.get(query, options);
        assertEquals(2, queryResult.getNumResults());

        query = new Query(REGION.key(), "1:14000000-160000000");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, overlaps(new Region("1:14000000-160000000"))));

        query = new Query(CHROMOSOME.key(), "1");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, overlaps(new Region("1"))));

        query = new Query(REGION.key(), "1");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, overlaps(new Region("1"))));

        options.put("sort", true);
        query = new Query(REGION.key(), "1:14000000-160000000");
        queryResult = dbAdaptor.get(query, options);
        assertThat(queryResult, everyResult(allVariants, overlaps(new Region("1:14000000-160000000"))));

        int lastStart = 0;
        for (Variant variant : queryResult.getResult()) {
            assertEquals("1", variant.getChromosome());
            assertTrue(lastStart <= variant.getStart());
            lastStart = variant.getStart();
        }
    }

    @Test
    public void testGetAllVariants_studies() {

        Query query = new Query(STUDIES.key(), studyConfiguration.getStudyName());
        long numResults = dbAdaptor.count(query).first();
        assertEquals(allVariants.getNumResults(), numResults);

        query = new Query(STUDIES.key(), studyConfiguration.getStudyId());
        numResults = dbAdaptor.count(query).first();
        assertEquals(allVariants.getNumResults(), numResults);

        query = new Query(STUDIES.key(), "!" + studyConfiguration.getStudyId());
        numResults = dbAdaptor.count(query).first();
        assertEquals(0, numResults);

        query = new Query(STUDIES.key(), "!" + studyConfiguration.getStudyName());
        numResults = dbAdaptor.count(query).first();
        assertEquals(0, numResults);

    }

    @Test
    public void testGetAllVariants_files() {

        Query query = new Query(FILES.key(), 6);
        long numResults = dbAdaptor.count(query).first();
        assertEquals(NUM_VARIANTS, numResults);

        query = new Query(FILES.key(), -1);
        numResults = dbAdaptor.count(query).first();
        assertEquals("There is no file with ID -1", 0, numResults);
    }

    @Test
    public void testGetAllVariants_returned_samples() {

//        queryResult = dbAdaptor.get(query, options);
        List<Variant> variants = allVariants.getResult();

        checkSamplesData("NA19600", variants);
        checkSamplesData("NA19660", variants);
        checkSamplesData("NA19661", variants);
        checkSamplesData("NA19685", variants);
        checkSamplesData("NA19600,NA19685", variants);
        checkSamplesData("NA19685,NA19600", variants);
        checkSamplesData("NA19660,NA19661,NA19600", variants);
        checkSamplesData("", variants);
    }

    public void checkSamplesData(String samples, List<Variant> allVariants) {
        Query query = new Query();
        QueryOptions options = new QueryOptions(QueryOptions.SORT, true); //no limit;

        System.out.println("options = " + options.toJson());
        query.put(RETURNED_SAMPLES.key(), samples);
        QueryResult<Variant> queryResult = dbAdaptor.get(query, options);
        List<String> samplesName;
        if (samples.isEmpty()) {
            samplesName = Collections.emptyList();
        } else {
            samplesName = query.getAsStringList(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key());
        }

        Iterator<Variant> it_1 = allVariants.iterator();
        Iterator<Variant> it_2 = queryResult.getResult().iterator();

        assertEquals(allVariants.size(), queryResult.getResult().size());

        LinkedHashMap<String, Integer> samplesPosition1 = null;
        LinkedHashMap<String, Integer> samplesPosition2 = null;
        for (int i = 0; i < queryResult.getNumResults(); i++) {
            Variant variant1 = it_1.next();
            Variant variant2 = it_2.next();

            assertEquals(variant1.toString(), variant2.toString());

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
        Query query = new Query(RETURNED_FILES.key(), 6);
        for (VariantDBIterator iterator = dbAdaptor.iterator(query, new QueryOptions()); iterator.hasNext(); ) {
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

        Query query = new Query(GENOTYPE.key(), na19600 + ":1|1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertEquals(282, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals("1|1", vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), STUDY_NAME + ":NA19600:1|1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertEquals(282, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals("1|1", vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), "NA19600:1|1")
                .append(STUDIES.key(), STUDY_NAME);
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertEquals(282, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals("1|1", vse.getSampleData("NA19600", "GT")
        )));

        query = new Query(GENOTYPE.key(), "NA19600:1|1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertEquals(282, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals("1|1", vse.getSampleData("NA19600", "GT")
        )));


        //get for each genotype. Should return all variants
        query = new Query(GENOTYPE.key(), na19600 + ":0|0,0|1,1|0,1|1,./.");
        long numResults = dbAdaptor.count(null).first();
        assertEquals(NUM_VARIANTS, numResults);

        //Get all missing genotypes for sample na19600
        query = new Query(GENOTYPE.key(), na19600 + ":./.");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertEquals(9, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals("./.", vse.getSampleData("NA19600", "GT")
        )));

//        //This works, but is incorrect. Better use "./."
//        query = new Query(GENOTYPE.key(), na19600 + ":-1/-1");
//        queryResult = dbAdaptor.get(query, new QueryOptions());
//        assertEquals(9, queryResult.getNumTotalResults());
//        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals("./.", vse.getSampleData("NA19600", "GT")
//        )));


        //Get all variants with 1|1 for na19600 and 0|0 or 1|0 for na19685
        query = new Query(GENOTYPE.key(), na19600 + ":1|1" + ";" + na19685 + ":0|0,1|0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertEquals(14, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> {
            assertEquals("1|1", vse.getSampleData("NA19600", "GT"));
            assertTrue(Arrays.asList("0|0", "1|0").contains(vse.getSampleData("NA19685", "GT")));
        }));
    }

    @Test
    public void testGetAllVariants_genotypes_wrong_values() {
        Query query = new Query(GENOTYPE.key(), "WRONG_SAMPLE:1|1");
        thrown.expect(VariantQueryException.class);
        queryResult = dbAdaptor.get(query, new QueryOptions());
    }

    @Test
    public void groupBy_gene() throws Exception {
        int limit = 10;
        QueryResult<Map<String, Object>> queryResult_count = dbAdaptor.groupBy(new Query(), "gene", new QueryOptions("limit", limit)
                .append("count", true));
        Map<String, Long> counts = queryResult_count.getResult().stream().collect(Collectors.toMap(o -> ((Map<String, Object>) o).get
                ("id").toString(), o -> Long.parseLong(((Map<String, Object>) o).get("count").toString())));
        QueryResult<Map<String, Object>> queryResult_group = dbAdaptor.groupBy(new Query(), "gene", new QueryOptions("limit", limit));

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
            QueryResult<Variant> queryResult3 = dbAdaptor.get(new Query(GENE.key(), id), queryOptions);
            assertEquals("Count for ID " + id, counts.get(id).longValue(), queryResult3.getNumTotalResults());
            assertEquals(1, queryResult3.getNumResults());
        }
    }

    @Test
    public void rank_gene() throws Exception {
        int limit = 40;
        QueryResult<Map<String, Object>> queryResult_rank = dbAdaptor.rank(new Query(), "gene", limit, false);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_rank));

        assertEquals(limit, queryResult_rank.getNumResults());
        for (Map<String, Object> map : queryResult_rank.getResult()) {
            QueryResult<Long> variantQueryResult = dbAdaptor.count(new Query(GENE.key(), map.get
                    ("id")));
            assertEquals(((Number) variantQueryResult.first()).intValue(), ((Number) map.get("count")).intValue());
        }
    }

    @Test
    public void rank_ct() throws Exception {
        int limit = 20;
        QueryResult<Map<String, Object>> queryResult_rank = dbAdaptor.rank(new Query(), "ct", limit, false);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult_rank));

        assertEquals(limit, queryResult_rank.getNumResults());
        for (Map<String, Object> map : queryResult_rank.getResult()) {
            QueryResult<Long> variantQueryResult = dbAdaptor.count(new Query(ANNOT_CONSEQUENCE_TYPE
                    .key(), map.get("id")));
            assertEquals(((Number) variantQueryResult.first()).intValue(), ((Number) map.get("count")).intValue());
        }
    }

    @Test
    public void testGetAllVariants_maf() throws Exception {

        QueryResult<Variant> queryResult;
        long numResults;
//        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), ">0.2")).first();
//        System.out.println("queryResult.getNumTotalResults() = " + numResults);

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:" + StudyEntry.DEFAULT_COHORT + ">0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy("1000g", withStats(StudyEntry.DEFAULT_COHORT, withMaf(gt(0.2))))));

        int expectedCount = (int) count(allVariants.getResult(), withStudy("1000g", withStats("cohort1", withMaf(gt(0.2)))));
        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), "1000g:cohort1>0.2")).first();
        assertEquals(expectedCount, numResults);
        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), "1:10>0.2")).first();
        assertEquals(expectedCount, numResults);
        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), "1000g:10>0.2")).first();
        assertEquals(expectedCount, numResults);
        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1:cohort1>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        queryResult = dbAdaptor.get(new Query(STUDIES.key(), "1000g").append(STATS_MAF.key(), "cohort1>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        queryResult = dbAdaptor.get(new Query(STUDIES.key(), "1000g").append(STATS_MAF.key(), "10>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1).append(STATS_MAF.key(), "10>0.2"), null);
        assertEquals(expectedCount, queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("1000g", withStats("cohort1", withMaf(gt(0.2))))));

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy("1000g", withStats("cohort2", withMaf(gt(0.2))))));

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2,1000g:cohort2<=0.2"), null);
        assertThat(queryResult, numResults(is(allVariants.getNumResults())));

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2;1000g:cohort2<=0.2"), null);
        assertThat(queryResult, numResults(is(0)));

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2;1000g:cohort1<0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy("1000g", allOf(
                withStats("cohort2", withMaf(gt(0.2))),
                withStats("cohort1", withMaf(lt(0.2)))))));

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2,1000g:cohort1<0.2"), null);
        assertThat(queryResult, everyResult(allVariants, withStudy("1000g", anyOf(
                withStats("cohort2", withMaf(gt(0.2))),
                withStats("cohort1", withMaf(lt(0.2)))))));
    }

    @Test
    public void testGetAllVariants_maf_cohortNotFound() throws Exception {
        VariantQueryException exception = VariantQueryException.cohortNotFound("cohort3", studyConfiguration.getStudyId(), studyConfiguration.getCohortIds().keySet());
        thrown.expect(instanceOf(exception.getClass()));
        thrown.expectCause(is(exception.getCause()));
        dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort3>0.2"), null);
    }

    @Test
    public void testGetAllVariants_mgf() throws Exception {
        queryResult = dbAdaptor.get(new Query(STATS_MGF.key(), "1000g:ALL>0.2"), null);
        System.out.println(queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("1000g", withStats("ALL", withMgf(gt(0.2))))));

        queryResult = dbAdaptor.get(new Query(STATS_MGF.key(), "1000g:ALL<0.2"), null);
        System.out.println(queryResult.getNumResults());
        assertThat(queryResult, everyResult(allVariants, withStudy("1000g", withStats("ALL", withMgf(lt(0.2))))));
    }

    @Test
    public void testGetAllVariants_cohorts() throws Exception {

        queryResult = dbAdaptor.get(new Query(COHORTS.key(), "1000g:cohort2"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(COHORTS.key(), "1000g:cohort1"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), "1000g")
                .append(COHORTS.key(), "cohort1"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(COHORTS.key(), "cohort1"), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(COHORTS.key(), 10), null);
        assertEquals(allVariants.getNumResults(), queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(COHORTS.key(), "!cohort1"), null);
        assertEquals(0, queryResult.getNumResults());

    }

    @Test
    public void testGetAllVariants_cohorts_fail1() throws Exception {
        VariantQueryException expected = VariantQueryException.cohortNotFound("cohort5_dont_exists", 1, studyConfiguration.getCohortIds().keySet());
        thrown.expect(expected.getClass());
        thrown.expectMessage(expected.getMessage());
        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(COHORTS.key(), "!cohort5_dont_exists"), null);
    }

    @Test
    public void testGetAllVariants_missingAllele() throws Exception {

        queryResult = dbAdaptor.get(new Query(MISSING_ALLELES.key(), "1000g:" + StudyEntry
                .DEFAULT_COHORT + ">4"), null);
        assertEquals(9, queryResult.getNumTotalResults());
        queryResult.getResult().stream().map(variant -> variant.getStudiesMap().get("1000g").getStats())
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

        queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "chromosome"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertNotNull(variant.getChromosome());
        }
    }

    @Test
    public void testExcludeStudies() {

        queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "studies"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
        }

    }

    @Test
    public void testExcludeStats() {
        for (String exclude : Arrays.asList("studies.stats", "stats")) {
            queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
            for (Variant variant : queryResult.getResult()) {
                assertThat(variant.getStudies().get(0).getStats(), not(is(Collections.emptyList())));
            }
        }

    }

    @Test
    public void testExcludeFiles() {
        for (String exclude : Arrays.asList("studies.files", "files")) {
            queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
            for (Variant variant : queryResult.getResult()) {
                assertThat(variant.getStudies().get(0).getFiles(), is(Collections.emptyList()));
            }
        }

    }

    @Test
    public void testExcludeSamples() {
        for (String exclude : Arrays.asList("studies.samplesData", "samplesData", "samples")) {
            queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.EXCLUDE, exclude));
            assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
            for (Variant variant : queryResult.getResult()) {
                assertThat(variant.getStudies().get(0).getSamplesData(), is(Collections.emptyList()));
            }
        }

    }

    @Test
    public void testExcludeAnnotation() {
        queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "annotation"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getAnnotation(), anyOf(is((VariantAnnotation) null), is(new VariantAnnotation())));
        }

    }

    @Test
    public void testInclude() {
        queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.INCLUDE, "studies"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getStats(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getFiles(), not(is(Collections.emptyList())));
            assertThat(variant.getStudies().get(0).getSamplesData(), not(is(Collections.emptyList())));
            assertNull(variant.getAnnotation());
        }

        queryResult = dbAdaptor.get(new Query(), new QueryOptions(QueryOptions.INCLUDE, "annotation"));
        assertEquals(allVariants.getResult().size(), queryResult.getResult().size());
        for (Variant variant : queryResult.getResult()) {
            assertThat(variant.getStudies(), is(Collections.emptyList()));
            assertNotNull(variant.getAnnotation());
        }

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
