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
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.annotation.CellBaseVariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
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
                    .append(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "json");
            StorageETLResult etlResult = runDefaultETL(smallInputUri, getVariantStorageManager(), studyConfiguration, params);
            source = VariantStorageManager.readVariantSource(Paths.get(etlResult.getTransformResult().getPath()), null);
            NUM_VARIANTS = getExpectedNumLoadedVariants(source);
            fileIndexed = true;
            Integer indexedFileId = studyConfiguration.getIndexedFiles().iterator().next();

            VariantStatisticsManager vsm = new VariantStatisticsManager();

            QueryOptions options = new QueryOptions(VariantStorageManager.Options.STUDY_ID.key(), STUDY_ID);
            options.put(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 100);
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
            dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
            URI stats = vsm.createStats(dbAdaptor, outputUri.resolve("cohort1.cohort2.stats"), cohorts, cohortIds, studyConfiguration,
                    options);
            vsm.loadStats(dbAdaptor, stats, studyConfiguration, options);


        }
        allVariants = dbAdaptor.get(new Query(), new QueryOptions());
        options = new QueryOptions();
        dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
    }

    @After
    public void after() throws IOException {
        dbAdaptor.close();
    }

    @Test
    public void testGetAllVariants() {
        long numResults = dbAdaptor.count(null).first();
        assertEquals(NUM_VARIANTS, numResults);
    }

    @Test
    public void testGetAllVariants_populationFrequency() {
        Query query = new Query(ANNOT_POPULATION_REFERENCE_FREQUENCY.key(), "1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(43, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("1000GENOMES_phase_1:AFR") && map.get("1000GENOMES_phase_1:AFR").getRefAlleleFreq() <= 0.05)));

        query = new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:African_American>0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(677, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && map.get("ESP_6500:African_American").getAltAlleleFreq() > 0.05)));

        query = new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(139, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("1000GENOMES_phase_1:AFR") && map.get("1000GENOMES_phase_1:AFR").getAltAlleleFreq() <= 0.05)));

        query = new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:African_American>0.05;" +
                "1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(22, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && map.get("ESP_6500:African_American").getAltAlleleFreq() > 0.05 && map.containsKey("1000GENOMES_phase_1:AFR") && map.get("1000GENOMES_phase_1:AFR").getAltAlleleFreq() <= 0.05)));

        query = new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:African_American>0.05," +
                "1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(794, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && map.get("ESP_6500:African_American")
                .getAltAlleleFreq() > 0.05
                || map.containsKey("1000GENOMES_phase_1:AFR") && map.get("1000GENOMES_phase_1:AFR").getAltAlleleFreq() <= 0.05)));

    }

    @Test
    public void testGetAllVariants_populationFrequency_no_indels() {
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
        Predicate<Variant> filterType = variant -> EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType());

        Query query = new Query(TYPE.key(), VariantType.SNV)
                .append(ANNOT_POPULATION_REFERENCE_FREQUENCY.key(), "1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
//        assertEquals(42, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(!map.containsKey("1000GENOMES_phase_1:AFR") || map.get("1000GENOMES_phase_1:AFR").getRefAlleleFreq() <= 0.05),
                filterType));
        assertEquals(0, filterPopulation(map -> !(map.getOrDefault("1000GENOMES_phase_1:AFR",  defaultPopulation).getRefAlleleFreq() <= 0.05), filterType));

        query = new Query(TYPE.key(), VariantType.SNV)
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:African_American>0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(672, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && map.get("ESP_6500:African_American").getAltAlleleFreq() > 0.05), filterType));

        query = new Query(TYPE.key(), VariantType.SNV)
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(0, filterPopulation(map -> !(!map.containsKey("1000GENOMES_phase_1:AFR") || map.get("1000GENOMES_phase_1:AFR").getAltAlleleFreq() <= 0.05), filterType));

        query = new Query(TYPE.key(), VariantType.SNV)
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:African_American>0.05;1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && map.get("ESP_6500:African_American").getAltAlleleFreq() > 0.05
                && (!map.containsKey("1000GENOMES_phase_1:AFR") || map.get("1000GENOMES_phase_1:AFR").getAltAlleleFreq() <= 0.05)), filterType));

        query = new Query(TYPE.key(), VariantType.SNV)
                .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "ESP_6500:African_American>0.05,1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && map.get("ESP_6500:African_American").getAltAlleleFreq() > 0.05
                || (!map.containsKey("1000GENOMES_phase_1:AFR") || map.get("1000GENOMES_phase_1:AFR").getAltAlleleFreq() <= 0.05)), filterType));

    }

    @Test
    public void testGetAllVariants_population_maf_no_indels() {
        final PopulationFrequency defaultPopulation = new PopulationFrequency(null, null, null, null, 0F, 0F, 0F, 0F, 0F);
        Predicate<Variant> filterType = variant -> EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType());

        Query query = new Query(TYPE.key(), VariantType.SNP + "," + VariantType.SNV)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
//        assertEquals(179, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(Math.min(map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.05), filterType));

        query = new Query(TYPE.key(), VariantType.SNP + "," + VariantType.SNV)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"ESP_6500:African_American>0.05");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(634, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && Math.min(map.get("ESP_6500:African_American").getRefAlleleFreq(),
                map.get("ESP_6500:African_American").getAltAlleleFreq()) > 0.05), filterType));

        query = new Query(TYPE.key(), VariantType.SNP + "," + VariantType.SNV)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
//        assertEquals(179, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(Math.min(map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.05), filterType));

        query = new Query(TYPE.key(), VariantType.SNP + "," + VariantType.SNV)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"ESP_6500:African_American>0.05;1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
//        assertEquals(32, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && Math.min(map.get("ESP_6500:African_American").getRefAlleleFreq(),
                map.get("ESP_6500:African_American").getAltAlleleFreq()) > 0.05
                && Math.min(map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.05), filterType));

        query = new Query(TYPE.key(), VariantType.SNP + "," + VariantType.SNV)
                .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),"ESP_6500:African_American>0.05,1000GENOMES_phase_1:AFR<=0.05");
        queryResult = dbAdaptor.get(query, options);
//        assertEquals(781, queryResult.getNumResults());
        assertEquals(0, filterPopulation(map -> !(map.containsKey("ESP_6500:African_American") && Math.min(map.get("ESP_6500:African_American").getRefAlleleFreq(),
                map.get("ESP_6500:African_American").getAltAlleleFreq()) > 0.05
                || Math.min(map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getRefAlleleFreq(),
                map.getOrDefault("1000GENOMES_phase_1:AFR", defaultPopulation).getAltAlleleFreq()) <= 0.05), filterType));

    }

    public long filterPopulation(Predicate<Map<String, PopulationFrequency>> predicate) {
        return filterPopulation(queryResult, predicate, v -> true);
    }

    public long filterPopulation(Predicate<Map<String, PopulationFrequency>> predicate, Predicate<Variant> filterVariants) {
        return filterPopulation(queryResult, predicate, filterVariants);
    }

    public long filterPopulation(QueryResult<Variant> queryResult, Predicate<Map<String, PopulationFrequency>> predicate, Predicate<Variant> filterVariants) {
        queryResult.getResult().forEach(variant -> {
            assertNotNull(variant);
            assertNotNull("In" + variant, variant.getAnnotation());
            assertNotNull("In" + variant, variant.getAnnotation().getPopulationFrequencies());
        });
        Set<Variant> set = allVariants.getResult()
                .stream()
                .filter(filterVariants.and(variant -> variant.getAnnotation() != null))
                .filter(variant -> predicate.negate().test(variant.getAnnotation().getPopulationFrequencies() == null
                        ? Collections.<String, PopulationFrequency>emptyMap()
                        : variant.getAnnotation().getPopulationFrequencies()
                        .stream()
                        .collect(Collectors.toMap(p -> p.getStudy() + ":" + p.getPopulation(), p -> p))))
                .collect(Collectors.toSet());

        for (Variant variant : set) {
            Set<Variant> result = new HashSet<>(queryResult.getResult());
            if (!result.contains(variant)) {
                System.out.println("variant missing = " + variant);
            }
        }
        for (Variant variant : queryResult.getResult()) {
            if (!set.contains(variant)) {
                System.out.println("variant not suppose to be = " + variant);
            }
        }

        assertEquals(set.size(), queryResult.getNumResults());
        return queryResult.getResult().stream()
                .map(variant -> variant.getAnnotation().getPopulationFrequencies().stream()
                        .collect(Collectors.toMap(p -> p.getStudy() + ":" + p.getPopulation(), p -> p)))
                .filter(predicate)
                .count();
    }

    @Test
    public void testGetAllVariants_id() {
        testGetAllVariants_rs(ID.key());
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
        assertTrue(variant.getIds().contains("rs1137005"));

        query = new Query(key, "rs1137005,rs150535390");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(2, queryResult.getNumResults());
        queryResult.getResult().forEach(v -> assertTrue(v.getIds().contains("rs1137005") || v.getIds().contains("rs150535390")));
    }

    @Test
    public void testGetAllVariants_ct() {
        Query query;

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566");
        queryResult = dbAdaptor.get(query, null);
        assertEquals(911, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "1566");
        queryResult = dbAdaptor.get(query, null);
        assertEquals(911, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566,SO:0001583");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(947, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "1566,SO:0001583");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(947, queryResult.getNumResults());

        query = new Query(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001566;SO:0001583");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(396, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariants_transcriptionAnnotationFlags() {
        //ANNOT_TRANSCRIPTION_FLAGS
        Query query;
        Map<String, Integer> flags = new HashMap<>();
        for (Variant variant : allVariants.getResult()) {
            Set<String> flagsInVariant = new HashSet<>();
            for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                flagsInVariant.addAll(consequenceType.getTranscriptAnnotationFlags());
            }
            for (String flag : flagsInVariant) {
                flags.put(flag, flags.getOrDefault(flag, 0) + 1);
            }
        }

        assertTrue(flags.containsKey("basic"));
        assertTrue(flags.containsKey("CCDS"));
        assertTrue(flags.containsKey("mRNA_start_NF"));
        assertTrue(flags.containsKey("mRNA_end_NF"));
        assertTrue(flags.containsKey("cds_start_NF"));
        assertTrue(flags.containsKey("cds_end_NF"));

        for (Map.Entry<String, Integer> entry : flags.entrySet()) {
            System.out.println(entry);
            query = new Query(ANNOT_TRANSCRIPTION_FLAGS.key(), entry.getKey());
            queryResult = dbAdaptor.get(query, null);
            assertEquals(entry.getValue().intValue(), queryResult.getNumResults());
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
    public void testGetAllVariants_proteinKeywords() {
        //ANNOT_PROTEIN_KEYWORDS
        Query query;
        Map<String, Integer> keywords = new HashMap<>();
        for (Variant variant : allVariants.getResult()) {
            Set<String> keywordsInVariant = new HashSet<>();
            for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                keywordsInVariant.addAll(consequenceType.getProteinVariantAnnotation().getKeywords());
            }
            for (String flag : keywordsInVariant) {
                keywords.put(flag, keywords.getOrDefault(flag, 0) + 1);
            }
        }

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
            for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
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
        //FUNCTIONAL_SCORE

        Query query;
        query = new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_scaled>5");
        assertTrue(countFunctionalScore("cadd_scaled", allVariants, s -> s > 5.0) > 0);
        System.out.println("countFunctionalScore(\"cadd_scaled\", allVariants, s -> s > 5.0) = " + countFunctionalScore("cadd_scaled", allVariants, s -> s > 5.0));

        assertEquals(countFunctionalScore("cadd_scaled", allVariants, s -> s > 5.0),
                countFunctionalScore("cadd_scaled", dbAdaptor.get(query, null), s -> s > 5.0));

        query = new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_raw<0.5");
        assertEquals(countFunctionalScore("cadd_raw", allVariants, s -> s < 0.5),
                countFunctionalScore("cadd_raw", dbAdaptor.get(query, null), s -> s < 0.5));

        query = new Query(ANNOT_FUNCTIONAL_SCORE.key(), "cadd_scaled<=0.5");
        assertEquals(countFunctionalScore("cadd_scaled", allVariants, s -> s <= 0.5),
                countFunctionalScore("cadd_scaled", dbAdaptor.get(query, null), s -> s <= 0.5));
    }

    private long countFunctionalScore(String source, QueryResult<Variant> variantQueryResult, Predicate<Double> doublePredicate) {
        long c = 0;
        for (Variant variant : variantQueryResult.getResult()) {
            for (Score score : variant.getAnnotation().getFunctionalScore()) {
                if (score.getSource().equals(source)) {
                    if (doublePredicate.test(score.getScore())) {
                        c++;
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
                assertTrue(next + " <= " + prev, next.getStart() <= prev.getStart());
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
        assertEquals(64, queryResult.getNumResults());

        query = new Query(CHROMOSOME.key(), "1");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(114, queryResult.getNumResults());

        query = new Query(REGION.key(), "1");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(114, queryResult.getNumResults());

        options.put("sort", true);
        query = new Query(REGION.key(), "1:14000000-160000000");
        queryResult = dbAdaptor.get(query, options);
        assertEquals(64, queryResult.getNumResults());

        int lastStart = 0;
        for (Variant variant : queryResult.getResult()) {
            assertEquals("1", variant.getChromosome());
            assertTrue(lastStart <= variant.getStart());
            lastStart = variant.getStart();
        }
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
        QueryOptions options = new QueryOptions("limit", 0); //no limit;

        Query query = new Query()
                .append(STUDIES.key(), studyConfiguration.getStudyId());
        queryResult = dbAdaptor.get(query, options);
        List<Variant> variants = queryResult.getResult();

        checkSamplesData("NA19600", variants, query, options);
        checkSamplesData("NA19660", variants, query, options);
        checkSamplesData("NA19661", variants, query, options);
        checkSamplesData("NA19685", variants, query, options);
        checkSamplesData("NA19600,NA19685", variants, query, options);
        checkSamplesData("NA19685,NA19600", variants, query, options);
        checkSamplesData("NA19660,NA19661,NA19600", variants, query, options);
        checkSamplesData("", variants, query, options);
    }

    public void checkSamplesData(String samples, List<Variant> allVariants, Query query, QueryOptions options) {
        query.put(RETURNED_SAMPLES.key(), samples);
        queryResult = dbAdaptor.get(query, options);
        List<String> samplesName;
        if (samples.isEmpty()) {
            samplesName = Collections.emptyList();
        } else {
            samplesName = query.getAsStringList(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key());
        }

        Iterator<Variant> it_1 = allVariants.iterator();
        Iterator<Variant> it_2 = queryResult.getResult().iterator();

        LinkedHashMap<String, Integer> samplesPosition1 = null;
        LinkedHashMap<String, Integer> samplesPosition2 = null;
        for (int i = 0; i < queryResult.getNumResults(); i++) {
            Variant variant1 = it_1.next();
            Variant variant2 = it_2.next();

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

        query = new Query(GENOTYPE.key(), "NA19600:1|1").append(
                STUDIES.key(), STUDY_NAME);
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

        //This works, but is incorrect. Better use "./."
        query = new Query(GENOTYPE.key(), na19600 + ":-1/-1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertEquals(9, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getStudiesMap().forEach((s, vse) -> assertEquals("./.", vse.getSampleData("NA19600", "GT")
        )));


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
        long numResults = dbAdaptor.count(new Query(STATS_MAF.key(), ">0.2")).first();
        System.out.println("queryResult.getNumTotalResults() = " + numResults);

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:" + StudyEntry.DEFAULT_COHORT +
                ">0.2"), null);
        assertEquals(625, queryResult.getNumResults());
        queryResult.getResult().stream().map(variant -> variant.getStudiesMap().get("1000g").getStats())
                .forEach(map -> assertTrue(map.get(StudyEntry.DEFAULT_COHORT).getMaf() > 0.2));

        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), "1000g:cohort1>0.2")).first();
        assertEquals(749, numResults);
        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), "1:10>0.2")).first();
        assertEquals(749, numResults);
        numResults = dbAdaptor.count(new Query(STATS_MAF.key(), "1000g:10>0.2")).first();
        assertEquals(749, numResults);
        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1:cohort1>0.2"), null);
        assertEquals(749, queryResult.getNumResults());
        queryResult = dbAdaptor.get(new Query(STUDIES.key(), "1000g")
                .append(STATS_MAF.key(), "cohort1>0.2"), null);
        assertEquals(749, queryResult.getNumResults());
        queryResult = dbAdaptor.get(new Query(STUDIES.key(), "1000g")
                .append(STATS_MAF.key(), "10>0.2"), null);
        assertEquals(749, queryResult.getNumResults());
        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(STATS_MAF.key(), "10>0.2"), null);
        assertEquals(749, queryResult.getNumResults());
        queryResult.getResult().stream().map(variant -> variant.getStudiesMap().get("1000g").getStats())
                .forEach(map -> assertTrue(map.get("cohort1").getMaf() > 0.2));

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2"), null);
        numResults = queryResult.getNumResults();
        assertEquals(690, numResults);
        queryResult.getResult().stream().forEach(
                variant -> assertTrue(variant.toString(), variant.getStudy("1000g").getStats("cohort2").getMaf() > 0.2)
        );


        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2,1000g:cohort2<=0" +
                ".2"), null);
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());
        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2;1000g:cohort2<=0" +
                ".2"), null);
        assertEquals(0, queryResult.getNumTotalResults());

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2;1000g:cohort1<0.2")
                , null);

        assertEquals(74, queryResult.getNumResults());
        queryResult.getResult().stream().map(variant -> variant.getStudiesMap().get("1000g").getStats())
                .forEach(map -> assertTrue(map.get("cohort2").getMaf() > 0.2 && map.get("cohort1").getMaf() < 0.2));

        queryResult = dbAdaptor.get(new Query(STATS_MAF.key(), "1000g:cohort2>0.2,1000g:cohort1<0.2"),
                null);
        assertEquals(865, queryResult.getNumResults());

        queryResult.getResult().stream().map(variant -> variant.getStudiesMap().get("1000g").getStats())
                .forEach(map -> assertTrue(map.get("cohort2").getMaf() > 0.2 || map.get("cohort1").getMaf() < 0.2));


    }

    @Test
    public void testGetAllVariants_cohorts() throws Exception {

        queryResult = dbAdaptor.get(new Query(COHORTS.key(), "1000g:cohort2"), null);
        assertEquals(NUM_VARIANTS, queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(COHORTS.key(), "1000g:cohort1"), null);
        assertEquals(NUM_VARIANTS, queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), "1000g")
                .append(COHORTS.key(), "cohort1"), null);
        assertEquals(NUM_VARIANTS, queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(COHORTS.key(), "cohort1"), null);
        assertEquals(NUM_VARIANTS, queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(COHORTS.key(), 10), null);
        assertEquals(NUM_VARIANTS, queryResult.getNumResults());

        queryResult = dbAdaptor.get(new Query(STUDIES.key(), 1)
                .append(COHORTS.key(), "!cohort1"), null);
        assertEquals(0, queryResult.getNumResults());

    }

    @Test
    public void testGetAllVariants_cohorts_fail1() throws Exception {
        thrown.expect(VariantQueryException.class);
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
