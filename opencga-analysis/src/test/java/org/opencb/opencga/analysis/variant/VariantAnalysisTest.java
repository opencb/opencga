/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.variant.*;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class VariantAnalysisTest {

    public static final String USER = "user";
    public static final String PASSWORD = "asdf";
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = "opencga_test_" + USER + "_" + PROJECT;
    private ToolRunner toolRunner;

    @Parameterized.Parameters(name="{0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public VariantAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(VariantAnalysisTest.storageEngine)) {
            indexed = false;
        }
        VariantAnalysisTest.storageEngine = storageEngine;
    }


    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;

    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();
    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    private static String storageEngine;
    private static boolean indexed = false;
    private static String token;
    private static File file;

    @Before
    public void setUp() throws Throwable {
        if (!indexed) {
            indexed = true;

            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                hadoopExternalResource.before();
            }
            opencga.after();
            opencga.before();

            catalogManager = opencga.getCatalogManager();
            variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());

            opencga.clearStorageDB(DB_NAME);

            StorageConfiguration storageConfiguration = opencga.getStorageConfiguration();
            storageConfiguration.getVariant().setDefaultEngine(storageEngine);
            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                HadoopVariantStorageTest.updateStorageConfiguration(storageConfiguration, hadoopExternalResource.getConf());
                ObjectMap variantHadoopOptions = storageConfiguration.getVariantEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID).getOptions();
                for (Map.Entry<String, String> entry : hadoopExternalResource.getConf()) {
                    variantHadoopOptions.put(entry.getKey(), entry.getValue());
                }
            }

            setUpCatalogManager();


            file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", token);
            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);

            for (int i = 0; i < file.getSampleIds().size(); i++) {
                if (i % 2 == 0) {
                    String id = file.getSampleIds().get(i);
                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
                }
            }

            List<Sample> samples = catalogManager.getSampleManager().get(STUDY, file.getSampleIds().subList(0, 2), QueryOptions.empty(), token).getResults();
            catalogManager.getCohortManager().create(STUDY, "c1", null, null, samples, null, null, token);
            samples = catalogManager.getSampleManager().get(STUDY, file.getSampleIds().subList(2, 4), QueryOptions.empty(), token).getResults();
            catalogManager.getCohortManager().create(STUDY, "c2", null, null, samples, null, null, token);

            Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
            Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
            List<Individual> individuals = new ArrayList<>(4);

            String father = "NA19661";
            String mother = "NA19660";
            String son = "NA19685";
            String daughter = "NA19600";
            // Father
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(father, father, IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), Collections.singletonList(father), null, token).first());
            // Mother
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(mother, mother, IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), Collections.singletonList(mother), null, token).first());
            // Son
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(son, son, IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(Collections.singletonList(disorder)), Collections.singletonList(son), null, token).first());
            // Daughter
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(daughter, daughter, IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(daughter), null, token).first());
            catalogManager.getFamilyManager().create(
                    STUDY,
                    new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
                    individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
                    token);


            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID, DB_NAME);
                VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants")).toUri());
            }
        }
        catalogManager = opencga.getCatalogManager();
        variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());

        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));
    }

    @AfterClass
    public static void afterClass() {
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            hadoopExternalResource.after();
        }
        opencga.after();
    }

    public void setUpCatalogManager() throws IOException, CatalogException {
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.FULL, null);
        token = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh37", new QueryOptions(), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);

        // Create 10 samples not indexed
        for (int i = 0; i < 10; i++) {
            Sample sample = new Sample().setId("SAMPLE_" + i);
            if (i % 2 == 0) {
                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
            }
            catalogManager.getSampleManager().create(STUDY, sample, null, token);
        }

    }

    @Test
    public void testVariantStats() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        List<String> samples = file.getSampleIds();

        VariantStatsAnalysis variantStatsAnalysis = new VariantStatsAnalysis()
                .setStudy(STUDY)
                .setSamples(samples.subList(1, 3));
        variantStatsAnalysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);

        ExecutionResult ar = variantStatsAnalysis.start();
        checkExecutionResult(ar);

        MutableInt count = new MutableInt();
        java.io.File file = getOutputFile(outDir);
        FileUtils.lineIterator(file).forEachRemaining(line -> {
            if (!line.startsWith("#")) {
                count.increment();
            }
        });
        assertEquals(variantStorageManager.count(new Query(VariantQueryParam.STUDY.key(), STUDY), token).first().intValue(),
                count.intValue());
    }

    @Test
    public void testVariantStatsTwoCohorts() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats_multi_cohort"));
        System.out.println("output = " + outDir.toAbsolutePath());

        VariantStatsAnalysis variantStatsAnalysis = new VariantStatsAnalysis()
                .setStudy(STUDY)
                .setCohort(Arrays.asList("c1", "c2"));
        variantStatsAnalysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);

        ExecutionResult ar = variantStatsAnalysis.start();
        checkExecutionResult(ar);

        MutableInt count = new MutableInt();
        java.io.File file = getOutputFile(outDir);
        FileUtils.lineIterator(file).forEachRemaining(line->{
            if (!line.startsWith("#")) {
                count.increment();
            }
        });
        assertEquals(variantStorageManager.count(new Query(VariantQueryParam.STUDY.key(), STUDY), token).first().intValue(),
                count.intValue());
    }

    @Test
    public void testVariantStatsWithFilter() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats_chr22"));
        System.out.println("output = " + outDir.toAbsolutePath());
        List<String> samples = file.getSampleIds();

        String region = "22";
        VariantStatsAnalysis variantStatsAnalysis = new VariantStatsAnalysis()
                .setStudy(STUDY)
                .setSamples(samples.subList(1, 3))
                .setRegion(region);
        variantStatsAnalysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);

        ExecutionResult ar = variantStatsAnalysis.start();
        checkExecutionResult(ar);

        MutableInt count = new MutableInt();
        java.io.File file = getOutputFile(outDir);
        FileUtils.lineIterator(file).forEachRemaining(line -> {
            if (!line.startsWith("#")) {
                count.increment();
            }
        });
        Query variantsQuery = new Query(VariantQueryParam.REGION.key(), region);
        System.out.println("variantsQuery = " + variantsQuery.toJson());
        assertEquals(variantStorageManager.count(new Query(variantsQuery).append(VariantQueryParam.STUDY.key(), STUDY), token).getNumMatches(),
                count.intValue());
    }

    private java.io.File getOutputFile(Path outDir) {
        return FileUtils.listFiles(outDir.toFile(), null, false)
                .stream()
                .filter(f -> !f.getName().endsWith(ExecutionResultManager.FILE_EXTENSION))
                .findFirst().orElse(null);
    }

    @Test
    public void testSampleStats() throws Exception {
        sampleVariantStats("1,2", "stats_1", false, 1, file.getSampleIds().subList(0, 2));
        sampleVariantStats("1,2", "stats_1", false, 1, file.getSampleIds().subList(2, 4));
        sampleVariantStats("1,2", "stats_2", false, 2, Collections.singletonList(ParamConstants.ALL));
        try {
            sampleVariantStats("1,2", "stats_1", false, 2, file.getSampleIds());
            fail();
        } catch (Exception e) {
            e.printStackTrace();
        }
        sampleVariantStats("1,2", "stats_3", true, 3, Collections.singletonList(ParamConstants.ALL));
        sampleVariantStats("1,2", "stats_1", true, 3, file.getSampleIds());
        sampleVariantStats("1,2", "stats_1", false, 2, Collections.singletonList(ParamConstants.ALL), true);
        sampleVariantStats("1,2", "stats_1", true, 3, Collections.singletonList(ParamConstants.ALL));
        sampleVariantStats(null, "ALL", false, 4, Collections.singletonList(ParamConstants.ALL));

        try {
            sampleVariantStats("4", "ALL", true, 4, Collections.singletonList(ParamConstants.ALL));
            fail();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ExecutionResult sampleVariantStats(String region, String indexId, boolean indexOverwrite, int expectedStats, List<String> samples)
            throws Exception {
        return sampleVariantStats(region, indexId, indexOverwrite, expectedStats, samples, false);
    }

    private ExecutionResult sampleVariantStats(String region, String indexId, boolean indexOverwrite, int expectedStats, List<String> samples, boolean nothingToDo)
            throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_sample_stats_" + indexId));
        System.out.println("output = " + outDir.toAbsolutePath());
        SampleVariantStatsAnalysisParams params = new SampleVariantStatsAnalysisParams()
                .setSample(samples)
                .setIndex(indexId != null)
                .setIndexId(indexId)
                .setIndexOverwrite(indexOverwrite)
                .setVariantQuery(new AnnotationVariantQueryParams().setRegion(region));
        ExecutionResult result = toolRunner.execute(SampleVariantStatsAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);

        if (nothingToDo) {
            assertEquals("All samples stats indexed. Nothing to do!", result.getEvents().get(0).getMessage());
        } else {
            checkExecutionResult(result, storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));

            List<SampleVariantStats> allStats = JacksonUtils.getDefaultObjectMapper().readerFor(SampleVariantStats.class).<SampleVariantStats>readValues(outDir.resolve("sample-variant-stats.json").toFile()).readAll();
            for (SampleVariantStats sampleVariantStats : allStats) {
//                System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(sampleVariantStats));
                List<String> expectedRegion = region == null
                        ? Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "X")
                        : Arrays.asList(region.split(","));
                assertEquals(new HashSet<>(expectedRegion), sampleVariantStats.getChromosomeCount().keySet());
            }
            if (samples.get(0).equals(ParamConstants.ALL)) {
                samples = file.getSampleIds();
            }
            for (String sample : samples) {
                Sample sampleObj = catalogManager.getSampleManager().get(STUDY, sample, QueryOptions.empty(), token).first();
                List<SampleQcVariantStats> variantStats = sampleObj.getQualityControl().getVariantMetrics().getVariantStats();
                assertEquals(expectedStats, variantStats.size());
                assertThat(variantStats.stream().map(SampleQcVariantStats::getId).collect(Collectors.toSet()), hasItem(indexId));
            }
        }
        return result;
    }

    @Test
    public void testCohortStats() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        CohortVariantStatsAnalysis analysis = new CohortVariantStatsAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_cohort_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);
        List<String> samples = file.getSampleIds();
        analysis.setStudy(STUDY)
                .setSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(0, 3)));
        checkExecutionResult(analysis.start(), storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));
    }

    @Test
    public void testCohortStatsIndex() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_cohort_stats_index"));
        System.out.println("output = " + outDir.toAbsolutePath());

        CohortVariantStatsAnalysisParams toolParams = new CohortVariantStatsAnalysisParams()
                .setCohort(StudyEntry.DEFAULT_COHORT)
                .setIndex(true);

        ExecutionResult result = toolRunner.execute(CohortVariantStatsAnalysis.class, toolParams,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
        checkExecutionResult(result, storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));

        Cohort cohort = catalogManager.getCohortManager().get(STUDY, StudyEntry.DEFAULT_COHORT, new QueryOptions(), token).first();
        assertEquals(1, cohort.getAnnotationSets().size());
        AnnotationSet annotationSet = cohort.getAnnotationSets().get(0);
        assertEquals(CohortVariantStatsAnalysis.VARIABLE_SET_ID, annotationSet.getId());
        assertEquals(CohortVariantStatsAnalysis.VARIABLE_SET_ID, annotationSet.getVariableSetId());
        Object variantCount = annotationSet.getAnnotations().get("variantCount");
        System.out.println("variantCount = " + variantCount);

        CohortUpdateParams updateParams = new CohortUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSet.getId(), "",  Collections.singletonMap("variantCount", 1))));
        QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, ParamUtils.CompleteUpdateAction.REPLACE));

        catalogManager.getCohortManager()
                .update(STUDY, StudyEntry.DEFAULT_COHORT, updateParams, true, options, token);

        toolParams = new CohortVariantStatsAnalysisParams()
                .setCohort(StudyEntry.DEFAULT_COHORT)
                .setIndex(true);

        result = toolRunner.execute(CohortVariantStatsAnalysis.class, toolParams,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
        checkExecutionResult(result, storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));


        cohort = catalogManager.getCohortManager().get(STUDY, StudyEntry.DEFAULT_COHORT, new QueryOptions(), token).first();
        assertEquals(1, cohort.getAnnotationSets().size());
        annotationSet = cohort.getAnnotationSets().get(0);
        assertEquals(CohortVariantStatsAnalysis.VARIABLE_SET_ID, annotationSet.getId());
        assertEquals(CohortVariantStatsAnalysis.VARIABLE_SET_ID, annotationSet.getVariableSetId());
        assertEquals(variantCount, annotationSet.getAnnotations().get("variantCount"));
    }

    @Test
    public void testExport() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_export"));
        System.out.println("outDir = " + outDir);
        VariantExportParams variantExportParams = new VariantExportParams();
        variantExportParams.appendQuery(new Query(VariantQueryParam.REGION.key(), "22"));
        assertEquals("22", variantExportParams.getRegion());
        variantExportParams.setCt("lof");
        variantExportParams.setCompress(true);
        variantExportParams.setOutputFileName("chr22");

        toolRunner.execute(VariantExportTool.class, variantExportParams.toObjectMap(), outDir, null, token);
    }

    @Test
    public void testGwas() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        GwasAnalysis analysis = new GwasAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);
        List<String> samples = file.getSampleIds();
        analysis.setStudy(STUDY)
                .setCaseCohortSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(0, 2)))
                .setControlCohortSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(2, 4)));
        checkExecutionResult(analysis.start(), storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));
    }

    @Test
    public void testGwasByPhenotype() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        GwasAnalysis analysis = new GwasAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas_phenotype"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);

        analysis.setStudy(STUDY)
                .setPhenotype(PHENOTYPE_NAME);
        checkExecutionResult(analysis.start(), storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));
    }

    @Test
    public void testGwasIndex() throws Exception {
        // Variant scores can not be loaded in mongodb
        Assume.assumeThat(storageEngine, CoreMatchers.is(CoreMatchers.not(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID)));

        ObjectMap executorParams = new ObjectMap();
        GwasAnalysis analysis = new GwasAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas_index"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);

        List<Sample> samples = catalogManager.getSampleManager().get(STUDY, file.getSampleIds().subList(0, 2), QueryOptions.empty(), token).getResults();
        catalogManager.getCohortManager().create(STUDY, new Cohort().setId("CASE").setSamples(samples), new QueryOptions(), token);
        samples = catalogManager.getSampleManager().get(STUDY, file.getSampleIds().subList(2, 4), QueryOptions.empty(), token).getResults();
        catalogManager.getCohortManager().create(STUDY, new Cohort().setId("CONTROL").setSamples(samples), new QueryOptions(), token);

        analysis.setStudy(STUDY)
                .setCaseCohort("CASE")
                .setControlCohort("CONTROL")
                .setIndex(true)
                .setIndexScoreId("GwasScore");
        checkExecutionResult(analysis.start());

        List<VariantScoreMetadata> scores = variantStorageManager.listVariantScores(STUDY, token);
        System.out.println("scores.get(0) = " + JacksonUtils.getDefaultObjectMapper().writeValueAsString(scores));
        assertEquals(1, scores.size());
        assertEquals("GwasScore", scores.get(0).getName());

        for (Variant variant : variantStorageManager.iterable(token)) {
            assertEquals("GwasScore", variant.getStudies().get(0).getScores().get(0).getId());
        }
    }

    @Test
    public void testKnockoutGenes() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class, params.toObjectMap(), outDir, null, token);
        checkExecutionResult(er, false);
    }

    @Test
    public void testKnockoutGenesSpecificGenes() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes_specific_genes"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());
        params.setGene(Arrays.asList("MIR1909", "DZIP3", "BTN3A2", "ITIH5"));

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class, params.toObjectMap().append("executionMethod", "byGene"), outDir, null, token);
        checkExecutionResult(er, false);
        assertEquals(4, er.getAttributes().get("otherGenesCount"));
        assertEquals(3, er.getAttributes().get("proteinCodingGenesCount"));
    }

    @Test
    public void testKnockoutGenesSpecificGenesAndBiotypeProteinCoding() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes_specific_genes_bt_protein_coding"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());
        params.setGene(Arrays.asList("MIR1909", "DZIP3", "BTN3A2", "ITIH5"));
        params.setBiotype(VariantAnnotationUtils.PROTEIN_CODING);

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class, params.toObjectMap(), outDir, null, token);
        checkExecutionResult(er, false);
        assertEquals(0, er.getAttributes().get("otherGenesCount"));
        assertEquals(3, er.getAttributes().get("proteinCodingGenesCount"));
    }

    @Test
    public void testKnockoutGenesSpecificGenesAndBiotypeNMD() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes_specific_genes_bt_NMD"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());
        params.setGene(Arrays.asList("MIR1909", "DZIP3", "BTN3A2", "ITIH5"));
        params.setBiotype("nonsense_mediated_decay");

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class, params.toObjectMap(), outDir, null, token);
        checkExecutionResult(er, false);
        assertEquals(3, er.getAttributes().get("otherGenesCount")); // MIR1909 only has miRNA biotype
        assertEquals(0, er.getAttributes().get("proteinCodingGenesCount"));
    }

    @Test
    public void testKnockoutGenesByBiotype() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes_by_biotype"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());
        params.setBiotype("miRNA,rRNA");
//        params.setBiotype("processed_transcript"
//                + "," + "processed_pseudogene"
//                + "," + "transcribed_unprocessed_pseudogene"
//                + "," + "nonsense_mediated_decay"
//                + "," + "retained_intron"
//                + "," + "antisense_RNA"
//                + "," + "miRNA"
//                + "," + "misc_RNA"
//                + "," + "unprocessed_pseudogene"
//                + "," + "sense_overlapping"
//                + "," + "lincRNA"
//                + "," + "rRNA"
//                + "," + "snRNA"
//                + "," + "polymorphic_pseudogene"
//                + "," + "transcribed_processed_pseudogene"
//                + "," + "transcribed_unitary_pseudogene"
//                + "," + "snoRNA"
//                + "," + "TEC"
//                + "," + "pseudogene"
//                + "," + "sense_intronic"
//                + "," + "non_stop_decay"
//                + "," + "TR_V_gene");

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class, params.toObjectMap(), outDir, null, token);
        checkExecutionResult(er, false);
    }

    @Test
    public void testSampleMultiVariantFilterAnalysis() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_SampleMultiVariantFilterAnalysis"));
        System.out.println("outDir = " + outDir);
        SampleEligibilityAnalysisParams params = new SampleEligibilityAnalysisParams();
        params.setQuery("(biotype=protein_coding AND ct=missense_variant AND gene=BRCA2) OR (gene=BTN3A2)");

        ExecutionResult er = toolRunner.execute(SampleEligibilityAnalysis.class, params.toObjectMap(), outDir, null, token);
//        checkExecutionResult(er, false);
    }

    public void checkExecutionResult(ExecutionResult er) {
        checkExecutionResult(er, true);
    }

    public void checkExecutionResult(ExecutionResult er, boolean customExecutor) {
        if (customExecutor) {
            if (storageEngine.equals("hadoop")) {
                assertEquals("hbase-mapreduce", er.getExecutor().getId());
            } else {
                assertEquals("mongodb-local", er.getExecutor().getId());
            }
        } else {
            assertEquals("opencga-local", er.getExecutor().getId());
        }
    }
}