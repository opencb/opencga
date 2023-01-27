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
import org.opencb.biodata.models.clinical.qc.HRDetect;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.hrdetect.HRDetectAnalysis;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantSampleIndexOperationTool;
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
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.individual.Location;
import org.opencb.opencga.core.models.operations.variant.VariantSampleIndexParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.variant.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

@RunWith(Parameterized.class)
public class VariantAnalysisTest {

    public static final String USER = "user";
    public static final String PASSWORD = TestParamConstants.PASSWORD;
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = "opencga_test_" + USER + "_" + PROJECT;
    private ToolRunner toolRunner;
    private static String father = "NA19661";
    private static String mother = "NA19660";
    private static String son = "NA19685";
    private static String daughter = "NA19600";

    public static final String CANCER_STUDY = "cancer";
    private static String cancer_sample = "AR2.10039966-01T";
    private static String germline_sample = "AR2.10039966-01G";


    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
//                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
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
//    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    private static String storageEngine;
    private static boolean indexed = false;
    private static String token;
    private static File file;

    @Before
    public void setUp() throws Throwable {
//        System.setProperty("opencga.log.level", "INFO");
//        Configurator.reconfigure();
        if (!indexed) {
            indexed = true;

            opencga.after();
            opencga.before(storageEngine);

            catalogManager = opencga.getCatalogManager();
            variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());


            setUpCatalogManager();

            file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", token);
            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);

            for (int i = 0; i < file.getSampleIds().size(); i++) {
                String id = file.getSampleIds().get(i);
                if (id.equals(son)) {
                    SampleUpdateParams updateParams = new SampleUpdateParams().setSomatic(true);
                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
                }
                if (i % 2 == 0) {
                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
                }
            }

            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c1")
                            .setSamples(file.getSampleIds().subList(0, 2).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
                    null, null, null, token);
            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c2")
                            .setSamples(file.getSampleIds().subList(2, 4).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
                    null, null, null, token);

            Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
            Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
            List<Individual> individuals = new ArrayList<>(4);

            // Father
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(father, father, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.singletonList(father), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
            // Mother
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(mother, mother, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.singletonList(mother), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
            // Son
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(son, son, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(Collections.singletonList(disorder)), Collections.singletonList(son), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
            // Daughter
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(daughter, daughter, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(daughter), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
            catalogManager.getFamilyManager().create(
                    STUDY,
                    new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
                    individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
                    token);

            // Cancer (SV)
            ObjectMap config = new ObjectMap();
//            config.put(VariantStorageOptions.ANNOTATE.key(), true);
            config.put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI);

            File file;
            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T_vs_AR2.10039966-01G.annot.brass.vcf.gz", token);
            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T.copynumber.caveman.vcf.gz", token);
            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T_vs_AR2.10039966-01G.annot.pindel.vcf.gz", token);
            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);

            SampleUpdateParams updateParams = new SampleUpdateParams().setSomatic(true);
            catalogManager.getSampleManager().update(CANCER_STUDY, cancer_sample, updateParams, null, token);

            opencga.getStorageConfiguration().getVariant().setDefaultEngine(storageEngine);
            VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants")).toUri());
            }
        }
        // Reset engines
        opencga.getStorageEngineFactory().close();
        catalogManager = opencga.getCatalogManager();
        variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());
        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));
        token = catalogManager.getUserManager().login("user", PASSWORD).getToken();
    }

    @AfterClass
    public static void afterClass() {
//        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
//            hadoopExternalResource.after();
//        }
        opencga.after();
    }

    public void setUpCatalogManager() throws IOException, CatalogException {
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.FULL, null);
        token = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);

        // Create 10 samples not indexed
        for (int i = 0; i < 10; i++) {
            Sample sample = new Sample().setId("SAMPLE_" + i);
            if (i % 2 == 0) {
                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
            }
            catalogManager.getSampleManager().create(STUDY, sample, null, token);
        }

        // Cancer
        List<Sample> samples = new ArrayList<>();
        catalogManager.getStudyManager().create(projectId, CANCER_STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);
        Sample sample = new Sample().setId(cancer_sample).setSomatic(true);
        samples.add(sample);
//        catalogManager.getSampleManager().create(CANCER_STUDY, sample, null, token);
        sample = new Sample().setId(germline_sample);
        samples.add(sample);
//        catalogManager.getSampleManager().create(CANCER_STUDY, sample, null, token);
        Individual individual = catalogManager.getIndividualManager()
                .create(CANCER_STUDY, new Individual("AR2.10039966-01", "AR2.10039966-01", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
                        samples, false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.emptyList(), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first();
        assertEquals(2, individual.getSamples().size());
    }

    @Test
    public void testMalformedVcfFileIndex() throws Exception {
        File file = opencga.createFile(STUDY, "variant-test-file-corrupted.vcf", token);
        try {
            toolRunner.execute(VariantIndexOperationTool.class,
                    new VariantIndexParams().setFile(file.getId()).setAnnotate(true), new ObjectMap(),
                    Paths.get(opencga.createTmpOutdir()), null, token);
        } catch (ToolException e) {
            System.out.println(ExceptionUtils.prettyExceptionMessage(e, true, true));
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
        FileUtils.lineIterator(file).forEachRemaining(line -> {
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
    public void testSampleStatsSampleFilter() throws Exception {
        Assume.assumeThat(storageEngine, CoreMatchers.is(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));
        sampleVariantStats(null, "stats_filter_GT", false, 1, Collections.singletonList(ParamConstants.ALL), false,
                new Query(VariantQueryParam.SAMPLE_DATA.key(), "GT=1|1"));
        sampleVariantStats(null, "stats_filter_DS", false, 2, Collections.singletonList(ParamConstants.ALL), false,
                new Query(VariantQueryParam.SAMPLE_DATA.key(), "DS>1"));
        sampleVariantStats(null, "stats_filter_DS_GT", false, 3, Collections.singletonList(ParamConstants.ALL), false,
                new Query(VariantQueryParam.SAMPLE_DATA.key(), "DS>1;GT!=1|1"));
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
        return sampleVariantStats(region, indexId, indexOverwrite, expectedStats, samples, nothingToDo, new Query(), true);
    }

    private ExecutionResult sampleVariantStats(String region, String indexId, boolean indexOverwrite, int expectedStats, List<String> samples, boolean nothingToDo,
                                               Query query)
            throws Exception {
        return sampleVariantStats(region, indexId, indexOverwrite, expectedStats, samples, nothingToDo, query, false);
    }

    private ExecutionResult sampleVariantStats(String region, String indexId, boolean indexOverwrite, int expectedStats, List<String> samples, boolean nothingToDo,
                                               Query query, boolean checkRegions)
            throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_sample_stats_" + indexId));
        System.out.println("output = " + outDir.toAbsolutePath());
        SampleVariantStatsAnalysisParams params = new SampleVariantStatsAnalysisParams()
                .setSample(samples)
                .setIndex(indexId != null)
                .setIndexId(indexId)
                .setBatchSize(2)
                .setIndexOverwrite(indexOverwrite);
        params.getVariantQuery()
                .appendQuery(query)
                .setRegion(region);
        ExecutionResult result = toolRunner.execute(SampleVariantStatsAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);

        if (nothingToDo) {
            assertEquals("All samples stats indexed. Nothing to do!", result.getEvents().get(0).getMessage());
        } else {
            checkExecutionResult(result, storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));

            if (checkRegions) {
                List<SampleVariantStats> allStats = JacksonUtils.getDefaultObjectMapper().readerFor(SampleVariantStats.class).<SampleVariantStats>readValues(outDir.resolve("sample-variant-stats.json").toFile()).readAll();
                for (SampleVariantStats sampleVariantStats : allStats) {
//                System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(sampleVariantStats));
                    List<String> expectedRegion = region == null
                            ? Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "X")
                            : Arrays.asList(region.split(","));
                    assertEquals(new HashSet<>(expectedRegion), sampleVariantStats.getChromosomeCount().keySet());
                }
            }
            if (samples.get(0).equals(ParamConstants.ALL)) {
                samples = file.getSampleIds();
            }
            for (String sample : samples) {
                Sample sampleObj = catalogManager.getSampleManager().get(STUDY, sample, QueryOptions.empty(), token).first();
                List<SampleQcVariantStats> variantStats = sampleObj.getQualityControl().getVariant().getVariantStats();
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
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSet.getId(), "", Collections.singletonMap("variantCount", 1))));
        QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, ParamUtils.CompleteUpdateAction.REPLACE));

        catalogManager.getCohortManager()
                .update(STUDY, StudyEntry.DEFAULT_COHORT, updateParams, true, options, token);

        toolParams = new CohortVariantStatsAnalysisParams()
                .setCohort(StudyEntry.DEFAULT_COHORT)
                .setIndex(true);

        outDir = Paths.get(opencga.createTmpOutdir("_cohort_stats_index_2"));
        System.out.println("output = " + outDir.toAbsolutePath());
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
        variantExportParams.setOutputFileName("chr22.vcf");

        toolRunner.execute(VariantExportTool.class,
                variantExportParams.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
        assertTrue(outDir.resolve(variantExportParams.getOutputFileName() + ".gz").toFile().exists());
    }

    @Test
    public void testExportVep() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_export_vep"));
        System.out.println("outDir = " + outDir);
        VariantExportParams variantExportParams = new VariantExportParams();
        variantExportParams.appendQuery(new Query(VariantQueryParam.REGION.key(), "22,1,5"));
        assertEquals("22,1,5", variantExportParams.getRegion());
        variantExportParams.setCt("lof");
        variantExportParams.setOutputFileName("chr1-5-22");
        variantExportParams.setOutputFileFormat(VariantWriterFactory.VariantOutputFormat.ENSEMBL_VEP.name());
        toolRunner.execute(VariantExportTool.class,
                variantExportParams.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
    }

    @Test
    public void testExportTped() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_export_tep"));
        System.out.println("outDir = " + outDir);
        VariantExportParams variantExportParams = new VariantExportParams();
        variantExportParams.setType("SNV");
        variantExportParams.setGenotype(son + ":0/0,0/1,1/1;" + daughter + ":0/0,0/1,1/1");
//        variantExportParams.setGenotype(son + ":0/1,1/1;" + father + ":0/0,0/1,1/1;" + mother + ":0/0,0/1,1/1");
//        variantExportParams.appendQuery(new Query(VariantQueryParam.REGION.key(), "22"));
//        assertEquals("22", variantExportParams.getRegion());

        variantExportParams.setOutputFileFormat(VariantWriterFactory.VariantOutputFormat.TPED.name());
        variantExportParams.setOutputFileName("myTped");

        variantExportParams.setInclude("id,studies.samples");

        toolRunner.execute(VariantExportTool.class,
                variantExportParams.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);

        System.out.println(outDir);
        Path tped = outDir.resolve(variantExportParams.getOutputFileName() + ".tped");
        Path tfam = outDir.resolve(variantExportParams.getOutputFileName() + ".tfam");
        assertTrue(tped.toFile().exists());
        assertTrue(tfam.toFile().exists());

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(tped.toFile())))) {
            br.lines().forEach(line -> {
                assertEquals(4 + 4, line.split("\t").length);
            });
        }
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
        Assume.assumeThat(storageEngine, CoreMatchers.is(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));

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

        variantStorageManager.iterator(new Query(VariantQueryParam.STUDY.key(), STUDY), new QueryOptions(), token).forEachRemaining(variant -> {
            assertEquals("GwasScore", variant.getStudies().get(0).getScores().get(0).getId());
        });
    }

    @Test
    public void testKnockoutGenes() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
        checkExecutionResult(er, false);
    }

    @Test
    public void testKnockoutGenesSpecificGenes() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes_specific_genes"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());
        params.setGene(Arrays.asList("MIR1909", "DZIP3", "BTN3A2", "ITIH5"));

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class,
                params.toObjectMap()
                        .append(ParamConstants.STUDY_PARAM, STUDY)
                        .append("executionMethod", "byGene"), outDir, null, token);
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
        params.setBiotype(VariantAnnotationConstants.PROTEIN_CODING);

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
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

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
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

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
        checkExecutionResult(er, false);
    }

    @Test
    public void testSampleMultiVariantFilterAnalysis() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_SampleMultiVariantFilterAnalysis"));
        System.out.println("outDir = " + outDir);
        SampleEligibilityAnalysisParams params = new SampleEligibilityAnalysisParams();
        params.setQuery("(biotype=protein_coding AND ct=missense_variant AND gene=BRCA2) OR (gene=BTN3A2)");

        ExecutionResult er = toolRunner.execute(SampleEligibilityAnalysis.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
//        checkExecutionResult(er, false);
    }

    @Test
    public void testVariantSecondarySampleIndex() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_VariantSecondarySampleIndex"));
        System.out.println("outDir = " + outDir);
        VariantSampleIndexParams params = new VariantSampleIndexParams();
        params.setFamilyIndex(true);
        params.setSample(Arrays.asList(son, daughter));

        ExecutionResult er = toolRunner.execute(VariantSampleIndexOperationTool.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, token);
//        checkExecutionResult(er, false);
    }

    @Test
    public void testMutationalSignatureFittingSNV() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_mutational_signature_fitting_snv"));
        System.out.println("outDir = " + outDir);

        URI uri = getResourceUri("mutational-signature-catalogue-snv.json");
        Path path = Paths.get(uri.getPath());
        Signature signature = JacksonUtils.getDefaultObjectMapper().readerFor(Signature.class).readValue(path.toFile());
        SampleQualityControl qc = new SampleQualityControl();
        qc.getVariant().setSignatures(Collections.singletonList(signature));
        SampleUpdateParams updateParams = new SampleUpdateParams().setQualityControl(qc);
        catalogManager.getSampleManager().update(CANCER_STUDY, cancer_sample, updateParams, null, token);

        MutationalSignatureAnalysisParams params = new MutationalSignatureAnalysisParams();
        params.setSample(cancer_sample);
        params.setId(signature.getId());
        params.setFitId("fitting-1");
        params.setFitMethod("FitMS");
        params.setFitSigVersion("RefSigv2");
        params.setFitOrgan("Breast");
        params.setFitNBoot(200);
        params.setFitThresholdPerc(5.0f);
        params.setFitThresholdPval(0.05f);
        params.setFitMaxRareSigs(1);
        params.setSkip("catalogue");

        toolRunner.execute(MutationalSignatureAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY),
                outDir, null, token);

        java.io.File catalogueFile = outDir.resolve(MutationalSignatureAnalysis.SIGNATURE_COEFFS_FILENAME).toFile();
        byte[] bytes = Files.readAllBytes(catalogueFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(catalogueFile.exists());

        java.io.File signatureFile = outDir.resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME).toFile();
        bytes = Files.readAllBytes(signatureFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(signatureFile.exists());

        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(CANCER_STUDY, cancer_sample, QueryOptions.empty(), token);
        Sample sample = sampleResult.first();
        List<Signature> signatures = sample.getQualityControl().getVariant().getSignatures();
        for (Signature sig : signatures) {
            if (sig.getId().equals(signature.getId())) {
                for (SignatureFitting fitting : sig.getFittings()) {
                    if (fitting.getId().equals(params.getFitId())) {
                        System.out.println(JacksonUtils.getDefaultObjectMapper().writerFor(SignatureFitting.class).writeValueAsString(fitting));
                        return;
                    }
                }
            }
        }
        fail("Mutational signature fitting not found in sample quality control");
    }

    @Test
    public void testMutationalSignatureCatalogueSV() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_mutational_signature_catalogue_sv"));
        System.out.println("outDir = " + outDir);

        Path opencgaHome = opencga.getOpencgaHome();
        System.out.println("OpenCGA home = " + opencgaHome);

        MutationalSignatureAnalysisParams params = new MutationalSignatureAnalysisParams();
        params.setSample(cancer_sample);
        params.setId("catalogue-1");
        params.setDescription("Catalogue #1");
        VariantQuery query = new VariantQuery();
        query.sample(cancer_sample);
        query.type(VariantType.SV.name());
        params.setQuery(query.toJson());
        params.setSkip("fitting");

        toolRunner.execute(MutationalSignatureAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY),
                outDir, null, token);

        java.io.File catalogueFile = outDir.resolve(MutationalSignatureAnalysis.CATALOGUES_FILENAME_DEFAULT).toFile();
        byte[] bytes = Files.readAllBytes(catalogueFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(catalogueFile.exists());

        java.io.File signatureFile = outDir.resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_DATA_MODEL_FILENAME).toFile();
        bytes = Files.readAllBytes(signatureFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(signatureFile.exists());

        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(CANCER_STUDY, cancer_sample, QueryOptions.empty(), token);
        Sample sample = sampleResult.first();
        List<Signature> signatures = sample.getQualityControl().getVariant().getSignatures();
        for (Signature signature : signatures) {
            if (signature.getId().equals(params.getId())) {
                return;
            }
        }
        fail("Signature not found in sample quality control");
    }

    @Test
    public void testMutationalSignatureFittingSV() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_mutational_signature_fitting"));
        System.out.println("outDir = " + outDir);

        URI uri = getResourceUri("2019_01_10_all_PCAWG_sigs_rearr.tsv");
        Path path = Paths.get(uri.getPath());
        catalogManager.getFileManager().createFolder(CANCER_STUDY, "signature", true, "", new QueryOptions(), token);
        catalogManager.getFileManager().link(CANCER_STUDY, uri, "signature", new ObjectMap(), token);
        String filename = Paths.get(uri.toURL().getFile()).toFile().getName();
        File file = catalogManager.getFileManager().get(CANCER_STUDY, filename, null, token).first();
        String signatureFileId = file.getId();

        uri = getResourceUri("mutational-signature-sv.json");
        path = Paths.get(uri.getPath());
        Signature signature = JacksonUtils.getDefaultObjectMapper().readerFor(Signature.class).readValue(path.toFile());
        SampleQualityControl qc = new SampleQualityControl();
        qc.getVariant().setSignatures(Collections.singletonList(signature));
        SampleUpdateParams updateParams = new SampleUpdateParams().setQualityControl(qc);
        catalogManager.getSampleManager().update(CANCER_STUDY, cancer_sample, updateParams, null, token);

        MutationalSignatureAnalysisParams params = new MutationalSignatureAnalysisParams();
        params.setSample(cancer_sample);
        params.setId(signature.getId());
        params.setFitId("fitting-1");
        params.setFitMethod("FitMS");
        params.setFitSigVersion("RefSigv2");
        params.setFitOrgan("Breast");
        params.setFitNBoot(200);
        params.setFitThresholdPerc(5.0f);
        params.setFitThresholdPval(0.05f);
        params.setFitMaxRareSigs(1);
        params.setFitSignaturesFile(signatureFileId);
        params.setFitRareSignaturesFile(signatureFileId);
        params.setSkip("catalogue");

        toolRunner.execute(MutationalSignatureAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY),
                outDir, null, token);

        java.io.File catalogueFile = outDir.resolve(MutationalSignatureAnalysis.SIGNATURE_COEFFS_FILENAME).toFile();
        byte[] bytes = Files.readAllBytes(catalogueFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(catalogueFile.exists());

        java.io.File signatureFile = outDir.resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME).toFile();
        bytes = Files.readAllBytes(signatureFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(signatureFile.exists());
    }

    @Test
    public void testHRDetect() throws Exception {
        Path snvFittingOutDir = Paths.get(opencga.createTmpOutdir("_snv_fitting"));
        Path svFittingOutDir = Paths.get(opencga.createTmpOutdir("_sv_fitting"));
        Path hrdetectOutDir = Paths.get(opencga.createTmpOutdir("_hrdetect"));

        // Read SNV signaure
        URI uri = getResourceUri("mutational-signature-catalogue-snv.json");
        Path path = Paths.get(uri.getPath());
        Signature snvSignature = JacksonUtils.getDefaultObjectMapper().readerFor(Signature.class).readValue(path.toFile());

        // Read SV signature
        uri = getResourceUri("mutational-signature-sv.json");
        path = Paths.get(uri.getPath());
        Signature svSignature = JacksonUtils.getDefaultObjectMapper().readerFor(Signature.class).readValue(path.toFile());

        // Update quality control for the cancer sample
        SampleQualityControl qc = new SampleQualityControl();
        qc.getVariant().setSignatures(Arrays.asList(snvSignature, svSignature));
        SampleUpdateParams updateParams = new SampleUpdateParams().setQualityControl(qc);
        catalogManager.getSampleManager().update(CANCER_STUDY, cancer_sample, updateParams, null, token);

        // SNV fitting
        MutationalSignatureAnalysisParams params = new MutationalSignatureAnalysisParams();
        params.setSample(cancer_sample);
        params.setId(snvSignature.getId());
        params.setFitId("snv-fitting-1");
        params.setFitMethod("FitMS");
        params.setFitSigVersion("RefSigv2");
        params.setFitOrgan("Breast");
        params.setFitNBoot(100);
        params.setFitThresholdPerc(5.0f);
        params.setFitThresholdPval(0.05f);
        params.setFitMaxRareSigs(1);
        params.setSkip("catalogue");

        toolRunner.execute(MutationalSignatureAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY),
                snvFittingOutDir, null, token);

        java.io.File snvSignatureFittingFile = snvFittingOutDir.resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME).toFile();
        assertTrue(snvSignatureFittingFile.exists());
        SignatureFitting snvFitting = JacksonUtils.getDefaultObjectMapper().readerFor(SignatureFitting.class).readValue(snvSignatureFittingFile);
        assertEquals(params.getFitId(), snvFitting.getId());

        // SV fitting
        uri = getResourceUri("2019_01_10_all_PCAWG_sigs_rearr.tsv");
        path = Paths.get(uri.getPath());
        catalogManager.getFileManager().createFolder(CANCER_STUDY, "signature", true, "", new QueryOptions(), token);
        catalogManager.getFileManager().link(CANCER_STUDY, uri, "signature", new ObjectMap(), token);
        String filename = Paths.get(uri.toURL().getFile()).toFile().getName();
        File file = catalogManager.getFileManager().get(CANCER_STUDY, filename, null, token).first();
        String signatureFileId = file.getId();

        params = new MutationalSignatureAnalysisParams();
        params.setSample(cancer_sample);
        params.setId(svSignature.getId());
        params.setFitId("fitting-sv-1");
        params.setFitMethod("FitMS");
        params.setFitSigVersion("RefSigv2");
        params.setFitOrgan("Breast");
        params.setFitNBoot(100);
        params.setFitThresholdPerc(5.0f);
        params.setFitThresholdPval(0.05f);
        params.setFitMaxRareSigs(1);
        params.setFitSignaturesFile(signatureFileId);
        params.setFitRareSignaturesFile(signatureFileId);
        params.setSkip("catalogue");

        toolRunner.execute(MutationalSignatureAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY),
                svFittingOutDir, null, token);

        java.io.File svSignatureFittingFile = svFittingOutDir.resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME).toFile();
        assertTrue(svSignatureFittingFile.exists());
        SignatureFitting svFitting = JacksonUtils.getDefaultObjectMapper().readerFor(SignatureFitting.class).readValue(svSignatureFittingFile);
        assertEquals(params.getFitId(), svFitting.getId());

        // HRDetect
        HRDetectAnalysisParams hrdParams = new HRDetectAnalysisParams();
        hrdParams.setId("hrd-1");
        hrdParams.setSampleId(cancer_sample);
        hrdParams.setSnvFittingId(snvFitting.getId());
        hrdParams.setSvFittingId(svFitting.getId());
        hrdParams.setCnvQuery("{\"sample\": \"" + cancer_sample + "\", \"type\": \"" + VariantType.CNV + "\"}");
        hrdParams.setIndelQuery("{\"sample\": \"" + cancer_sample + "\", \"type\": \"" + VariantType.INDEL + "\"}");
        hrdParams.setBootstrap(true);

        toolRunner.execute(HRDetectAnalysis.class, hrdParams, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY), hrdetectOutDir, null, token);

        java.io.File hrDetectFile = hrdetectOutDir.resolve(HRDetectAnalysis.HRDETECT_SCORES_FILENAME_DEFAULT).toFile();
        byte[] bytes = Files.readAllBytes(hrDetectFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(hrDetectFile.exists());

        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(CANCER_STUDY, cancer_sample, QueryOptions.empty(), token);
        Sample sample = sampleResult.first();
        List<HRDetect> hrDetects = sample.getQualityControl().getVariant().getHrDetects();
        for (HRDetect hrDetect : hrDetects) {
            if (hrDetect.getId().equals(hrDetect.getId())) {
                if (hrDetect.getScores().containsKey("del.mh.prop")) {
                    Assert.assertEquals(hrDetect.getScores().getFloat("del.mh.prop"), 0.172413793103448f, 0.00001f);
                    return;
                }
            }
        }
        fail("HRDetect result not found in sample quality control");
    }

    @Test
    public void testHRDetectParseResults() throws Exception {
        Path hrdetectOutDir = Paths.get(opencga.createTmpOutdir("_hrdetect"));
        URI uri = getResourceUri("hrdetect_output_38.tsv");
        java.io.File file = Paths.get(uri.getPath()).toFile();
        FileUtils.copyFile(file, hrdetectOutDir.resolve(HRDetectAnalysis.HRDETECT_SCORES_FILENAME_DEFAULT).toFile());

        HRDetectAnalysisParams hrdParams = new HRDetectAnalysisParams();
        hrdParams.setId("hrd-1");
        hrdParams.setSampleId(cancer_sample);
        hrdParams.setSnvFittingId("snvFittingId");
        hrdParams.setSvFittingId("svFittingId");
        hrdParams.setCnvQuery("{\"sample\": \"" + cancer_sample + "\", \"type\": \"" + VariantType.CNV + "\"}");
        hrdParams.setIndelQuery("{\"sample\": \"" + cancer_sample + "\", \"type\": \"" + VariantType.INDEL + "\"}");

        HRDetectAnalysis analysis = new HRDetectAnalysis();
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager,
                hrdParams.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), hrdetectOutDir,
                "job-1", token);
        HRDetect hrDetect = analysis.parseResult(hrdetectOutDir);
        for (Map.Entry<String, Object> entry : hrDetect.getScores().entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        assertTrue(hrDetect.getScores().containsKey("hrd"));
        assertEquals(-0.102769986f, hrDetect.getScores().getFloat("hrd"), 0.00001f);
        assertTrue(hrDetect.getScores().containsKey("Probability"));
        assertEquals(0.998444f, hrDetect.getScores().getFloat("Probability"), 0.00001f);
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