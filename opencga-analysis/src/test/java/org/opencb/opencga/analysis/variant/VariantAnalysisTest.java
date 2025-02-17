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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
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
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisLoadTask;
import org.opencb.opencga.analysis.resource.ResourceFetcherTool;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.hrdetect.HRDetectAnalysis;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.manager.VariantOperationsTest;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.analysis.wrappers.liftover.LiftoverWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisLoadParams;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.individual.Location;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.resource.ResourceFetcherToolParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.variant.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static org.opencb.opencga.core.api.FieldConstants.LIFTOVER_VCF_INPUT_FOLDER;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

@RunWith(Parameterized.class)
@Category(LongTests.class)
public class VariantAnalysisTest {

    public static final String ORGANIZATION = "test";
    public static final String USER = "user";
    public static final String PASSWORD = TestParamConstants.PASSWORD;
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = VariantStorageManager.buildDatabaseName("opencga_test", ORGANIZATION, PROJECT);
    private ToolRunner toolRunner;
    private static String father = "NA19661";
    private static String mother = "NA19660";
    private static String son = "NA19685";
    private static String daughter = "NA19600";

    public static final String CANCER_STUDY = "cancer";
    private static String cancer_sample = "AR2.10039966-01T";
    private static String germline_sample = "AR2.10039966-01G";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
            opencga.after();
            opencga.before(storageEngine);

            catalogManager = opencga.getCatalogManager();
            variantStorageManager = opencga.getVariantStorageManager();
            variantStorageManager.getStorageConfiguration().setMode(StorageConfiguration.Mode.READ_WRITE);


            setUpCatalogManager();

            VariantOperationsTest.dummyVariantSetup(variantStorageManager, STUDY, token);
            VariantOperationsTest.dummyVariantSetup(variantStorageManager, CANCER_STUDY, token);

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
            Disorder disorder1 = new Disorder("disorder id 1", "disorder name 1", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
            Disorder disorder2 = new Disorder("disorder id 2", "disorder name 2", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
            List<Disorder> disorderList = new ArrayList<>(Arrays.asList(disorder1, disorder2));
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
                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(disorderList), Collections.singletonList(son), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
            // Daughter
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(daughter, daughter, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(daughter), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
            catalogManager.getFamilyManager().create(
                    STUDY,
                    new Family("f1", "f1", Collections.singletonList(phenotype), disorderList, null, null, 3, null, null),
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
            indexed = true;
        }
        // Reset engines
        opencga.getStorageEngineFactory().close();
        catalogManager = opencga.getCatalogManager();
        variantStorageManager = opencga.getVariantStorageManager();
        variantStorageManager.getStorageConfiguration().setMode(StorageConfiguration.Mode.READ_ONLY);
        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();
    }

    @AfterClass
    public static void afterClass() {
//        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
//            hadoopExternalResource.after();
//        }
        opencga.after();
    }

    public void setUpCatalogManager() throws IOException, CatalogException {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(ORGANIZATION), QueryOptions.empty(), opencga.getAdminToken());
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, ORGANIZATION, null, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update(ORGANIZATION, new OrganizationUpdateParams().setAdmins(Collections.singletonList(USER)),
                null,
                opencga.getAdminToken());
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(new ProjectCreateParams()
                        .setId(PROJECT)
                        .setDescription("Project about some genomes")
                        .setOrganism(new ProjectOrganism("hsapiens", "GRCh38")),
                new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
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
                    new VariantIndexParams().setFile(file.getId()).setAnnotate(true),
                    Paths.get(opencga.createTmpOutdir()), null, false, token);
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
        variantStatsAnalysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", false, token);

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
        variantStatsAnalysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", false, token);

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
        VariantStatsAnalysisParams params = new VariantStatsAnalysisParams()
                .setSamples(samples.subList(1, 3))
                .setRegion(region);
        ExecutionResult ar = toolRunner.execute(VariantStatsAnalysis.class, STUDY, params, outDir, "", false, token);
        checkExecutionResult(ar);

        MutableInt count = new MutableInt();
        java.io.File file = getOutputFile(outDir);

        FileUtils.lineIterator(file).forEachRemaining(line -> {
            if (!line.startsWith("#")) {
                count.increment();
            }
        });
        Query variantsQuery = new VariantQuery().region(region).study(STUDY);
        assertEquals(variantStorageManager.count(variantsQuery, token).first().intValue(),
                count.intValue());
    }

    private java.io.File getOutputFile(Path outDir) {
        return FileUtils.listFiles(outDir.toFile(), null, false)
                .stream()
                .filter(f -> !ExecutionResultManager.isExecutionResultFile(f.getName()))
                .findFirst().orElse(null);
    }

    @Test
    public void testSampleStatsSampleFilter() throws Exception {
        Assume.assumeThat(storageEngine, CoreMatchers.is(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));
        // Reset quality control stats
        for (Sample sample : catalogManager.getSampleManager().search(STUDY, new Query(), new QueryOptions(), token).getResults()) {
            SampleQualityControl qualityControl = sample.getQualityControl();
            if (qualityControl != null && qualityControl.getVariant() != null && CollectionUtils.isNotEmpty(qualityControl.getVariant().getVariantStats())) {
                qualityControl.getVariant().setVariantStats(Collections.emptyList());
                catalogManager.getSampleManager().update(STUDY, sample.getId(), new SampleUpdateParams()
                        .setQualityControl(qualityControl), new QueryOptions(), token);
            }
        }
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
        ExecutionResult result = toolRunner.execute(SampleVariantStatsAnalysis.class, STUDY, params, outDir, null, false, token);

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
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", false, token);
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

        ExecutionResult result = toolRunner.execute(CohortVariantStatsAnalysis.class, STUDY, toolParams,
                outDir, null, false, token);
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
        result = toolRunner.execute(CohortVariantStatsAnalysis.class, STUDY, toolParams, outDir, null, false, token);
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
                variantExportParams.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);
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
                variantExportParams.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);
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
                variantExportParams.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);

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
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", false, token);
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
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", false, token);

        analysis.setStudy(STUDY)
                .setPhenotype(PHENOTYPE_NAME);
        checkExecutionResult(analysis.start(), storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));
    }

    @Test
    public void testKnockoutGenes() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes"));
        System.out.println("outDir = " + outDir);
        KnockoutAnalysisParams params = new KnockoutAnalysisParams();
        params.setSample(file.getSampleIds());

        ExecutionResult er = toolRunner.execute(KnockoutAnalysis.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);
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
                        .append("executionMethod", "byGene"), outDir, null, false, token);
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
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);
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
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);
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
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);
        checkExecutionResult(er, false);
    }

    @Test
    public void testSampleMultiVariantFilterAnalysis() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_SampleMultiVariantFilterAnalysis"));
        System.out.println("outDir = " + outDir);
        SampleEligibilityAnalysisParams params = new SampleEligibilityAnalysisParams();
        params.setQuery("(biotype=protein_coding AND ct=missense_variant AND gene=BRCA2) OR (gene=BTN3A2)");

        ExecutionResult er = toolRunner.execute(SampleEligibilityAnalysis.class,
                params.toObjectMap().append(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);
//        checkExecutionResult(er, false);
    }

    @Test
    public void testMutationalSignatureFittingSNV() throws Exception {
        Assume.assumeTrue(Files.exists(opencga.getOpencgaHome().resolve(ResourceManager.ANALYSIS_DIRNAME).resolve(ResourceManager.RESOURCES_DIRNAME).resolve(ResourceManager.REFERENCE_GENOMES)));

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
                outDir, null, false, token);

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
        Assume.assumeTrue(Files.exists(opencga.getOpencgaHome().resolve(ResourceManager.ANALYSIS_DIRNAME).resolve(ResourceManager.RESOURCES_DIRNAME).resolve(ResourceManager.REFERENCE_GENOMES)));

        Path outDir = Paths.get(opencga.createTmpOutdir("_mutational_signature_catalogue_sv"));
        System.out.println("outDir = " + outDir);

        Path opencgaHome = opencga.getOpencgaHome().toAbsolutePath();
        System.out.println("OpenCGA home = " + opencgaHome);

        MutationalSignatureAnalysisParams params = new MutationalSignatureAnalysisParams();
        params.setSample(cancer_sample);
        params.setId("catalogue-1");
        params.setDescription("Catalogue #1");
        VariantQuery query = new VariantQuery()
                .sample(cancer_sample)
                .type(VariantType.SV.name())
                //.file("AR2.10039966-01T_vs_AR2.10039966-01G.annot.brass.vcf.gz");
                .fileData("AR2.10039966-01T_vs_AR2.10039966-01G.annot.brass.vcf.gz:BAS>=0;BKDIST>=-1")
                .region("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y");

        //https://ws.opencb.org/opencga-test/webservices/rest/v2/analysis/variant/mutationalSignature/query
        // ?study=serena@cancer38:test38
        // &fitting=false
        // &sample=AR2.10039966-01T
        // &fileData=AR2.10039966-01T_vs_AR2.10039966-01G.annot.brass.vcf.gz:BAS>=0;BKDIST>=-1;EXT_PS_SOM>=4;EXT_RC_SOM>=0
        // &region=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y
        // &type=SV


        params.setQuery(query.toJson());
        params.setSkip("fitting");

        toolRunner.execute(MutationalSignatureAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY),
                outDir, null, false, token);

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
        Assume.assumeTrue(Files.exists(opencga.getOpencgaHome().resolve(ResourceManager.ANALYSIS_DIRNAME).resolve(ResourceManager.RESOURCES_DIRNAME).resolve(ResourceManager.REFERENCE_GENOMES)));

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
                outDir, null, false, token);

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
        Assume.assumeTrue(Files.exists(opencga.getOpencgaHome().resolve(ResourceManager.ANALYSIS_DIRNAME).resolve(ResourceManager.RESOURCES_DIRNAME).resolve(ResourceManager.REFERENCE_GENOMES)));

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
                snvFittingOutDir, null, false, token);

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
                svFittingOutDir, null, false, token);

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

        toolRunner.execute(HRDetectAnalysis.class, hrdParams, new ObjectMap(ParamConstants.STUDY_PARAM, CANCER_STUDY), hrdetectOutDir, null, false, token);

        java.io.File hrDetectFile = hrdetectOutDir.resolve(HRDetectAnalysis.HRDETECT_SCORES_FILENAME_DEFAULT).toFile();
        assertTrue("File missing : " + hrDetectFile, hrDetectFile.exists());
        byte[] bytes = Files.readAllBytes(hrDetectFile.toPath());
        System.out.println(new String(bytes));
        assertTrue(hrDetectFile.exists());

        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(CANCER_STUDY, cancer_sample, QueryOptions.empty(), token);
        Sample sample = sampleResult.first();
        List<HRDetect> hrDetects = sample.getQualityControl().getVariant().getHrDetects();
        for (HRDetect hrDetect : hrDetects) {
            if (hrDetect.getId().equals(hrDetect.getId())) {
                System.out.println("HRDetect scores for " + hrDetect.getId());
                for (Map.Entry<String, Object> entry : hrDetect.getScores().entrySet()) {
                    System.out.println("\t" + entry.getKey() + ": " + entry.getValue());
                }
                if (hrDetect.getScores().containsKey("del.mh.prop")) {
                    Assert.assertEquals(-1.5702984, hrDetect.getScores().getFloat("del.mh.prop"), 0.00001f);
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

        HRDetect hrDetect = HRDetectAnalysis.parseResult(hrdParams, hrdetectOutDir);
        for (Map.Entry<String, Object> entry : hrDetect.getScores().entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        assertTrue(hrDetect.getScores().containsKey("hrd"));
        assertEquals(-1.95208666666667, hrDetect.getScores().getFloat("hrd"), 0.00001f);
        assertTrue(hrDetect.getScores().containsKey("Probability"));
        assertEquals(4.21293910790655e-05, hrDetect.getScores().getFloat("Probability"), 0.00001f);
    }

    @Test
    public void testPedigreeGraph() throws CatalogException {
        String base64 = "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";

        OpenCGAResult<Family> results = catalogManager.getFamilyManager().get(STUDY, "f1", QueryOptions.empty(), token);
        Family family = results.first();

        assertNotNull(family.getPedigreeGraph());
        assertEquals(base64, family.getPedigreeGraph().getBase64());
    }

    @Test
    public void testClinicalAnalysisLoading() throws IOException, ToolException, CatalogException {
        String fileStr = "clinical_analyses.json.gz";
        File file;
        try (InputStream stream = getClass().getResourceAsStream("/biofiles/" + fileStr)) {
            file = catalogManager.getFileManager().upload(CANCER_STUDY, stream, new File().setPath("biofiles/" + fileStr), false, true, false, token).first();
        }

        System.out.println("file ID = " + file.getId());
        System.out.println("file name = " + file.getName());

        // Run clinical analysis load task
        Path loadingOutDir = Paths.get(opencga.createTmpOutdir("_clinical_analysis_outdir"));
        System.out.println("Clinical analysis load task out dir = " + loadingOutDir);

        ClinicalAnalysisLoadParams params = new ClinicalAnalysisLoadParams();
        params.setFile(file.getId());

        toolRunner.execute(ClinicalAnalysisLoadTask.class, params, new ObjectMap(ParamConstants.STUDY_PARAM,
                CANCER_STUDY), loadingOutDir, null, false, token);

        String ca1Id = "SAP-45016-1";
        String ca2Id = "OPA-6607-1";

        Query query = new Query();
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().search(CANCER_STUDY, query, QueryOptions.empty(),
                token);
        Assert.assertTrue(result.getResults().stream().map(ca -> ca.getId()).collect(Collectors.toList()).contains(ca1Id));
        Assert.assertTrue(result.getResults().stream().map(ca -> ca.getId()).collect(Collectors.toList()).contains(ca2Id));

        query.put("id", ca1Id);
        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().search(CANCER_STUDY, query, QueryOptions.empty(),
                token).first();
        Assert.assertEquals(ca1Id, clinicalAnalysis.getId());

        query.put("id", ca2Id);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().search(CANCER_STUDY, query, QueryOptions.empty(),
                token).first();
        Assert.assertEquals(ca2Id, clinicalAnalysis.getId());
    }

    @Test
    public void testCellbaseConfigure() throws Exception {
        String project = "Project_test_cellbase_configure";
        catalogManager.getProjectManager().create(new ProjectCreateParams(project, project, "", "", "", new ProjectOrganism("hsapiens", "grch38"), null, null), QueryOptions.empty(), token);

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("The storage engine is in mode=READ_ONLY");
        variantStorageManager.setCellbaseConfiguration(project, new CellBaseConfiguration("https://uk.ws.zettagenomics.com/cellbase/", "v5.2", "1", ""), false, null, token);
    }

    @Test
    public void testLiftoverDestinationJobDir() throws IOException, ToolException, CatalogException {
        // Run clinical analysis load task
        Path liftOutdir = Paths.get(opencga.createTmpOutdir("_liftOutdir"));
        System.out.println("Liftover outdir = " + liftOutdir);

        Assume.assumeTrue(areLiftoverResourcesReady());

        String basename = "NA12877_S1.1k";
        File file = prepareLiftoverInputFile(basename + ".vcf.gz", "biofiles");

        LiftoverWrapperParams params = new LiftoverWrapperParams()
                .setFiles(Collections.singletonList(file.getName()))
                .setTargetAssembly("hg38");

        toolRunner.execute(LiftoverWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), liftOutdir, null, false, token);

        Assert.assertTrue(Files.exists(liftOutdir.resolve(basename + ".hg38.liftover.vcf.gz")));
        Assert.assertTrue(liftOutdir.resolve(basename + ".hg38.liftover.vcf.gz").toFile().length() > 0);
    }

    @Test
    public void testLiftoverDestinationVcfInputFolder() throws IOException, ToolException, CatalogException {
        // Run clinical analysis load task
        Path liftOutdir = Paths.get(opencga.createTmpOutdir("_liftOutdir"));
        System.out.println("Liftover outdir = " + liftOutdir);

        Assume.assumeTrue(areLiftoverResourcesReady());

        String basename = "NA12877_S1.1k";
        File file = prepareLiftoverInputFile(basename + ".vcf.gz", "biofiles");

        LiftoverWrapperParams params = new LiftoverWrapperParams()
                .setFiles(Collections.singletonList(file.getName()))
                .setTargetAssembly("hg38")
                .setVcfDestination(LIFTOVER_VCF_INPUT_FOLDER);

        toolRunner.execute(LiftoverWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), liftOutdir, null, false, token);

        Assert.assertTrue(Files.exists(Paths.get(file.getUri().getPath()).getParent().resolve(basename + ".hg38.liftover.vcf.gz")));
        Assert.assertTrue(Paths.get(file.getUri().getPath()).getParent().resolve(basename + ".hg38.liftover.vcf.gz").toFile().length() > 0);
    }

    @Test
    public void testLiftoverDestinationUserFolder() throws IOException, ToolException, CatalogException {
        // Run clinical analysis load task
        Path liftOutdir = Paths.get(opencga.createTmpOutdir("_liftOutdir"));
        System.out.println("Liftover outdir = " + liftOutdir);

        Assume.assumeTrue(areLiftoverResourcesReady());

        Path folderPath = Paths.get("custom", "folder");
        File destCustomFolder = catalogManager.getFileManager().createFolder(STUDY, folderPath.toString(), true, null, QueryOptions.empty(),
                token).first();
        System.out.println("destCustomFolder = " + destCustomFolder);
        catalogManager.getIoManagerFactory().get(destCustomFolder.getUri()).createDirectory(destCustomFolder.getUri(), true);

        String basename = "NA12877_S1.1k";
        File file = prepareLiftoverInputFile(basename + ".vcf.gz", "biofiles");

        LiftoverWrapperParams params = new LiftoverWrapperParams()
                .setFiles(Collections.singletonList(file.getName()))
                .setTargetAssembly("hg38")
                .setVcfDestination(destCustomFolder.getPath());

        toolRunner.execute(LiftoverWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), liftOutdir, null, false, token);

        Assert.assertTrue(Files.exists(Paths.get(destCustomFolder.getUri().getPath()).resolve(basename + ".hg38.liftover.vcf.gz")));
        Assert.assertTrue(Paths.get(destCustomFolder.getUri().getPath()).resolve(basename + ".hg38.liftover.vcf.gz").toFile().length() > 0);
    }

    //-------------------------------------------------------------------------
    // Utilities
    //-------------------------------------------------------------------------

    private File prepareLiftoverInputFile(String filename, String folder) throws IOException, CatalogException {
        File file;
        try {
            file = catalogManager.getFileManager().get(STUDY, filename, QueryOptions.empty(), token).first();
        } catch (CatalogException e) {
            file = null;
        }
        if (file == null) {
            try (InputStream stream = getClass().getResourceAsStream("/" + folder + "/" + filename)) {
                file = catalogManager.getFileManager().upload(STUDY, stream, new File().setPath(folder + "/" + filename), false, true, false, token).first();
            }
        }
        return file;
    }

    private boolean areLiftoverResourcesReady() throws IOException {
        Configuration configuration = opencga.getConfiguration();
        configuration.getAnalysis().getResource().setBasePath(opencga.getOpencgaHome().resolve(ResourceManager.ANALYSIS_DIRNAME).resolve(ResourceManager.RESOURCES_DIRNAME));
        configuration.getAnalysis().getResource().setBaseUrl("http://resources.opencb.org/opencb/opencga/analysis/resources/");
        JacksonUtils.getDefaultObjectMapper().writerFor(Configuration.class).writeValue(opencga.getOpencgaHome().resolve("conf/configuration.yml").toFile(), configuration);

        ResourceManager resourceManager = new ResourceManager(opencga.getOpencgaHome());

        try {
            resourceManager.checkResourcePath("REFERENCE_GENOME_HG38_FA");
            resourceManager.checkResourcePath("REFERENCE_GENOME_HG19_FA");
            resourceManager.checkResourcePath("REFERENCE_GENOME_HG38_CHAIN");
            return true;
        } catch (ResourceException e) {
            System.out.println("First checking if Liftover resources are ready, failed. So they will be downloaded");
        }

        try {
            ResourceFetcherToolParams params = new ResourceFetcherToolParams()
                    .setResources(Arrays.asList("REFERENCE_GENOME_HG*"));

            Path fetcherOutdir = Paths.get(opencga.createTmpOutdir());
            toolRunner.execute(ResourceFetcherTool.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, ParamConstants.ADMIN_STUDY),
                    fetcherOutdir, null, false, opencga.getAdminToken());

            System.out.println("fetcherOutdir = " + fetcherOutdir);

            resourceManager.checkResourcePath("REFERENCE_GENOME_HG38_FA");
            resourceManager.checkResourcePath("REFERENCE_GENOME_HG19_FA");
            resourceManager.checkResourcePath("REFERENCE_GENOME_HG38_CHAIN");
            return true;
        } catch (ResourceException | ToolException e) {
            e.printStackTrace();
            System.out.println("Error downloading Liftover resources via ResourceFetcherTool, so JUnit tests won't be executed");
            return false;
        }
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