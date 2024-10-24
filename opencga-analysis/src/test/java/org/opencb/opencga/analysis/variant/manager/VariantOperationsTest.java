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

package org.opencb.opencga.analysis.variant.manager;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.operations.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.common.IndexStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.*;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.study.VariantSetupResult;
import org.opencb.opencga.core.models.variant.VariantSetupParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
@Category(LongTests.class)
public class VariantOperationsTest {

    public static final String ORGANIZATION = "test";
    public static final String USER = "user";
    public static final String PASSWORD = TestParamConstants.PASSWORD;
    public static final String PROJECT = "project";
    public static final String PROJECT_FQN = ORGANIZATION + '@' + PROJECT;
    public static final String STUDY = "study";
    public static final String STUDY_FQN = PROJECT_FQN + ':' + STUDY;
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = VariantStorageManager.buildDatabaseName("opencga_test", ORGANIZATION, PROJECT);
    private static String father = "NA19661";
    private static String mother = "NA19660";
    private static String son = "NA19685";
    private static String daughter = "NA19600";
    private static List<String> samples = Arrays.asList(father, mother, son, daughter);


    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
//                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {DummyVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public VariantOperationsTest(String storageEngine) {
//        if (!storageEngine.equals(VariantOperationsTest.storageEngine)) {
//            indexed = false;
//        }
        VariantOperationsTest.storageEngine = storageEngine;
    }


    private static CatalogManager catalogManager;
    private static VariantStorageManager variantStorageManager;
    private static ToolRunner toolRunner;

    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();
    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource;

    public static VariantSolrExternalResource solrExternalResource = new VariantSolrExternalResource();

    private static String storageEngine;
//    private static boolean indexed = false;
    private static String token;
    private static File file;

    @Before
    public void setUp() throws Throwable {
//        System.setProperty("opencga.log.level", "INFO");
//        Configurator.reconfigure();
//        if (!indexed) {
//            indexed = true;
//            loadDataset();
//        }
        loadDataset();

        // Reset engines
        opencga.getStorageEngineFactory().close();
//        catalogManager = opencga.getCatalogManager();
//        variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());
//        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();
    }

    @After
    public void tearDown() {
        if (hadoopExternalResource != null) {

            try {
                VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
                if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                    VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants_AFTER")).toUri());
                }
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).error("Ignoring exception printing variants", e);
            }

            hadoopExternalResource.after();
            hadoopExternalResource = null;
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable()) {
            solrExternalResource.before();
        }
    }

    @AfterClass
    public static void afterClass() {
        opencga.after();
        if (hadoopExternalResource != null) {
            hadoopExternalResource.after();
            hadoopExternalResource = null;
        }
        if (HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable()) {
            solrExternalResource.after();
        }
    }

    private void loadDataset() throws Throwable {
        opencga.after();
        opencga.before();
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            if (hadoopExternalResource == null) {
                hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();
                hadoopExternalResource.before();
            } else {
                hadoopExternalResource.clearHBase();
            }
        } else if (storageEngine.equals(DummyVariantStorageEngine.STORAGE_ENGINE_ID)) {
            DummyVariantStorageEngine.configure(opencga.getStorageEngineFactory(), true);
        }

        catalogManager = opencga.getCatalogManager();
        if (HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable()) {
            variantStorageManager = opencga.getVariantStorageManager(solrExternalResource);
        } else {
            variantStorageManager = opencga.getVariantStorageManager();
        }
        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager);

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
        if (HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable()) {
        solrExternalResource.configure(variantStorageManager.getVariantStorageEngine(STUDY, token));
            solrExternalResource.configure(variantStorageManager.getVariantStorageEngineForStudyOperation(STUDY, new ObjectMap(), token));
        }

        dummyVariantSetup(variantStorageManager, STUDY, token);

        file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", token);
//            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);
        toolRunner.execute(VariantIndexOperationTool.class, STUDY,
                new VariantIndexParams()
                        .setFile(file.getId())
                        .setAnnotate(false)
                        .setLoadHomRef(YesNoAuto.YES.name()),
                Paths.get(opencga.createTmpOutdir("_index")), "index", false, token);
        toolRunner.execute(VariantAnnotationIndexOperationTool.class, STUDY,
                new VariantAnnotationIndexParams(),
                Paths.get(opencga.createTmpOutdir("_annotation-index")), "index", false, token);

        for (int i = 0; i < file.getSampleIds().size(); i++) {
            if (i % 2 == 0) {
                String id = file.getSampleIds().get(i);
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
                new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 4, null, null),
                individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
                token);


        opencga.getStorageConfiguration().getVariant().setDefaultEngine(storageEngine);
        VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants")).toUri());
        }
    }

    public static void dummyVariantSetup(VariantStorageManager variantStorageManager, String study, String token)
            throws CatalogException, StorageEngineException {
        variantStorageManager.variantSetup(study, new VariantSetupParams()
                        .setAverageFileSize("100B")
                        .setExpectedFiles(5)
                        .setExpectedSamples(5)
                        .setVariantsPerSample(1000), token);
    }

    public void setUpCatalogManager() throws Exception {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(ORGANIZATION), QueryOptions.empty(),
                opencga.getAdminToken());
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, ORGANIZATION, null, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update(ORGANIZATION, new OrganizationUpdateParams().setAdmins(Collections.singletonList("user")),
                null,
                opencga.getAdminToken());
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();

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

    }

    @Test
    public void testSetup() throws Exception {
        String study2 = "study2";
        String study2fqn = catalogManager.getStudyManager()
                .create(PROJECT, study2, null, "Phase 1", "Done", null, null, null, null, null, token)
                .first().getFqn();
        File file = opencga.createFile(study2, "variant-test-file.vcf.gz", token);

        try {
            toolRunner.execute(VariantIndexOperationTool.class, study2,
                    new VariantIndexParams()
                            .setFile(file.getId())
                            .setAnnotate(false)
                            .setLoadHomRef(YesNoAuto.YES.name()),
                    Paths.get(opencga.createTmpOutdir("_index")), "index", false, token);
            fail("Should have thrown an exception");
        } catch (ToolException e) {
            MatcherAssert.assertThat(e.getCause().getMessage(), CoreMatchers.containsString("The variant storage has not been setup for study"));
        }

        try {
            VariantSetupParams setupParams = new VariantSetupParams()
                    .setFileType(VariantSetupParams.FileType.GENOME_VCF)
                    .setDataDistribution(VariantSetupParams.DataDistribution.MULTIPLE_SAMPLES_PER_FILE)
                    .setExpectedFiles(20)
                    .setExpectedSamples(100)
                    .setNormalizeExtensions(Arrays.asList("VS", "SV"));
            variantStorageManager.variantSetup(study2, setupParams, token);
            fail("should have failed");
        } catch (Exception e) {
            System.err.println(e.getMessage());
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("Unsupported normalize extensions"));
        }

        try {
            VariantSetupParams setupParams = new VariantSetupParams()
                    .setFileType(VariantSetupParams.FileType.GENOME_VCF)
                    .setDataDistribution(VariantSetupParams.DataDistribution.MULTIPLE_SAMPLES_PER_FILE)
                    .setExpectedSamples(100)
                    .setNormalizeExtensions(Arrays.asList("VS", "SV"));
            variantStorageManager.variantSetup(study2, setupParams, token);
            fail("should have failed");
        } catch (Exception e) {
            System.err.println(e.getMessage());
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("Missing expectedFiles"));
        }

        VariantSetupParams setupParams = new VariantSetupParams()
                .setFileType(VariantSetupParams.FileType.GENOME_VCF)
                .setDataDistribution(VariantSetupParams.DataDistribution.MULTIPLE_FILES_PER_SAMPLE)
                .setExpectedFiles(20)
                .setAverageSamplesPerFile(2.5f)
                .setExpectedSamples(10)
                .setNormalizeExtensions(Arrays.asList("SV", "VAF"));
        VariantSetupResult result = variantStorageManager.variantSetup(study2, setupParams, token);
        assertEquals(VariantSetupResult.Status.READY, result.getStatus());

        toolRunner.execute(VariantIndexOperationTool.class, study2,
                new VariantIndexParams()
                        .setFile(file.getId())
                        .setLoadHomRef(YesNoAuto.YES.name()),
                Paths.get(opencga.createTmpOutdir("_index")), "index", false, token);

        VariantStorageMetadataManager metadataManager = opencga.getVariantStorageEngineByProject(PROJECT_FQN).getMetadataManager();
        int studyId = metadataManager.getStudyId(study2fqn);
        int sampleId = metadataManager.getSampleId(studyId, "NA19600");
        SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
        assertEquals(VariantStorageEngine.SplitData.MULTI, sampleMetadata.getSplitData());

        try {
            variantStorageManager.variantSetup(STUDY, setupParams, token);
            fail("Should fail");
        } catch (Exception e) {
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("Unable to execute variant-setup on study"));
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("It already has indexed files."));
        }
    }

    @Test
    public void testVariantFileReload() throws Exception {
        try {
            toolRunner.execute(VariantIndexOperationTool.class, STUDY,
                    new VariantIndexParams()
                            .setForceReload(false)
                            .setFile(file.getId()),
                    Paths.get(opencga.createTmpOutdir()), "index_reload", false, token);
            fail("Should have thrown an exception");
        } catch (ToolException e) {
            assertEquals(StorageEngineException.class, e.getCause().getClass());
            assertEquals("We can only INDEX VCF files not transformed, the status is READY", e.getCause().getMessage());
        }

        toolRunner.execute(VariantIndexOperationTool.class, STUDY,
                new VariantIndexParams()
                        .setForceReload(true)
                        .setFile(file.getId()),
                Paths.get(opencga.createTmpOutdir()), "index_reload", false, token);

    }

    @Test
    public void testVariantSecondaryAnnotationIndex() throws Exception {
        Assume.assumeTrue(HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable());
        for (String sample : samples) {
            SampleInternalVariantSecondaryAnnotationIndex index = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex();
            assertEquals(IndexStatus.NONE, index.getStatus().getId());
        }
        assertEquals(IndexStatus.NONE, catalogManager.getFileManager().get(STUDY, file.getId(), new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());

        toolRunner.execute(VariantSecondaryAnnotationIndexOperationTool.class, STUDY,
                new VariantSecondaryAnnotationIndexParams(),
                Paths.get(opencga.createTmpOutdir()), "annotation_index", false, token);

        for (String sample : samples) {
            SampleInternalVariantSecondaryAnnotationIndex index = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex();
            assertEquals(IndexStatus.READY, index.getStatus().getId());
        }
        assertEquals(IndexStatus.READY, catalogManager.getFileManager().get(STUDY, file.getId(), new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());

    }

    @Test
    public void testVariantSecondarySampleIndex() throws Exception {
        Assume.assumeThat(storageEngine, anyOf(
//                is(DummyVariantStorageEngine.STORAGE_ENGINE_ID),
                is(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)
        ));
        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(sample, IndexStatus.READY, sampleIndex.getStatus().getId());
            assertEquals(sample, IndexStatus.NONE, sampleIndex.getFamilyStatus().getId());
            assertEquals(sample, 1, sampleIndex.getVersion().intValue());
        }

        try {
            toolRunner.execute(VariantSecondarySampleIndexOperationTool.class, STUDY,
                    new VariantSecondarySampleIndexParams()
                            .setFamilyIndex(true)
                            .setSample(Arrays.asList(mother)),
                    Paths.get(opencga.createTmpOutdir()), "index", false, token);
            fail("Expected to fail");
        } catch (ToolException e) {
            assertEquals("Exception from step 'familyIndex'", e.getMessage());
            assertEquals("No trios found for samples [" + mother + "]", e.getCause().getMessage());
        }

        // Run family index. The family index status should be READY on offspring
        ExecutionResult result = toolRunner.execute(VariantSecondarySampleIndexOperationTool.class, STUDY,
                new VariantSecondarySampleIndexParams()
                        .setFamilyIndex(true)
                        .setSample(Arrays.asList(ParamConstants.ALL)),
                Paths.get(opencga.createTmpOutdir()), "index", false, token);
        assertEquals(0, result.getEvents().size());

        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(sample, IndexStatus.READY, sampleIndex.getStatus().getId());
            if (sample.equals(daughter) || sample.equals(son)) {
                assertEquals(sample, IndexStatus.READY, sampleIndex.getFamilyStatus().getId());
            }
            assertEquals(sample, 1, sampleIndex.getVersion().intValue());
        }

        // Change the sample index configuration.
        OpenCGAResult<Job> jobs = variantStorageManager.configureSampleIndex(STUDY, SampleIndexConfiguration.defaultConfiguration()
                .addPopulation(new SampleIndexConfiguration.Population("1000G", "SAS")), false, token);

        // Initially nothing should change, even after running a manual synchronization
        toolRunner.execute(VariantStorageMetadataSynchronizeOperationTool.class,
                new VariantStorageMetadataSynchronizeParams().setStudy(STUDY_FQN),
                Paths.get(opencga.createTmpOutdir()), "", false, catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken());

        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token)
                    .first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(IndexStatus.READY, sampleIndex.getStatus().getId());
            if (sample.equals(daughter) || sample.equals(son)) {
                assertEquals(sample, IndexStatus.READY, sampleIndex.getFamilyStatus().getId());
            }
            assertEquals(1, sampleIndex.getVersion().intValue());
        }
        // Run reconfiguration jobs
        for (Job job : jobs.getResults()) {
            toolRunner.execute(job, Paths.get(opencga.createTmpOutdir()), token);
        }

        // Everything should look the same, but with newer version
        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager()
                    .get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(IndexStatus.READY, sampleIndex.getStatus().getId());
            if (sample.equals(daughter) || sample.equals(son)) {
                assertEquals(sample, IndexStatus.READY, sampleIndex.getFamilyStatus().getId());
            }
            assertEquals(2, sampleIndex.getVersion().intValue());
        }

        // Same. Rerun configuration and change version.
        jobs = variantStorageManager.configureSampleIndex(STUDY, SampleIndexConfiguration.defaultConfiguration(), false, token);
        for (Job job : jobs.getResults()) {
            toolRunner.execute(job, Paths.get(opencga.createTmpOutdir()), token);
        }
        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(IndexStatus.READY, sampleIndex.getStatus().getId());
            if (sample.equals(daughter) || sample.equals(son)) {
                assertEquals(sample, IndexStatus.READY, sampleIndex.getFamilyStatus().getId());
            }
            assertEquals(3, sampleIndex.getVersion().intValue());
        }
    }

    @Test
    public void testVariantSecondarySampleIndexPartialFamily() throws Exception {
        Assume.assumeThat(storageEngine, anyOf(
//                is(DummyVariantStorageEngine.STORAGE_ENGINE_ID),
                is(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)
        ));
        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(sample, IndexStatus.READY, sampleIndex.getStatus().getId());
            assertEquals(sample, IndexStatus.NONE, sampleIndex.getFamilyStatus().getId());
            assertEquals(sample, 1, sampleIndex.getVersion().intValue());
        }

        Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
        Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());

        catalogManager.getFamilyManager().delete(STUDY, Collections.singletonList("f1"), null, token);
        catalogManager.getIndividualManager().update(STUDY, daughter, new IndividualUpdateParams()
                .setMother(new IndividualReferenceParam(null, null)), null, token);
        catalogManager.getFamilyManager().create(
                STUDY,
                new Family("f2", "f2", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 2, null, null),
                Arrays.asList(father, daughter), new QueryOptions(),
                token);

        // Run family index. The family index status should be READY on offspring
        toolRunner.execute(VariantSecondarySampleIndexOperationTool.class, STUDY,
                new VariantSecondarySampleIndexParams()
                        .setFamilyIndex(true)
                        .setSample(Arrays.asList(daughter)),
                Paths.get(opencga.createTmpOutdir()), "index", false, token);

        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(sample, IndexStatus.READY, sampleIndex.getStatus().getId());
            if (sample.equals(daughter)) {
                assertEquals(sample, IndexStatus.READY, sampleIndex.getFamilyStatus().getId());
            } else {
                assertEquals(sample, IndexStatus.NONE, sampleIndex.getFamilyStatus().getId());
            }
            assertEquals(sample, 1, sampleIndex.getVersion().intValue());
        }
    }

    @Test
    public void testGwasIndex() throws Exception {
        // Variant scores can not be loaded in mongodb nor dummy
        Assume.assumeThat(storageEngine, CoreMatchers.is(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));

        ObjectMap executorParams = new ObjectMap();
        GwasAnalysis analysis = new GwasAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas_index"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", false, token);

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
    public void testCellbaseConfigure() throws Exception {
        String project = "Project_test_cellbase_configure";
        catalogManager.getProjectManager().create(new ProjectCreateParams(project, project, "", "", "", new ProjectOrganism("hsapiens", "GRCh38"), null, null), QueryOptions.empty(), token);

        CellBaseUtils cellBaseUtils = variantStorageManager.getVariantStorageEngineByProject(project, null, token).getCellBaseUtils();
        assertEquals(ParamConstants.CELLBASE_URL, cellBaseUtils.getURL());
        assertEquals(ParamConstants.CELLBASE_VERSION, cellBaseUtils.getVersion());
        assertEquals("hsapiens", cellBaseUtils.getSpecies());
        assertEquals("GRCh38", cellBaseUtils.getAssembly());

        String newCellbase = "https://uk.ws.zettagenomics.com/cellbase/";
        String newCellbaseVersion = "v5.2";
        String newCellbaseDataRelease = "1";

        assertNotEquals(newCellbase, cellBaseUtils.getURL());
        assertNotEquals(newCellbaseVersion, cellBaseUtils.getVersion());
        assertNotEquals(newCellbaseDataRelease, cellBaseUtils.getDataRelease());

        variantStorageManager.setCellbaseConfiguration(project, new CellBaseConfiguration(newCellbase, newCellbaseVersion, newCellbaseDataRelease, ""), false, null, token);
        CellBaseConfiguration cellbaseConfiguration = catalogManager.getProjectManager().get(project, new QueryOptions(), token).first().getCellbase();

        assertEquals(newCellbase, cellbaseConfiguration.getUrl());
        assertEquals(newCellbaseVersion, cellbaseConfiguration.getVersion());
        assertEquals(newCellbaseDataRelease, cellbaseConfiguration.getDataRelease());

//        assertTrue(family.getPedigreeGraph() != null);
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