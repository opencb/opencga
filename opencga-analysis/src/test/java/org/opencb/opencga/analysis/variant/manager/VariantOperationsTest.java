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

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.operations.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.common.IndexStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.individual.Location;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationIndexParams;
import org.opencb.opencga.core.models.operations.variant.VariantSecondaryAnnotationIndexParams;
import org.opencb.opencga.core.models.operations.variant.VariantSecondarySampleIndexParams;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.models.variant.VariantStorageMetadataSynchronizeParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class VariantOperationsTest {

    public static final String USER = "user";
    public static final String PASSWORD = TestParamConstants.PASSWORD;
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = "opencga_test_" + USER + "_" + PROJECT;
    private static String father = "NA19661";
    private static String mother = "NA19660";
    private static String son = "NA19685";
    private static String daughter = "NA19600";
    private static List<String> samples = Arrays.asList(father, mother, son, daughter);


    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
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

    @ClassRule
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
        token = catalogManager.getUserManager().login("user", PASSWORD).getToken();
    }

    @After
    public void tearDown() {
        if (hadoopExternalResource != null) {
            hadoopExternalResource.after();
            hadoopExternalResource = null;
        }
    }

    @AfterClass
    public static void afterClass() {
        opencga.after();
        if (hadoopExternalResource != null) {
            hadoopExternalResource.after();
            hadoopExternalResource = null;
        }
    }

    private void loadDataset() throws Throwable {
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            if (hadoopExternalResource == null) {
                hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();
                hadoopExternalResource.before();
            } else {
                hadoopExternalResource.clearHBase();
            }
        }
        opencga.after();
        opencga.before();

        catalogManager = opencga.getCatalogManager();
        variantStorageManager = opencga.getVariantStorageManager(solrExternalResource);
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
        solrExternalResource.configure(variantStorageManager.getVariantStorageEngine(STUDY, token));
        solrExternalResource.configure(variantStorageManager.getVariantStorageEngineForStudyOperation(STUDY, new ObjectMap(), token));


        file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", token);
//            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);
        toolRunner.execute(VariantIndexOperationTool.class, STUDY,
                new VariantIndexParams()
                        .setFile(file.getId())
                        .setAnnotate(false)
                        .setLoadHomRef(YesNoAuto.YES.name()),
                Paths.get(opencga.createTmpOutdir("_index")), "index", token);
        toolRunner.execute(VariantAnnotationIndexOperationTool.class, STUDY,
                new VariantAnnotationIndexParams(),
                Paths.get(opencga.createTmpOutdir("_annotation-index")), "index", token);

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
                new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
                individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
                token);


        opencga.getStorageConfiguration().getVariant().setDefaultEngine(storageEngine);
        VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants")).toUri());
        }
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

    }

    @Test
    public void testVariantSecondaryAnnotationIndex() throws Exception {

        for (String sample : samples) {
            SampleInternalVariantSecondaryAnnotationIndex index = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex();
            assertEquals(IndexStatus.NONE, index.getStatus().getId());
        }
        assertEquals(IndexStatus.NONE, catalogManager.getFileManager().get(STUDY, file.getId(), new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());

        toolRunner.execute(VariantSecondaryAnnotationIndexOperationTool.class, STUDY,
                new VariantSecondaryAnnotationIndexParams(),
                Paths.get(opencga.createTmpOutdir()), "annotation_index", token);

        for (String sample : samples) {
            SampleInternalVariantSecondaryAnnotationIndex index = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex();
            assertEquals(IndexStatus.READY, index.getStatus().getId());
        }
        assertEquals(IndexStatus.READY, catalogManager.getFileManager().get(STUDY, file.getId(), new QueryOptions(), token).first().getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());

    }

    @Test
    public void testVariantSecondarySampleIndex() throws Exception {
        Assume.assumeThat(storageEngine, is(HadoopVariantStorageEngine.STORAGE_ENGINE_ID));
        for (String sample : samples) {
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
            assertEquals(sample, IndexStatus.READY, sampleIndex.getStatus().getId());
            assertEquals(sample, IndexStatus.NONE, sampleIndex.getFamilyStatus().getId());
            assertEquals(sample, 1, sampleIndex.getVersion().intValue());
        }

        // Run family index. The family index status should be READY on offspring
        toolRunner.execute(VariantSecondarySampleIndexOperationTool.class, STUDY,
                new VariantSecondarySampleIndexParams()
                        .setFamilyIndex(true)
                        .setSample(Arrays.asList(ParamConstants.ALL)),
                Paths.get(opencga.createTmpOutdir()), "index", token);

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
                new VariantStorageMetadataSynchronizeParams().setStudy(STUDY),
                Paths.get(opencga.createTmpOutdir()), "", catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken());

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
            SampleInternalVariantSecondarySampleIndex sampleIndex = catalogManager.getSampleManager().get(STUDY, sample, new QueryOptions(), token).first().getInternal().getVariant().getSecondarySampleIndex();
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