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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.manager.VariantOperationsTest;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.regenie.RegenieWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.variant.RegenieWrapperParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import static org.opencb.opencga.core.api.FieldConstants.REGENIE_STEP1;
import static org.opencb.opencga.core.api.FieldConstants.REGENIE_STEP2;

@RunWith(Parameterized.class)
@Category(LongTests.class)
public class RegenieWrapperAnalysisTest {

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

    public RegenieWrapperAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(RegenieWrapperAnalysisTest.storageEngine)) {
            indexed = false;
        }
        RegenieWrapperAnalysisTest.storageEngine = storageEngine;
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

            file = opencga.createFile(STUDY, "1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", token);

            for (int i = 0; i < file.getSampleIds().size(); i++) {
                String id = file.getSampleIds().get(i);
                SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
                catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
            }

//            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c1")
//                            .setSamples(file.getSampleIds().subList(0, 2).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
//                    null, null, null, token);
//            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c2")
//                            .setSamples(file.getSampleIds().subList(2, 4).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
//                    null, null, null, token);
//
//            Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
//            Disorder disorder1 = new Disorder("disorder id 1", "disorder name 1", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
//            Disorder disorder2 = new Disorder("disorder id 2", "disorder name 2", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
//            List<Disorder> disorderList = new ArrayList<>(Arrays.asList(disorder1, disorder2));
//            List<Individual> individuals = new ArrayList<>(4);
//
//            // Father
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(father, father, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.singletonList(father), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            // Mother
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(mother, mother, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.singletonList(mother), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            // Son
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(son, son, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(disorderList), Collections.singletonList(son), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            // Daughter
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(daughter, daughter, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(daughter), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            catalogManager.getFamilyManager().create(
//                    STUDY,
//                    new Family("f1", "f1", Collections.singletonList(phenotype), disorderList, null, null, 3, null, null),
//                    individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
//                    token);

            ObjectMap objectMap = new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true)
                    .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "GT,FT");

            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), objectMap, token);
//            variantStorageManager.familyIndexBySamples(STUDY, file.getSampleIds(), new ObjectMap(), token);

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
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).first().getToken();
    }

    @AfterClass
    public static void afterClass() {
        opencga.after();
    }

    public void setUpCatalogManager() throws IOException, CatalogException {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(ORGANIZATION), QueryOptions.empty(), opencga.getAdminToken());
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, ORGANIZATION, null, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update(ORGANIZATION, new OrganizationUpdateParams().setAdmins(Collections.singletonList(USER)),
                null,
                opencga.getAdminToken());
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).first().getToken();

        String projectId = catalogManager.getProjectManager().create(new ProjectCreateParams()
                        .setId(PROJECT)
                        .setDescription("Project about some genomes")
                        .setOrganism(new ProjectOrganism("hsapiens", "GRCh38")),
                new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);

//        // Create 10 samples not indexed
//        for (int i = 0; i < 10; i++) {
//            Sample sample = new Sample().setId("SAMPLE_" + i);
//            if (i % 2 == 0) {
//                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
//            }
//            catalogManager.getSampleManager().create(STUDY, sample, null, token);
//        }
    }

    @Test
    public void testRegenieStep2() throws IOException, ToolException, CatalogException {
        // Run clinical analysis load task
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenieOutdir"));

//        Assume.assumeTrue(areLiftoverResourcesReady());

        Path customPath = Paths.get("custom");
        File opencgaCustomFolder = catalogManager.getFileManager().createFolder(STUDY, customPath.toString(), true, null, QueryOptions.empty(),
                token).first();
        System.out.println("opencgaCustomFolder.getUri() = " + opencgaCustomFolder.getUri());
        catalogManager.getIoManagerFactory().get(opencgaCustomFolder.getUri()).createDirectory(opencgaCustomFolder.getUri(), true);

        // Phenotype file
        Path phenoFile = Paths.get(opencgaCustomFolder.getUri()).resolve("phenotype.txt");
        InputStream resourceAsStream = RegenieWrapperAnalysisTest.class.getClassLoader().getResourceAsStream("regenie_walker/phenotype.txt");
        Files.copy(resourceAsStream, phenoFile, StandardCopyOption.REPLACE_EXISTING);
        FileLinkParams linkParams = new FileLinkParams()
                .setUri(phenoFile.toString())
                .setPath(customPath.toString());
        File opencgaPhenoFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
        System.out.println("opencgaPhenoFile.getUri() = " + opencgaPhenoFile.getUri());

        // Prediction from step1
        Path predPath = customPath.resolve("step1");
        File opencgaPredPath = catalogManager.getFileManager().createFolder(STUDY, predPath.toString(), true, null, QueryOptions.empty(),
                token).first();
        System.out.println("opencgaPredPath.getUri() = " + opencgaPredPath.getUri());
        catalogManager.getIoManagerFactory().get(opencgaPredPath.getUri()).createDirectory(opencgaPredPath.getUri(), true);
        resourceAsStream = RegenieWrapperAnalysisTest.class.getClassLoader().getResourceAsStream("regenie_walker/step1/step1_1.loco");
        Files.copy(resourceAsStream, Paths.get(opencgaCustomFolder.getUri()).resolve("step1/step1_1.loco"), StandardCopyOption.REPLACE_EXISTING);
        resourceAsStream = RegenieWrapperAnalysisTest.class.getClassLoader().getResourceAsStream("regenie_walker/step1/step1_pred.list");
        Files.copy(resourceAsStream, Paths.get(opencgaCustomFolder.getUri()).resolve("step1/step1_pred.list"), StandardCopyOption.REPLACE_EXISTING);

        RegenieWrapperParams params = new RegenieWrapperParams()
                .setStep(REGENIE_STEP2)
                .setPhenoFile(opencgaPhenoFile.getId())
                .setPredPath(opencgaPredPath.getId());

        toolRunner.execute(RegenieWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir,
                null, false, token);
//
//        Assert.assertTrue(Files.exists(Paths.get(destCustomFolder.getUri().getPath()).resolve(basename + ".hg38.liftover.vcf.gz")));
//        Assert.assertTrue(Paths.get(destCustomFolder.getUri().getPath()).resolve(basename + ".hg38.liftover.vcf.gz").toFile().length() > 0);

        System.out.println("Regenie outdir = " + regenieOutdir);
    }

    @Test
    public void testRegenieStep2WithDockerImage() throws IOException, ToolException, CatalogException {
        // Run clinical analysis load task
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_withdockerimage_regenieOutdir"));

        RegenieWrapperParams params = new RegenieWrapperParams()
                .setStep(REGENIE_STEP2)
                .setWalkerDockerName("joaquintarraga/regenie-walker-test:1");

        toolRunner.execute(RegenieWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir,
                null, false, token);

        System.out.println("Regenie outdir = " + regenieOutdir);
    }
}