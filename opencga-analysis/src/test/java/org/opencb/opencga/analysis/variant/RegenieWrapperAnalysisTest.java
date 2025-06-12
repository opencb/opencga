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
import org.apache.commons.lang.math.RandomUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.manager.VariantOperationsTest;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.regenie.RegenieBuilderWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.regenie.RegenieStep1WrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.regenie.RegenieStep2WrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.individual.Location;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.variant.regenie.RegenieBuilderWrapperParams;
import org.opencb.opencga.core.models.variant.regenie.RegenieDockerParams;
import org.opencb.opencga.core.models.variant.regenie.RegenieStep1WrapperParams;
import org.opencb.opencga.core.models.variant.regenie.RegenieStep2WrapperParams;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils.*;

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
    public static final String PHENOTYPE_NAME_1 = "myPhenotype-1";
    public static final Phenotype PHENOTYPE_1 = new Phenotype(PHENOTYPE_NAME_1, PHENOTYPE_NAME_1, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String PHENOTYPE_NAME_2 = "myPhenotype-2";
    public static final Phenotype PHENOTYPE_2 = new Phenotype(PHENOTYPE_NAME_2, PHENOTYPE_NAME_2, "mySource")
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

    private static String CASE_COHORT = "case-cohort";
    private static String CONTROL_COHORT = "control-cohort";

    private File opencgaVcfFile;
    private File opencgaBedFile;
    private File opencgaBimFile;
    private File opencgaFamFile;
    private File opencgaPhenoFile;
    private String predWithCatalogFilename = "step1_pred.list.with.catalog";
    private File opencgaPredWithCatalogFile;
    private String predWithPathFilename = "step1_pred.list.with.path";
    private File opencgaPredWithPathFile;
    private String predWithFilenameFilename = "step1_pred.list.with.filename";
    private File opencgaPredWithFilenameFile;
    private File opencgaLocoFile;

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

            //file = opencga.createFile(STUDY, "1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", token);
            file = opencga.createFile(STUDY, "regenie/input.vcf.gz", token);

            // Custom
            Path customPath = Paths.get("custom");
            File opencgaCustomFolder = catalogManager.getFileManager().createFolder(STUDY, customPath.toString(), true, null,
                    QueryOptions.empty(), token).first();
            System.out.println("opencgaCustomFolder.getUri() = " + opencgaCustomFolder.getUri());
            catalogManager.getIoManagerFactory().get(opencgaCustomFolder.getUri()).createDirectory(opencgaCustomFolder.getUri(), true);

            // Add the files: phenotype, VCF, BED, BIM and FAM in the OpenCGA catalog
            List<String> filenames = Arrays.asList("phenotype.txt", "input.vcf.gz", "input.bed.gz", "input.bim.gz", "input.fam.gz");
            for (String filename : filenames) {
                Path path = Paths.get(opencgaCustomFolder.getUri()).resolve(filename);
                InputStream resourceAsStream = RegenieWrapperAnalysisTest.class.getClassLoader().getResourceAsStream("regenie/" + filename);
                Files.copy(resourceAsStream, path, StandardCopyOption.REPLACE_EXISTING);
                if (filename.equals("input.bed.gz") || filename.equals("input.bim.gz") || filename.equals("input.fam.gz")) {
                    Runtime.getRuntime().exec("gunzip " + path.toAbsolutePath());
                    path = Paths.get(opencgaCustomFolder.getUri()).resolve(filename.substring(0, filename.length() - 3));
                }
                FileLinkParams linkParams = new FileLinkParams()
                        .setUri(path.toString())
                        .setPath(customPath.toString());
                switch (filename) {
                    case "phenotype.txt": {
                        opencgaPhenoFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
                        break;
                    }
                    case "input.vcf.gz": {
                        opencgaVcfFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
                        break;
                    }
                    case "input.bed.gz": {
                        opencgaBedFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
                        break;
                    }
                    case "input.bim.gz": {
                        opencgaBimFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
                        break;
                    }
                    case "input.fam.gz": {
                        opencgaFamFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
                        break;
                    }
                    default: {
                        Assert.fail("Invalid filename: " + filename);
                    }
                }
            }

            // Step1 files
            Path step1Path = Paths.get("custom/step1");
            File opencgaStep1Folder = catalogManager.getFileManager().createFolder(STUDY, step1Path.toString(), true, null,
                    QueryOptions.empty(), token).first();
            System.out.println("opencgaPredFolder.getUri() = " + opencgaStep1Folder.getUri());
            catalogManager.getIoManagerFactory().get(opencgaStep1Folder.getUri()).createDirectory(opencgaStep1Folder.getUri(), true);

            String locoFilename = "step1_1.loco";
            Path locoPath = Paths.get(opencgaStep1Folder.getUri()).resolve(locoFilename);
            InputStream resourceAsStream = RegenieWrapperAnalysisTest.class.getClassLoader().getResourceAsStream("regenie/step1/" + locoFilename);
            Files.copy(resourceAsStream, locoPath, StandardCopyOption.REPLACE_EXISTING);
            FileLinkParams linkParams = new FileLinkParams()
                    .setUri(locoPath.toString())
                    .setPath(step1Path + "/" + locoPath.getFileName());
            opencgaLocoFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
            // Pred list with OpenCGA catalog file ID
            Path predPath = Paths.get(opencgaStep1Folder.getUri()).resolve(predWithCatalogFilename);
            PrintWriter pw = new PrintWriter(predPath.toFile());
            pw.println("PHENO\t" + FILE_PREFIX + opencgaLocoFile.getId());
            pw.close();
            linkParams = new FileLinkParams()
                    .setUri(predPath.toString())
                    .setPath(step1Path + "/" + predPath.getFileName());
            opencgaPredWithCatalogFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
            // Pred list with physical path
            predPath = Paths.get(opencgaStep1Folder.getUri()).resolve(predWithPathFilename);
            pw = new PrintWriter(predPath.toFile());
            pw.println("PHENO\t" + Paths.get(opencgaLocoFile.getUri().getPath()).toAbsolutePath());
            pw.close();
            linkParams = new FileLinkParams()
                    .setUri(predPath.toString())
                    .setPath(step1Path + "/" + predPath.getFileName());
            opencgaPredWithPathFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();
            // Pred list with filename
            predPath = Paths.get(opencgaStep1Folder.getUri()).resolve(predWithFilenameFilename);
            pw = new PrintWriter(predPath.toFile());
            pw.println("PHENO\t" + locoPath.getFileName());
            pw.close();
            linkParams = new FileLinkParams()
                    .setUri(predPath.toString())
                    .setPath(step1Path + "/" + locoPath.getFileName());
            opencgaPredWithFilenameFile = catalogManager.getFileManager().link(STUDY, linkParams, true, token).first();

            // Create case and control cohorts, and individuals with phenotypes
            List<String> lines = Files.readAllLines(Paths.get(opencgaPhenoFile.getUri().getPath()));
            Map<String, String> samplePhenotypeMap = new HashMap<>();
            for (String line : lines) {
                String[] split = line.split("[\t ]");
                String sampleId = split[0];
                String phenotype = split[2];
                samplePhenotypeMap.put(sampleId, phenotype);
            }

            // Create a cohorts for the case and control samples; and individuals
            List<String> caseSampleIds = new ArrayList<>();
            List<String> controlSampleIds = new ArrayList<>();
            Disorder disorder1 = new Disorder("disorder id 1", "disorder name 1", "", "", Collections.singletonList(PHENOTYPE),
                    Collections.emptyMap());
            List<Disorder> disorderList = new ArrayList<>(Arrays.asList(disorder1));
            for (int i = 0; i < file.getSampleIds().size(); i++) {
                String id = file.getSampleIds().get(i);
                if (samplePhenotypeMap.containsKey(id) && samplePhenotypeMap.get(id).equalsIgnoreCase("1")) {
                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
                    catalogManager.getIndividualManager().create(STUDY, new Individual(id, id, new Individual(), new Individual(),
                                    new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
                                    Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(),
                                    Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap())
                                    .setPhenotypes(Collections.singletonList(PHENOTYPE)).setDisorders(disorderList),
                            Collections.singletonList(id), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token);
                    caseSampleIds.add(id);
                } else {
                    catalogManager.getIndividualManager().create(STUDY, new Individual(id, id, new Individual(), new Individual(),
                                    new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
                                    Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(),
                                    Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()),
                            Collections.singletonList(id), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token);
                    controlSampleIds.add(id);
                }
            }

            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId(CASE_COHORT)
                    .setSamples(caseSampleIds.stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())), null, null, null, token);
            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId(CONTROL_COHORT)
                    .setSamples(controlSampleIds.stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())), null, null, null, token);


            // Index
            ObjectMap objectMap = new ObjectMap()
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
                    .append(VariantStorageOptions.ANNOTATE.key(), true)
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

    //---------------------------------------------------
    // R E G E N I E     S T E P     1
    //---------------------------------------------------

    @Test
    public void testRegenieStep1WithPhenoFileAndBedFilePushDocker() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1, lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenofile_bedfile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--bed", FILE_PREFIX + opencgaBedFile.getPath())
                .append("--phenoFile", FILE_PREFIX + opencgaPhenoFile.getPath())
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword));
    }

    @Test
    public void testRegenieStep1WithCohortsAndBedFilePushDocker() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1, lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_cohort_bedfile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--bed", FILE_PREFIX + opencgaBedFile.getPath())
                .append("--phenoFile", COHORT_PREFIX + CASE_COHORT + "," + CONTROL_COHORT)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword));
    }

    @Test
    public void testRegenieStep1WithPhenotypeAndBedFilePushDocker() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1, lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenotype_bedfile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--bed", FILE_PREFIX + opencgaBedFile.getPath())
                .append("--phenoFile", PHENOTYPE_PREFIX + PHENOTYPE_NAME)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword));
    }

    @Test
    public void testRegenieStep1WithPhenoFileAndVcfFilePushDocker() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1, lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenofile_vcffile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", FILE_PREFIX + opencgaPhenoFile.getPath())
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setVcfFile(opencgaVcfFile.getId())
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword));
    }

    @Test
    public void testRegenieStep1WithCohortsAndVcfFilePushDocker() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1, lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_cohort_vcffile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", COHORT_PREFIX + CASE_COHORT + "," + CONTROL_COHORT)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setVcfFile(opencgaVcfFile.getId())
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword));
    }

    @Test
    public void testRegenieStep1WithPhenotypeAndVcfFilePushDocker() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1, lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenotype_vcffile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", PHENOTYPE_PREFIX + PHENOTYPE_NAME)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setVcfFile(opencgaVcfFile.getId())
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword));
    }

    @Test
    public void testRegenieStep1WithPhenoFileAndBedFileBuildDockerfile() throws IOException, ToolException, InterruptedException {
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenofile_bedfile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--bed", FILE_PREFIX + opencgaBedFile.getPath())
                .append("--phenoFile", FILE_PREFIX + opencgaPhenoFile.getPath())
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options);

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(Files.exists(regenieOutdir.resolve("Dockerfile")));
    }

    @Test
    public void testRegenieStep1WithCohortsAndBedFileBuildDockerfile() throws IOException, ToolException, InterruptedException {
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_cohort_bedfile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--bed", FILE_PREFIX + opencgaBedFile.getPath())
                .append("--phenoFile", COHORT_PREFIX + CASE_COHORT + "," + CONTROL_COHORT)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options);

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(Files.exists(regenieOutdir.resolve("Dockerfile")));
    }

    @Test
    public void testRegenieStep1WithPhenotypeAndBedFileBuildDockerfile() throws IOException, ToolException, InterruptedException {
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenotype_bedfile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--bed", FILE_PREFIX + opencgaBedFile.getPath())
                .append("--phenoFile", PHENOTYPE_PREFIX + PHENOTYPE_NAME)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options);

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(Files.exists(regenieOutdir.resolve("Dockerfile")));
    }

    @Test
    public void testRegenieStep1WithPhenoFileAndVcfFileBuildDockerfile() throws IOException, ToolException, InterruptedException {
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenofile_vcffile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", FILE_PREFIX + opencgaPhenoFile.getPath())
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setVcfFile(opencgaVcfFile.getId());

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(Files.exists(regenieOutdir.resolve("Dockerfile")));
    }

    @Test
    public void testRegenieStep1WithCohortsAndVcfFileBuildDockerfile() throws IOException, ToolException, InterruptedException {
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_cohort_vcffile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", COHORT_PREFIX + CASE_COHORT + "," + CONTROL_COHORT)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setVcfFile(opencgaVcfFile.getId());

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(Files.exists(regenieOutdir.resolve("Dockerfile")));
    }

    @Test
    public void testRegenieStep1WithPhenotypeAndVcfFileBuildDockerfile() throws IOException, ToolException, InterruptedException {
        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step1_phenotype_vcffile_outdir"));
        System.out.println("Regenie step1 outdir = " + regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", PHENOTYPE_PREFIX + PHENOTYPE_NAME)
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieStep1WrapperParams params = new RegenieStep1WrapperParams()
                .setRegenieParams(options)
                .setVcfFile(opencgaVcfFile.getId());

        ExecutionResult executeResult = toolRunner.execute(RegenieStep1WrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie step1 outdir = " + regenieOutdir);
        Assert.assertTrue(Files.exists(regenieOutdir.resolve("Dockerfile")));
    }

    //---------------------------------------------------
    // R E G E N I E     B U I L D E R
    //---------------------------------------------------

    @Test
    public void testRegenieBuilderWithCatalog() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1,lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_builder_catalog_outdir"));
        System.out.println("Regenie step1 outdir = "+regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", FILE_PREFIX + opencgaPhenoFile.getPath())
                .append("--pred", FILE_PREFIX + opencgaPredWithCatalogFile.getPath())
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieBuilderWrapperParams params = new RegenieBuilderWrapperParams()
                .setRegenieFileOptions(options)
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieBuilderWrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie builer outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage,dockerUsername,dockerPassword));
    }

    @Test
    public void testRegenieBuilderWithPath() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1,lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_builder_path_outdir"));
        System.out.println("Regenie step1 outdir = "+regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", FILE_PREFIX + opencgaPhenoFile.getPath())
                .append("--pred", FILE_PREFIX + opencgaPredWithPathFile.getPath())
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieBuilderWrapperParams params = new RegenieBuilderWrapperParams()
                .setRegenieFileOptions(options)
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieBuilderWrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie builer outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage,dockerUsername,dockerPassword));
    }

    @Test
    public void testRegenieBuilderWithFilename() throws IOException, ToolException, InterruptedException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1,lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerName = split[0] + "/regenie-walker";
        String dockerUsername = split[1];
        String dockerPassword = split[2];
        String dockerTag = String.valueOf(System.currentTimeMillis());

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_builder_filename_outdir"));
        System.out.println("Regenie step1 outdir = "+regenieOutdir);

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", FILE_PREFIX + opencgaPhenoFile.getPath())
                .append("--pred", FILE_PREFIX + opencgaPredWithFilenameFile.getPath())
                .append("--bsize", 1000)
                .append("--bt", FLAG_TRUE);

        RegenieBuilderWrapperParams params = new RegenieBuilderWrapperParams()
                .setRegenieFileOptions(options)
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        ExecutionResult executeResult = toolRunner.execute(RegenieBuilderWrapperAnalysis.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir, null, false, token);

        System.out.println("Regenie builer outdir = " + regenieOutdir);
        Assert.assertTrue(executeResult.getAttributes().containsKey(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY));
        String walkerDockerImage = executeResult.getAttributes().getString(RegenieUtils.OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY);
        Assert.assertTrue(walkerDockerImage.startsWith(dockerName));
        // Need to wait for a while to allow the docker image to be available
        Thread.sleep(5000);
        Assert.assertTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage,dockerUsername,dockerPassword));
    }

    //---------------------------------------------------
    // R E G E N I E     S T E P     2
    //---------------------------------------------------

    @Test
    public void testRegenieStep2() throws IOException, ToolException {
        // Check if credentials are present to run the test
        Path credentialsPath = Paths.get("/opt/resources/DH");
        Assume.assumeTrue(Files.exists(credentialsPath));
        List<String> lines = Files.readAllLines(credentialsPath);
        Assert.assertEquals(1, lines.size());
        String[] split = lines.get(0).split(" ");
        String dockerUsername = split[1];
        String dockerPassword = split[2];

        String dockerName = "joaquintarraga/regenie-walker";
//        String dockerTag = "1749449224616";
//        String dockerTag = "1747916358-3872";
        String dockerTag = "1749455891002"; // to use with /opt/app/pred/step1_pred.list.with.catalog
        String walkerDockerImage = dockerName + ":" + dockerTag;
        Assume.assumeTrue(RegenieUtils.isDockerImageAvailable(walkerDockerImage, dockerUsername, dockerPassword));

        Path regenieOutdir = Paths.get(opencga.createTmpOutdir("_regenie_step2_with_dockerimage_outdir"));

        ObjectMap options = new ObjectMap()
                .append("--phenoFile", "/opt/app/" + opencgaPhenoFile.getName())
//                .append("--pred", "/opt/app/pred/step1_pred.list")
                .append("--pred", "/opt/app/pred/step1_pred.list.with.catalog")
                .append("--bsize", 500)
                .append("--bt", FLAG_TRUE);

        RegenieStep2WrapperParams params = new RegenieStep2WrapperParams()
                .setRegenieParams(options)
                .setDocker(new RegenieDockerParams(dockerName, dockerTag, dockerUsername, dockerPassword));

        toolRunner.execute(RegenieStep2WrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), regenieOutdir,
                null, false, token);

        System.out.println("Regenie step2 outdir = " + regenieOutdir);
        Path resultsPath = regenieOutdir.resolve(REGENIE_RESULTS_FILENAME);
        Assert.assertTrue(Files.exists(resultsPath));
        lines = FileUtils.readLines(resultsPath.toFile());
        Assert.assertEquals(235, lines.size());
        Assert.assertTrue(lines.get(0).startsWith(REGENIE_HEADER_PREFIX));
    }
}