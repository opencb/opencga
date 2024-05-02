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

package org.opencb.opencga.analysis.alignment;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.alignment.qc.AlignmentFastQcMetricsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentFlagStatsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentGeneCoverageStatsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentStatsAnalysis;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.bwa.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.*;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.commons.io.FileUtils.readLines;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class AlignmentAnalysisTest {

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


    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
//                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public AlignmentAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(AlignmentAnalysisTest.storageEngine)) {
            indexed = false;
        }
        AlignmentAnalysisTest.storageEngine = storageEngine;
    }


    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();
    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    private static String storageEngine;
    private static boolean indexed = false;
    private static String token;

    @Before
    public void setUp() throws Throwable {
//        System.setProperty("opencga.log.level", "INFO");
//        Configurator.reconfigure();
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

//            file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", token);
//            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);

//            for (int i = 0; i < file.getSampleIds().size(); i++) {
//                String id = file.getSampleIds().get(i);
//                if (id.equals(son)) {
//                    SampleUpdateParams updateParams = new SampleUpdateParams().setSomatic(true);
//                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
//                }
//                if (i % 2 == 0) {
//                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
//                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
//                }
//            }

//            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c1")
//                            .setSamples(file.getSampleIds().subList(0, 2).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
//                    null, null, null, token);
//            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c2")
//                            .setSamples(file.getSampleIds().subList(2, 4).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
//                    null, null, null, token);

//            Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
//            Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
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
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(Collections.singletonList(disorder)), Collections.singletonList(son), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            // Daughter
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(daughter, daughter, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(daughter), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            catalogManager.getFamilyManager().create(
//                    STUDY,
//                    new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
//                    individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
//                    token);
//
//            // Cancer (SV)
//            ObjectMap config = new ObjectMap();
////            config.put(VariantStorageOptions.ANNOTATE.key(), true);
//            config.put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI);
//
//            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T_vs_AR2.10039966-01G.annot.brass.vcf.gz", token);
//            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
//            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T.copynumber.caveman.vcf.gz", token);
//            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
//            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T_vs_AR2.10039966-01G.annot.pindel.vcf.gz", token);
//            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
//
//            SampleUpdateParams updateParams = new SampleUpdateParams().setSomatic(true);
//            catalogManager.getSampleManager().update(CANCER_STUDY, cancer_sample, updateParams, null, token);

            opencga.getStorageConfiguration().getVariant().setDefaultEngine(storageEngine);
            VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
//            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
//                VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants")).toUri());
//            }
        }
        // Reset engines
        opencga.getStorageEngineFactory().close();
        catalogManager = opencga.getCatalogManager();
        variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());
        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();
    }

    @AfterClass
    public static void afterClass() {
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            hadoopExternalResource.after();
        }
        opencga.after();
    }

    public void setUpCatalogManager() throws CatalogException, IOException {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId("test"), null, opencga.getAdminToken());
        catalogManager.getUserManager().create(new User().setId(USER).setName("User Name").setEmail("mail@ebi.ac.uk").setOrganization("test"),
                PASSWORD, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update("test", new OrganizationUpdateParams().setOwner(USER), null, opencga.getAdminToken());

        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);

        // Create 10 samples not indexed
//        for (int i = 0; i < 10; i++) {
//            Sample sample = new Sample().setId("SAMPLE_" + i);
//            if (i % 2 == 0) {
//                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
//            }
//            catalogManager.getSampleManager().create(STUDY, sample, null, token);
//        }
//
//        // Cancer
//        List<Sample> samples = new ArrayList<>();
//        catalogManager.getStudyManager().create(projectId, CANCER_STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);
//        Sample sample = new Sample().setId(cancer_sample).setSomatic(true);
//        samples.add(sample);
////        catalogManager.getSampleManager().create(CANCER_STUDY, sample, null, token);
//        sample = new Sample().setId(germline_sample);
//        samples.add(sample);
////        catalogManager.getSampleManager().create(CANCER_STUDY, sample, null, token);
//        Individual individual = catalogManager.getIndividualManager()
//                .create(CANCER_STUDY, new Individual("AR2.10039966-01", "AR2.10039966-01", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
//                        samples, false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.emptyList(), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first();
//        assertEquals(2, individual.getSamples().size());
    }

    //-----------------------------------------
    // S A M T O O L S
    //-----------------------------------------

    @Test
    public void testSamtoolsSort() throws IOException, CatalogException, ToolException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testSamtoolsSort"));
        File bamFile = copyBamFile("testSamtoolsSort", outPath);

        SamtoolsWrapperParams params = new SamtoolsWrapperParams();
        params.setInputFile(bamFile.getId());
        Map<String, String> samtoolsParams = new HashMap<>();
        samtoolsParams.put("o", "sorted.bam");
        params.setSamtoolsParams(samtoolsParams);
        params.setCommand("sort");

        toolRunner.execute(SamtoolsWrapperAnalysis.class, params, new ObjectMap(), outPath, null, token);
        assertTrue(outPath.resolve(samtoolsParams.get("o")).toFile().exists());
    }

    @Test
    public void testSamtoolsIndex() throws IOException, CatalogException, ToolException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testSamtoolsIndex"));

        // Prepare BAM file
        Path bamPath = outPath.resolve("bam");
        Files.createDirectories(bamPath);
        File bamFile = copyBamFile("testSamtoolsIndex", bamPath);

        SamtoolsWrapperParams params = new SamtoolsWrapperParams();
        params.setInputFile(bamFile.getId());
        Map<String, String> samtoolsParams = new HashMap<>();
        params.setSamtoolsParams(samtoolsParams);
        params.setCommand("index");

        toolRunner.execute(SamtoolsWrapperAnalysis.class, params, new ObjectMap(), outPath, null, token);

        // Check results
        assertTrue(outPath.resolve(bamFile.getName() + ".bai").toFile().exists());
    }

    @Test
    public void testSamtoolsFlagstats() throws IOException, CatalogException, ToolException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testSamtoolsFlagstats"));
        File bamFile = copyBamFile("testSamtoolsFlagstats", outPath);

        SamtoolsWrapperParams params = new SamtoolsWrapperParams();
        params.setInputFile(bamFile.getId());
        params.setCommand("flagstat");

        toolRunner.execute(SamtoolsWrapperAnalysis.class, params, new ObjectMap(), outPath, null, token);

        // Check results
        java.io.File stdoutFile = outPath.resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
        assertTrue(stdoutFile.exists());
        List<String> lines = readLines(stdoutFile, Charset.defaultCharset());
        if (CollectionUtils.isEmpty(lines) && !lines.get(0).contains("QC-passed")) {
            fail();
        }
    }

    @Test
    public void testSamtoolsStats() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testSamtoolsStats"));
        File bamFile = copyBamFile("testSamtoolsStats", outPath);

        SamtoolsWrapperParams params = new SamtoolsWrapperParams();
        params.setInputFile(bamFile.getId());
        params.setCommand("stats");

        toolRunner.execute(SamtoolsWrapperAnalysis.class, params, new ObjectMap(), outPath, null, token);

        // Check results
        java.io.File stdoutFile = outPath.resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
        assertTrue(stdoutFile.exists());
        List<String> lines = readLines(stdoutFile, Charset.defaultCharset());
        if (CollectionUtils.isEmpty(lines) && !lines.get(0).startsWith("# This file was produced by samtools stats")) {
            fail();
        }
    }

    @Test
    public void testAlignmentFlagStats() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testAlignmentFlagStats"));
        File bamFile = copyBamFile("testAlignmentFlagStats", outPath);

        AlignmentFlagStatsParams params = new AlignmentFlagStatsParams();
        params.setFile(bamFile.getId());

        toolRunner.execute(AlignmentFlagStatsAnalysis.class, params, new ObjectMap(), outPath, null, token);

        // Check results
        assertTrue(AlignmentFlagStatsAnalysis.getResultPath(outPath.toAbsolutePath().toString(), bamFile.getName()).toFile().exists());
    }

    @Test
    public void testAlignmentStats() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testAlignmentStats"));
        File bamFile = copyBamFile("testAlignmentStats", outPath);

        AlignmentStatsParams params = new AlignmentStatsParams();
        params.setFile(bamFile.getId());

        toolRunner.execute(AlignmentStatsAnalysis.class, params, new ObjectMap(), outPath, null, token);

        // Check results
        assertTrue(AlignmentStatsAnalysis.getResultPath(outPath.toAbsolutePath().toString(), bamFile.getName()).toFile().exists());
    }

    //-----------------------------------------
    // F A S T Q C
    //-----------------------------------------

    @Test
    public void testFastQcZip() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testFastQcZip"));
        File bamFile = copyBamFile("testFastQcZip", outPath);

        FastqcWrapperParams params = new FastqcWrapperParams();
        params.setInputFile(bamFile.getId());
        Map<String, String> fastqcParams = new HashMap<>();
        params.setFastqcParams(fastqcParams);

        toolRunner.execute(FastqcWrapperAnalysis.class, params, new ObjectMap(), outPath, null, token);

        // Check results
        String basename = bamFile.getName().replace(".bam", "");
        assertTrue(outPath.resolve(basename + "_fastqc.zip").toFile().exists());
        assertTrue(outPath.resolve(basename + "_fastqc.html").toFile().exists());
    }

    @Test
    public void testFastQc() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testFastQc"));
        File bamFile = copyBamFile("testFastQc", outPath);

        FastqcWrapperParams params = new FastqcWrapperParams();
        params.setInputFile(bamFile.getId());
        Map<String, String> fastqcParams = new HashMap<>();
        fastqcParams.put("extract", "true");
        params.setFastqcParams(fastqcParams);

        toolRunner.execute(FastqcWrapperAnalysis.class, params, new ObjectMap(), outPath, null, token);

        // Check restults
        FastQcMetrics fastQcMetrics = AlignmentFastQcMetricsAnalysis.parseResults(outPath);
        assertEquals("PASS", fastQcMetrics.getSummary().getBasicStatistics());
        assertEquals("46", fastQcMetrics.getBasicStats().get("%GC"));
        assertEquals(8, fastQcMetrics.getFiles().size());
    }

    //-----------------------------------------
    // D E E P T O O L S
    //-----------------------------------------

    @Test
    public void testDeeptoolsCoverage() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testDeeptoolsCoverage"));
        File bamFile = copyBamFile("testDeeptoolsCoverage", outPath);

        // Execute samtools index
        Path samtoolsIndexJobPath = outPath.resolve("samtools_index");
        Files.createDirectories(samtoolsIndexJobPath);

        SamtoolsWrapperParams samtoolsIndexParams = new SamtoolsWrapperParams();
        samtoolsIndexParams.setInputFile(bamFile.getId());
        Map<String, String> samtoolsParams = new HashMap<>();
        samtoolsIndexParams.setSamtoolsParams(samtoolsParams);
        samtoolsIndexParams.setCommand("index");

        toolRunner.execute(SamtoolsWrapperAnalysis.class, samtoolsIndexParams, new ObjectMap(), samtoolsIndexJobPath, "testDeeptoolsCoverageSamtoolsIndexJobId", token);
        assertTrue(samtoolsIndexJobPath.resolve(bamFile.getName() + ".bai").toFile().exists());

        File baiFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(outPath.resolve(bamFile.getName() + ".bai").toString(), "testDeeptoolsCoverage", "", "", null, null, null,
                null, null), false, token).first();
        assertEquals(bamFile.getName() + ".bai", baiFile.getName());

        // Execute bamCoverage
        DeeptoolsWrapperParams params = new DeeptoolsWrapperParams();
        params.setCommand("bamCoverage");
        Map<String, String> deeptoolsParams = new HashMap<>();
        deeptoolsParams.put("b", bamFile.getId());
        deeptoolsParams.put("o", bamFile.getName() + ".bw");
        deeptoolsParams.put("binSize", "500");
        deeptoolsParams.put("outFileFormat", "bigwig");
        deeptoolsParams.put("minMappingQuality", "20");
        params.setDeeptoolsParams(deeptoolsParams);

        toolRunner.execute(DeeptoolsWrapperAnalysis.class, params, new ObjectMap(), outPath, "testDeeptoolsCoverageDeeptoolsBamCoverageJobId", token);
        assertTrue(outPath.resolve(bamFile.getName() + ".bw").toFile().exists());
    }

    //-----------------------------------------
    // C O V E R A G E
    //-----------------------------------------

    @Test
    public void testCoverage() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testCoverage"));
        File bamFile = copyBamFile("testCoverage", outPath);

        // Execute samtools index
        Path samtoolsIndexJobPath = outPath.resolve("samtools_index");
        Files.createDirectories(samtoolsIndexJobPath);

        SamtoolsWrapperParams samtoolsIndexParams = new SamtoolsWrapperParams();
        samtoolsIndexParams.setInputFile(bamFile.getId());
        Map<String, String> samtoolsParams = new HashMap<>();
        samtoolsIndexParams.setSamtoolsParams(samtoolsParams);
        samtoolsIndexParams.setCommand("index");

        toolRunner.execute(SamtoolsWrapperAnalysis.class, samtoolsIndexParams, new ObjectMap(), samtoolsIndexJobPath, "testCoverageSamtoolsIndexJobId", token);
        assertTrue(samtoolsIndexJobPath.resolve(bamFile.getName() + ".bai").toFile().exists());

        File baiFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(outPath.resolve(bamFile.getName() + ".bai").toString(), "testCoverage", "", "", null, null, null,
                null, null), false, token).first();
        assertEquals(bamFile.getName() + ".bai", baiFile.getName());

        // Execute coverage
        CoverageIndexParams coverageIndexParams = new CoverageIndexParams();
        coverageIndexParams.setBamFileId(bamFile.getId());
        coverageIndexParams.setBaiFileId(baiFile.getId());
        coverageIndexParams.setWindowSize(500);

        toolRunner.execute(AlignmentCoverageAnalysis.class, coverageIndexParams, new ObjectMap(), outPath, "testCoverageCoverageIndexJobId", token);
        assertTrue(outPath.resolve(bamFile.getName() + ".bw").toFile().exists());
    }

    @Test
    public void testGeneCoverageStats() throws IOException, ToolException, CatalogException {
        // Create JUnit test path and copy BAM file there
        Path outPath = Paths.get(opencga.createTmpOutdir("_testGeneCoverageStats"));
        File bamFile = copyBamFile("testGeneCoverageStats", outPath);

        // Execute samtools index
        Path samtoolsIndexJobPath = outPath.resolve("samtools_index");
        Files.createDirectories(samtoolsIndexJobPath);

        SamtoolsWrapperParams samtoolsIndexParams = new SamtoolsWrapperParams();
        samtoolsIndexParams.setInputFile(bamFile.getId());
        Map<String, String> samtoolsParams = new HashMap<>();
        samtoolsIndexParams.setSamtoolsParams(samtoolsParams);
        samtoolsIndexParams.setCommand("index");

        toolRunner.execute(SamtoolsWrapperAnalysis.class, samtoolsIndexParams, new ObjectMap(), samtoolsIndexJobPath, "testGeneCoverageStatsSamtoolsIndexJobId", token);

        // Check results
        assertTrue(samtoolsIndexJobPath.resolve(bamFile.getName() + ".bai").toFile().exists());

        // Execute gene coverage stats
        AlignmentGeneCoverageStatsParams params = new AlignmentGeneCoverageStatsParams();
        params.setBamFile(bamFile.getId());
        String geneName = "BRCA2";
        params.setGenes(Arrays.asList(geneName));

        toolRunner.execute(AlignmentGeneCoverageStatsAnalysis.class, params, new ObjectMap(), outPath, "test", token);

        // Check results
        File file = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFile.getId()), null, null,
                false, token).first();
        assertEquals(1, file.getQualityControl().getCoverage().getGeneCoverageStats().size());
        assertEquals(geneName, file.getQualityControl().getCoverage().getGeneCoverageStats().get(0).getGeneName());
        assertEquals(10, file.getQualityControl().getCoverage().getGeneCoverageStats().get(0).getStats().size());
    }

    //-----------------------------------------
    // B W A
    //-----------------------------------------

    @Test
    public void testBwaIndex() throws IOException, CatalogException, ToolException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_bwa_index"));
        System.out.println("outdir = " + outdir);

        String fastaFilename = opencga.getResourceUri("biofiles/cram/hg19mini.fasta").toString();
        File fastasFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastaFilename, "", "", "", null, null, null,
                null, null), false, token).first();

        BwaWrapperParams params = new BwaWrapperParams();
        params.setFastaFile(fastasFile.getId());
        params.setCommand("index");

        toolRunner.execute(BwaWrapperAnalysis.class, params, new ObjectMap(), outdir, null, token);
        assertTrue(outdir.resolve(fastasFile.getName() + ".sa").toFile().exists());
        assertTrue(outdir.resolve(fastasFile.getName() + ".pac").toFile().exists());
        assertTrue(outdir.resolve(fastasFile.getName() + ".ann").toFile().exists());
        assertTrue(outdir.resolve(fastasFile.getName() + ".amb").toFile().exists());
        assertTrue(outdir.resolve(fastasFile.getName() + ".bwt").toFile().exists());
    }


    @Test
    public void testBwaMem() throws IOException, CatalogException, ToolException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_bwa_index"));
        System.out.println("outdir = " + outdir);

        String faFilename = opencga.getResourceUri("biofiles/cram/hg19mini.fasta").toString();
        File faFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(faFilename, "", "", "", null, null, null,
                null, null), false, token).first();

        BwaWrapperParams params = new BwaWrapperParams();
        params.setFastaFile(faFile.getId());
        params.setCommand("index");

        toolRunner.execute(BwaWrapperAnalysis.class, params, new ObjectMap(), outdir, null, token);
        assertTrue(outdir.resolve(faFile.getName() + ".sa").toFile().exists());
        assertTrue(outdir.resolve(faFile.getName() + ".pac").toFile().exists());
        assertTrue(outdir.resolve(faFile.getName() + ".ann").toFile().exists());
        assertTrue(outdir.resolve(faFile.getName() + ".amb").toFile().exists());
        assertTrue(outdir.resolve(faFile.getName() + ".bwt").toFile().exists());

        String fqFilename = opencga.getResourceUri("biofiles/cram/reads.fq").toString();
        File fqFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fqFilename, "", "", "", null, null, null,
                null, null), false, token).first();

        params.setFastq1File(fqFile.getId());
        params.setCommand("mem");
        Map<String, String> bwaParams = new HashMap<>();
        bwaParams.put("o", fqFile.getName() + ".sam");
        params.setBwaParams(bwaParams);

        toolRunner.execute(BwaWrapperAnalysis.class, params, new ObjectMap(), outdir, null, token);
        assertTrue(outdir.resolve(fqFile.getName() + ".sam").toFile().exists());
    }

    private File copyBamFile(String catalogPathName, Path outPath) throws CatalogException, IOException {
        // setup BAM files
        Path sourcePath = Paths.get(opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam").getPath());
        Path targetPath = outPath.resolve(sourcePath.getFileName());
        Files.copy(sourcePath, targetPath);
        return catalogManager.getFileManager().link(STUDY, new FileLinkParams(targetPath.toAbsolutePath().toString(), catalogPathName, "", "",
                null, null, null, null, null), true, token).first();
    }
}