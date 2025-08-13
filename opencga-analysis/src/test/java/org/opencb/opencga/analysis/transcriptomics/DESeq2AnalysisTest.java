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

package org.opencb.opencga.analysis.transcriptomics;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.deseq2.DESeq2WrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2AnalysisContrast;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2Output;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2WrapperParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.FILE_PREFIX;
import static org.opencb.opencga.analysis.wrappers.deseq2.DESeq2WrapperAnalysis.*;

@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class DESeq2AnalysisTest {

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

    public DESeq2AnalysisTest(String storageEngine) {
        if (!storageEngine.equals(DESeq2AnalysisTest.storageEngine)) {
            indexed = false;
        }
        DESeq2AnalysisTest.storageEngine = storageEngine;
    }

    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();
    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

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
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).first().getToken();
    }

    @AfterClass
    public static void afterClass() {
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            hadoopExternalResource.after();
        }
        opencga.after();
    }

    public void setUpCatalogManager() throws CatalogException {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId("test"), null, opencga.getAdminToken());
        catalogManager.getUserManager().create(new User().setId(USER).setName("User Name").setEmail("mail@ebi.ac.uk").setOrganization("test"),
                PASSWORD, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update("test", new OrganizationUpdateParams().setOwner(USER), null, opencga.getAdminToken());

        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).first().getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);
    }

    @Test
    public void testDESeq2Wald() throws IOException, ToolException, CatalogException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_deseq2"));

        Path datadir = Paths.get(opencga.createTmpOutdir("_deseq2_data"));
        Path countsPath = datadir.resolve("deseq2_counts.csv");
        try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream("deseq2/" + countsPath.getFileName().toString())) {
            Files.copy(stream, countsPath, StandardCopyOption.REPLACE_EXISTING);
        }
        Path metadataPath = datadir.resolve("deseq2_metadata.csv");
        try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream("deseq2/" + metadataPath.getFileName().toString())) {
            Files.copy(stream, metadataPath, StandardCopyOption.REPLACE_EXISTING);
        }

        File countsFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(countsPath.toAbsolutePath().toString(), "", "", "", null, null,
                null, null, null), false, token).first();
        File metadataFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(metadataPath.toAbsolutePath().toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        String basename = "deseq2_results_wald";
        DESeq2WrapperParams params = new DESeq2WrapperParams();
        params.getDESeq2Params2Params().getInput().setCountsFile(FILE_PREFIX + countsFile.getId());
        params.getDESeq2Params2Params().getInput().setMetadataFile(FILE_PREFIX + metadataFile.getId());
        params.getDESeq2Params2Params().getAnalysis().setDesignFormula("~condition");
        params.getDESeq2Params2Params().getAnalysis().setContrast(new DESeq2AnalysisContrast("condition", "treated", "control"));
        params.getDESeq2Params2Params().setOutput(new DESeq2Output(basename, true, true, true));

        toolRunner.execute(DESeq2WrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "deseq2", false, token);

        List<String> outFilenames = Arrays.asList(basename + RESULTS_FILE_SUFIX, basename + PCA_PLOT_RESULTS_FILE_SUFIX,
                basename + MA_PLOT_RESULTS_FILE_SUFIX, basename + VST_COUNTS_RESULTS_FILE_SUFIX);
        for (String outFilename : outFilenames) {
            System.out.println("Checking output file: " + outdir.resolve(outFilename).toAbsolutePath());
            Assert.assertTrue(Files.exists(outdir.resolve(outFilename)));
            System.out.println("OK.");
        }
    }

    @Test
    public void testDESeq2Lrt() throws IOException, ToolException, CatalogException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_deseq2"));

        Path datadir = Paths.get(opencga.createTmpOutdir("_deseq2_data"));
        Path countsPath = datadir.resolve("deseq2_timecourse_counts.csv");
        try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream("deseq2/" + countsPath.getFileName().toString())) {
            Files.copy(stream, countsPath, StandardCopyOption.REPLACE_EXISTING);
        }
        Path metadataPath = datadir.resolve("deseq2_timecourse_metadata.csv");
        try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream("deseq2/" + metadataPath.getFileName().toString())) {
            Files.copy(stream, metadataPath, StandardCopyOption.REPLACE_EXISTING);
        }

        File countsFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(countsPath.toAbsolutePath().toString(), "", "", "", null, null,
                null, null, null), false, token).first();
        File metadataFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(metadataPath.toAbsolutePath().toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        String basename = "deseq2_results_lrt";
        DESeq2WrapperParams params = new DESeq2WrapperParams();
        params.getDESeq2Params2Params().getInput().setCountsFile(FILE_PREFIX + countsFile.getId());
        params.getDESeq2Params2Params().getInput().setMetadataFile(FILE_PREFIX + metadataFile.getId());
        params.getDESeq2Params2Params().getAnalysis().setDesignFormula("~genotype + time + genotype:time");
        params.getDESeq2Params2Params().getAnalysis().setReducedFormula("~genotype + time");
        params.getDESeq2Params2Params().getAnalysis().setTestMethod(LRT_TEST_METHOD);
//        params.getDESeq2Params2Params().getAnalysis().setContrast(new DESeq2AnalysisContrast("condition", "treated", "control"));
        params.getDESeq2Params2Params().setOutput(new DESeq2Output(basename, false, false, false));

        toolRunner.execute(DESeq2WrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "deseq2", false, token);

        List<String> outFilenames = Arrays.asList(basename + RESULTS_FILE_SUFIX);
//        , basename + PCA_PLOT_RESULTS_FILE_SUFIX,
//                basename + MA_PLOT_RESULTS_FILE_SUFIX, basename + VST_COUNTS_RESULTS_FILE_SUFIX);
        for (String outFilename : outFilenames) {
            System.out.println("Checking output file: " + outdir.resolve(outFilename).toAbsolutePath());
            Assert.assertTrue(Files.exists(outdir.resolve(outFilename)));
            System.out.println("OK.");
        }
    }
}
