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
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.star.StarWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.wrapper.MultiQcWrapperParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.wrapper.StarWrapperParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class MultiQcAnalysisTest {

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

    private static Path multiQcDataPath = Paths.get("/opt/multiqc-data");
    private static String fastqcResultRelativePath = "fastqc_result";
    private static String fastqcResultZip = "HG00096.chrom20.small_fastqc.zip";
    private static String fastqcResultHtml = "HG00096.chrom20.small_fastqc.html";

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
//                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public MultiQcAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(MultiQcAnalysisTest.storageEngine)) {
            indexed = false;
        }
        MultiQcAnalysisTest.storageEngine = storageEngine;
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
    public void testMultiQc() throws IOException, ToolException, CatalogException {
        // Assume STAR data exists
        Assume.assumeTrue(Files.exists(multiQcDataPath));

        Path datadir = Paths.get(opencga.createTmpOutdir("_multiqc_data"));
        Path outdir = Paths.get(opencga.createTmpOutdir("_multiqc_fastqc_results"));

        // Prepare MultiQC data, copying it to the data dir
        prepareMultiQcData(datadir);

        Path fastqcResultPath = datadir.resolve(fastqcResultRelativePath).toAbsolutePath();
        File fastqcResultDir = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastqcResultPath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        MultiQcWrapperParams params = new MultiQcWrapperParams();
        params.getMultiQcParams().getInput().add(WrapperUtils.FILE_PREFIX + fastqcResultDir.getId());

        toolRunner.execute(MultiQcWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "multiqc-fastqc-results", false, token);

//        List<String> outFilenames = Arrays.asList("chrStart.txt", "chrName.txt", "chrNameLength.txt", "chrLength.txt",
//                "genomeParameters.txt", "Genome", "SA", "SAindex", "Log.out");
//        for (String outFilename : outFilenames) {
//            Assert.assertTrue(Files.exists(outdir.resolve(outFilename)));
//        }
    }

    private void prepareMultiQcData(Path datadir) throws IOException {
        // Genome (FASTA files)
        Path path = Files.createDirectories(datadir.resolve(fastqcResultRelativePath));
        Files.copy(multiQcDataPath.resolve(fastqcResultRelativePath).resolve(fastqcResultZip), path.resolve(fastqcResultZip));
        Files.copy(multiQcDataPath.resolve(fastqcResultRelativePath).resolve(fastqcResultHtml), path.resolve(fastqcResultHtml));
    }
}