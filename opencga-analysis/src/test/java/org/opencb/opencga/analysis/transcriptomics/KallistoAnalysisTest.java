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

import org.apache.commons.io.FileUtils;
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
import org.opencb.opencga.analysis.wrappers.kallisto.KallistoWrapperAnalysis;
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
import org.opencb.opencga.core.models.wrapper.kallisto.KallistoWrapperParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.opencb.opencga.core.models.wrapper.kallisto.KallistoParams.*;

@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class KallistoAnalysisTest {

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


    private static Path kallistoDataPath = Paths.get("/opt/kallisto-data");
    private static String genomeRelativePath = "genome";
    private static String genomeIndexRelativePath = "genome-index";
    private static String fastqRelativePath = "fastq";

    private static String fastaFilename = "Homo_sapiens.GRCh38.dna.chromosome.20.fa";
    private static String fastqFilename = "HG00096.chrom20.small.fastq";


    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
//                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public KallistoAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(KallistoAnalysisTest.storageEngine)) {
            indexed = false;
        }
        KallistoAnalysisTest.storageEngine = storageEngine;
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
    public void testKallistoIndex() throws IOException, ToolException, CatalogException {
        // Assume Kallisto data exists
        Assume.assumeTrue(Files.exists(kallistoDataPath));

        Path datadir = Paths.get(opencga.createTmpOutdir("_kallisto_index_data"));
        Path outdir = Paths.get(opencga.createTmpOutdir("_kallisto_index"));

        // Prepare STAR data, copying it to the data dir
        prepareKallistoData(datadir);

        Path fastaFilePath = datadir.resolve(genomeRelativePath).resolve(fastaFilename).toAbsolutePath();
        File fastaFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastaFilePath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        KallistoWrapperParams params = new KallistoWrapperParams();
        String kallistoIndexFilename = "kallisto.idx";
        params.getKallistoParams().setCommand(INDEX_CMD);
        params.getKallistoParams().getInput().add(fastaFile.getId());
        params.getKallistoParams().getOptions().put(INDEX_PARAM, kallistoIndexFilename);

        toolRunner.execute(KallistoWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "kallisto-index", false, token);

        System.out.println("Checking Kallisto index filename " + outdir.resolve(kallistoIndexFilename));
        Assert.assertTrue(Files.exists(outdir.resolve(kallistoIndexFilename)));
        System.out.println("Ok");
    }

    @Test
    public void testKallistoQuant() throws IOException, ToolException, CatalogException {
        // Assume Kallisto data exists
        Assume.assumeTrue(Files.exists(kallistoDataPath));

        Path datadir = Paths.get(opencga.createTmpOutdir("_kallisto_quant_data"));
        Path outdir = Paths.get(opencga.createTmpOutdir("_kallisto_index"));

        // Prepare STAR data, copying it to the data dir
        prepareKallistoData(datadir);

        Path fastaFilePath = datadir.resolve(genomeRelativePath).resolve(fastaFilename).toAbsolutePath();
        File fastaFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastaFilePath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        KallistoWrapperParams params = new KallistoWrapperParams();
        String kallistoIndexFilename = "kallisto.idx";
        params.getKallistoParams().setCommand(INDEX_CMD);
        params.getKallistoParams().getInput().add(fastaFile.getId());
        params.getKallistoParams().getOptions().put(INDEX_PARAM, kallistoIndexFilename);

        toolRunner.execute(KallistoWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "kallisto-index", false, token);

        System.out.println("Checking Kallisto index filename " + outdir.resolve(kallistoIndexFilename));
        Assert.assertTrue(Files.exists(outdir.resolve(kallistoIndexFilename)));
        System.out.println("Ok");

        Path outdir2 = Paths.get(opencga.createTmpOutdir("_kallisto_quant"));

        Path fastqFilePath = datadir.resolve(fastqRelativePath).resolve(fastqFilename).toAbsolutePath();
        File fastqFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastqFilePath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        File indexFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(outdir.resolve(kallistoIndexFilename).toAbsolutePath().toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        KallistoWrapperParams params2 = new KallistoWrapperParams();
        params2.getKallistoParams().setCommand(QUANT_CMD);
        params2.getKallistoParams().getInput().add(fastqFile.getId());
        params2.getKallistoParams().getOptions().put(INDEX_PARAM, indexFile.getId());
        params2.getKallistoParams().getOptions().put(SINGLE_PARAM, " ");
        params2.getKallistoParams().getOptions().put(FRAGMENT_LENGTH_PARAM, "100");
        params2.getKallistoParams().getOptions().put(SD_PARAM, "2.0");

        toolRunner.execute(KallistoWrapperAnalysis.class, params2, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir2, "kallisto-quant", false, token);

        System.out.println("Checking Kallisto index filename " + outdir2.resolve(kallistoIndexFilename));
        Assert.assertTrue(Files.exists(outdir2.resolve(kallistoIndexFilename)));
        System.out.println("Ok");
    }

    @Test
    public void testKallistoCite() throws IOException, ToolException, CatalogException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_kallisto_cite"));

        KallistoWrapperParams params = new KallistoWrapperParams();
        params.getKallistoParams().setCommand(CITE_CMD);

        toolRunner.execute(KallistoWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "kallisto-cite", false, token);

        String stdoutFilename = "stdout.txt";
        System.out.println("Checking Kallisto stdout filename " + outdir.resolve(stdoutFilename));
        Assert.assertTrue(Files.exists(outdir.resolve(stdoutFilename)));
        Assert.assertTrue(FileUtils.readLines(outdir.resolve(stdoutFilename).toFile()).get(0).startsWith("When using this program in your research, please cite"));
        System.out.println("Ok");
    }


    @Test
    public void testKallistoVersion() throws IOException, ToolException, CatalogException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_kallisto_version"));

        KallistoWrapperParams params = new KallistoWrapperParams();
        params.getKallistoParams().setCommand(VERSION_CMD);

        toolRunner.execute(KallistoWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "kallisto-version", false, token);

        String stdoutFilename = "stdout.txt";
        System.out.println("Checking Kallisto stdout ilename " + outdir.resolve(stdoutFilename));
        Assert.assertTrue(Files.exists(outdir.resolve(stdoutFilename)));
        Assert.assertTrue(FileUtils.readLines(outdir.resolve(stdoutFilename).toFile()).get(0).startsWith("kallisto, version"));
        System.out.println("Ok");
    }

    private void prepareKallistoData(Path datadir) throws IOException {
        // Genome (FASTA files)
        Path path = Files.createDirectories(datadir.resolve(genomeRelativePath));
        Files.copy(kallistoDataPath.resolve(genomeRelativePath).resolve(fastaFilename), path.resolve(fastaFilename));

        // Genome index
        Files.createDirectories(datadir.resolve(genomeIndexRelativePath));

        // Read (FASTQ files)
        path = Files.createDirectories(datadir.resolve(fastqRelativePath));
        Files.copy(kallistoDataPath.resolve(fastqRelativePath).resolve(fastqFilename), path.resolve(fastqFilename));
    }
}