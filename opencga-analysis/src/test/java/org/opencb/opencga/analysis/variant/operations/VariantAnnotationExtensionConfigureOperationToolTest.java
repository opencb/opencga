package org.opencb.opencga.analysis.variant.operations;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.manager.VariantOperationsTest;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationExtensionConfigureParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.CosmicVariantAnnotatorExtensionTaskTest.COSMIC_ASSEMBLY;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.CosmicVariantAnnotatorExtensionTaskTest.COSMIC_VERSION;

@RunWith(Parameterized.class)
public class VariantAnnotationExtensionConfigureOperationToolTest {
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

    public VariantAnnotationExtensionConfigureOperationToolTest(String storageEngine) {
        if (!storageEngine.equals(VariantAnnotationExtensionConfigureOperationToolTest.storageEngine)) {
            indexed = false;
        }
        VariantAnnotationExtensionConfigureOperationToolTest.storageEngine = storageEngine;
    }


    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;

    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();
//    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    private static String storageEngine;
    private static boolean indexed = false;
    private static String token;

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
        variantStorageManager.getStorageConfiguration().setMode(StorageConfiguration.Mode.READ_WRITE);
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
    }


    @Test
    public void testVariantAnnotationExtensionConfigure() throws Exception {
        String cosmicVersion = "v101";
        String cosmicAssembly = "GRCh38";

        Path outDir = Paths.get(opencga.createTmpOutdir("_annotationExtensionConfigureTest"));

        File cosmicResourceFile = getCosmicResourceFile(STUDY, catalogManager, token);
        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams()
                .setExtension(CosmicVariantAnnotatorExtensionTask.ID)
                .setResources(Collections.singletonList(cosmicResourceFile.getId()))
                .setParams(new ObjectMap(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_KEY, cosmicVersion)
                        .append(CosmicVariantAnnotatorExtensionTask.COSMIC_ASSEMBLY_KEY, cosmicAssembly));


        toolRunner.execute(VariantAnnotationExtensionConfigureOperationTool.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);

        // Verify project configuration was updated
        Project project = catalogManager.getProjectManager().get(PROJECT, QueryOptions.empty(), token).first();
        ObjectMap options = project.getInternal().getDatastores().getVariant().getOptions();
        Assert.assertTrue(options.getList(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key()).contains(CosmicVariantAnnotatorExtensionTask.ID));
        Assert.assertEquals(cosmicVersion, options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key()));
        Assert.assertEquals(cosmicAssembly, options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key()));
        Assert.assertEquals(Paths.get(cosmicResourceFile.getUri().getPath() + CosmicVariantAnnotatorExtensionTask.COSMIC_ANNOTATOR_INDEX_SUFFIX).toString(), options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key()));
    }

    @Test
    public void testVariantAnnotationExtensionConfigureNoOverwrite() throws Exception {
        String cosmicVersion = "v101";
        String cosmicAssembly = "GRCh38";

        Path outDir = Paths.get(opencga.createTmpOutdir("_annotationExtensionConfigureTestNoOverwrite"));

        File cosmicResourceFile = getCosmicResourceFile(STUDY, catalogManager, token);
        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams()
                .setExtension(CosmicVariantAnnotatorExtensionTask.ID)
                .setResources(Collections.singletonList(cosmicResourceFile.getId()))
                .setParams(new ObjectMap(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_KEY, cosmicVersion)
                        .append(CosmicVariantAnnotatorExtensionTask.COSMIC_ASSEMBLY_KEY, cosmicAssembly));

        toolRunner.execute(VariantAnnotationExtensionConfigureOperationTool.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);

        Project project = catalogManager.getProjectManager().get(PROJECT, QueryOptions.empty(), token).first();
        ObjectMap options = project.getInternal().getDatastores().getVariant().getOptions();
        String indexCreationDate = options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_INDEX_CREATION_DATE.key());


        Path outDir2 = Paths.get(opencga.createTmpOutdir("_annotationExtensionConfigureTestNoOverwrite2"));

        toolRunner.execute(VariantAnnotationExtensionConfigureOperationTool.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir2, null, false, token);

        // Verify project configuration was updated
        project = catalogManager.getProjectManager().get(PROJECT, QueryOptions.empty(), token).first();
        options = project.getInternal().getDatastores().getVariant().getOptions();
        Assert.assertTrue(options.getList(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key()).contains(CosmicVariantAnnotatorExtensionTask.ID));
        Assert.assertEquals(cosmicVersion, options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key()));
        Assert.assertEquals(cosmicAssembly, options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key()));
        Assert.assertEquals(indexCreationDate, options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_INDEX_CREATION_DATE.key()));
        Assert.assertEquals(Paths.get(cosmicResourceFile.getUri().getPath() + CosmicVariantAnnotatorExtensionTask.COSMIC_ANNOTATOR_INDEX_SUFFIX).toString(), options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key()));
    }

    @Test
    public void testVariantAnnotationExtensionConfigureOvewrite() throws Exception {
        String cosmicVersion = "v101";
        String cosmicAssembly = "GRCh38";

        Path outDir = Paths.get(opencga.createTmpOutdir("_annotationExtensionConfigureTestOverwrite"));

        File cosmicResourceFile = getCosmicResourceFile(STUDY, catalogManager, token);
        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams()
                .setExtension(CosmicVariantAnnotatorExtensionTask.ID)
                .setResources(Collections.singletonList(cosmicResourceFile.getId()))
                .setParams(new ObjectMap(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_KEY, cosmicVersion)
                        .append(CosmicVariantAnnotatorExtensionTask.COSMIC_ASSEMBLY_KEY, cosmicAssembly));


        toolRunner.execute(VariantAnnotationExtensionConfigureOperationTool.class, params,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir, null, false, token);

        String cosmicVersion2 = "v102";
        String cosmicAssembly2 = "GRCh38";

        Path outDir2 = Paths.get(opencga.createTmpOutdir("_annotationExtensionConfigureTestNoOverwrite2"));

        File cosmicResourceFile2 = getCosmicResourceFile(STUDY, catalogManager, token);
        VariantAnnotationExtensionConfigureParams params2 = new VariantAnnotationExtensionConfigureParams()
                .setExtension(CosmicVariantAnnotatorExtensionTask.ID)
                .setResources(Collections.singletonList(cosmicResourceFile2.getId()))
                .setParams(new ObjectMap(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_KEY, cosmicVersion2)
                        .append(CosmicVariantAnnotatorExtensionTask.COSMIC_ASSEMBLY_KEY, cosmicAssembly2))
                .setOverwrite(true);

        toolRunner.execute(VariantAnnotationExtensionConfigureOperationTool.class, params2,
                new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outDir2, null, false, token);

        // Verify project configuration was updated
        Project project = catalogManager.getProjectManager().get(PROJECT, QueryOptions.empty(), token).first();
        project = catalogManager.getProjectManager().get(PROJECT, QueryOptions.empty(), token).first();
        ObjectMap options = project.getInternal().getDatastores().getVariant().getOptions();
        Assert.assertTrue(options.getList(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key()).contains(CosmicVariantAnnotatorExtensionTask.ID));
        Assert.assertEquals(cosmicVersion2, options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key()));
        Assert.assertEquals(cosmicAssembly2, options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key()));
        Assert.assertEquals(Paths.get(cosmicResourceFile2.getUri().getPath() + CosmicVariantAnnotatorExtensionTask.COSMIC_ANNOTATOR_INDEX_SUFFIX).toString(), options.get(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key()));
    }

    public static File getCosmicResourceFile(String study, CatalogManager catalogManager, String token) throws IOException, CatalogException {
        String folder = "I_tmp_" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS").format(new Date());
        Path tmpOutdir = Files.createDirectories(Paths.get(catalogManager.getConfiguration().getJobDir()).resolve(folder));

        if (!Files.isDirectory(tmpOutdir) && !tmpOutdir.toFile().mkdirs()) {
            throw new IOException("Error creating the COSMIC path: " + tmpOutdir.toAbsolutePath());
        }

        Path cosmicFilePath = initCosmicPath(tmpOutdir);

        if (!Files.exists(cosmicFilePath)) {
            throw new IOException("Error copying COSMIC file to " + cosmicFilePath);
        }

        try {
            return catalogManager.getFileManager().get(study, cosmicFilePath.getFileName().toString(), QueryOptions.empty(), token).first();
        } catch (CatalogException e) {
            File file = new File()
                    .setName(cosmicFilePath.getFileName().toString())
                    .setPath(ParamConstants.RESOURCES_FOLDER + "/cosmic/" + cosmicFilePath.getFileName().toString())
                    .setResource(true);
            InputStream inputStream = Files.newInputStream(cosmicFilePath);
            return catalogManager.getFileManager().upload(study, inputStream, file, false, true, null, null, token).first();
        }
    }

    public static Path initCosmicPath(Path cosmicPath) throws IOException {
        String cosmicFilename = "Small_Cosmic_" + COSMIC_VERSION + "_" + COSMIC_ASSEMBLY + ".tar.gz";
        Path targetPath = Paths.get(VariantStorageBaseTest.getResourceUri("custom_annotation/" + cosmicFilename, cosmicPath));

        if (!Files.exists(targetPath)) {
            throw new IOException("Error copying COSMIC file to " + targetPath);
        }

        return targetPath;
    }

}