package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbIndexTask;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbUpdateAclTask;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.junit.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.tools.ToolFactory;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.CvdbIndexStatus;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;

public class CvdbTaskTest {

    protected String organizationId = "test";
    protected String projectId = "project1";
    protected Study study;

    private ToolRunner toolRunner;

    public CvdbSolrExtenalResource cvdbSolrExternalResource;

    @Rule
    public OpenCGAEnterpriseCatalogManagerExternalResource catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();

    protected CatalogManager catalogManager;

    private String opencgaToken;
    protected String sessionIdUser;
    private FamilyManager familyManager;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void before() throws Throwable {
        // Catalog
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVBD
        CollectionNameGenerator collectionNameGenerator = new CollectionNameGenerator(catalogManager);
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId, sessionIdUser);
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, organizationId, projectId, collectionPrefix);
        cvdbSolrExternalResource.before();

        // Copy the enterprise configuration in the opencga home
        InputStream stream = CvdbIndexTask.class.getClassLoader().getResourceAsStream("enterprise-configuration.yml");
        Path confPath = catalogManagerResource.getOpencgaHome().toAbsolutePath().resolve("conf");
        confPath.toFile().mkdirs();
        Files.copy(stream, confPath.resolve("enterprise-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        // Prepare storage engine factory
        StorageConfiguration storageConfig = StorageConfiguration.load(CvdbIndexTask.class.getClassLoader().getResource("storage-configuration.yml").openStream());
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfig);

        toolRunner = new ToolRunner(catalogManagerResource.getOpencgaHome().toString(), catalogManager, storageEngineFactory);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(ADMIN_PASSWORD).first().getToken();

        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setName("User Name").setOrganization(organizationId), PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user2").setName("User Name2").setOrganization(organizationId), PASSWORD, opencgaToken);

        catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams()
                        .setOwner("user"),
                null, opencgaToken);

        sessionIdUser = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).first().getToken();

        catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first();
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null,
                INCLUDE_RESULT, sessionIdUser).first();
    }

    @Test
    public void testIndexTask() throws CatalogException, IOException, ToolException {
        Assume.assumeTrue(solrIsAlive());

        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, sessionIdUser, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, sessionIdUser);

        // Run clinical analysis load task
        Path indexOutDir = getTempDir();
        System.out.println("Clinical analysis index task out dir = " + indexOutDir.toAbsolutePath());

        CvdbIndexTaskParams params = new CvdbIndexTaskParams();
        params.setAllProject(true);
        params.setOverwrite(true);

        toolRunner.execute(CvdbIndexTask.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, study.getId()), indexOutDir, null,
                false, sessionIdUser);

        ExecutionResult result = JacksonUtils.getDefaultObjectMapper().readerFor(ExecutionResult.class)
                .readValue(indexOutDir.resolve(CvdbIndexTask.ID + ".result.json").toFile());
        int numIndexed = result.getAttributes().getInt(CvdbIndexTask.NUM_INDEXED_ATTR);
        Assert.assertEquals(2, numIndexed);
        int numFailures = result.getAttributes().getInt(CvdbIndexTask.NUM_NOT_INDEXED_ATTR);
        Assert.assertEquals(0, numFailures);

        indexOutDir = getTempDir();
        System.out.println("Clinical analysis index task out dir = " + indexOutDir.toAbsolutePath());

        params = new CvdbIndexTaskParams();
        params.setAllProject(true);
        params.setOverwrite(false);

        toolRunner.execute(CvdbIndexTask.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, study.getId()), indexOutDir, null,
                false, sessionIdUser);

        result = JacksonUtils.getDefaultObjectMapper().readerFor(ExecutionResult.class)
                .readValue(indexOutDir.resolve(CvdbIndexTask.ID + ".result.json").toFile());
        numIndexed = result.getAttributes().getInt(CvdbIndexTask.NUM_INDEXED_ATTR);
        Assert.assertEquals(0, numIndexed);
        numFailures = result.getAttributes().getInt(CvdbIndexTask.NUM_NOT_INDEXED_ATTR);
        Assert.assertEquals(2, numFailures);
    }

    @Test
    public void testFactoryToolCvdbIndexTask() throws ToolException {
        Class<? extends OpenCgaTool> tool = new ToolFactory().getToolClass(CvdbIndexTask.ID, Arrays.asList("org.opencb.opencga", "com.zettagenomics.opencga.enterprise"));
        System.out.println("tool.getName() = " + tool.getName());
        Assert.assertTrue(tool.getName().endsWith("CvdbIndexTask"));
    }

    @Test
    public void testFactoryToolCvdbUpdateAclTask() throws ToolException {
        Class<? extends OpenCgaTool> tool = new ToolFactory().getToolClass(CvdbUpdateAclTask.ID, Arrays.asList("org.opencb.opencga", "com.zettagenomics.opencga.enterprise"));
        System.out.println("tool.getName() = " + tool.getName());
        Assert.assertTrue(tool.getName().endsWith("CvdbUpdateAclTask"));
    }

    @Test
    public void testIndexTaskOverwrite() throws CatalogException, IOException, ToolException {
        Assume.assumeTrue(solrIsAlive());

        catalogManagerResource.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId(),
                sessionIdUser);

        // Run clinical analysis load task
        Path indexOutDir = getTempDir();
        System.out.println("Clinical analysis index task out dir = " + indexOutDir.toAbsolutePath());

        CvdbIndexTaskParams params = new CvdbIndexTaskParams();
        params.setAllProject(true);
        params.setOverwrite(true);

        toolRunner.execute(CvdbIndexTask.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, study.getId()), indexOutDir, null,
                false, sessionIdUser);

        ExecutionResult result = JacksonUtils.getDefaultObjectMapper().readerFor(ExecutionResult.class)
                .readValue(indexOutDir.resolve(CvdbIndexTask.ID + ".result.json").toFile());
        int numIndexed = result.getAttributes().getInt(CvdbIndexTask.NUM_INDEXED_ATTR);
        Assert.assertEquals(2, numIndexed);
        int numFailures = result.getAttributes().getInt(CvdbIndexTask.NUM_NOT_INDEXED_ATTR);
        Assert.assertEquals(0, numFailures);
    }

    public static Path getTempDir() throws IOException {
        Path path;
        int c = 0;
        do {
            path = Paths.get("target/test-data").resolve("junit_tmp_" + TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (path.toFile().exists());
        Files.createDirectories(path);
        return path;
    }

    public static boolean solrIsAlive() throws IOException {
        // Get enterprise configuration to set the CVDB engine
        EnterpriseConfiguration enterpriseConfiguration = EnterpriseConfiguration.load(CvdbIndexTask.class.getClassLoader()
                .getResource("enterprise-configuration.yml").openStream());

        return new SolrManager(enterpriseConfiguration.getCvdb().getDatabase().getHosts(),
                enterpriseConfiguration.getCvdb().getDatabase().getMode(),
                enterpriseConfiguration.getCvdb().getDatabase().getTimeout()).isAlive();
    }

}
