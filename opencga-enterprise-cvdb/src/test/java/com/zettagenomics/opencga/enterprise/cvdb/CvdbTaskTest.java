package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbIndexTask;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.junit.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

public class CvdbTaskTest {

    protected String projectId = "project1";
    protected Study study;

    private ToolRunner toolRunner;

    @Rule
    public CvdbSolrExtenalResource cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, projectId);;

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;

    private String opencgaToken;
    protected String sessionIdUser;
    private FamilyManager familyManager;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void before() throws Exception {
        // Catalog
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

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

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(CatalogManagerExternalResource.ADMIN_PASSWORD).getToken();

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", CatalogManagerExternalResource.PASSWORD, "", null,
                Account.AccountType.FULL, opencgaToken);
        sessionIdUser = catalogManager.getUserManager().login("user", CatalogManagerExternalResource.PASSWORD).getToken();

        catalogManager.getUserManager().create("user2", "User Name2", "mail2@ebi.ac.uk", CatalogManagerExternalResource.PASSWORD, "", null,
                Account.AccountType.GUEST, opencgaToken);

        catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first();
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null,
                sessionIdUser).first();
    }

    @Test
    public void testIndexTask() throws CatalogException, IOException, ToolException {
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
                sessionIdUser);

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
                sessionIdUser);

        result = JacksonUtils.getDefaultObjectMapper().readerFor(ExecutionResult.class)
                .readValue(indexOutDir.resolve(CvdbIndexTask.ID + ".result.json").toFile());
        numIndexed = result.getAttributes().getInt(CvdbIndexTask.NUM_INDEXED_ATTR);
        Assert.assertEquals(0, numIndexed);
        numFailures = result.getAttributes().getInt(CvdbIndexTask.NUM_NOT_INDEXED_ATTR);
        Assert.assertEquals(2, numFailures);
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
                sessionIdUser);

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

        CvdbSolrEngine cvdbEngine = new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), null);
        return cvdbEngine.getSolrManager().isAlive();
    }

}
