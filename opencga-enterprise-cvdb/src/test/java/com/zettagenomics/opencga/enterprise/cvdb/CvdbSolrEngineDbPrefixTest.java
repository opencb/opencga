package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.CvdbIndexStatus;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CvdbSolrEngineDbPrefixTest {

    protected CvdbSolrEngine cvdbEngine;
    protected CollectionNameGenerator collectionNameGenerator;
    protected String organizationId = "test";
    protected String projectId = "project1";
    protected Project project;
    protected Study study;

    protected String CVDB_PREFIX = "opencga_prefix_in_cvdb_datastore";

    @Rule
//    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();
    public OpenCGAEnterpriseCatalogManagerExternalResource catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();

    public CvdbSolrExtenalResource cvdbSolrExternalResource;

    protected CatalogManager catalogManager;
    private String opencgaToken;
    protected String userToken;
    private FamilyManager familyManager;

    public static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void before() throws Throwable {
        // Catalog
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVDB
        collectionNameGenerator = new CollectionNameGenerator(catalogManager);
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId, userToken);
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, organizationId, projectId, collectionPrefix);
        cvdbSolrExternalResource.before();

        cvdbEngine = cvdbSolrExternalResource.configure();
        cvdbEngine.setCatalogManager(catalogManager);
        cvdbEngine.setCollectionNameGenerator(collectionNameGenerator);

        if (!cvdbEngine.existCollections(collectionPrefix)) {
            cvdbEngine.createCollections(projectId, collectionPrefix, userToken);

            project = catalogManager.getProjectManager().get(projectId, QueryOptions.empty(), userToken).first();
            System.out.println("project.getInternal().getDatastores().getCvdb().toString() = " + project.getInternal().getDatastores().getCvdb().toString());
            Assert.assertEquals("solr", project.getInternal().getDatastores().getCvdb().getStorageEngine());
        }
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

        userToken = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).first().getToken();

        Project project = catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();

        // Add other projects
        catalogManager.getProjectManager().create("Project-2", "Project 2", "", "Homo sapiens", null, "GRCh38", INCLUDE_RESULT, userToken);
        catalogManager.getProjectManager().create("Project-3", "Project 3", "", "Homo sapiens", null, "GRCh38", INCLUDE_RESULT, userToken);

        DataStore cvdbDataStore = VariantStorageManager.defaultCvdbDataStore(CVDB_PREFIX, project.getFqn());
        catalogManager.getProjectManager().setDatastoreCvdb(projectId, cvdbDataStore, userToken);

        Project updatedProject = catalogManager.getProjectManager().get(projectId, QueryOptions.empty(), userToken).first();
        Assert.assertTrue(updatedProject.getInternal().getDatastores().getCvdb().getDbName().startsWith(CVDB_PREFIX));

        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();
    }

    @Test
    public void testIndexProjectDbPrefixFromDatastore() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        // CVDB index from catalog project
        cvdbEngine.indexProject(projectId, true, userToken);

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId, userToken);

        assertTrue(collectionPrefix.startsWith(CVDB_PREFIX));
        assertFalse(collectionPrefix.startsWith(catalogManager.getConfiguration().getDatabasePrefix()));

        String collectionName = collectionNameGenerator.getClinicalAnalysisCollectionName(collectionPrefix);
        QueryResponse response = cvdbEngine.getSolrClient().query(collectionName, solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(3, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);


        DataStore cvdbDatastore = cvdbEngine.getCvdbDatastore(project.getFqn(), userToken);
        System.out.println("cvdbDatastore = " + cvdbDatastore);
        Assert.assertEquals("solr", cvdbDatastore.getStorageEngine());
        Assert.assertTrue(cvdbDatastore.getDbName().startsWith(CVDB_PREFIX));
        Assert.assertEquals(CVDB_PREFIX + "_cvdb_" + organizationId + "_" + projectId, cvdbDatastore.getDbName());

        List<String> cvdbProjectIds = cvdbEngine.getCvdbProjects(Collections.singletonList(organizationId), userToken);
        System.out.println("cvdbProjectIds = " + StringUtils.join(cvdbProjectIds, ", "));
        Assert.assertEquals(1, cvdbProjectIds.size());
        Assert.assertEquals(projectId, FqnUtils.getProject(cvdbProjectIds.get(0)));
    }
}


