package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.CollectionPrefixUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.CvdbIndexStatus;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.util.Arrays;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.CLINICAL_ANALYSES_COLLECTION_SUFFIX;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.CollectionPrefixUtils.CVDB_DBPREFIX_KEY;
import static org.junit.Assert.*;

public class CvdbSolrEngineDbPrefixTest {

    protected CvdbSolrEngine cvdbEngine;
    protected String organizationId = "test";
    protected String projectId = "project1";
    protected Study study;

    protected String CVDB_PREFIX = "opencga_prefix_attrs";

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
        String collectionPrefix = CollectionPrefixUtils.getInstance(catalogManager).getCollectionPrefix(organizationId, projectId,
                userToken);
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, organizationId, projectId, collectionPrefix);
        cvdbSolrExternalResource.before();

        cvdbEngine = cvdbSolrExternalResource.configure();
        cvdbEngine.setCatalogManager(catalogManager);
        cvdbEngine.setVariantStorageMetadataManager(new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory()));

        if (!cvdbEngine.existCollections(collectionPrefix)) {
            cvdbEngine.createCollections(collectionPrefix);
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

        catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();

        ObjectMap objectMap = new ObjectMap();
        ObjectMap attrs = new ObjectMap();
        attrs.put(CVDB_DBPREFIX_KEY, CVDB_PREFIX);
        objectMap.put(ProjectDBAdaptor.QueryParams.ATTRIBUTES.key(), attrs);

        OpenCGAResult<Project> update = catalogManager.getProjectManager().update(projectId, objectMap, INCLUDE_RESULT, userToken);
        assertEquals(1, update.getNumResults());
        assertTrue(MapUtils.isNotEmpty(update.first().getAttributes()));
        assertTrue(update.first().getAttributes().containsKey(CVDB_DBPREFIX_KEY));
        assertEquals(CVDB_PREFIX, update.first().getAttributes().get(CVDB_DBPREFIX_KEY));

        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();
    }

    @Test
    public void testIndexProjectDbPrefixInAttrs() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        // CVDB index from catalog project
        cvdbEngine.indexProject(projectId, catalogManager, true, userToken);

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        String collectionPrefix = CollectionPrefixUtils.getInstance(catalogManager).getCollectionPrefix(organizationId, projectId,
                userToken);

        assertTrue(collectionPrefix.startsWith(CVDB_PREFIX));
        assertFalse(collectionPrefix.startsWith(catalogManager.getConfiguration().getDatabasePrefix()));

        QueryResponse response = cvdbEngine.getSolrClient().query(CollectionPrefixUtils.getCollectionName(collectionPrefix,
                        CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(3, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);
    }
}


