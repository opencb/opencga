package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.CollectionPrefixUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.commons.datastore.solr.FacetQueryParser.FACET_SEPARATOR;

public class ClinicalAggregationTest {

    protected static CvdbSolrEngine cvdbEngine;
    protected static String organizationId = "test";
    protected static String projectId = "project1";
    protected static Study study;

    public static CvdbSolrExtenalResource cvdbSolrExternalResource;

    public static OpenCGAEnterpriseCatalogManagerExternalResource catalogManagerResource;

    protected static CatalogManager catalogManager;
    private static String opencgaToken;
    protected static String userToken;
    private static FamilyManager familyManager;

    public static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @BeforeClass
    public static void before() throws Throwable {
        catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();
        catalogManagerResource.before();

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
            cvdbEngine.createCollections(projectId, collectionPrefix, userToken);
        }

        // Load and index
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca3.json.gz"), study, userToken, opencgaToken,
                catalogManager);

        // CVDB index from catalog
        CvdbIndexResult indexResult = cvdbEngine.indexProject(projectId, catalogManager, true, userToken);
        System.out.println(indexResult.getFailures());
        assertEquals(2, indexResult.getNumIndexed());
        assertEquals(0, indexResult.getFailures().size());
    }

    public static void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
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
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();
    }

    //-----------------------------------------------------------------------
    // T E S T S
    //-----------------------------------------------------------------------

    @Test
    public void testFacetClinicalAnalyses() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();

        // Check existing type
        String facetName = "disorderId";
//        queryOptions.put(QueryOptions.FACET, CA_DISORDER_ID_NAME);
        queryOptions.put(QueryOptions.FACET, facetName);
        query = new Query(PROJECT_PARAM_NAME, projectId);
        DataResult<FacetField> facetResult = cvdbEngine.facetClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, facetResult.getNumResults());
        assertEquals(2L, facetResult.first().getCount());
        assertEquals("disorderId", facetResult.first().getName());
        for (FacetField result : facetResult.getResults()) {
            System.out.println(result);
        }
    }

    @Test
    public void testMultipleFacetClinicalAnalyses() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();

        // Check existing type
//        queryOptions.put(QueryOptions.FACET, CA_TYPE_NAME + FACET_SEPARATOR + CA_DISORDER_ID_NAME);
        queryOptions.put(QueryOptions.FACET, "type" + FACET_SEPARATOR + "disorderId");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        DataResult<FacetField> facetResult = cvdbEngine.facetClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(2, facetResult.getNumResults());
        Set<String> fieldNames = facetResult.getResults().stream().map(f -> f.getName()).collect(Collectors.toSet());
        for (String name : queryOptions.getString(QueryOptions.FACET).split(FACET_SEPARATOR)) {
            assertTrue(fieldNames.contains(name));
        }
        for (FacetField result : facetResult.getResults()) {
            System.out.println(result);
        }
    }

    @Test
    public void testNestedFacetClinicalAnalyses() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();

        // Check existing type
        queryOptions.put(QueryOptions.FACET, "type" + FacetQueryParser.NESTED_FACET_SEPARATOR + "disorderId");
//        queryOptions.put(QueryOptions.FACET, CA_TYPE_NAME + FacetQueryParser.NESTED_FACET_SEPARATOR + CA_DISORDER_ID_NAME);
        query = new Query(PROJECT_PARAM_NAME, projectId);
        DataResult<FacetField> facetResult = cvdbEngine.facetClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, facetResult.getNumResults());
        assertEquals("type", facetResult.first().getName());
        assertEquals("disorderId", facetResult.first().getBuckets().get(0).getFacetFields().get(0).getName());
        for (FacetField result : facetResult.getResults()) {
            System.out.println(result);
        }
    }
}
