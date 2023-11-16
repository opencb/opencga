package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
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
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import javax.validation.constraints.AssertTrue;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.commons.datastore.solr.FacetQueryParser.FACET_SEPARATOR;

public class ClinicalAggregationTest {

    protected static CvdbSolrEngine cvdbEngine;
    protected static String projectId = "project1";
    protected static Study study;

    public static CvdbSolrExtenalResource cvdbSolrExternalResource;

    public static CatalogManagerExternalResource catalogManagerResource;

    protected static CatalogManager catalogManager;
    private static String opencgaToken;
    protected static String sessionIdUser;
    private static FamilyManager familyManager;

    public static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @BeforeClass
    public static void before() throws Throwable {
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, projectId);
        cvdbSolrExternalResource.before();

        catalogManagerResource = new CatalogManagerExternalResource();
        catalogManagerResource.before();

        // Catalog
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVDB
        cvdbEngine = cvdbSolrExternalResource.configure();
        cvdbEngine.setCatalogManager(catalogManager);
        cvdbEngine.setVariantStorageMetadataManager(new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory()));

        if (!cvdbEngine.existCollections(projectId)) {
            cvdbEngine.createCollections(projectId);
        }

        // Load and index
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog
        CvdbIndexResult indexResult = cvdbEngine.indexProject(projectId, catalogManager, true, sessionIdUser);
        System.out.println(indexResult.getFailures());
        assertEquals(2, indexResult.getNumIndexed());
        assertEquals(0, indexResult.getFailures().size());
    }

    public static void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(ADMIN_PASSWORD).getToken();

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null,
                Account.AccountType.FULL, opencgaToken);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        catalogManager.getUserManager().create("user2", "User Name2", "mail2@ebi.ac.uk", PASSWORD, "", null,
                Account.AccountType.GUEST, opencgaToken);

        catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first();
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null,
                sessionIdUser).first();
    }

    //-----------------------------------------------------------------------
    // T E S T S
    //-----------------------------------------------------------------------

    @Test
    public void testFacetClinicalAnalyses() throws IOException, SolrServerException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();

        // Check existing type
        queryOptions.put(QueryOptions.FACET, CA_DISORDER_ID_NAME);
        query = new Query(PROJECT_PARAM_NAME, projectId);
        DataResult<FacetField> facetResult = cvdbEngine.facetClinicalAnalyses(query, queryOptions, null);
        assertEquals(1, facetResult.getNumResults());
        assertEquals(2, facetResult.first().getCount());
        assertEquals(CA_DISORDER_ID_NAME, facetResult.first().getName());
        for (FacetField result : facetResult.getResults()) {
            System.out.println(result);
        }
    }

    @Test
    public void testMultipleFacetClinicalAnalyses() throws IOException, SolrServerException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();

        // Check existing type
        queryOptions.put(QueryOptions.FACET, CA_TYPE_NAME + FACET_SEPARATOR + CA_DISORDER_ID_NAME);
        query = new Query(PROJECT_PARAM_NAME, projectId);
        DataResult<FacetField> facetResult = cvdbEngine.facetClinicalAnalyses(query, queryOptions, null);
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
    public void testNestedFacetClinicalAnalyses() throws IOException, SolrServerException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();

        // Check existing type
        queryOptions.put(QueryOptions.FACET, CA_TYPE_NAME + FacetQueryParser.NESTED_FACET_SEPARATOR + CA_DISORDER_ID_NAME);
        query = new Query(PROJECT_PARAM_NAME, projectId);
        DataResult<FacetField> facetResult = cvdbEngine.facetClinicalAnalyses(query, queryOptions, null);
        assertEquals(1, facetResult.getNumResults());
        assertEquals(CA_TYPE_NAME, facetResult.first().getName());
        assertEquals(CA_DISORDER_ID_NAME, facetResult.first().getBuckets().get(0).getFacetFields().get(0).getName());
        for (FacetField result : facetResult.getResults()) {
            System.out.println(result);
        }
    }

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

    private static void loadClinicalAnalsysesInCatalog(List<String> caFilenames, String studyId) throws IOException, CatalogException {
        for (String caFilename : caFilenames) {
            URL resource = ClinicalInterpretationConverterTest.class.getClassLoader().getResource(caFilename);
            ClinicalAnalysisLoadResult loadResult = catalogManager.getClinicalAnalysisManager().load(studyId, Paths.get(resource.getPath()),
                    sessionIdUser);
            System.out.println(loadResult);
        }
        OpenCGAResult<ClinicalAnalysis> results = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), new Query(),
                QueryOptions.empty(), opencgaToken);
        for (ClinicalAnalysis clinicalAnalysis : results.getResults()) {
            catalogManager.getClinicalAnalysisManager().updateAcl(study.getFqn(), Collections.singletonList(clinicalAnalysis.getId()),
                    "user", new ClinicalAnalysisAclUpdateParams(null, "VIEW"), ParamUtils.AclAction.SET, false, opencgaToken);
        }
    }
}
