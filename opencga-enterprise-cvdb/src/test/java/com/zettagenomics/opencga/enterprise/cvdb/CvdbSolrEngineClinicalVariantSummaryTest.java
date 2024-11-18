package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CvdbSolrEngineClinicalVariantSummaryTest {

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
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, projectId);
        cvdbSolrExternalResource.before();

        catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();
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
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);

        // CVDB index from catalog
        CvdbIndexResult indexResult = cvdbEngine.indexProject(projectId, catalogManager, true, userToken);
        System.out.println(indexResult.getFailures());
        assertEquals(2, indexResult.getNumIndexed());
        assertEquals(0, indexResult.getFailures().size());
    }

    public static void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(ADMIN_PASSWORD).getToken();

        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setName("User Name").setOrganization(organizationId), PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user2").setName("User Name2").setOrganization(organizationId), PASSWORD, opencgaToken);

        catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams()
                        .setOwner("user"),
                null, opencgaToken);

        userToken = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).getToken();

        catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null,
                userToken).first();
    }

    //-----------------------------------------------------------------------
    // T E S T S
    //-----------------------------------------------------------------------

    @Test
    public void testCvdbContent() throws IOException, CvdbException, CatalogException {
        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> caResult = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        System.out.println("num. ca = " + caResult.getNumResults());
        assertTrue(caResult.getNumResults() > 0);
        DataResult<Interpretation> ciResult = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        System.out.println("num. ci = " + ciResult.getNumResults());
        assertTrue(ciResult.getNumResults() > 0);
        DataResult<ClinicalVariant> cvResult = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        System.out.println("num. cv = " + cvResult.getNumResults());
        assertTrue(cvResult.getNumResults() > 0);
        DataResult<ClinicalVariantEvidence> cveResult = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        System.out.println("num. cve = " + cveResult.getNumResults());
        assertTrue(cveResult.getNumResults() > 0);
    }

    @Test
    public void testClinicalVariantSummaryStatsFromVariantId() throws IOException, CvdbException, CatalogException {
        // CVDB query
        String variantId = "X:54751204:C:T";

        DataResult<ClinicalVariantSummaryStats> result = cvdbEngine.getClinicalVariantSummaryStats(variantId, null, null, userToken);

        Assert.assertEquals(1, result.getNumResults());
        Assert.assertEquals(1, result.first().getNumCases());
//        Assert.assertEquals(1, result.first().getNumPrimaryInterpretations());
//        Assert.assertEquals(4, result.first().getNumSecondaryInterpretations());
//        Assert.assertEquals(1, result.first().getPrimaryInterpretationSummary().getEvidencePhenotypeCounts().size());
//        Assert.assertEquals(2, (int) result.first().getPrimaryInterpretationSummary().getEvidencePhenotypeCounts().get("VACTERL-like phenotypes"));
//        Assert.assertEquals(1, result.first().getPrimaryInterpretationSummary().getEvidenceReviewTierCounts().size());
//        Assert.assertEquals(2, (int) result.first().getPrimaryInterpretationSummary().getEvidenceReviewTierCounts().get("TIER3"));

        System.out.println("============================================");
        System.out.println("result.first() = " + result.first());
        System.out.println("============================================");
    }

    @Test
    public void testClinicalVariantSummaryStatsFromVariantIds() throws IOException, CvdbException, CatalogException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        DataResult<ClinicalVariantSummaryStats> result = cvdbEngine.getClinicalVariantSummaryStats(variantIds, null, projectId, userToken);

        Assert.assertEquals(2, result.getNumResults());
        Assert.assertEquals(1, result.first().getNumCases());
//        Assert.assertEquals(1, result.first().getNumPrimaryInterpretations());
//        Assert.assertEquals(4, result.first().getNumSecondaryInterpretations());
//        Assert.assertEquals(1, result.first().getPrimaryInterpretationSummary().getEvidencePhenotypeCounts().size());
//        Assert.assertEquals(2, (int) result.first().getPrimaryInterpretationSummary().getEvidencePhenotypeCounts().get("VACTERL-like phenotypes"));
//        Assert.assertEquals(1, result.first().getPrimaryInterpretationSummary().getEvidenceReviewTierCounts().size());
//        Assert.assertEquals(2, (int) result.first().getPrimaryInterpretationSummary().getEvidenceReviewTierCounts().get("TIER3"));
        Assert.assertEquals(1, result.getResults().get(1).getNumCases());
//        Assert.assertEquals(1, result.getResults().get(1).getNumPrimaryInterpretations());
//        Assert.assertEquals(3, result.getResults().get(1).getNumSecondaryInterpretations());
//        Assert.assertEquals(1, result.getResults().get(1).getPrimaryInterpretationSummary().getEvidencePhenotypeCounts().size());
//        Assert.assertEquals(2, (int) result.getResults().get(1).getPrimaryInterpretationSummary().getEvidencePhenotypeCounts().get("Ultra-rare undescribed monogenic disorders"));
//        Assert.assertEquals(1, result.getResults().get(1).getPrimaryInterpretationSummary().getEvidenceReviewTierCounts().size());
//        Assert.assertEquals(2, (int) result.getResults().get(1).getPrimaryInterpretationSummary().getEvidenceReviewTierCounts().get("TIER3"));

        System.out.println("============================================");
        System.out.println("result.getResults().get(0) = " + result.getResults().get(0));
        System.out.println("result.getResults().get(1) = " + result.getResults().get(1));
        System.out.println("============================================");
    }

    @Test
    public void testUpdateSummaryStats() throws IOException, CvdbException, CatalogException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        DataResult<ClinicalVariantSummaryStats> result = cvdbEngine.getClinicalVariantSummaryStats(variantIds, null, projectId, userToken);
        Assert.assertEquals(2, result.getNumResults());

        System.out.println("============================================");
        System.out.println("result.getResults().get(0) = " + result.getResults().get(0));
        System.out.println("result.getResults().get(1) = " + result.getResults().get(1));
        System.out.println("After updating 1 with 0");
        cvdbEngine.updateSummaryStats(result.getResults().get(0), result.getResults().get(1));
        System.out.println("result.getResults().get(1) = " + result.getResults().get(1));
        System.out.println("============================================");
    }
}

