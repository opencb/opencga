package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.checkerframework.checker.units.qual.A;
import org.junit.*;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME;
import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.STUDY_PARAM_NAME;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CV_VARIANT_ID_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.commons.datastore.core.QueryOptions.EXCLUDE;

public class CvdbSolrEngineClinicalVariantSummaryTest {

    protected static CvdbSolrEngine cvdbEngine;
    protected static CollectionNameGenerator collectionNameGenerator;
    protected static String organizationId = "test";
    protected static String projectId1 = "project1";
    protected static Study study1;

    protected static String projectId2 = "project2";
    protected static Study study2;

    protected static String projectId3 = "project3";
    protected static Study study3;

    public static CvdbSolrExtenalResource cvdbSolrExternalResource;

    public static OpenCGAEnterpriseCatalogManagerExternalResource catalogManagerResource;

    protected static CatalogManager catalogManager;
    private static String opencgaToken;
    protected static String userToken;
    private static FamilyManager familyManager;

    public static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @BeforeClass
    public static void before() throws Throwable {
        // Catalog setup
        catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();
        catalogManagerResource.before();
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVDB
        collectionNameGenerator = new CollectionNameGenerator(catalogManager);
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId1, userToken);
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(false, organizationId, projectId1, collectionPrefix);
        cvdbSolrExternalResource.before();

        cvdbEngine = cvdbSolrExternalResource.configure();
        cvdbEngine.setCollectionNameGenerator(collectionNameGenerator);
        cvdbEngine.setCatalogManager(catalogManager);

        if (cvdbEngine.getSolrManager().isAlive()) {
            // Enterprise factory init
            InputStream is = CvdbSolrEngineClinicalVariantSummaryTest.class.getClassLoader().getResourceAsStream("enterprise-configuration.yml");
            EnterpriseConfiguration config = EnterpriseConfiguration.load(is);
            EnterpriseFactory.init(catalogManager, config);


            if (cvdbEngine.existCollections(collectionPrefix)) {
                cvdbEngine.removeCollections(collectionPrefix);
            }
            cvdbEngine.createCollections(projectId1, collectionPrefix, userToken);

            collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId2, userToken);
            if (cvdbEngine.existCollections(collectionPrefix)) {
                cvdbEngine.removeCollections(collectionPrefix);
            }
            cvdbEngine.createCollections(projectId2, collectionPrefix, userToken);

            // For testing, the same case is loaded in two different projects
            TestUtilities.loadClinicalAnalsysesInCatalog(Collections.singletonList("ca3.json.gz"), study1, userToken, opencgaToken, catalogManager);
            TestUtilities.loadClinicalAnalsysesInCatalog(Collections.singletonList("ca3.json.gz"), study2, userToken, opencgaToken, catalogManager);

            // CVDB index from catalog
            CvdbIndexResult indexResult = cvdbEngine.indexProject(projectId1, true, userToken);
            System.out.println(indexResult.getFailures());
            assertEquals(1, indexResult.getNumIndexed());
            assertEquals(0, indexResult.getFailures().size());

            indexResult = cvdbEngine.indexProject(projectId2, true, userToken);
            System.out.println(indexResult.getFailures());
            assertEquals(1, indexResult.getNumIndexed());
            assertEquals(0, indexResult.getFailures().size());

            // Check CVDB datastores
            Assert.assertTrue(cvdbEngine.isAvailableCvdbDataStore(projectId1, userToken));
            Assert.assertTrue(cvdbEngine.isAvailableCvdbDataStore(projectId2, userToken));
            Assert.assertFalse(cvdbEngine.isAvailableCvdbDataStore(projectId3, userToken));
        }
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

        catalogManager.getProjectManager().create(projectId1, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study1 = catalogManager.getStudyManager().create(projectId1, "phase1", null, "Phase 1", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();

        catalogManager.getProjectManager().create(projectId2, "Project #2 about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study2 = catalogManager.getStudyManager().create(projectId2, "phase3", null, "Phase 3", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();

        catalogManager.getProjectManager().create(projectId3, "Project #3 about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study3 = catalogManager.getStudyManager().create(projectId3, "phase3", null, "Phase 3", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();
    }

    //-----------------------------------------------------------------------
    // T E S T S
    //-----------------------------------------------------------------------

    @Test
    public void testClinicalVariantSummaryStatsFromVariantId() throws IOException, CvdbException, CatalogException {
        Assume.assumeTrue(cvdbEngine.getSolrManager().isAlive());

        // CVDB query
        String variantId = "X:124481724:T:TA";
        Query query = new Query()
                .append(PROJECT_PARAM_NAME, projectId1)
                .append(STUDY_PARAM_NAME, study1.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId);
        DataResult<ClinicalAnalysis> caResult = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);
        long ciPrim = 0L;
        long ciSec = 0L;
        for (ClinicalAnalysis ca : caResult.getResults()) {
            for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                if (cv.getId().equals(variantId)) {
                    ciPrim++;
                    break;
                }
            }
            if (ca.getInterpretation().getStatus() != null) {
                System.out.println("ca.getInterpretation().getStatus().getId() = " + ca.getInterpretation().getStatus().getId());
            }
            for (Interpretation ci : ca.getSecondaryInterpretations()) {
                if (ci.getStatus() != null) {
                    System.out.println("ci.getStatus().getId() = " + ci.getStatus().getId());
                }
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    if (cv.getId().equals(variantId)) {
                        ciSec++;
                        break;
                    }
                }
            }
        }

        DataResult<ClinicalVariantSummaryStats> result = cvdbEngine.getClinicalVariantSummaryStats(variantId, null, userToken);
        Assert.assertEquals(3, result.getNumResults());
        // Check result IDs, they must match project1, project2 and  ALL
        Assert.assertTrue(result.getResults().stream().anyMatch(r -> r.getId().equals(FqnUtils.buildFqn(organizationId, projectId1))));
        Assert.assertTrue(result.getResults().stream().anyMatch(r -> r.getId().equals(FqnUtils.buildFqn(organizationId, projectId2))));
        Assert.assertTrue(result.getResults().stream().anyMatch(r -> r.getId().equals("ALL")));

        // Check num primary interpretations fopr project1, project2, ALL)
        Assert.assertEquals(1, result.getResults().stream().filter(r -> r.getId().equals(FqnUtils.buildFqn(organizationId, projectId1))).findFirst().get().getNumPrimaryInterpretations());
        Assert.assertEquals(1, result.getResults().stream().filter(r -> r.getId().equals(FqnUtils.buildFqn(organizationId, projectId2))).findFirst().get().getNumPrimaryInterpretations());
        Assert.assertEquals(2, result.getResults().stream().filter(r -> r.getId().equals("ALL")).findFirst().get().getNumPrimaryInterpretations());

        System.out.println("============================================");
        System.out.println("result.first() = " + result.first());
        System.out.println("============================================");
    }

    @Test
    public void testClinicalVariantSummaryStatsFromVariantIds() throws IOException, CvdbException, CatalogException {
        Assume.assumeTrue(cvdbEngine.getSolrManager().isAlive());

        String variantId1 = "X:124481724:T:TA";
        String variantId2 = "X:53196017:G:A";

        // CVDB query
        List<String> variantIds = Arrays.asList(variantId1, variantId2);
        List<String> projectIds = Arrays.asList(FqnUtils.buildFqn(organizationId, projectId1), FqnUtils.buildFqn(organizationId, projectId2), "ALL");

        DataResult<ClinicalVariantSummaryStats> result = cvdbEngine.getClinicalVariantSummaryStats(variantIds, null, userToken);

        Assert.assertEquals(projectIds.size() * variantIds.size(), result.getNumResults());
        for (String variantId : variantIds) {
            for (String projectId : projectIds) {
                Assert.assertTrue(result.getResults().stream().anyMatch(r -> r.getId().equalsIgnoreCase(projectId) && r.getVariantId().equals(variantId)));
            }
        }

        System.out.println("ClinicalVariantSummaryStats, result.getResults().get(0) = " + result.getResults().get(0));
        System.out.println("ClinicalVariantSummaryStats, result.getResults().get(1) = " + result.getResults().get(1));
    }
}

