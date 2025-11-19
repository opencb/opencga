package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.junit.Assert.*;
import static org.opencb.commons.datastore.core.QueryOptions.LIMIT;

public class CvdbSolrEngineQueryPermissionsTest {

    protected static CvdbSolrEngine cvdbEngine;
    protected static CollectionNameGenerator collectionNameGenerator;
    protected static String organizationId = "test";
    protected static String projectId = "project1";
    protected static Study study;

    public static CvdbSolrExtenalResource cvdbSolrExternalResource;

    public static OpenCGAEnterpriseCatalogManagerExternalResource catalogManagerResource;

    protected static CatalogManager catalogManager;
    private static String opencgaToken;
    protected static String userToken;
    private static FamilyManager familyManager;

    private static String collectionPrefix;

    public static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    protected static String user2ViewerforCaId = null;

    @BeforeClass
    public static void before() throws Throwable {

        catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();
        catalogManagerResource.before();

        // Catalog
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVDB
        collectionNameGenerator = new CollectionNameGenerator(catalogManager);
        collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId, userToken);
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, organizationId, projectId, collectionPrefix);
        cvdbSolrExternalResource.before();

        cvdbEngine = cvdbSolrExternalResource.configure();
        cvdbEngine.setCatalogManager(catalogManager);
        cvdbEngine.setCollectionNameGenerator(collectionNameGenerator);

        if (!cvdbEngine.existCollections(collectionPrefix)) {
            cvdbEngine.createCollections(projectId, collectionPrefix, userToken);
        }

        // Load and index
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca3.json.gz"), study);

        // CVDB index from catalog
        CvdbIndexResult indexResult = cvdbEngine.indexProject(projectId, true, userToken);
        System.out.println(indexResult.getFailures());
        assertEquals(2, indexResult.getNumIndexed());
        assertEquals(0, indexResult.getFailures().size());
    }

    public static void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(ADMIN_PASSWORD).first().getToken();

        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setName("User Name").setOrganization(organizationId), PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user2").setName("User Name2").setOrganization(organizationId), PASSWORD, opencgaToken);

        catalogManager.getUserManager().create(new User().setId("user4").setName("User Name4").setOrganization(organizationId), PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user5").setName("User Name5").setOrganization(organizationId), PASSWORD, opencgaToken);

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

    @Test(expected = CatalogAuthenticationException.class)
    public void testQueryClinicalAnalyses() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");

        // "user" can access to all clinical analyses
        String token = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).first().getToken();
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
        assertEquals(2, result.getNumResults());
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
        }

        // "user2" only can access the clinical analysis "user2ViewerforCaId"
        token = catalogManager.getUserManager().login(organizationId, "user2", PASSWORD).first().getToken();
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
        assertEquals(1, result.getNumResults());
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(user2ViewerforCaId, ca.getId());
            assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
        }

        // "user3" can not access any clinical analyses
        // (expected = CatalogAuthenticationException.class)
        token = catalogManager.getUserManager().login(organizationId, "user3", PASSWORD).first().getToken();
        cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testQueryClinicalVariants() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");

        // "user" can access to all clinical analyses
        String token = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).first().getToken();
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, token);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String ciId = (String) cv.getAttributes().get(OPENCGA_INTERPRETATION_ID);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = TestUtilities.getClinicalInterpretation(ciId, projectId, cvdbEngine, userToken);
                assertTrue(ci.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
                alreadyChecked.add(ciId);
            }
        }

        // "user2" only can access the clinical analysis "user2ViewerforCaId"
        token = catalogManager.getUserManager().login(organizationId, "user2", PASSWORD).first().getToken();
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, token);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String ciId = (String) cv.getAttributes().get(OPENCGA_INTERPRETATION_ID);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = TestUtilities.getClinicalInterpretation(ciId, projectId, cvdbEngine, userToken);
                assertTrue(ci.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
                assertEquals(user2ViewerforCaId, ci.getClinicalAnalysisId());
                alreadyChecked.add(ciId);
            }
        }

        // "user3" can not access any clinical analyses
        // (expected = CatalogAuthenticationException.class)
        token = catalogManager.getUserManager().login(organizationId, "user3", PASSWORD).first().getToken();
        cvdbEngine.searchClinicalVariants(query, queryOptions, token);
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testQueryClinicalInterpretations() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");

        // "user" can access to all clinical analyses
        String token = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).first().getToken();
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, token);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(ci.getPrimaryFindings().stream().map(v -> v.getType().name()).collect(Collectors.toList()).contains(query.getString(CV_TYPE_NAME)));
        }

        // "user2" only can access the clinical analysis "user2ViewerforCaId"
        token = catalogManager.getUserManager().login(organizationId, "user2", PASSWORD).first().getToken();
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, token);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(ci.getPrimaryFindings().stream().map(v -> v.getType().name()).collect(Collectors.toList()).contains(query.getString(CV_TYPE_NAME)));
            assertEquals(user2ViewerforCaId, ci.getClinicalAnalysisId());
        }

        // "user3" can not access any clinical analyses
        // expected = CatalogAuthenticationException.class
        token = catalogManager.getUserManager().login(organizationId, "user3", PASSWORD).first().getToken();
        cvdbEngine.searchClinicalInterpretations(query, queryOptions, token);
    }

    @Test(expected = CatalogAuthenticationException.class)
    public void testQueryClinicalVariantEvidences() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");

        String token = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).first().getToken();
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, token);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String variantId = (String) cve.getAttributes().get(OPENCGA_VARIANT_ID);
            assertTrue(StringUtils.isNotEmpty(variantId));
            ClinicalVariant cv = TestUtilities.getClinicalVariant(variantId, projectId, cvdbEngine, userToken);
            assertEquals(query.getString(CV_TYPE_NAME), cv.getType().name());
        }

        // "user2" only can access the clinical analysis "user2ViewerforCaId"
        token = catalogManager.getUserManager().login(organizationId, "user2", PASSWORD).first().getToken();
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, token);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String variantId = (String) cve.getAttributes().get(OPENCGA_VARIANT_ID);
            assertTrue(StringUtils.isNotEmpty(variantId));
            ClinicalVariant cv = TestUtilities.getClinicalVariant(variantId, projectId, cvdbEngine, userToken);
            assertEquals(query.getString(CV_TYPE_NAME), cv.getType().name());

            String ciId = (String) cve.getAttributes().get(OPENCGA_INTERPRETATION_ID);
            Interpretation ci = TestUtilities.getClinicalInterpretation(ciId, projectId, cvdbEngine, userToken);
            assertEquals(user2ViewerforCaId, ci.getClinicalAnalysisId());
        }


        // "user3" can not access any clinical analyses
        // (expected = CatalogAuthenticationException.class)
        token = catalogManager.getUserManager().login(organizationId, "user3", PASSWORD).first().getToken();
        cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, token);
    }

    @Test(expected = CatalogException.class)
    public void testAnonymous() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");

        // No token provided
        // expected = CatalogException.class
        cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
    }

    @Test
    public void testUpdateViewers() throws IOException, CvdbException, CatalogException, SolrServerException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, study.getId());
        query.put(CA_TYPE_NAME, "FAMILY");

        // "user4" can not access to these clinical analyses
        String token = catalogManager.getUserManager().login(organizationId, "user4", PASSWORD).first().getToken();

        DataResult<ClinicalAnalysis> result = null;
        try {
            result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
            fail();
        } catch (Exception e) {
            // Expected
            Assert.assertTrue(e instanceof CatalogAuthorizationException);
            Assert.assertEquals(null, result);
        }

        OpenCGAResult<ClinicalAnalysis> results = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), new Query(),
                QueryOptions.empty(), opencgaToken);
        for (ClinicalAnalysis clinicalAnalysis : results.getResults()) {
            catalogManager.getClinicalAnalysisManager().updateAcl(study.getFqn(),
                    Collections.singletonList(clinicalAnalysis.getId()), "user4", new ClinicalAnalysisAclUpdateParams(null, "VIEW"),
                    ParamUtils.AclAction.SET, false, opencgaToken);

            OpenCGAResult<Acl> aclResult = catalogManager.getAdminManager().getEffectivePermissions(study.getFqn(), Collections.singletonList(clinicalAnalysis.getId()),
                    Collections.singletonList(ClinicalAnalysisPermissions.VIEW.name()), Enums.Resource.CLINICAL_ANALYSIS.name(), opencgaToken);
            cvdbEngine.indexViewers(aclResult.first().getId(), study.getId(), aclResult.first().getPermissions().get(0).getUserIds(), collectionPrefix);
            // Only one clinical analysis is updated
            break;
        }

        token = catalogManager.getUserManager().login(organizationId, "user4", PASSWORD).first().getToken();
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testUpdateViewersWhenUserAddedAsAdmin() throws IOException, CvdbException, CatalogException, SolrServerException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, study.getId());
        query.put(CA_TYPE_NAME, "FAMILY");

        // "user4" can not access to these clinical analyses
        String token = catalogManager.getUserManager().login(organizationId, "user5", PASSWORD).first().getToken();

        DataResult<ClinicalAnalysis> result = null;
        try {
            result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
            fail();
        } catch (Exception e) {
            // Expected
            Assert.assertTrue(e instanceof CatalogAuthorizationException);
            Assert.assertEquals(null, result);
        }

        // Add user4 as admin
        catalogManager.getStudyManager().updateGroup(study.getFqn(), StudyManager.ADMINS, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user5")), opencgaToken);

        OpenCGAResult<ClinicalAnalysis> results = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), new Query(),
                QueryOptions.empty(), opencgaToken);
        for (ClinicalAnalysis clinicalAnalysis : results.getResults()) {
            OpenCGAResult<Acl> aclResult = catalogManager.getAdminManager().getEffectivePermissions(study.getFqn(), Collections.singletonList(clinicalAnalysis.getId()),
                    Collections.singletonList(ClinicalAnalysisPermissions.VIEW.name()), Enums.Resource.CLINICAL_ANALYSIS.name(), opencgaToken);
            cvdbEngine.indexViewers(aclResult.first().getId(), study.getId(), aclResult.first().getPermissions().get(0).getUserIds(), collectionPrefix);
            // Only one clinical analysis is updated
            break;
        }

        token = catalogManager.getUserManager().login(organizationId, "user5", PASSWORD).first().getToken();
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
        assertEquals(1, result.getNumResults());
    }

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

    private ClinicalAnalysis getClinicalAnalyis(String caId) throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_ID_NAME, caId);
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(caId, result.first().getId());
        return result.first();
    }

    private Interpretation getClinicalInterpretation(String ciId) throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, ciId);
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(ciId, result.first().getId());
        return result.first();
    }

    private ClinicalVariant getClinicalVariant(String cvId) throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ID_NAME, cvId);
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(cvId, result.first().getId());
        return result.first();
    }

    private boolean existsVariantId(String variantId, ClinicalAnalysis clinicalAnalysis) {
        if (existsVariantId(variantId, clinicalAnalysis.getInterpretation())) {
            return true;
        }
        for (Interpretation interpretation : clinicalAnalysis.getSecondaryInterpretations()) {
            if (existsVariantId(variantId, interpretation)) {
                return true;
            }
        }
        return false;
    }

    private boolean existsVariantId(String variantId, Interpretation interpretation) {
        if (existsVariantId(variantId, interpretation.getPrimaryFindings())) {
            return true;
        }
        if (existsVariantId(variantId, interpretation.getSecondaryFindings())) {
            return true;
        }
        return false;
    }

    private boolean existsVariantId(String variantId, List<ClinicalVariant> clinicalVariants) {
        for (ClinicalVariant clinicaVariant : clinicalVariants) {
            if (clinicaVariant.toString().equals(variantId)) {
                return true;
            }
        }
        return false;
    }

    public static void loadClinicalAnalsysesInCatalog(List<String> caFilenames, Study study) throws IOException, CatalogException {
        for (String caFilename : caFilenames) {
            URL resource = ClinicalInterpretationConverterTest.class.getClassLoader().getResource(caFilename);
            ClinicalAnalysisLoadResult loadResult = catalogManager.getClinicalAnalysisManager().load(study.getFqn(), Paths.get(resource.getPath()),
                    userToken);
            System.out.println(loadResult);
        }
        OpenCGAResult<ClinicalAnalysis> results = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), new Query(),
                QueryOptions.empty(), opencgaToken);
        for (ClinicalAnalysis clinicalAnalysis : results.getResults()) {
            catalogManager.getClinicalAnalysisManager().updateAcl(study.getFqn(), Collections.singletonList(clinicalAnalysis.getId()),
                    "user", new ClinicalAnalysisAclUpdateParams(null, "VIEW"), ParamUtils.AclAction.SET, false, opencgaToken);
            if (StringUtils.isEmpty(user2ViewerforCaId)) {
                user2ViewerforCaId = clinicalAnalysis.getId();
                catalogManager.getClinicalAnalysisManager().updateAcl(study.getFqn(), Collections.singletonList(clinicalAnalysis.getId()),
                        "user2", new ClinicalAnalysisAclUpdateParams(null, "VIEW"), ParamUtils.AclAction.SET, false, opencgaToken);
            }
        }
    }
}


