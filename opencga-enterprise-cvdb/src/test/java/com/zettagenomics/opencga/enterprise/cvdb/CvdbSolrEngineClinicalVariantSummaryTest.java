package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantSummary;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.junit.Assert.*;
import static org.opencb.commons.datastore.core.QueryOptions.*;

public class CvdbSolrEngineClinicalVariantSummaryTest {

    protected static CvdbSolrEngine cvdbEngine;
    protected static String organizationId = "test";
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

        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setName("User Name").setOrganization(organizationId), PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user2").setName("User Name2").setOrganization(organizationId), PASSWORD, opencgaToken);

        catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams()
                        .setOwner("user"),
                null, opencgaToken);

        sessionIdUser = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).getToken();

        catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first();
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null,
                sessionIdUser).first();
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

        DataResult<ClinicalAnalysis> caResult = cvdbEngine.searchClinicalAnalyses(query, queryOptions, sessionIdUser);
        System.out.println("num. ca = " + caResult.getNumResults());
        assertTrue(caResult.getNumResults() > 0);
        DataResult<Interpretation> ciResult = cvdbEngine.searchClinicalInterpretations(query, queryOptions, sessionIdUser);
        System.out.println("num. ci = " + ciResult.getNumResults());
        assertTrue(ciResult.getNumResults() > 0);
        DataResult<ClinicalVariant> cvResult = cvdbEngine.searchClinicalVariants(query, queryOptions, sessionIdUser);
        System.out.println("num. cv = " + cvResult.getNumResults());
        assertTrue(cvResult.getNumResults() > 0);
        DataResult<ClinicalVariantEvidence> cveResult = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, sessionIdUser);
        System.out.println("num. cve = " + cveResult.getNumResults());
        assertTrue(cveResult.getNumResults() > 0);
    }

    @Test
    public void testClinicalVariantSummaryFromVariantId() throws IOException, CvdbException, CatalogException {
        // CVDB query
        String variantId = "X:54751204:C:T";

        DataResult<ClinicalVariantSummary> result = cvdbEngine.getClinicalVariantSummary(variantId, projectId, null, sessionIdUser);

        Assert.assertEquals(1, result.getNumResults());
        Assert.assertEquals(1, result.first().getNumCases());
        Assert.assertEquals(1, result.first().getNumPrimaryInterpretations());
        Assert.assertEquals(1, result.first().getNumSecondaryInterpretations());
        Assert.assertEquals(1, result.first().getEvidencePhenotypeCounts().size());
        Assert.assertEquals(2, (int) result.first().getEvidencePhenotypeCounts().get("VACTERL-like phenotypes"));
        Assert.assertEquals(1, result.first().getEvidenceTierCounts().size());
        Assert.assertEquals(2, (int) result.first().getEvidenceTierCounts().get("TIER3"));
    }

    @Test
    public void testClinicalVariantSummaryFromVariantIds() throws IOException, CvdbException, CatalogException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        DataResult<ClinicalVariantSummary> result = cvdbEngine.getClinicalVariantSummary(variantIds, projectId, null, sessionIdUser);

        Assert.assertEquals(2, result.getNumResults());
        Assert.assertEquals(1, result.first().getNumCases());
        Assert.assertEquals(1, result.first().getNumPrimaryInterpretations());
        Assert.assertEquals(1, result.first().getNumSecondaryInterpretations());
        Assert.assertEquals(1, result.first().getEvidencePhenotypeCounts().size());
        Assert.assertEquals(2, (int) result.first().getEvidencePhenotypeCounts().get("VACTERL-like phenotypes"));
        Assert.assertEquals(1, result.first().getEvidenceTierCounts().size());
        Assert.assertEquals(2, (int) result.first().getEvidenceTierCounts().get("TIER3"));
        Assert.assertEquals(1, result.getResults().get(1).getNumCases());
        Assert.assertEquals(1, result.getResults().get(1).getNumPrimaryInterpretations());
        Assert.assertEquals(0, result.getResults().get(1).getNumSecondaryInterpretations());
        Assert.assertEquals(1, result.getResults().get(1).getEvidencePhenotypeCounts().size());
        Assert.assertEquals(2, (int) result.getResults().get(1).getEvidencePhenotypeCounts().get("Ultra-rare undescribed monogenic disorders"));
        Assert.assertEquals(1, result.getResults().get(1).getEvidenceTierCounts().size());
        Assert.assertEquals(2, (int) result.getResults().get(1).getEvidenceTierCounts().get("TIER3"));
    }

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

    private ClinicalAnalysis getClinicalAnalyis(String caId) throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        query.put(CA_ID_NAME, caId);
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(caId, result.first().getId());
        return result.first();
    }

    private Interpretation getClinicalInterpretation(String ciId) throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        query.put(CI_ID_NAME, ciId);
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(ciId, result.first().getId());
        return result.first();
    }

    private ClinicalVariant getClinicalVariant(String cvId) throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        query.put(CV_ID_NAME, cvId);
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, QueryOptions.empty(), sessionIdUser);
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

    public static void loadClinicalAnalsysesInCatalog(List<String> caFilenames, String studyId) throws IOException, CatalogException {
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


