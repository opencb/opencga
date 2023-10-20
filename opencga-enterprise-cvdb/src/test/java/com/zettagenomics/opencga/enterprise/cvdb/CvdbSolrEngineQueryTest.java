package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalAnalysisQueryParam.*;
import static org.junit.Assert.*;
import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.commons.datastore.core.QueryOptions.LIMIT;

public class CvdbSolrEngineQueryTest {

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

        if (!cvdbEngine.existCollections(projectId)) {
            cvdbEngine.createCollections(projectId);
        }

        // Load and index
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog
        cvdbEngine.index(projectId, catalogManager, true, sessionIdUser);
    }

    public static void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
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
    public void testQueryClinicalAnalysesFromVariantId() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        String variantId = "X:54751204:C:T";
        String panelId = "VACTERL-like_phenotypes-PanelAppId-101";

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(VARIANT_QUERY_PARAM, variantId);
        query.put(PANEL_ID_QUERY_PARAM, panelId);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 10);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        Assert.assertEquals(1, result.getNumResults());
        for (ClinicalAnalysis clinicalAnalysis : result.getResults()) {
            if (!existsVariantId(variantId, clinicalAnalysis)) {
                fail();
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesFromVariantIdList() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(VARIANT_QUERY_PARAM, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 10);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        Assert.assertEquals(2, result.getNumResults());
        for (ClinicalAnalysis clinicalAnalysis : result.getResults()) {
            if (!existsVariantId(variantIds.get(0), clinicalAnalysis) && !existsVariantId(variantIds.get(1), clinicalAnalysis)) {
                fail();
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesInclude() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(VARIANT_QUERY_PARAM, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 1);

        queryOptions.put(INCLUDE, "id,type,disorder.attributes");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(MapUtils.isNotEmpty(result.first().getDisorder().getAttributes()));

        queryOptions.put(INCLUDE, "id,type,disorder.id");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(MapUtils.isEmpty(result.first().getDisorder().getAttributes()));

        queryOptions.put(INCLUDE, "id,type");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertEquals(null, result.first().getDisorder());
    }

    @Test
    public void testQueryClinicalInterpretationsInclude() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(VARIANT_QUERY_PARAM, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 1);

        queryOptions.put(INCLUDE, "id");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(CollectionUtils.isEmpty(result.first().getPrimaryFindings()));

        queryOptions.put(INCLUDE, "id,primaryFindings.evidences.phenotypes.id");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(CollectionUtils.isNotEmpty(result.first().getPrimaryFindings()));
        Phenotype phenotype = result.first().getPrimaryFindings().get(0).getEvidences().get(0).getPhenotypes().get(0);
        assertEquals("VACTERL-like phenotypes", phenotype.getId());
        assertTrue(StringUtils.isEmpty(phenotype.getSource()));

        queryOptions.put(INCLUDE, "id,primaryFindings.evidences.phenotypes");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(CollectionUtils.isNotEmpty(result.first().getPrimaryFindings()));
        phenotype = result.first().getPrimaryFindings().get(0).getEvidences().get(0).getPhenotypes().get(0);
        assertEquals("VACTERL-like phenotypes", phenotype.getId());
        assertEquals("non-standard", phenotype.getSource());
    }

    @Test
    public void testQueryClinicalAnalysesByCaFilters() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                    .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
        }
    }

    @Test
    public void testQueryClinicalInterpretationByCaFilters() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        Query query;
        Set<String> alreadyCaChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyCaChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = getClinicalAnalyis(ci.getClinicalAnalysisId());
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyCaChecked.add(ci.getClinicalAnalysisId());
            }
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyCaChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = getClinicalAnalyis(ci.getClinicalAnalysisId());
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyCaChecked.add(ci.getClinicalAnalysisId());
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyCaChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = getClinicalAnalyis(ci.getClinicalAnalysisId());
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyCaChecked.add(ci.getClinicalAnalysisId());
            }
        }
    }

    @Test
    public void testQueryClinicalVariantByCaFilters() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        Query query;
        Set<String> alreadyCaChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyCaChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyCaChecked.add(caId);
            }
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyCaChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyCaChecked.add(caId);
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyCaChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyCaChecked.add(caId);
            }
        }
    }

    @Test
    public void testQueryClinicalVariantEvidenceByCaFilters() throws CatalogException, IOException, CvdbException, SolrServerException {
        // CVDB query
        Query query;
        Set<String> alreadyCaChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyCaChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyCaChecked.add(caId);
            }
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyCaChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyCaChecked.add(caId);
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyCaChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyCaChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyCaChecked.add(caId);
            }
        }
    }

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

    private ClinicalAnalysis getClinicalAnalyis(String caId) throws IOException, CvdbException {
        // Check existing type
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_ID_NAME, caId);
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), null);
        assertEquals(1, result.getNumResults());
        assertEquals(caId, result.first().getId());
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

    private static void loadClinicalAnalsysesInCatalog(List<String> caFilenames, String studyId) throws IOException, CatalogException {
        for (String caFilename : caFilenames) {
            InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream(caFilename);
            GZIPInputStream gzipInputStream = new GZIPInputStream(is);
            ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class)
                    .readValue(gzipInputStream);

            // Import panels
            try {
                List<String> panelIds = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getPanels())) {
                    panelIds = clinicalAnalysis.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList());
                }
                catalogManager.getPanelManager().importFromSource(studyId, "panelapp", StringUtils.join(panelIds, ","), sessionIdUser);
            } catch (CatalogException e) {
                System.out.println("---------------------------------------------------------------------------------");
                System.out.println("Impossible to load clinical analysis file " + caFilename + ": " + e.getMessage());
                System.out.println("---------------------------------------------------------------------------------");
                continue;
            }

            // Create family
            if (clinicalAnalysis.getFamily() != null) {
                catalogManager.getFamilyManager().create(studyId, clinicalAnalysis.getFamily(), INCLUDE_RESULT, sessionIdUser);
            }

            // Create clinical analysis
            clinicalAnalysis.getInterpretation().setId(null);
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                    secondaryInterpretation.setId(null);
                }
            }
            catalogManager.getClinicalAnalysisManager().create(studyId, clinicalAnalysis, true, INCLUDE_RESULT, sessionIdUser);
        }
    }
}


