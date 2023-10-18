package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PANEL_ID_QUERY_PARAM;
import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.VARIANT_QUERY_PARAM;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static org.junit.Assert.*;
import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.commons.datastore.core.QueryOptions.LIMIT;

public class CvdbSolrEngineQueryTest {

    protected CvdbSolrEngine cvdbEngine;
    protected String projectId = "project1";
    protected Study study;

    @Rule
    public CvdbSolrExtenalResource cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, projectId);;

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    private String opencgaToken;
    protected String sessionIdUser;
    private FamilyManager familyManager;

    public static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void before() throws CatalogException, IOException, CvdbException {
        // Catalog
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVDB
        cvdbEngine = cvdbSolrExternalResource.configure();

        if (!cvdbEngine.existCollections(projectId)) {
            cvdbEngine.createCollections(projectId);
        }
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
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
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog
        cvdbEngine.index(projectId, catalogManager, true, sessionIdUser);

        // CVDB query
        String variantId = "X:54751204:C:T";
        String panelId = "VACTERL-like_phenotypes-PanelAppId-101";

        Query query = new Query();
        query.put(com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME, projectId);
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
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog
        cvdbEngine.index(projectId, catalogManager, true, sessionIdUser);

        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME, projectId);
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
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog
        cvdbEngine.index(projectId, catalogManager, true, sessionIdUser);

        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME, projectId);
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

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

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

    private void loadClinicalAnalsysesInCatalog(List<String> caFilenames, String studyId) throws IOException, CatalogException {
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


