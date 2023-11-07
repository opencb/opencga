package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.converters.SearchConverter;
import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParser;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
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
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
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
        cvdbEngine.setVariantStorageMetadataManager(new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory()));

        if (!cvdbEngine.existCollections(projectId)) {
            cvdbEngine.createCollections(projectId);
        }

        // Load and index
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog
        CvdbIndexResult indexResult = cvdbEngine.index(projectId, catalogManager, true, sessionIdUser);
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
    public void testCvdbContent() throws IOException, CvdbException {
        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> caResult = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        System.out.println("num. ca = " + caResult.getNumResults());
        assertTrue(caResult.getNumResults() > 0);
        DataResult<Interpretation> ciResult = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        System.out.println("num. ci = " + ciResult.getNumResults());
        assertTrue(ciResult.getNumResults() > 0);
        DataResult<ClinicalVariant> cvResult = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        System.out.println("num. cv = " + cvResult.getNumResults());
        assertTrue(cvResult.getNumResults() > 0);
        DataResult<ClinicalVariantEvidence> cveResult = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        System.out.println("num. cve = " + cveResult.getNumResults());
        assertTrue(cveResult.getNumResults() > 0);
    }

    @Test
    public void testQueryClinicalAnalysesFromVariantId() throws IOException, CvdbException {
        // CVDB query
        String variantId = "X:54751204:C:T";
        String panelId = "VACTERL-like_phenotypes-PanelAppId-101";

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ID_NAME, variantId);
        query.put(CI_PANEL_ID_NAME, panelId);

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
    public void testQueryClinicalAnalysesFromVariantIdList() throws IOException, CvdbException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ID_NAME, StringUtils.join(variantIds, ","));

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
    public void testQueryClinicalAnalysesInclude() throws IOException, CvdbException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ID_NAME, StringUtils.join(variantIds, ","));

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
    public void testQueryClinicalInterpretationsInclude() throws IOException, CvdbException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ID_NAME, StringUtils.join(variantIds, ","));

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
        assertTrue(StringUtils.isNotEmpty(phenotype.getSource()));

        queryOptions.put(INCLUDE, "id,primaryFindings.evidences.phenotypes");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(CollectionUtils.isNotEmpty(result.first().getPrimaryFindings()));
        phenotype = result.first().getPrimaryFindings().get(0).getEvidences().get(0).getPhenotypes().get(0);
        assertEquals("VACTERL-like phenotypes", phenotype.getId());
        assertEquals("non-standard", phenotype.getSource());
    }

    @Test
    public void testQueryClinicalVariantInclude() throws IOException, CvdbException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");
        DataResult<ClinicalVariant> result;

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ID_NAME, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 1);

        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() != null);
        assertTrue(result.first().getAnnotation() != null);
        System.out.println(result.first().toJson());

        queryOptions.put(INCLUDE, "type");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() != null);
        assertTrue(result.first().getAnnotation() == null);

        queryOptions.put(INCLUDE, "annotation");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);

        queryOptions.put(INCLUDE, "annotation.start");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() == null);

        queryOptions.put(INCLUDE, "annotation.consequenceTypes");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms() != null);

        queryOptions.put(INCLUDE, "annotation.consequenceTypes.geneName");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms() == null);

        queryOptions.put(INCLUDE, "annotation.consequenceTypes.sequenceOntologyTerms");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms() != null);
    }

    @Test
    public void testQueryClinicalAnalysesByCaFilters() throws IOException, CvdbException {
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
    public void testQueryClinicalInterpretationByCaFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = getClinicalAnalyis(ci.getClinicalAnalysisId());
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyChecked.add(ci.getClinicalAnalysisId());
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
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = getClinicalAnalyis(ci.getClinicalAnalysisId());
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyChecked.add(ci.getClinicalAnalysisId());
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = getClinicalAnalyis(ci.getClinicalAnalysisId());
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyChecked.add(ci.getClinicalAnalysisId());
            }
        }
    }

    @Test
    public void testQueryClinicalVariantByCaFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyChecked.add(caId);
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
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyChecked.add(caId);
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyChecked.add(caId);
            }
        }
    }

    @Test
    public void testQueryClinicalVariantEvidenceByCaFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyChecked.add(caId);
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
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyChecked.add(caId);
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(CA_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = getClinicalAnalyis(caId);
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyChecked.add(caId);
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesByCiFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertTrue(ca.getInterpretation().getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
        }

        // Check existing panel name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like phenotypes");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertTrue(ca.getInterpretation().getPanels().stream().map(p -> p.getName()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
        }

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.1");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(query.getString(CI_ID_NAME), ca.getInterpretation().getId());
        }

        // Check analyst email
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ANALYIST_EMAIL_NAME, "mail@ebi.ac.uk");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(query.getString(CI_ANALYIST_EMAIL_NAME), ca.getInterpretation().getAnalyst().getEmail());
        }
    }

    @Test
    public void testDateFilter() throws IOException, CvdbException, ParseException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();

        // ciId = OPA-6522-1.1, analyst date = 20231030104137
        // ciId = SAP-32015-1.1, analyst date = 20231030104144

        // Check single date
        query = new Query(PROJECT_PARAM_NAME, projectId);
        String strDate = "20231030104137";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        System.out.println(result.getNumResults());

        // Check multiple date
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "20231030104137,20231030104144";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        System.out.println(result.getNumResults());

        // Check range date
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "20221030104137-20241030104144";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        System.out.println(result.getNumResults());

        // Check range date (no start date)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "-20231030104144";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        System.out.println(result.getNumResults());

        // Check range date (no end date)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "20231030104137-";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        System.out.println(result.getNumResults());

        //        assertTrue(result.getNumResults() > 0);
//        for (Interpretation ci : result.getResults()) {
//            assertEquals(query.getString(CI_ANALYIST_EMAIL_NAME), ci.getAnalyst().getDate());
//        }
    }

    @Test
    public void testIntegerFilter() throws IOException, CvdbException, ParseException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();
        DataResult<Interpretation> result;

        // ciId = OPA-6522-1.1, version = 1
        // ciId = SAP-32015-1.1, version = 1

        // Check single integer
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_VERSION_NAME, 1);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertEquals(query.getInt(CI_VERSION_NAME), ci.getVersion());
        }

        // Check multiple integers
        query = new Query(PROJECT_PARAM_NAME, projectId);
        List<Integer> versions = Arrays.asList(1, 3);
        query.put(CI_VERSION_NAME, StringUtils.join(versions, ","));
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(versions.contains(ci.getVersion()));
        }

        // Check non-existing integer
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_VERSION_NAME, "555");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testBooleanFilter() throws IOException, CvdbException, ParseException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();
        DataResult<Interpretation> result;

        // ciId = OPA-6522-1.1, primary = true
        // ciId = SAP-32015-1.1, primary = true

        // Check boolean (true)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            ClinicalAnalysis clinicalAnalyis = getClinicalAnalyis(ci.getClinicalAnalysisId());
            assertEquals(ci.getId(), clinicalAnalyis.getInterpretation().getId());
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, "true");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            ClinicalAnalysis clinicalAnalyis = getClinicalAnalyis(ci.getClinicalAnalysisId());
            assertEquals(ci.getId(), clinicalAnalyis.getInterpretation().getId());
        }

        // Check boolean (false)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.FALSE);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, "false");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check boolean (non-valid value -> false)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, "toto");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testTextFilter() throws IOException, CvdbException, ParseException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();
        DataResult<ClinicalVariantEvidence> result;
        List<String> words;

        // review text: "Classified as: Tier3, passed the XLinkedSimpleRecessive segregation filter"

        // Check single word
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, "passed");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(cve.getReview().getDiscussion().getText().contains(query.getString(CVE_REVIEW_TEXT_NAME)));
        }

        // Check non-existing value
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, "toto");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check multiple words separated by , (i.e., OR)
        words = Arrays.asList("passed", "toto");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, StringUtils.join(words, ","));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            boolean found = false;
            for (String word : words) {
                if (cve.getReview().getDiscussion().getText().contains(word)) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check multiple words separated by ; (i.e., AND)
        words = Arrays.asList("passed", "segregation");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, StringUtils.join(words, ";"));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            for (String word : words) {
                assertTrue(cve.getReview().getDiscussion().getText().contains(word));
            }
        }

        // Check multiple words separated by ; (i.e., AND)
        words = Arrays.asList("passed", "toto");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, StringUtils.join(words, ";"));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalInterpretationsByCiFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(ci.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
        }

        // Check existing panel name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like phenotypes");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(ci.getPanels().stream().map(p -> p.getName()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
        }

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.1");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertEquals(query.getString(CI_ID_NAME), ci.getId());
        }

        // Check analyst email
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ANALYIST_EMAIL_NAME, "mail@ebi.ac.uk");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertEquals(query.getString(CI_ANALYIST_EMAIL_NAME), ci.getAnalyst().getEmail());
        }
    }

    @Test
    public void testQueryClinicalVariantsByCiFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String ciId = (String) cv.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertTrue(ci.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
                alreadyChecked.add(ciId);
            }
        }

        // Check existing panel name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like phenotypes");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String ciId = (String) cv.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertTrue(ci.getPanels().stream().map(p -> p.getName()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
                alreadyChecked.add(ciId);
            }
        }

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.1");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String ciId = (String) cv.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertEquals(query.getString(CI_ID_NAME), ci.getId());
                alreadyChecked.add(ciId);
            }
        }

        // Check analyst email
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ANALYIST_EMAIL_NAME, "mail@ebi.ac.uk");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String ciId = (String) cv.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertEquals(query.getString(CI_ANALYIST_EMAIL_NAME), ci.getAnalyst().getEmail());
                alreadyChecked.add(ciId);
            }
        }
    }

    @Test
    public void testQueryClinicalVariantEvidencesByCiFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String ciId = (String) cve.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertTrue(ci.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
                alreadyChecked.add(ciId);
            }
        }

        // Check existing panel name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like phenotypes");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String ciId = (String) cve.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertTrue(ci.getPanels().stream().map(p -> p.getName()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
                alreadyChecked.add(ciId);
            }
        }

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.1");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String ciId = (String) cve.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertEquals(query.getString(CI_ID_NAME), ci.getId());
                alreadyChecked.add(ciId);
            }
        }

        // Check analyst email
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ANALYIST_EMAIL_NAME, "mail@ebi.ac.uk");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String ciId = (String) cve.getAttributes().get(CI_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = getClinicalInterpretation(ciId);
                assertEquals(query.getString(CI_ANALYIST_EMAIL_NAME), ci.getAnalyst().getEmail());
                alreadyChecked.add(ciId);
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesByCvFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertTrue(ca.getInterpretation().getPrimaryFindings().stream().map(v -> v.getType().name()).collect(Collectors.toList()).contains(query.getString(CV_TYPE_NAME)));
        }
    }

    @Test
    public void testQueryClinicalInterpretationsByCvFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(ci.getPrimaryFindings().stream().map(v -> v.getType().name()).collect(Collectors.toList()).contains(query.getString(CV_TYPE_NAME)));
        }
    }

    @Test
    public void testQueryClinicalVariantsByCvFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            assertEquals(query.getString(CV_TYPE_NAME), cv.getType().name());
        }
    }

    @Test
    public void testQueryClinicalVariantEvidencesByCvFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String cvId = (String) cve.getAttributes().get(CV_ID_NAME);
            assertTrue(StringUtils.isNotEmpty(cvId));
            ClinicalVariant cv = getClinicalVariant(cvId);
            assertEquals(query.getString(CV_TYPE_NAME), cv.getType().name());
        }
    }

    @Test
    public void testQueryClinicalAnalysesByCveFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_TIER_NAME).equals(cve.getClassification().getTier())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        // Check gene name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "RP1L1");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        // Check panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_PANEL_ID_NAME).equals(cve.getPanelId())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        // Check consequence type ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "SO:0001821");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getAccession).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        // Check filter with multiple values
        List<String> geneNames = Arrays.asList("TENM1","CSF2RA");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, StringUtils.join(geneNames, ","));
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (geneNames.contains(cve.getGenomicFeature().getGeneName())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        List<String> soTerms = Arrays.asList("inframe_insertion","splice_region_variant");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        List<String> cveSoTerms = cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList());
                        for (String soTerm : soTerms) {
                            if (cveSoTerms.contains(soTerm)) {
                                found = true;
                            }
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        // Check multiple filters
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "CSF2RA");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ca.getId());
            }
        }

        // Check moi
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "X_LINKED_DOMINANT");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            boolean found = false;
            Interpretation ci = ca.getInterpretation();
            for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                    if (cve.getModeOfInheritances().stream().map(m -> m.name()).collect(Collectors.toList())
                            .contains(query.getString(CVE_MOI_NAME))) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            boolean found = false;
            Interpretation ci = ca.getInterpretation();
            for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                    if (query.getString(CVE_PENETRANCE_NAME).equals(cve.getPenetrance().name())) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, null);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalInterpretationsByCveFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_TIER_NAME).equals(cve.getClassification().getTier())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        // Check gene name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "RP1L1");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        // Check panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_PANEL_ID_NAME).equals(cve.getPanelId())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        // Check consequence type ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "SO:0001821");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getAccession).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        // Check filter with multiple values
        List<String> geneNames = Arrays.asList("TENM1","CSF2RA");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, StringUtils.join(geneNames, ","));
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (geneNames.contains(cve.getGenomicFeature().getGeneName())) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        List<String> soTerms = Arrays.asList("inframe_insertion","splice_region_variant");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        List<String> cveSoTerms = cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList());
                        for (String soTerm : soTerms) {
                            if (cveSoTerms.contains(soTerm)) {
                                found = true;
                            }
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        // Check multiple filters
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "CSF2RA");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                            found = true;
                        }
                    }
                }
                assertTrue(found);
                alreadyChecked.add(ci.getId());
            }
        }

        // Check moi
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "X_LINKED_DOMINANT");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            boolean found = false;
            for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                    if (cve.getModeOfInheritances().stream().map(m -> m.name()).collect(Collectors.toList())
                            .contains(query.getString(CVE_MOI_NAME))) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            boolean found = false;
            for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                    if (query.getString(CVE_PENETRANCE_NAME).equals(cve.getPenetrance().name())) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, null);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalVariantByCveFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_TIER_NAME).equals(cve.getClassification().getTier())) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check gene name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "RP1L1");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_PANEL_ID_NAME).equals(cve.getPanelId())) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check consequence type ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "SO:0001821");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getAccession).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check filter with multiple values
        List<String> geneNames = Arrays.asList("TENM1","CSF2RA");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, StringUtils.join(geneNames, ","));
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (geneNames.contains(cve.getGenomicFeature().getGeneName())) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        List<String> soTerms = Arrays.asList("inframe_insertion","splice_region_variant");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                List<String> cveSoTerms = cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                        .collect(Collectors.toList());
                for (String soTerm : soTerms) {
                    if (cveSoTerms.contains(soTerm)) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

        // Check multiple filters
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                        && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                        .collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "CSF2RA");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                        && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                        .collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check moi
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "X_LINKED_RECESSIVE");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (cve.getModeOfInheritances().stream().map(m -> m.name()).collect(Collectors.toList())
                        .contains(query.getString(CVE_MOI_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_PENETRANCE_NAME).equals(cve.getPenetrance().name())) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, null);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalVariantEvidencesByCveFilters() throws IOException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_TIER_NAME), cve.getClassification().getTier());
        }

        // Check gene name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "RP1L1");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_GENE_NAME_NAME), cve.getGenomicFeature().getGeneName());
        }

        // Check panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_PANEL_ID_NAME), cve.getPanelId());
        }

        // Check consequence type ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "SO:0001821");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getAccession).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME)));
        }

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME)));
        }

        // Check filter with multiple values
        List<String> geneNames = Arrays.asList("TENM1","CSF2RA");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, StringUtils.join(geneNames, ","));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(geneNames.contains(cve.getGenomicFeature().getGeneName()));
        }

        List<String> soTerms = Arrays.asList("inframe_insertion","splice_region_variant");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            List<String> cveSoTerms = cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                    .collect(Collectors.toList());
            boolean found = false;
            for (String soTerm : soTerms) {
                if (cveSoTerms.contains(soTerm)) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }

        // Check multiple filters
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertFalse(cve.getGenomicFeature().getGeneName().equals(query.getString("CSF2RA")));
            assertEquals(query.getString(CVE_GENE_NAME_NAME), cve.getGenomicFeature().getGeneName());
            assertTrue(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME)));
            assertFalse(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains("missense_variant"));
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "CSF2RA");
        query.put(CVE_CONSEQUENCE_TYPE_ID_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertFalse(cve.getGenomicFeature().getGeneName().equals(query.getString("TENM1")));
            assertEquals(query.getString(CVE_GENE_NAME_NAME), cve.getGenomicFeature().getGeneName());
            assertTrue(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_CONSEQUENCE_TYPE_ID_NAME)));
            assertFalse(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains("splice_region_variant"));
        }

        // Check moi
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "AUTOSOMAL_RECESSIVE");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(cve.getModeOfInheritances().stream().map(m -> m.name()).collect(Collectors.toList()).contains(query.getString(CVE_MOI_NAME)));
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_PENETRANCE_NAME), cve.getPenetrance().name());
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, null);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            System.out.println(StringUtils.join(cve.getModeOfInheritances().stream().map(m -> m.name()).collect(Collectors.toList()), ", "));
        }
    }

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

    private ClinicalAnalysis getClinicalAnalyis(String caId) throws IOException, CvdbException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_ID_NAME, caId);
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), null);
        assertEquals(1, result.getNumResults());
        assertEquals(caId, result.first().getId());
        return result.first();
    }

    private Interpretation getClinicalInterpretation(String ciId) throws IOException, CvdbException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, ciId);
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, QueryOptions.empty(), null);
        assertEquals(1, result.getNumResults());
        assertEquals(ciId, result.first().getId());
        return result.first();
    }

    private ClinicalVariant getClinicalVariant(String cvId) throws IOException, CvdbException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ID_NAME, cvId);
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, QueryOptions.empty(), null);
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


