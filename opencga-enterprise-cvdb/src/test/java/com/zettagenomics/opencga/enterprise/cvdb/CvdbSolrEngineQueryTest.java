package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalIncludeHandler;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
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
import org.opencb.opencga.core.models.sample.Sample;
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
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.junit.Assert.*;
import static org.opencb.commons.datastore.core.QueryOptions.*;

public class CvdbSolrEngineQueryTest {

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

    public static ClinicalAnalysisQueryParser caParser;
    public static ClinicalInterpretationQueryParser ciParser;
    public static ClinicalVariantQueryParser cvParser;
    public static ClinicalVariantEvidenceQueryParser cveParser;

    @BeforeClass
    public static void before() throws Throwable {

        catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();
        catalogManagerResource.before();

        // Catalog
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVDB
        String collectionPrefix = CollectionPrefixUtils.getInstance(catalogManager).getCollectionPrefix(organizationId, projectId, userToken);
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, organizationId, projectId, collectionPrefix);
        cvdbSolrExternalResource.before();

        cvdbEngine = cvdbSolrExternalResource.configure();
        cvdbEngine.setCatalogManager(catalogManager);
        cvdbEngine.setVariantStorageMetadataManager(new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory()));

        if (!cvdbEngine.existCollections(collectionPrefix)) {
            cvdbEngine.createCollections(collectionPrefix);
        }

        // Load and index
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca3.json.gz"), study, userToken, opencgaToken,
                catalogManager);

        // CVDB index from catalog
        CvdbIndexResult indexResult = cvdbEngine.indexProject(projectId, catalogManager, true, userToken);
        System.out.println(indexResult.getFailures());
        assertEquals(2, indexResult.getNumIndexed());
        assertEquals(0, indexResult.getFailures().size());

        caParser = new ClinicalAnalysisQueryParser(cvdbEngine.getVariantStorageMetadataManager());
        ciParser = new ClinicalInterpretationQueryParser(cvdbEngine.getVariantStorageMetadataManager());
        cvParser = new ClinicalVariantQueryParser(cvdbEngine.getVariantStorageMetadataManager());
        cveParser = new ClinicalVariantEvidenceQueryParser(cvdbEngine.getVariantStorageMetadataManager());
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
    public void testCvdbContent() throws IOException, CvdbException, CatalogException {
        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
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
    public void testQueryClinicalAnalysesFromVariantId() throws IOException, CvdbException, CatalogException {
        // CVDB query
        String variantId = "X:54751204:C:T";
        String panelId = "VACTERL-like_phenotypes-PanelAppId-101";

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_VARIANT_ID_NAME, variantId);
        query.put(CI_PANEL_ID_NAME, panelId);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 10);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        Assert.assertEquals(1, result.getNumResults());
        for (ClinicalAnalysis clinicalAnalysis : result.getResults()) {
            if (!TestUtilities.existsVariantId(variantId, clinicalAnalysis)) {
                fail();
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesFromVariantIdList() throws IOException, CvdbException, CatalogException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_VARIANT_ID_NAME, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 10);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        Assert.assertEquals(2, result.getNumResults());
        for (ClinicalAnalysis clinicalAnalysis : result.getResults()) {
            if (!TestUtilities.existsVariantId(variantIds.get(0), clinicalAnalysis)
                    && !TestUtilities.existsVariantId(variantIds.get(1), clinicalAnalysis)) {
                fail();
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesInclude() throws IOException, CvdbException, CatalogException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_VARIANT_ID_NAME, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 1);

        queryOptions.put(INCLUDE, "id,type,disorder.attributes");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(MapUtils.isNotEmpty(result.first().getDisorder().getAttributes()));

        queryOptions.put(INCLUDE, "id,type,disorder.id");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(MapUtils.isEmpty(result.first().getDisorder().getAttributes()));

        queryOptions.put(INCLUDE, "id,type");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertEquals(null, result.first().getDisorder());
    }

    @Test
    public void testQueryClinicalInterpretationsInclude() throws IOException, CvdbException, CatalogException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_VARIANT_ID_NAME, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 1);

        queryOptions.put(INCLUDE, "id");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(CollectionUtils.isEmpty(result.first().getPrimaryFindings()));

        queryOptions.put(INCLUDE, "id,primaryFindings.evidences.phenotypes.id");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(CollectionUtils.isNotEmpty(result.first().getPrimaryFindings()));
        Phenotype phenotype = result.first().getPrimaryFindings().get(0).getEvidences().get(0).getPhenotypes().get(0);
        assertEquals("VACTERL-like phenotypes", phenotype.getId());
        assertTrue(StringUtils.isEmpty(phenotype.getSource()));

        queryOptions.put(INCLUDE, "id,primaryFindings.evidences.phenotypes");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(CollectionUtils.isNotEmpty(result.first().getPrimaryFindings()));
        phenotype = result.first().getPrimaryFindings().get(0).getEvidences().get(0).getPhenotypes().get(0);
        assertEquals("VACTERL-like phenotypes", phenotype.getId());
        assertEquals("non-standard", phenotype.getSource());
    }

    @Test
    public void testQueryClinicalVariantInclude() throws IOException, CvdbException, CatalogException {
        // CVDB query
        List<String> variantIds = Arrays.asList("X:54751204:C:T", "X:53196017:G:A");
        DataResult<ClinicalVariant> result;

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, projectId);
        query.put(CV_VARIANT_ID_NAME, StringUtils.join(variantIds, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 1);

        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() != null);
        assertTrue(result.first().getAnnotation() != null);
        System.out.println(result.first().toJson());

        queryOptions.put(INCLUDE, "type");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() != null);
        assertTrue(result.first().getAnnotation() == null);

        queryOptions.put(INCLUDE, "annotation");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);

        queryOptions.put(INCLUDE, "annotation.start");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() == null);

        queryOptions.put(INCLUDE, "annotation.consequenceTypes");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms() != null);

        queryOptions.put(INCLUDE, "annotation.consequenceTypes.geneName");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms() == null);

        queryOptions.put(INCLUDE, "annotation.consequenceTypes.sequenceOntologyTerms");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(queryOptions.getInt(LIMIT), result.getNumResults());
        assertTrue(result.first().getType() == null);
        assertTrue(result.first().getAnnotation() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes() != null);
        assertTrue(result.first().getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms() != null);
    }

    @Test
    public void testQueryClinicalAnalysesByCaFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                    .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
        }
    }

    @Test
    public void testQueryClinicalInterpretationByCaFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyChecked.add(ci.getClinicalAnalysisId());
            }
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyChecked.add(ci.getClinicalAnalysisId());
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getClinicalAnalysisId())) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyChecked.add(ci.getClinicalAnalysisId());
            }
        }
    }

    @Test
    public void testQueryClinicalVariantByCaFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(OPENCGA_CLINICAL_ANALYSIS_ID);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(caId, projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyChecked.add(caId);
            }
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(OPENCGA_CLINICAL_ANALYSIS_ID);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(caId, projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyChecked.add(caId);
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String caId = (String) cv.getAttributes().get(OPENCGA_CLINICAL_ANALYSIS_ID);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(caId, projectId, cvdbEngine, userToken);
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyChecked.add(caId);
            }
        }
    }

    @Test
    public void testQueryClinicalVariantEvidenceByCaFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "FAMILY");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(OPENCGA_CLINICAL_ANALYSIS_ID);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(caId, projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CA_TYPE_NAME), ca.getType().name());
                alreadyChecked.add(caId);
            }
        }

        // Check non-existing type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_TYPE_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check disorder
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_DISORDER_ID_NAME, "Ultra-rare undescribed monogenic disorders");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(OPENCGA_CLINICAL_ANALYSIS_ID);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(caId, projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CA_DISORDER_ID_NAME), ca.getDisorder().getId());
                alreadyChecked.add(caId);
            }
        }

        // Check family member
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_FAMILY_MEMBER_ID_NAME, "NR_111002765_3102043");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String caId = (String) cve.getAttributes().get(OPENCGA_CLINICAL_ANALYSIS_ID);
            assertTrue(StringUtils.isNotEmpty(caId));
            if (!alreadyChecked.contains(caId)) {
                ClinicalAnalysis ca = TestUtilities.getClinicalAnalyis(caId, projectId, cvdbEngine, userToken);
                assertTrue(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList())
                        .contains(query.getString(CA_FAMILY_MEMBER_ID_NAME)));
                alreadyChecked.add(caId);
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesByCiFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertTrue(ca.getInterpretation().getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
        }

//        // Check existing panel name
//        query = new Query(PROJECT_PARAM_NAME, projectId);
//        query.put(CI_PANEL_ID_NAME, "VACTERL-like phenotypes");
//        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
//        assertTrue(result.getNumResults() > 0);
//        for (ClinicalAnalysis ca : result.getResults()) {
//            assertTrue(ca.getInterpretation().getPanels().stream().map(p -> p.getName()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
//        }

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.1");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        boolean found = false;
        for (ClinicalAnalysis ca : result.getResults()) {
            if (ca.getInterpretation() != null && query.getString(CI_ID_NAME).equals(ca.getInterpretation().getId())) {
                found = true;
                break;
            }
            for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                if (query.getString(CI_ID_NAME).equals(secondaryInterpretation.getId())) {
                    found = true;
                    break;
                }
            }
        }
        assertTrue(found);
    }

    @Test
    public void testDateFilter() throws IOException, CvdbException, ParseException, CatalogException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();

        // ciId = OPA-6522-1.1, analyst date = 20231030104137
        // ciId = SAP-32015-1.1, analyst date = 20231030104144

        // Check single date
        query = new Query(PROJECT_PARAM_NAME, projectId);
        String strDate = "20231030104137";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        System.out.println(result.getNumResults());

        // Check multiple date
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "20231030104137,20231030104144";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        System.out.println(result.getNumResults());

        // Check range date
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "20221030104137-20241030104144";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        System.out.println(result.getNumResults());

        // Check range date (no start date)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "-20231030104144";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        System.out.println(result.getNumResults());

        // Check range date (no end date)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        strDate = "20231030104137-";
        query.put(CI_ANALYIST_DATE_NAME, strDate);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        System.out.println(result.getNumResults());

        //        assertTrue(result.getNumResults() > 0);
//        for (Interpretation ci : result.getResults()) {
//            assertEquals(query.getString(CI_ANALYIST_EMAIL_NAME), ci.getAnalyst().getDate());
//        }
    }

    @Test
    public void testIntegerFilter() throws IOException, CvdbException, ParseException, CatalogException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();
        DataResult<Interpretation> result;

        // ciId = OPA-6522-1.1, version = 1
        // ciId = SAP-32015-1.1, version = 1

        // Check single integer
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_VERSION_NAME, 1);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertEquals(query.getInt(CI_VERSION_NAME), ci.getVersion());
        }

        // Check multiple integers
        query = new Query(PROJECT_PARAM_NAME, projectId);
        List<Integer> versions = Arrays.asList(1, 3);
        query.put(CI_VERSION_NAME, StringUtils.join(versions, ","));
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(versions.contains(ci.getVersion()));
        }

        // Check non-existing integer
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_VERSION_NAME, "555");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testBooleanFilter() throws IOException, CvdbException, ParseException, CatalogException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();
        DataResult<Interpretation> result;

        // ciId = OPA-6522-1.2, primary = true
        // ciId = SAP-32015-1.2, primary = true

        // Check boolean (true)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            ClinicalAnalysis clinicalAnalyis = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
            assertEquals(ci.getId(), clinicalAnalyis.getInterpretation().getId());
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, "true");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            ClinicalAnalysis clinicalAnalyis = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
            assertEquals(ci.getId(), clinicalAnalyis.getInterpretation().getId());
        }

        // Check boolean (false)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.FALSE);
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            ClinicalAnalysis clinicalAnalyis = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
            assertTrue(clinicalAnalyis.getSecondaryInterpretations().stream().map(Interpretation::getId).collect(Collectors.toList()).contains(ci.getId()));
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, "false");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            ClinicalAnalysis clinicalAnalyis = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
            assertTrue(clinicalAnalyis.getSecondaryInterpretations().stream().map(Interpretation::getId).collect(Collectors.toList()).contains(ci.getId()));
        }

        // Check boolean (non-valid value -> false)
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, "toto");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            ClinicalAnalysis clinicalAnalyis = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
            assertTrue(clinicalAnalyis.getSecondaryInterpretations().stream().map(Interpretation::getId).collect(Collectors.toList()).contains(ci.getId()));
        }
    }

    @Test
    public void testTextFilter() throws IOException, CvdbException, ParseException, CatalogException {
        // CVDB query
        Query query;
        QueryOptions queryOptions = new QueryOptions();
        DataResult<ClinicalVariantEvidence> result;
        List<String> words;

        // review text: "Classified as: Tier3, passed the XLinkedSimpleRecessive segregation filter"

        // Check single word
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, "passed");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(cve.getReview().getDiscussion().getText().contains(query.getString(CVE_REVIEW_TEXT_NAME)));
        }

        // Check non-existing value
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, "toto");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check multiple words separated by , (i.e., OR)
        words = Arrays.asList("passed", "toto");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_REVIEW_TEXT_NAME, StringUtils.join(words, ","));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalInterpretationsByCiFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(ci.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
        }

//        // Check existing panel name
//        query = new Query(PROJECT_PARAM_NAME, projectId);
//        query.put(CI_PANEL_ID_NAME, "VACTERL-like phenotypes");
//        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
//        assertTrue(result.getNumResults() > 0);
//        for (Interpretation ci : result.getResults()) {
//            assertTrue(ci.getPanels().stream().map(p -> p.getName()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
//        }

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.1");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertEquals(query.getString(CI_ID_NAME), ci.getId());
        }
    }

    @Test
    public void testQueryClinicalVariantsByCiFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.2");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariant cv : result.getResults()) {
            String ciId = (String) cv.getAttributes().get(OPENCGA_INTERPRETATION_ID);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = TestUtilities.getClinicalInterpretation(ciId, projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CI_ID_NAME), ci.getId());
                alreadyChecked.add(ciId);
            }
        }
    }

    @Test
    public void testQueryClinicalVariantEvidencesByCiFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String ciId = (String) cve.getAttributes().get(OPENCGA_INTERPRETATION_ID);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = TestUtilities.getClinicalInterpretation(ciId, projectId, cvdbEngine, userToken);
                assertTrue(ci.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList()).contains(query.getString(CI_PANEL_ID_NAME)));
                alreadyChecked.add(ciId);
            }
        }

        // Check non-existing panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, "TOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check interpretation ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_ID_NAME, "OPA-6522-1.2");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        alreadyChecked.clear();
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String ciId = (String) cve.getAttributes().get(OPENCGA_INTERPRETATION_ID);
            assertTrue(StringUtils.isNotEmpty(ciId));
            if (!alreadyChecked.contains(ciId)) {
                Interpretation ci = TestUtilities.getClinicalInterpretation(ciId, projectId, cvdbEngine, userToken);
                assertEquals(query.getString(CI_ID_NAME), ci.getId());
                alreadyChecked.add(ciId);
            }
        }
    }

    @Test
    public void testQueryClinicalAnalysesByCvFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            assertTrue(ca.getInterpretation().getPrimaryFindings().stream().map(v -> v.getType().name()).collect(Collectors.toList()).contains(query.getString(CV_TYPE_NAME)));
        }
    }

    @Test
    public void testQueryClinicalInterpretationsByCvFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            assertTrue(ci.getPrimaryFindings().stream().map(v -> v.getType().name()).collect(Collectors.toList()).contains(query.getString(CV_TYPE_NAME)));
        }
    }

    @Test
    public void testQueryClinicalVariantsByCvFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            assertEquals(query.getString(CV_TYPE_NAME), cv.getType().name());
        }
    }

    @Test
    public void testQueryClinicalVariantEvidencesByCvFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check type
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_TYPE_NAME, "INDEL");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            String variantId = (String) cve.getAttributes().get(OPENCGA_VARIANT_ID);
            assertTrue(StringUtils.isNotEmpty(variantId));
            ClinicalVariant cv = TestUtilities.getClinicalVariant(variantId, projectId, cvdbEngine, userToken);
            assertEquals(query.getString(CV_TYPE_NAME), cv.getType().name());
        }
    }

    @Test
    public void testQueryClinicalAnalysesByCveFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getAccession).collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
                            found = true;
                        }
                    }
                }
            }
        }

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_SO_TERM_NAME_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
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
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
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
        query.put(CVE_SO_TERM_NAME_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
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
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_SO_TERM_NAME_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
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
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalAnalysis ca : result.getResults()) {
            if (!alreadyChecked.contains(ca.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
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
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalInterpretationsByCveFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;
        Set<String> alreadyChecked = new HashSet<>();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
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

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_SO_TERM_NAME_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
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
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
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
        query.put(CVE_SO_TERM_NAME_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
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
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_SO_TERM_NAME_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
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
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (Interpretation ci : result.getResults()) {
            if (!alreadyChecked.contains(ci.getId())) {
                boolean found = false;
                for (ClinicalVariant cv : ci.getPrimaryFindings()) {
                    for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                        if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                                && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
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
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalVariantByCveFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_SO_TERM_NAME_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check filter with multiple values
        List<String> geneNames = Arrays.asList("TENM1","CSF2RA");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, StringUtils.join(geneNames, ","));
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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
        query.put(CVE_SO_TERM_NAME_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_SO_TERM_NAME_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                        && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                        .collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "CSF2RA");
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariant cv : result.getResults()) {
            boolean found = false;
            for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                if (query.getString(CVE_GENE_NAME_NAME).equals(cve.getGenomicFeature().getGeneName())
                        && cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                        .collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME))) {
                    found = true;
                }
            }
            assertTrue(found);
        }

        // Check moi
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "X_LINKED_RECESSIVE");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
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
        result = cvdbEngine.searchClinicalVariants(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());
    }

    @Test
    public void testQueryClinicalVariantEvidencesByCveFilters() throws IOException, CvdbException, CatalogException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_TIER_NAME), cve.getClassification().getTier());
        }

        // Check gene name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "RP1L1");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_GENE_NAME_NAME), cve.getGenomicFeature().getGeneName());
        }

        // Check panel ID
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PANEL_ID_NAME, "VACTERL-like_phenotypes-PanelAppId-101");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_PANEL_ID_NAME), cve.getPanelId());
        }

        // Check consequence type name
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_SO_TERM_NAME_NAME, "inframe_insertion");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME)));
        }

        // Check filter with multiple values
        List<String> geneNames = Arrays.asList("TENM1","CSF2RA");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, StringUtils.join(geneNames, ","));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(geneNames.contains(cve.getGenomicFeature().getGeneName()));
        }

        List<String> soTerms = Arrays.asList("inframe_insertion","splice_region_variant");
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_SO_TERM_NAME_NAME, StringUtils.join(soTerms, ","));
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
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
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "TENM1");
        query.put(CVE_SO_TERM_NAME_NAME, "splice_region_variant");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertFalse(cve.getGenomicFeature().getGeneName().equals(query.getString("CSF2RA")));
            assertEquals(query.getString(CVE_GENE_NAME_NAME), cve.getGenomicFeature().getGeneName());
            assertTrue(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME)));
            assertFalse(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains("missense_variant"));
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_GENE_NAME_NAME, "CSF2RA");
        query.put(CVE_SO_TERM_NAME_NAME, "missense_variant");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertFalse(cve.getGenomicFeature().getGeneName().equals(query.getString("TENM1")));
            assertEquals(query.getString(CVE_GENE_NAME_NAME), cve.getGenomicFeature().getGeneName());
            assertTrue(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains(query.getString(CVE_SO_TERM_NAME_NAME)));
            assertFalse(cve.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName).collect(Collectors.toList()).contains("splice_region_variant"));
        }

        // Check moi
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "AUTOSOMAL_RECESSIVE");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertTrue(cve.getModeOfInheritances().stream().map(m -> m.name()).collect(Collectors.toList()).contains(query.getString(CVE_MOI_NAME)));
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_MOI_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        // Check penetrance
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "COMPLETE");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_PENETRANCE_NAME), cve.getPenetrance().name());
        }

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_PENETRANCE_NAME, "TOTOTOTO");
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(0, result.getNumResults());

        query = new Query(PROJECT_PARAM_NAME, projectId);
        result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            System.out.println(StringUtils.join(cve.getModeOfInheritances().stream().map(m -> m.name())
                    .collect(Collectors.toList()), ", "));
        }
    }

    //-----------------------------------------------------------------------
    // Count
    //-----------------------------------------------------------------------

    @Test
    public void testCountQueryClinicalVariantEvidences() throws IOException, CvdbException, CatalogException {
        int limit = 2;

        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, limit);

        // Check tier
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_TIER_NAME, "TIER3");
        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, userToken);
        assertEquals(limit, result.getNumResults());
        assertEquals(13, result.getNumMatches());
        System.out.println("result.getNumResults() = " + result.getNumResults() + ", result.getNumMatches() = " + result.getNumMatches());
        assertTrue(result.getNumResults() > 0);
        for (ClinicalVariantEvidence cve : result.getResults()) {
            assertEquals(query.getString(CVE_TIER_NAME), cve.getClassification().getTier());
        }
    }

    //-----------------------------------------------------------------------
    // Exclude
    //-----------------------------------------------------------------------

    @Test
    public void testExcludeQueryClinicalAInterpretationsUsingMediumJson() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

//        result = cvdbEngine.searchClinicalInterpretations(query, queryOptions, userToken);
//        System.out.println("result.getNumResults() = " + result.getNumResults() + ", result.getNumMatches() = " + result.getNumMatches());
//        assertTrue(result.getNumResults() > 0);
//
//        for (Interpretation ci : result.getResults()) {
//            assertTrue(CollectionUtils.isEmpty(ci.getPanels()));
//            assertTrue(CollectionUtils.isEmpty(ci.getPrimaryFindings()));
//            ClinicalAnalysis clinicalAnalyis = TestUtilities.getClinicalAnalyis(ci.getClinicalAnalysisId(), projectId, cvdbEngine, userToken);
//            assertEquals(ci.getId(), clinicalAnalyis.getInterpretation().getId());
//        }

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");
        queryOptions.put(EXCLUDE, "panels,interpretation.panels,secondaryInterpretations.panels");

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("mediumJson", solrQuery.getFields());
    }

    @Test
    public void testExcludeQueryClinicalInterpretationsUsingMaxJson1() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

//        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
//        System.out.println("result.getNumResults() = " + result.getNumResults() + ", result.getNumMatches() = " + result.getNumMatches());
//        assertTrue(result.getNumResults() > 0);
//
//        for (ClinicalAnalysis ca : result.getResults()) {
//            assertTrue(StringUtils.isNotEmpty(ca.getId()));
//            assertTrue(ca.getType() == null);
//            assertTrue(StringUtils.isEmpty(ca.getDescription()));
//            assertTrue(ca.getFamily() != null);
//            assertTrue(CollectionUtils.isNotEmpty(ca.getFamily().getMembers()));
//            assertTrue(CollectionUtils.isEmpty(ca.getFamily().getPhenotypes()));
//        }
//    }
//
//    @Test
//    public void testBuildClinicalAnalysisNotUsingJsonAndMultipleFields() throws IOException, CvdbException, CatalogException {
//        // CVDB query
//        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(EXCLUDE, "panels,interpretation.panels");

//        // Check boolean (true)
//        query = new Query(PROJECT_PARAM_NAME, projectId);
//        query.put(CI_PRIMARY_NAME, Boolean.TRUE);
//        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
//        System.out.println("result.getNumResults() = " + result.getNumResults() + ", result.getNumMatches() = " + result.getNumMatches());
//        assertTrue(result.getNumResults() > 0);
//
//        for (ClinicalAnalysis ca : result.getResults()) {
//            assertTrue(StringUtils.isNotEmpty(ca.getId()));
//            assertTrue(ca.getType() == null);
//            assertTrue(StringUtils.isEmpty(ca.getDescription()));
//            assertTrue(ca.getFamily() != null);
//            assertTrue(CollectionUtils.isNotEmpty(ca.getFamily().getMembers()));
//            assertTrue(CollectionUtils.isEmpty(ca.getFamily().getPhenotypes()));
//            assertTrue(CollectionUtils.isNotEmpty(ca.getPanels()));
//            for (Panel panel : ca.getPanels()) {
//                assertTrue(StringUtils.isNotEmpty(panel.getId()));
//                assertTrue(StringUtils.isNotEmpty(panel.getName()));
//                assertTrue(MapUtils.isNotEmpty(panel.getStats()));

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(ca.getDisorder() != null);
            assertTrue(ca.getPanels() == null);
            assertTrue(ca.getInterpretation().getPanels() == null);
            for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                assertTrue(secondaryInterpretation.getPanels() != null);
            }
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    @Test
    public void testExcludeQueryClinicalInterpretationsUsingMaxJson2() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

//        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
//        System.out.println("result.getNumResults() = " + result.getNumResults() + ", result.getNumMatches() = " + result.getNumMatches());
//        assertTrue(result.getNumResults() > 0);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(EXCLUDE, "panels");

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(ca.getDisorder() != null);
            assertTrue(ca.getPanels() == null);
            assertTrue(ca.getInterpretation().getPanels() != null);
            for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                assertTrue(secondaryInterpretation.getPanels() != null);
            }
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    //-----------------------------------------------------------------------
    // Include
    //-----------------------------------------------------------------------

    @Test
    public void testIncludeClinicalAnalysisUsingIndexedFields() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "id,family.members.id");
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertTrue(solrQuery.getFields().contains("id"));
        assertTrue(solrQuery.getFields().contains("familyMemberIds"));

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isNotEmpty(ca.getId()));
            assertTrue(StringUtils.isEmpty(ca.getDescription()));
            for (Individual member : ca.getFamily().getMembers()) {
                assertTrue(StringUtils.isNotEmpty(member.getId()));
                assertTrue(StringUtils.isEmpty(member.getName()));
            }
            assertTrue(ca.getDisorder() == null);
            assertTrue(ca.getPanels() == null);
            assertTrue(ca.getInterpretation() == null);
            assertTrue(ca.getSecondaryInterpretations() == null);
        }
    }

    @Test
    public void testIncludeClinicalAnalysisUsingMinJsonIncludeWithField() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "id,family.members.sex");

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isNotEmpty(ca.getId()));
            assertTrue(StringUtils.isEmpty(ca.getDescription()));
            for (Individual member : ca.getFamily().getMembers()) {
                assertTrue(StringUtils.isEmpty(member.getId()));
                assertTrue(StringUtils.isEmpty(member.getName()));
                assertTrue(member.getSex() != null);
            }
            assertTrue(ca.getDisorder() == null);
            assertTrue(ca.getPanels() == null);
            assertTrue(ca.getInterpretation() == null);
            assertTrue(ca.getSecondaryInterpretations() == null);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalAnalysisUsingMinJsonIncludeWithField1() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "id,type,proband.id,proband.samples.id,family.id,family.members.id,disorder.id,interpretation.id,interpretation.stats,panels.id,panels.name,panels.source,panels.stats");

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isNotEmpty(ca.getId()));
            assertTrue(ca.getType() != null);
            assertTrue(StringUtils.isNotEmpty(ca.getProband().getId()));
            for (Sample sample : ca.getProband().getSamples()) {
                assertTrue(StringUtils.isNotEmpty(sample.getId()));
            }
            assertTrue(StringUtils.isNotEmpty(ca.getFamily().getId()));
            for (Individual member : ca.getFamily().getMembers()) {
                assertTrue(StringUtils.isNotEmpty(member.getId()));
            }
            assertTrue(StringUtils.isNotEmpty(ca.getDisorder().getId()));
            assertTrue(StringUtils.isNotEmpty(ca.getInterpretation().getId()));
            assertTrue(ca.getInterpretation().getId() != null);
            for (Panel panel : ca.getPanels()) {
                assertTrue(StringUtils.isNotEmpty(panel.getId()));
                assertTrue(StringUtils.isNotEmpty(panel.getName()));
                assertTrue(panel.getSource() != null);
                assertTrue(panel.getStats() != null);
            }
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }


    @Test
    public void testIncludeClinicalAnalysisUsingMediumJsonIncludeWithField() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");
        queryOptions.put(INCLUDE, "id,family.members.name,interpretation.primaryFindings.annotation");

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("mediumJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalAnalysisMinJson() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");
        queryOptions.put(INCLUDE, "id,family.members.name");

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalAnalysisUsingMaxJsonNoInclude() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

//        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
//        System.out.println("result.getNumResults() = " + result.getNumResults() + ", result.getNumMatches() = " + result.getNumMatches());
//        assertTrue(result.getNumResults() > 0);

        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isNotEmpty(ca.getId()));
            for (Individual member : ca.getFamily().getMembers()) {
                assertTrue(StringUtils.isNotEmpty(member.getId()));
                assertTrue(StringUtils.isNotEmpty(member.getName()));
            }
            assertTrue(ca.getDisorder() != null);
            assertTrue(ca.getPanels() != null);
            for (Panel panel : ca.getPanels()) {
                assertTrue(StringUtils.isNotEmpty(panel.getId()));
                assertTrue(StringUtils.isNotEmpty(panel.getName()));
                assertTrue(panel.getSource() != null);
                assertTrue(MapUtils.isNotEmpty(panel.getStats()));
            }
            assertTrue(ca.getInterpretation() != null);
            assertTrue(StringUtils.isNotEmpty(ca.getInterpretation().getId()));
            assertTrue(StringUtils.isNotEmpty(ca.getInterpretation().getDescription()));
            assertTrue(ca.getInterpretation().getStats() != null);
            assertTrue(ca.getSecondaryInterpretations() != null);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalAnalysisUsingMaxJson() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "panels");

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isEmpty(ca.getId()));
            assertTrue(ca.getDisorder() == null);
            assertTrue(ca.getPanels() != null);
            for (Panel panel : ca.getPanels()) {
                assertTrue(StringUtils.isNotEmpty(panel.getId()));
                assertTrue(StringUtils.isNotEmpty(panel.getName()));
                assertTrue(panel.getSource() != null);
                assertTrue(MapUtils.isNotEmpty(panel.getStats()));
            }
            assertTrue(ca.getInterpretation() == null);
            assertTrue(ca.getSecondaryInterpretations() == null);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalAnalysisUsingMinJson() throws IOException, CvdbException, CatalogException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "interpretation.primaryFindings.id");

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isEmpty(ca.getId()));
            assertTrue(ca.getDisorder() == null);
            assertTrue(ca.getPanels() == null);
            assertTrue(ca.getInterpretation() != null);
            assertTrue(ca.getSecondaryInterpretations() == null);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalInterpretationNoneJson() throws CvdbException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "id");
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, catalogManager.getConfiguration().getDatabasePrefix());

        SolrQuery solrQuery = ciParser.parse(query, queryOptions);
        assertEquals("id", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalInterpretationMinJson() throws CvdbException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "primaryFindings.evidences");
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, catalogManager.getConfiguration().getDatabasePrefix());

        SolrQuery solrQuery = ciParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalInterpretationMediumJson() throws CvdbException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");
        queryOptions.put(INCLUDE, "primaryFindings.annotation.id");

        SolrQuery solrQuery = ciParser.parse(query, queryOptions);
        assertEquals("mediumJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalInterpretationMaxJson() throws CvdbException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "panels.genes");
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, catalogManager.getConfiguration().getDatabasePrefix());

        SolrQuery solrQuery = ciParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalInterpretationMinJson2() throws CvdbException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, ClinicalIncludeHandler.INTERNAL_INCLUDE_MINIMUM_JSON);
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");

        SolrQuery solrQuery = ciParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalInterpretationMediumJson2() throws CvdbException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");
        queryOptions.put(INCLUDE, ClinicalIncludeHandler.INTERNAL_INCLUDE_MEDIUM_JSON);

        SolrQuery solrQuery = ciParser.parse(query, queryOptions);
        assertEquals("mediumJson", solrQuery.getFields());
    }

    @Test
    public void testIncludeClinicalAnalysistionMediumJson2() throws CvdbException, CatalogException, IOException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PRIMARY_NAME, Boolean.TRUE);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, ClinicalIncludeHandler.INTERNAL_INCLUDE_MEDIUM_JSON);
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("mediumJson", solrQuery.getFields());

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isNotEmpty(ca.getId()));
            assertTrue(ca.getDisorder() != null);
            assertTrue(ca.getPanels() != null);
            for (Panel panel : ca.getPanels()) {
                assertTrue(StringUtils.isNotEmpty(panel.getId()));
                assertTrue(StringUtils.isNotEmpty(panel.getName()));
                assertTrue(panel.getSource() != null);
                assertTrue(MapUtils.isNotEmpty(panel.getStats()));
            }
            assertTrue(ca.getInterpretation() != null);
            assertTrue(ca.getSecondaryInterpretations() != null);
        }
    }

    @Test
    public void testClinicalAnalysisQueryByCaReport() throws CvdbException, CatalogException, IOException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CA_REPORT_NAME, "testing");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, ClinicalIncludeHandler.INTERNAL_INCLUDE_MINIMUM_JSON);

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(results.getNumResults() > 0);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(ca.getReport().getDiscussion().getText().contains(query.getString(CA_REPORT_NAME)));
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }

    @Test
    public void testClinicalAnalysisQueryByCiDescription() throws CvdbException, CatalogException, IOException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_DESCRIPTION_NAME, "genomics_england_tiering");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(INCLUDE, "interpretation.description");

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(results.getNumResults() > 0);
        for (ClinicalAnalysis ca : results.getResults()) {
            boolean found = false;
            if (StringUtils.isNotEmpty(ca.getInterpretation().getDescription())
                    && ca.getInterpretation().getDescription().contains(query.getString(CI_DESCRIPTION_NAME))) {
                found = true;
                break;
            }
            assertTrue(found);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("minJson", solrQuery.getFields());
    }

    @Test
    public void testClinicalAnalysisQueryByCvCt() throws CvdbException, CatalogException, IOException {
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ANNOT_CONSEQUENCE_TYPE_NAME, "missense_variant");

        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(results.getNumResults() > 0);
        for (ClinicalAnalysis ca : results.getResults()) {
            boolean found = false;
            for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                for (ConsequenceType ct : cv.getAnnotation().getConsequenceTypes()) {
                    for (SequenceOntologyTerm sot : ct.getSequenceOntologyTerms()) {
                        if (sot.getName().equals(query.getString(CV_ANNOT_CONSEQUENCE_TYPE_NAME))) {
                            found = true;
                        }
                    }
                }
            }
            assertTrue(found);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    @Test
    public void testClinicalAnalysisQueryByCvCtAND() throws CvdbException, CatalogException, IOException {
        List<String> soTerms = Arrays.asList("stop_gained", "missense_variant");

        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ANNOT_CONSEQUENCE_TYPE_NAME, StringUtils.join(soTerms, ";"));

        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(results.getNumResults() > 0);
        for (ClinicalAnalysis ca : results.getResults()) {
            boolean found = false;
            for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                for (ConsequenceType ct : cv.getAnnotation().getConsequenceTypes()) {
                    List<String> soList = ct.getSequenceOntologyTerms().stream().map(sot -> sot.getName()).collect(Collectors.toList());
                    if (soList.contains(soTerms.get(0)) && soList.contains(soTerms.get(1))) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    @Test
    public void testClinicalAnalysisQueryByCvCtOR() throws CvdbException, CatalogException, IOException {
        List<String> soTerms = Arrays.asList("stop_gained", "missense_variant");

        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_ANNOT_CONSEQUENCE_TYPE_NAME, StringUtils.join(soTerms, ","));

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(CollectionPrefixUtils.CVDB_DBPREFIX_KEY, "test");

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(results.getNumResults() > 0);
        for (ClinicalAnalysis ca : results.getResults()) {
            boolean found = false;
            for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                for (ConsequenceType ct : cv.getAnnotation().getConsequenceTypes()) {
                    List<String> soList = ct.getSequenceOntologyTerms().stream().map(sot -> sot.getName()).collect(Collectors.toList());
                    if (soList.contains(soTerms.get(0)) || soList.contains(soTerms.get(1))) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void testClinicalAnalysisQueryByCveClinicalSignificance() throws CvdbException, CatalogException, IOException {
        // &cveClinicalSignificance=likely_benign

        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CVE_CLINICAL_SIGNIFICANCE_NAME, ClinicalProperty.ClinicalSignificance.LIKELY_BENIGN);

        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertTrue(results.getNumResults() > 0);
        for (ClinicalAnalysis ca : results.getResults()) {
            boolean found = false;
            for (ClinicalVariant cv : ca.getInterpretation().getPrimaryFindings()) {
                for (ClinicalVariantEvidence cve : cv.getEvidences()) {
                    if (cve.getClassification().getClinicalSignificance() == ClinicalProperty.ClinicalSignificance.LIKELY_BENIGN) {
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

        SolrQuery solrQuery = caParser.parse(query, queryOptions);
        assertEquals("maxJson", solrQuery.getFields());
    }

    /* Code to update clinical analysis for testing:

        ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(ClinicalAnalysis.class);
        if ("OPA-6522-1".equals(ca.getId())) {
          ca.setReport(new ClinicalReport().setDiscussion(new ClinicalDiscussion().setText("Text for testing purposes")));
          ca.getInterpretation().getPrimaryFindings().get(0).getEvidences().get(0).getClassification().setClinicalSignificance(ClinicalProperty.ClinicalSignificance.LIKELY_BENIGN);
          ca.getInterpretation().getPrimaryFindings().get(1).getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms().add(new SequenceOntologyTerm("SO:0001587", "stop_gained"));
          ca.getInterpretation().getPrimaryFindings().get(2).getAnnotation().getConsequenceTypes().get(0).getSequenceOntologyTerms().add(new SequenceOntologyTerm("SO:0001821", "inframe_insertion"));

          objectWriter.writeValue(Paths.get("/tmp/ca1.new.json").toFile(), ca);
        }
     */

    @Test
    public void test() throws CatalogException, IOException, CvdbException {
        // https://test.app.zettagenomics.com/task-5516/opencga/webservices/rest/v2/analysis/clinical
        // /cvdb/case/query?studyId=eglh&ciPanelId=Congenital_neutropaenia-PanelAppId-28&sid=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0IiwiYXVkIjoiT3BlbkNHQSB1c2VycyIsImlhdCI6MTcwOTEzMTMzNywiZXhwIjoxNzA5MTM0OTM3fQ.jR3Fh7-5aRitqmKl64IJAKXG2Z5_omRbHmxTt5fB8es&limit=1
        String panelId = "VACTERL-like_phenotypes-PanelAppId-101";
        Query query;
        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CI_PANEL_ID_NAME, panelId);

        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> results = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        for (ClinicalAnalysis ca : results.getResults()) {
            assertTrue(StringUtils.isNotEmpty(ca.getId()));
        }
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

    private ClinicalVariant getClinicalVariant(String variantId) throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(CV_VARIANT_ID_NAME, variantId);
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(variantId, result.first().getId());
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
                    userToken);
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

