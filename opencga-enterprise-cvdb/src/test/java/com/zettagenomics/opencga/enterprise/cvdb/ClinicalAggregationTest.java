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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.junit.Assert.assertEquals;

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

//    @Test
//    public void testAggregationWithFacetPivot() throws IOException, SolrServerException {
//        SolrQuery solrQuery = new SolrQuery("*:*");
//        solrQuery.setFacet(true);
//        solrQuery.addFacetPivotField("ciId,cvId,geneName,panelId");
//
//        // Execute the Solr query
//        System.out.println("solr query = " + solrQuery.toQueryString());
//        solrQuery.setShowDebugInfo(true);
//        QueryResponse response = cvdbEngine.getSolrClient().query(CvdbSolrEngine.getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
//                solrQuery);
//
//        // Print out the results
//        int count = 0;
//        NamedList<List<PivotField>> pivotFacets = response.getFacetPivot();
//        for (Map.Entry<String, List<PivotField>> entry : pivotFacets) {
//            System.out.println("key = " + entry.getKey());
//            for (PivotField pivotField1 : entry.getValue()) {
//                System.out.println("\tvalue = " + pivotField1.getValue() + ", count = " + pivotField1.getCount());
//                for (PivotField pivotField2 : pivotField1.getPivot()) {
//                    System.out.println("\t\tvalue = " + pivotField2.getValue() + ", count = " + pivotField2.getCount());
//                    for (PivotField pivotField3 : pivotField2.getPivot()) {
//                        System.out.println("\t\t\tvalue = " + pivotField3.getValue() + ", count = " + pivotField3.getCount());
//                        for (PivotField pivotField4 : pivotField3.getPivot()) {
//                            System.out.println("\t\t\t\tvalue = " + pivotField4.getValue() + ", count = " + pivotField4.getCount());
//                            count++;
//                        }
//                    }
//                }
//            }
//        }
//        Assert.assertEquals(8, count);
//    }
//
//    @Test
//    public void testAggregationWithFacetField() throws IOException, SolrServerException {
//        String fieldName = "panelId";
//        SolrQuery solrQuery = new SolrQuery("*:*");
//        solrQuery.setFacet(true);
//        solrQuery.addFacetField(fieldName);
//
//        // Execute the Solr query
//        System.out.println("solr query = " + solrQuery.toQueryString());
//        solrQuery.setShowDebugInfo(true);
//        QueryResponse response = cvdbEngine.getSolrClient().query(CvdbSolrEngine.getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
//                solrQuery);
//        FacetField facetField = response.getFacetField(fieldName);
//
//        // Print out the results
//        Assert.assertEquals(3, facetField.getValues().stream().count());
//        System.out.println("name = " + facetField.getName());
//        for (FacetField.Count facetCount : facetField.getValues()) {
//            System.out.println("\tvalue = " + facetCount.getName() + ", count = " + facetCount.getCount());
//            switch (facetCount.getName() + ":" + facetCount.getCount()) {
//                case "VACTERL-like_phenotypes-PanelAppId-101:5":
//                case "Periodic_fever_syndromes-PanelAppId-60:0":
//                case "Severe_multi-system_atopic_disease_with_high_IgE-PanelAppId-62:3":
//                    break;
//                default:
//                    Assert.fail();
//            }
//        }
//    }

    @Test
    public void testFacetClinicalAnalyses() throws IOException, SolrServerException, CvdbException {
        // CVDB query
        Query query;

        QueryOptions queryOptions = new QueryOptions();

        // Check existing type
        queryOptions.put(QueryOptions.FACET, CA_STATUS_NAME);
        query = new Query(PROJECT_PARAM_NAME, projectId);
        DataResult<FacetField> facetResult = cvdbEngine.facetClinicalAnalyses(query, queryOptions, null);
        for (FacetField result : facetResult.getResults()) {
            System.out.println(result);
        }
    }

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

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


