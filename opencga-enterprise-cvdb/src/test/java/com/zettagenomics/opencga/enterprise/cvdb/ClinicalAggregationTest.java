package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX;

public class ClinicalAggregationTest {

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

        setUpCvdb(catalogManager);
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


    public void setUpCvdb(CatalogManager catalogManager) throws IOException, CatalogException, CvdbException {
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog
        cvdbEngine.index(projectId, catalogManager, true, sessionIdUser);
    }
    //-----------------------------------------------------------------------
    // T E S T S
    //-----------------------------------------------------------------------

    @Test
    public void testAggregationWithFacetPivot() throws IOException, SolrServerException {
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setFacet(true);
        solrQuery.addFacetPivotField("ciId,cvId,geneName,panelId");

        // Execute the Solr query
        System.out.println("solr query = " + solrQuery.toQueryString());
        solrQuery.setShowDebugInfo(true);
        QueryResponse response = cvdbEngine.getSolrClient().query(CvdbSolrEngine.getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        int count = 0;
        NamedList<List<PivotField>> pivotFacets = response.getFacetPivot();
        for (Map.Entry<String, List<PivotField>> entry : pivotFacets) {
            System.out.println("key = " + entry.getKey());
            for (PivotField pivotField1 : entry.getValue()) {
                System.out.println("\tvalue = " + pivotField1.getValue() + ", count = " + pivotField1.getCount());
                for (PivotField pivotField2 : pivotField1.getPivot()) {
                    System.out.println("\t\tvalue = " + pivotField2.getValue() + ", count = " + pivotField2.getCount());
                    for (PivotField pivotField3 : pivotField2.getPivot()) {
                        System.out.println("\t\t\tvalue = " + pivotField3.getValue() + ", count = " + pivotField3.getCount());
                        for (PivotField pivotField4 : pivotField3.getPivot()) {
                            System.out.println("\t\t\t\tvalue = " + pivotField4.getValue() + ", count = " + pivotField4.getCount());
                            count++;
                        }
                    }
                }
            }
        }
        Assert.assertEquals(8, count);
    }

    @Test
    public void testAggregationWithFacetField() throws IOException, SolrServerException {
        String fieldName = "panelId";
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setFacet(true);
        solrQuery.addFacetField(fieldName);

        // Execute the Solr query
        System.out.println("solr query = " + solrQuery.toQueryString());
        solrQuery.setShowDebugInfo(true);
        QueryResponse response = cvdbEngine.getSolrClient().query(CvdbSolrEngine.getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
                solrQuery);
        FacetField facetField = response.getFacetField(fieldName);

        // Print out the results
        Assert.assertEquals(3, facetField.getValues().stream().count());
        System.out.println("name = " + facetField.getName());
        for (FacetField.Count facetCount : facetField.getValues()) {
            System.out.println("\tvalue = " + facetCount.getName() + ", count = " + facetCount.getCount());
            switch (facetCount.getName() + ":" + facetCount.getCount()) {
                case "VACTERL-like_phenotypes-PanelAppId-101:5":
                case "Periodic_fever_syndromes-PanelAppId-60:0":
                case "Severe_multi-system_atopic_disease_with_high_IgE-PanelAppId-62:3":
                    break;
                default:
                    Assert.fail();
            }
        }
    }

    //-----------------------------------------------------------------------
    //-----------------------------------------------------------------------

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


