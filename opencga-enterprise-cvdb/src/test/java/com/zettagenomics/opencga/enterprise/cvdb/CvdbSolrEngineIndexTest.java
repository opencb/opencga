package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.CLINICAL_ANALYSES_COLLECTION_SUFFIX;
import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.getCollectionName;

public class CvdbSolrEngineIndexTest {

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

    @Test
    public void testIndexProject() throws CatalogException, IOException, CvdbException, SolrServerException {
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog project
        cvdbEngine.indexProject(projectId, catalogManager, true, sessionIdUser);

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvdbEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(2, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }
    }

    @Test
    public void testIndexStudy() throws CatalogException, IOException, CvdbException, SolrServerException {
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        // CVDB index from catalog study
        cvdbEngine.indexStudy(study.getFqn(), catalogManager, true, sessionIdUser);

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvdbEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(2, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }
    }

    @Test
    public void testIndexClinicalAnalyses() throws CatalogException, IOException, CvdbException, SolrServerException {
        loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study.getId());

        OpenCGAResult<ClinicalAnalysis> caResults = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), new Query(),
                QueryOptions.empty(), sessionIdUser);

        List<String> ids = caResults.getResults().stream().map(r -> r.getId()).collect(Collectors.toList());
        CvdbIndexResult indexResult = cvdbEngine.indexClinicalAnalyses(Collections.singletonList(ids.get(0)), study.getFqn(),
                catalogManager, true, sessionIdUser);
        System.out.println(indexResult);
        Assert.assertEquals(1, indexResult.getNumIndexed());

        indexResult = cvdbEngine.indexClinicalAnalyses(ids, study.getFqn(), catalogManager, false, sessionIdUser);
        System.out.println(indexResult);
        Assert.assertEquals(1, indexResult.getNumIndexed());

        indexResult = cvdbEngine.indexClinicalAnalyses(ids, study.getFqn(), catalogManager, false, sessionIdUser);
        System.out.println(indexResult);
        Assert.assertEquals(0, indexResult.getNumIndexed());

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvdbEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(2, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
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
                System.out.println("Error importing panel for clinical analysis file " + caFilename + ": " + e.getMessage());
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
            try {
                catalogManager.getClinicalAnalysisManager().create(studyId, clinicalAnalysis, true, INCLUDE_RESULT, sessionIdUser);
            } catch (CatalogException e) {
                System.out.println("---------------------------------------------------------------------------------");
                System.out.println("Impossible to load clinical analysis file " + caFilename + ": " + e.getMessage());
                System.out.println("---------------------------------------------------------------------------------");
            }
        }
    }

//    private void loadClinicalAnalsysesInSolr() throws IOException, CvdbException {
//        List<String> names = Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz");
//        for (String name : names) {
//            InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream(name);
//            GZIPInputStream gzipInputStream = new GZIPInputStream(is);
//            ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class)
//                    .readValue(gzipInputStream);
//
//            cvdbEngine.index(clinicalAnalysis, projectId, true);
//            System.out.println("Clinical analysis " + clinicalAnalysis.getId() + " loaded !");
//        }
//    }

//    private void loadClinicalVariants() throws IOException, CvdbException {
//        String name = "ca1.json.gz";
//        InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream(name);
//        GZIPInputStream gzipInputStream = new GZIPInputStream(is);
//        ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class)
//                .readValue(gzipInputStream);
//
//        List<ClinicalVariant> primaryFindings = clinicalAnalysis.getInterpretation().getPrimaryFindings();
//        System.out.println(primaryFindings.size());
//
//        List<ClinicalVariant> clinicalVariantList = new ArrayList<>();
//        clinicalVariantList.add(primaryFindings.get(0));
//        clinicalVariantList.add(primaryFindings.get(1));
//
//        cvdbEngine.index(clinicalVariantList, false, projectId);
//    }
}


