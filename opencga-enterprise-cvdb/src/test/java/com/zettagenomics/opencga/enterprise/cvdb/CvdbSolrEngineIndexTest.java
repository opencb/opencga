package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalSolrIterator;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalVariantSearch;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.CLINICAL_ANALYSES_COLLECTION_SUFFIX;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CA_ID_NAME;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CI_ID_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.commons.datastore.core.QueryOptions.LIMIT;

public class CvdbSolrEngineIndexTest {

    protected CvdbSolrEngine cvdbEngine;
    protected String organizationId = "test";
    protected String projectId = "project1";
    protected Study study;

    @Rule
//    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();
    public OpenCGAEnterpriseCatalogManagerExternalResource catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();

    @Rule
    public CvdbSolrExtenalResource cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, organizationId, projectId);

    protected CatalogManager catalogManager;
    private String opencgaToken;
    protected String userToken;
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
        cvdbEngine.setCatalogManager(catalogManager);
        cvdbEngine.setVariantStorageMetadataManager(new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory()));

        if (!cvdbEngine.existCollections(organizationId, projectId)) {
            cvdbEngine.createCollections(organizationId, projectId);
        }
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(ADMIN_PASSWORD).getToken();

        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setName("User Name").setOrganization(organizationId), PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user2").setName("User Name2").setOrganization(organizationId), PASSWORD, opencgaToken);

        catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams()
                        .setOwner("user"),
                null, opencgaToken);

        userToken = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).getToken();

        catalogManager.getProjectManager().create(projectId, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null,
                userToken).first();
    }

    @Test
    public void testIndexProject() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        // CVDB index from catalog project
        cvdbEngine.indexProject(projectId, catalogManager, true, userToken);

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvdbEngine.getSolrClient().query(cvdbEngine.getCollectionName(organizationId, projectId,
                        CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(3, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);
    }

    @Test
    public void testIndexStudy() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        // CVDB index from catalog study
        cvdbEngine.indexStudy(study.getFqn(), catalogManager, true, userToken);

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvdbEngine.getSolrClient().query(cvdbEngine.getCollectionName(organizationId, projectId,
                        CLINICAL_ANALYSES_COLLECTION_SUFFIX), solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(3, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);
    }

    @Test
    public void testIndexClinicalAnalyses() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        OpenCGAResult<ClinicalAnalysis> caResults = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), new Query(),
                QueryOptions.empty(), userToken);

        List<String> ids = caResults.getResults().stream().map(r -> r.getId()).collect(Collectors.toList());
        CvdbIndexResult indexResult = cvdbEngine.indexClinicalAnalyses(Collections.singletonList(ids.get(0)), study.getFqn(),
                catalogManager, true, userToken);
        System.out.println(indexResult);
        Assert.assertEquals(1, indexResult.getNumIndexed());

        indexResult = cvdbEngine.indexClinicalAnalyses(ids, study.getFqn(), catalogManager, false, userToken);
        System.out.println(indexResult);
        Assert.assertEquals(2, indexResult.getNumIndexed());

        indexResult = cvdbEngine.indexClinicalAnalyses(ids, study.getFqn(), catalogManager, false, userToken);
        System.out.println(indexResult);
        Assert.assertEquals(0, indexResult.getNumIndexed());

        // CVDB queries
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvdbEngine.getSolrClient().query(cvdbEngine.getCollectionName(organizationId, projectId,
                        CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(3, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);
    }

    @Test
    public void testOverwriteTrue() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        // CVDB index from catalog project
        cvdbEngine.indexProject(projectId, catalogManager, true, userToken);

        // CVDB query
        String caId = "OPA-6522-1";
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(ORGANIZATION_PARAM_NAME, organizationId);
        query.put(CA_ID_NAME, caId);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, result.getNumResults());

        ClinicalAnalysis clinicalAnalysis = result.first();
        assertTrue(StringUtils.isEmpty(clinicalAnalysis.getDescription()));

        // Update clinical analysis in catalog
        String newDescription = "This analysis is for testing the overwrite functionality";
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams();
        updateParams.setDescription(newDescription);
        catalogManager.getClinicalAnalysisManager().update(study.getFqn(), caId, updateParams, QueryOptions.empty(), userToken);

        // CVDB index the given clinical analysis from catalog
        cvdbEngine.indexClinicalAnalyses(Collections.singletonList(caId), study.getFqn(), catalogManager,
                true, userToken);
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, result.getNumResults());
        ClinicalAnalysis updatedClinicalAnalysis = result.first();
        assertEquals(newDescription, updatedClinicalAnalysis.getDescription());

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);
    }

    @Test
    public void testOverwriteTrueRemovePrimaryFindings() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        // CVDB index from catalog project
        cvdbEngine.indexProject(projectId, catalogManager, true, userToken);

        // CVDB query
        String caId = "OPA-6522-1";
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(ORGANIZATION_PARAM_NAME, organizationId);
        query.put(CA_ID_NAME, caId);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, result.getNumResults());
        ClinicalAnalysis prevCa = result.first();

        ClinicalAnalysis clinicalAnalysis = result.first();
        assertTrue(StringUtils.isEmpty(clinicalAnalysis.getDescription()));

        // Update clinical analysis in catalog
        String newDescription = "This analysis is for testing the overwrite functionality";
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams();
        updateParams.setDescription(newDescription);
        catalogManager.getClinicalAnalysisManager().update(study.getFqn(), caId, updateParams, QueryOptions.empty(), opencgaToken);

        InterpretationUpdateParams ciUpdateParams = new InterpretationUpdateParams();
        Variant v = new Variant("9:99999:A:T");
        v.setId(v.toStringSimple());
        ClinicalVariant cv = new ClinicalVariant(v.getImpl());
        List<ClinicalVariant> primaryFindings = new ArrayList<>();
        primaryFindings.add(cv);
//        primaryFindings.addAll(prevCa.getInterpretation().getPrimaryFindings());
        for (ClinicalVariant primaryFinding : primaryFindings) {
            primaryFinding.setType(VariantType.CNV);
        }
        ciUpdateParams.setPrimaryFindings(primaryFindings);

        queryOptions = new QueryOptions();
        queryOptions.put(ParamConstants.INCLUDE_RESULT_PARAM, true);
        ObjectMap actionMap = new ObjectMap();
        actionMap.put(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.SET);
        queryOptions.put(Constants.ACTIONS, actionMap);
        Interpretation interpreation = catalogManager.getInterpretationManager().update(study.getFqn(), prevCa.getId(),
                prevCa.getInterpretation().getId(), ciUpdateParams, null, queryOptions, opencgaToken).first();

        // CVDB index the given clinical analysis from catalog
        cvdbEngine.indexClinicalAnalyses(Collections.singletonList(caId), study.getFqn(), catalogManager,
                true, userToken);
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, result.getNumResults());
        ClinicalAnalysis updatedClinicalAnalysis = result.first();
        assertEquals(newDescription, updatedClinicalAnalysis.getDescription());

        display1("previous analysis", "previous interpretation", prevCa);
        display1("updated analysis", "updated interpretation", result.first());

        System.out.println("Checking clinical variants in collection for interpretation " + interpreation.getId());
        query = new Query();
        query.put(STUDY_PARAM_NAME, study.getFqn());
        query.put(CI_ID_NAME, interpreation.getId());
        ClinicalSolrIterator<ClinicalVariantSearch> solrIterator = cvdbEngine.clinicalVariantNativeIterator(query, QueryOptions.empty(),
                userToken);
        int counter = 0;
        ClinicalVariantSearch cvs = null;
        while (solrIterator.hasNext()) {
            cvs = solrIterator.next();
            counter++;
        }
        Assert.assertEquals(1, counter);
        cv = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalVariant.class).readValue(cvs.getJson());
        System.out.println("cv.getId() = " + cv.getId());
        System.out.println("v.getId() = " + v.getId());
        Assert.assertEquals(v.getId(), cv.getId());

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);
    }

    public void display1(String msg1, String msg2, ClinicalAnalysis ca) {
        int counter = 0;
        System.out.println(msg1);
        System.out.println("ca.getId() = " + ca.getId());
        display2(msg2, ca.getInterpretation());
    }

    public void display2(String msg, Interpretation ci) {
        int counter = 0;
        System.out.println(msg);
        System.out.println("ci.getId() = " + ci.getId());
        for (ClinicalVariant primaryFinding : ci.getPrimaryFindings()) {
            System.out.println("#" + (counter++) + " " + primaryFinding.getId() + ", " + primaryFinding.getType());
//            for (Map.Entry<String, Object> entry : primaryFinding.getEvidences().get(0).getAttributes().entrySet()) {
//                System.out.println("entry.getKey() = " + entry.getKey() + " -> value = " + entry.getValue());
//            }
        }
    }

    @Test
    public void testOverwriteFalse() throws CatalogException, IOException, CvdbException, SolrServerException {
        TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz"), study, userToken, opencgaToken, catalogManager);
        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.NONE, study, catalogManager, userToken);

        // CVDB index from catalog project
        cvdbEngine.indexProject(projectId, catalogManager, true, userToken);

        // CVDB query
        String caId = "OPA-6522-1";
        Query query;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LIMIT, 100);

        query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(ORGANIZATION_PARAM_NAME, organizationId);
        query.put(CA_ID_NAME, caId);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, result.getNumResults());

        ClinicalAnalysis clinicalAnalysis = result.first();
        assertTrue(StringUtils.isEmpty(clinicalAnalysis.getDescription()));

        // Update clinical analysis in catalog
        String newDescription = "This analysis is for testing the overwrite functionality";
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams();
        updateParams.setDescription(newDescription);
        catalogManager.getClinicalAnalysisManager().update(study.getFqn(), caId, updateParams, QueryOptions.empty(), userToken);

        // CVDB index the given clinical analysis from catalog but overwrite to FALSE (i.e., no index is performed)
        cvdbEngine.indexClinicalAnalyses(Collections.singletonList(caId), study.getFqn(), catalogManager,
                false, userToken);
        result = cvdbEngine.searchClinicalAnalyses(query, queryOptions, userToken);
        assertEquals(1, result.getNumResults());
        ClinicalAnalysis updatedClinicalAnalysis = result.first();
        assertTrue(StringUtils.isEmpty(updatedClinicalAnalysis.getDescription()));

        TestUtilities.checkClinicalAnalysisIndexStatus(CvdbIndexStatus.READY, study, catalogManager, userToken);
    }
}


