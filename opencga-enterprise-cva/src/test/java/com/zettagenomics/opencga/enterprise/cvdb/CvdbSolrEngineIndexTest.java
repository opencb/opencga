package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;

public class CvdbSolrEngineIndexTest {

    protected CvdbSolrEngine cvaEngine;
    protected String projectId = "project1";

    @Rule
    public CvaSolrExtenalResource cvaSolrExternalResource = new CvaSolrExtenalResource(true, projectId);;

    @Before
    public void before() {
        cvaEngine = cvaSolrExternalResource.configure();

        try {
            cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX));
            cvaEngine.getSolrManager().remove(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX));
            cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
            cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
        } catch (Exception e) {
            // Nothing to do
        }

        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                CLINICAL_ANALYSIS_CONFIGSET);
        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX), INTERPRETATION_CONFIGSET);
        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX),
                CLINICAL_VARIANT_CONFIGSET);
        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
                CLINICAL_VARIANT_EVIDENCE_CONFIGSET);
    }

    @Test
    public void testClinicalAnalsyisIndex() throws IOException, SolrServerException, CvdbException {
        loadClinicalAnalsyses();

        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvaEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical analysis: " + response.getResults().getNumFound());
        Assert.assertEquals(3, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical analysis #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        // Execute the Solr query
        response = cvaEngine.getSolrClient().query(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX), solrQuery);

        // Print out the results
        System.out.println("Number of interpretations: " + response.getResults().getNumFound());
        Assert.assertEquals(9, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Interpretation #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        // Execute the Solr query
        response = cvaEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX), solrQuery);
        // Print out the results
        System.out.println("Number of clinical variants: " + response.getResults().getNumFound());
        Assert.assertEquals(54, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical variant #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println("\tvariant ID: " + response.getResults().get(i).getFieldValue("variantId"));
            System.out.println("\tInterpretation ID: " + response.getResults().get(i).getFieldValue("cvInterpretationId"));
            System.out.println();
        }

        // Execute the Solr query
        response = cvaEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX), solrQuery);
        // Print out the results
        System.out.println("Number of clinical variant evidences: " + response.getResults().getNumFound());
        Assert.assertEquals(59, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical variant evidence #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println("\tvariant ID: " + response.getResults().get(i).getFieldValue("cveVariantId"));
            System.out.println();
        }
    }

//    @Test
//    public void testJoin() throws IOException, SolrServerException, CvdbException {
//        loadClinicalAnalsyses();
//
//        SolrQuery solrQuery = new SolrQuery("*:*");
//        solrQuery.setFields("id", "methodName");
//
//        String joinFilterQuery = "{!join from=ciId to=id fromIndex=" + getCollectionName(projectId,
//                CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX) + "}variantId:\"6:31356248:G:C\"";
//        solrQuery.addFilterQuery(joinFilterQuery);
//        System.out.println("solr query = " + solrQuery.toQueryString());
//
//        // Execute the Solr query
//        solrQuery.setShowDebugInfo(true);
//        QueryResponse response = cvaEngine.getSolrClient().query(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX),
//                solrQuery);
//
//        // Print out the results
//        System.out.println("Number of interpretations: " + response.getResults().getNumFound());
//        Assert.assertEquals(1, response.getResults().getNumFound());
//        for (int i = 0; i < response.getResults().size(); i++) {
//            System.out.println("Interpretation #" + i + ":");
//            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
//            System.out.println();
//        }
//    }

    @Test
    public void testClinicalVariantIndex() throws IOException, SolrServerException, CvdbException {
        loadClinicalVariants();

        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setFields("id");
//
//        String joinFilterQuery = "{!join from=ciId to=id fromIndex=" + getCollectionName(projectId,
//                CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX) + "}variantId:\"6:31356248:G:C\"";
//        solrQuery.addFilterQuery(joinFilterQuery);
//        System.out.println("solr query = " + solrQuery.toQueryString());
//
        // Execute the Solr query
        solrQuery.setShowDebugInfo(true);
        QueryResponse response = cvaEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX),
                solrQuery);

        // Print out the results
        System.out.println("Number of clinical variants: " + response.getResults().getNumFound());
        Assert.assertEquals(1, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical variant #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("variantId"));
            System.out.println();
        }
    }

    private void loadClinicalAnalsyses() throws IOException, CvdbException {
        List<String> names = Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz");
        for (String name : names) {
            InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream(name);
            GZIPInputStream gzipInputStream = new GZIPInputStream(is);
            ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class).readValue(gzipInputStream);
            cvaEngine.index(clinicalAnalysis, projectId);
            System.out.println("Clinical analysis " + clinicalAnalysis.getId() + " loaded !");
        }
    }

    private void loadClinicalVariants() throws IOException, CvdbException {
//            InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream("clinical_variants.json.gz");
        InputStream is = new FileInputStream(Paths.get("/home/jtarraga/data/reanalysis/test/clinical_variants.test.json.gz").toFile());
        GZIPInputStream gzipInputStream = new GZIPInputStream(is);

        Map<Integer, ClinicalVariant> cvs = JacksonUtils.getDefaultObjectMapper().readerFor(Map.class).readValue(gzipInputStream);
        List<ClinicalVariant> clinicalVariantList = new ArrayList<>();
        for (ClinicalVariant cv : cvs.values()) {
            clinicalVariantList.add(cv);
        }
        cvaEngine.index(clinicalVariantList, false, projectId);
    }
}


