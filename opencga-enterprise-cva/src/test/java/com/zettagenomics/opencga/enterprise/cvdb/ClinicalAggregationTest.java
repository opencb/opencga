package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
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
import org.opencb.opencga.core.common.JacksonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;

public class ClinicalAggregationTest {

    private CvdbSolrEngine cvaEngine;
    private String projectId = "project1";

    @Rule
    public CvaSolrExtenalResource cvaSolrExternalResource = new CvaSolrExtenalResource(false, projectId);

//    @Before
//    public void before() throws IOException, CvdbException {
//        cvaEngine = cvaSolrExternalResource.configure();
//
//        try {
//            cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX));
//            cvaEngine.getSolrManager().remove(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX));
//            cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
//            cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
//        } catch (Exception e) {
//            // Nothing to do
//        }
//
//        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
//                CLINICAL_ANALYSIS_CONFIGSET);
//        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX), INTERPRETATION_CONFIGSET);
//        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX),
//                CLINICAL_VARIANT_CONFIGSET);
//        cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
//                CLINICAL_VARIANT_EVIDENCE_CONFIGSET);
//
//        InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream("interpretation1.json.gz");
//        GZIPInputStream gzipInputStream = new GZIPInputStream(is);
//        org.opencb.opencga.core.models.clinical.Interpretation interpretation = JacksonUtils.getDefaultObjectMapper()
//                .readerFor(org.opencb.opencga.core.models.clinical.Interpretation.class).readValue(gzipInputStream);
//        cvaEngine.index(interpretation, true, projectId);
//        System.out.println("Interpretation " + interpretation.getClinicalAnalysisId() + " loaded !");
//
//
//        is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream("interpretation2.json.gz");
//        gzipInputStream = new GZIPInputStream(is);
//        interpretation = JacksonUtils.getDefaultObjectMapper().readerFor(org.opencb.opencga.core.models.clinical.Interpretation.class)
//                .readValue(gzipInputStream);
//        cvaEngine.index(interpretation, true, projectId);
//        System.out.println("Interpretation " + interpretation.getClinicalAnalysisId() + " loaded !");
//    }
//
//    @Test
//    public void testAggregationWithFacetPivot() throws IOException, SolrServerException {
//        SolrQuery solrQuery = new SolrQuery("*:*");
//        solrQuery.setFacet(true);
//        solrQuery.addFacetPivotField("ciId,cvId,geneName,acmgs");
//
//        // Execute the Solr query
//        System.out.println("solr query = " + solrQuery.toQueryString());
//        solrQuery.setShowDebugInfo(true);
//        QueryResponse response = cvaEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
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
//        Assert.assertEquals(10, count);
//    }
//
//    @Test
//    public void testAggregationWithFacetField() throws IOException, SolrServerException {
//        SolrQuery solrQuery = new SolrQuery("*:*");
//        solrQuery.setFacet(true);
//        solrQuery.addFacetField("acmgs");
//
//        // Execute the Solr query
//        System.out.println("solr query = " + solrQuery.toQueryString());
//        solrQuery.setShowDebugInfo(true);
//        QueryResponse response = cvaEngine.getSolrClient().query(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
//                solrQuery);
//        FacetField facetField = response.getFacetField("acmgs");
//
//        // Print out the results
//        Assert.assertEquals(4, facetField.getValues().stream().count());
//        System.out.println("name = " + facetField.getName());
//        for (FacetField.Count facetCount : facetField.getValues()) {
//            System.out.println("\tvalue = " + facetCount.getName() + ", count = " + facetCount.getCount());
//            switch (facetCount.getName() + ":" + facetCount.getCount()) {
//                case "PM2:11":
//                case "BP6:4":
//                case "BP7:4":
//                case "P2:1":
//                    break;
//                default:
//                    Assert.fail();
//            }
//        }
//    }
}


