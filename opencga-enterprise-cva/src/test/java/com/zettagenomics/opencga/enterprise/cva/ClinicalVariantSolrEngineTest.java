package com.zettagenomics.opencga.enterprise.cva;

import com.zettagenomics.opencga.enterprise.cva.exceptions.CvaException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.*;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;

import java.io.IOException;
import java.io.InputStream;

import static com.zettagenomics.opencga.enterprise.cva.ClinicalVariantSolrEngine.*;

public class ClinicalVariantSolrEngineTest {

    private ClinicalVariantSolrEngine cvaEngine;

    @Rule
    public CvaSolrExtenalResource cvaSolrExternalResource = new CvaSolrExtenalResource(true);

    @Before
    public void before() throws IOException, CatalogException, RgaException, SolrServerException, CvaException {
//        try (InputStream is = RgaEngineTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml")) {
//            storageConfiguration = StorageConfiguration.load(is);
//        }
//        Configuration configuration;
//        try (InputStream is = RgaEngineTest.class.getClassLoader().getResourceAsStream("configuration-test.yml")) {
//            configuration = Configuration.load(is);
//        }
//        this.catalogManager = new CatalogManager(configuration);
//
//        this.variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));

        cvaEngine = cvaSolrExternalResource.configure();

        try {
            cvaEngine.getSolrManager().remove(INTERPRETATIONS_COLLECTION);
            cvaEngine.getSolrManager().remove(CLINICAL_VARIANTS_COLLECTION);
            cvaEngine.getSolrManager().remove(CLINICAL_VARIANT_EVIDENCES_COLLECTION);
        } catch (Exception e) {
            // Nothing to do
        }

        cvaEngine.getSolrManager().createCore(INTERPRETATIONS_COLLECTION, INTERPRETATION_CONFIGSET);
        cvaEngine.getSolrManager().createCore(CLINICAL_VARIANTS_COLLECTION, CLINICAL_VARIANT_CONFIGSET);
        cvaEngine.getSolrManager().createCore(CLINICAL_VARIANT_EVIDENCES_COLLECTION, CLINICAL_VARIANT_EVIDENCE_CONFIGSET);

        InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream("interpretation1.json");
        org.opencb.opencga.core.models.clinical.Interpretation interpretation = JacksonUtils.getDefaultObjectMapper().readerFor(org.opencb.opencga.core.models.clinical.Interpretation.class).readValue(is);
        cvaEngine.insert(interpretation, true);
        System.out.println("Interpretation " + interpretation.getClinicalAnalysisId() + " loaded !");


        is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream("interpretation2.json");
        interpretation = JacksonUtils.getDefaultObjectMapper().readerFor(org.opencb.opencga.core.models.clinical.Interpretation.class).readValue(is);
        cvaEngine.insert(interpretation, true);
        System.out.println("Interpretation " + interpretation.getClinicalAnalysisId() + " loaded !");
    }

    @Test
    public void testIndex() throws IOException, SolrServerException {
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(100);

        // Execute the Solr query
        QueryResponse response = cvaEngine.getSolrClient().query(INTERPRETATIONS_COLLECTION, solrQuery);

        // Print out the results
        System.out.println("Number of interpretations: " + response.getResults().getNumFound());
        Assert.assertEquals(2, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Interpretation #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }

        // Execute the Solr query
        response = cvaEngine.getSolrClient().query(CLINICAL_VARIANTS_COLLECTION, solrQuery);
        // Print out the results
        System.out.println("Number of clinical variants: " + response.getResults().getNumFound());
        Assert.assertEquals(4, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical variant #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println("\tvariant ID: " + response.getResults().get(i).getFieldValue("variantId"));
            System.out.println("\tInterpretation ID: " + response.getResults().get(i).getFieldValue("cvInterpretationId"));
            System.out.println();
        }

        // Execute the Solr query
        response = cvaEngine.getSolrClient().query(CLINICAL_VARIANT_EVIDENCES_COLLECTION, solrQuery);
        // Print out the results
        System.out.println("Number of clinical variant evidences: " + response.getResults().getNumFound());
        Assert.assertEquals(15, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Clinical variant evidence #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println("\tvariant ID: " + response.getResults().get(i).getFieldValue("cveVariantId"));
            System.out.println();
        }
    }

    @Test
    public void testJoin() throws IOException, SolrServerException {
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setFields("id", "methodName");

        String joinFilterQuery = "{!join from=ciId to=id fromIndex=" + CLINICAL_VARIANT_EVIDENCES_COLLECTION + "}*:*";
        solrQuery.addFilterQuery(joinFilterQuery);
        System.out.println("solr query = " + solrQuery.toQueryString());

        // Execute the Solr query
        solrQuery.setShowDebugInfo(true);
        QueryResponse response = cvaEngine.getSolrClient().query(INTERPRETATIONS_COLLECTION, solrQuery);

        // Print out the results
        System.out.println("Number of interpretations: " + response.getResults().getNumFound());
        Assert.assertEquals(2, response.getResults().getNumFound());
        for (int i = 0; i < response.getResults().size(); i++) {
            System.out.println("Interpretation #" + i + ":");
            System.out.println("\tID: " + response.getResults().get(i).getFieldValue("id"));
            System.out.println();
        }
    }
}


