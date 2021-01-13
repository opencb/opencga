package org.opencb.opencga.storage.core.rga;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.rga.RgaUtilsTest.createKnockoutByIndividual;

public class RgaEngineTest {

    private StorageConfiguration storageConfiguration;

    @Rule
    public RgaSolrExtenalResource solr = new RgaSolrExtenalResource();

    @Before
    public void before() throws IOException {
        try (InputStream is = RgaEngineTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml")) {
            storageConfiguration = StorageConfiguration.load(is);
        }
    }

//    @Test
//    public void load1000g() throws Exception {
//        System.out.println(storageConfiguration.getRga());
//        RgaEngine rgaEngine = new RgaEngine(storageConfiguration);
//
//        if (!rgaEngine.exists("rga-short")) {
//            rgaEngine.create("rga-short");
//        }
//        rgaEngine.load("rga-short", Paths.get("/data/datasets/knockout/knockout.individuals.json"));
//    }


    @Test
    public void testIndividualQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList);
        OpenCGAResult<KnockoutByIndividual> result = rgaEngine.individualQuery(collection, new Query(), new QueryOptions());

        assertEquals(2, result.getNumResults());
        for (int i = 0; i < knockoutByIndividualList.size(); i++) {
            assertEquals(JacksonUtils.getDefaultObjectMapper().writeValueAsString(knockoutByIndividualList.get(i)),
                    JacksonUtils.getDefaultObjectMapper().writeValueAsString(result.getResults().get(i)));
        }

        result = rgaEngine.individualQuery(collection, new Query(), new QueryOptions());
        assertEquals(2, result.getNumResults());

        Query query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId1");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());
        assertEquals("id1", result.first().getId());

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId2");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());
        assertEquals("id2", result.first().getId());

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId6");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001822");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1,disorder")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testGeneQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList);
        OpenCGAResult<KnockoutByGene> result = rgaEngine.geneQuery(collection, new Query(), new QueryOptions());

        assertEquals(4, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertTrue(resultResult.getIndividuals().get(0).getId().equals("id1")
                    || resultResult.getIndividuals().get(0).getId().equals("id2"));
        }

        Query query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId1");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertEquals("id1", resultResult.getIndividuals().get(0).getId());
        }

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId2");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertEquals("id2", resultResult.getIndividuals().get(0).getId());
        }

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId6");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(4, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertTrue(resultResult.getIndividuals().get(0).getId().equals("id1")
                    || resultResult.getIndividuals().get(0).getId().equals("id2"));
        }

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001822");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1,disorder")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void testFacet() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList);

        QueryOptions options = new QueryOptions(QueryOptions.FACET, RgaQueryParams.DISORDERS.key());
        DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(collection, new Query(), options);
        assertEquals(1, facetFieldDataResult.getNumResults());
        assertEquals(RgaDataModel.DISORDERS, facetFieldDataResult.first().getName());
        assertEquals(4, facetFieldDataResult.first().getBuckets().size());
    }

}
