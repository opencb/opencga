package org.opencb.opencga.storage.core.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.storage.core.search.iterators.SolrVariantSearchIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by wasim on 22/11/16.
 */
@Ignore
public class SearchManagerTest extends GenericTest {

    private List<Variant> variantList;
    private JsonFactory factory;
    private InputStream variantsStream;
    private JsonParser variantsParser;
    private ObjectMapper jsonObjectMapper;
    private SearchManager searchManager;
    private int TOTAL_VARIANTS = 97;

    @Before
    public void setUp() throws Exception {
        factory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper();
        initJSONParser(new File(VariantStorageBaseTest.getResourceUri("variant-solr-sample.json.gz")));
        variantList = readNextVariantFromJSON(100);
        searchManager = new SearchManager();
    }

    @Test
    public void insertSingleVariantIntoSolrTest() {

        String test = "Test_Variant_Insert_";
        List<Variant> variants = modifyVariantsID(test);

        searchManager.insert(variants.get(0));
        Query query = new Query();
        query.append("dbSNP", test + "*");
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.LIMIT, 500);

        SolrVariantSearchIterator iterator = searchManager.iterator(query, queryOptions);
        List<VariantSearch> results = new ArrayList<>();

        iterator.forEachRemaining(results::add);

        Assert.assertEquals(1, results.size());
    }

    @Test
    public void insertMultipleVariantsIntoSolrTest() {

        String test = "Test_Variants_Insert_";
        List<Variant> variants = modifyVariantsID(test);

        searchManager.insert(variants);
        Query query = new Query();
        query.append("dbSNP", test + "*");
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.LIMIT, 500);

        SolrVariantSearchIterator iterator = searchManager.iterator(query, queryOptions);
        List<VariantSearch> results = new ArrayList<>();

        iterator.forEachRemaining(results::add);

        Assert.assertEquals(variants.size(), results.size());
    }

    @Test
    public void verifyInsertedVariantTest() {

        String test = "Test_Variant_Verification_";
        List<Variant> variants = modifyVariantsID(test);

        searchManager.insert(variants.get(0));
        Query query = new Query();
        query.append("dbSNP", test + "*");
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.LIMIT, 500);

        SolrVariantSearchIterator iterator = searchManager.iterator(query, queryOptions);
        List<VariantSearch> results = new ArrayList<>();

        iterator.forEachRemaining(results::add);

        Assert.assertEquals(1, results.size());
        Assert.assertTrue(variants.get(0).getStart() == results.get(0).getStart());
    }

    @Test
    public void queryNonInsertedVariantTest() {

        String test = "Test_Variant_Non_Inserted_";

        Query query = new Query();
        query.append("dbSNP", test + "*");
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.LIMIT, 500);

        SolrVariantSearchIterator iterator = searchManager.iterator(query, queryOptions);
        List<VariantSearch> results = new ArrayList<>();

        iterator.forEachRemaining(results::add);

        Assert.assertEquals(0, results.size());
    }

    @Test
    public void variantToVariantSearchConversionTest() {

        Variant variant = variantList.get(0);
        VariantSearch variantSearch = SearchManager.getVariantSearchFactory().create(variant);
        Assert.assertEquals(variantSearch.getId(), getVariantSolrID(variant));
        Assert.assertEquals(variantSearch.getDbSNP(), variant.getId());
        Assert.assertEquals(variantSearch.getChromosome(), variant.getChromosome());
        Assert.assertEquals(variantSearch.getType().toString(), variant.getType().toString());
    }

    @Test
    public void variantFacetFiledCountTest() {

        String facetFieldName = "chromosome";
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();
        query.append("ids", facetFieldName);
        query.append("facet.field", facetFieldName);
        Variant variant = variantList.get(0);
        variant.setId(facetFieldName);
        searchManager.insert(variant);
        VariantSearchFacet variantSearchFacet = searchManager.getFacet(query, queryOptions);

        Assert.assertEquals(variantSearchFacet.getFacetFields().get(0).getName(), facetFieldName);
        Assert.assertEquals(1, variantSearchFacet.getFacetFields().get(0).getValueCount());
    }

    @Test
    public void variantFacetFiledsCountTest() {

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();
        query.append("ids", "*");
        query.append("facet.fields", "type,sift");
        searchManager.insert(variantList);

        VariantSearchFacet variantSearchFacet = searchManager.getFacet(query, queryOptions);

        Assert.assertEquals(variantSearchFacet.getFacetFields().get(0).getName(), "type");
        Assert.assertEquals(variantSearchFacet.getFacetFields().get(1).getName(), "sift");

        Assert.assertEquals(TOTAL_VARIANTS, variantSearchFacet.getFacetFields().get(0).getValues().get(0).getCount());
        Assert.assertEquals(TOTAL_VARIANTS, variantSearchFacet.getFacetFields().get(1).getValues().get(0).getCount());
    }

    @Test
    public void variantFacetRangeTest() {

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();
        query.append("ids", "*");

        Map<String, Map<String, Double>> rangeFields = new HashMap<>();

        Map<String, Double> sift = new HashMap<>();
        sift.put("facet.range.start", 0.0);
        sift.put("facet.range.end", 11.0);
        sift.put("facet.range.gap", 2.0);
        rangeFields.put("sift", sift);

        query.append("facet.range", rangeFields);
        searchManager.insert(variantList);

        VariantSearchFacet variantSearchFacet = searchManager.getFacet(query, queryOptions);

        List<RangeFacet.Count> rangeEntries = variantSearchFacet.getFacetRanges().get(0).getCounts();

        Assert.assertNotNull(rangeEntries);
        Assert.assertEquals(0, rangeEntries.get(0).getCount());
        Assert.assertEquals(0, rangeEntries.get(1).getCount());
        Assert.assertEquals(0, rangeEntries.get(2).getCount());
        Assert.assertEquals(0, rangeEntries.get(3).getCount());
        Assert.assertEquals(0, rangeEntries.get(4).getCount());
        Assert.assertEquals(97, rangeEntries.get(5).getCount());
    }

    @Test
    public void variantSolrQueryLimitTest() {

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();
        query.append("ids", "*");
        queryOptions.append(QueryOptions.LIMIT, 15);
        searchManager.insert(variantList);

        SolrVariantSearchIterator iterator = searchManager.iterator(query, queryOptions);
        List<VariantSearch> results = new ArrayList<>();

        iterator.forEachRemaining(results::add);

        Assert.assertEquals(15, results.size());
    }

    @Test
    public void queryOptionSortTest() {

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();
        query.append("ids", "*");
        queryOptions.append(QueryOptions.LIMIT, 15);
        queryOptions.add(QueryOptions.SORT, "start");
        queryOptions.add(QueryOptions.ORDER, QueryOptions.DESCENDING);
        searchManager.insert(variantList);

        SolrVariantSearchIterator iterator = searchManager.iterator(query, queryOptions);
        List<VariantSearch> results = new ArrayList<>();

        iterator.forEachRemaining(results::add);

        Assert.assertTrue(results.get(0).getStart() > results.get(14).getStart()) ;
    }

    private String getVariantSolrID(Variant variant) {
        VariantAnnotation variantAnnotation = variant.getAnnotation();
        return variantAnnotation.getChromosome() + "_" + variantAnnotation.getStart() + "_"
                + variantAnnotation.getReference() + "_" + variantAnnotation.getAlternate();
    }


    private List<Variant> modifyVariantsID(String prefix) {
        List<Variant> modifiedVariants = new ArrayList<>();
        for (Variant variant : variantList) {
            Variant var = variant;
            var.setId(prefix + variant.getId());
            modifiedVariants.add(var);
        }
        return modifiedVariants;
    }

    private void initJSONParser(File file) {
        try {
            this.variantsStream = new GZIPInputStream(new FileInputStream(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            this.variantsParser = this.factory.createParser(this.variantsStream);
            this.variantsParser.setCodec((ObjectCodec) this.jsonObjectMapper);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<Variant> readNextVariantFromJSON(int bucket) {
        List<Variant> variants = new ArrayList<Variant>();
        int i = 0;
        try {
            while (this.variantsParser.nextToken() != null && i++ < bucket) {
                Variant var = (Variant) this.variantsParser.readValueAs(Variant.class);
                variants.add(var);
            }
        } catch (IOException e) {
            //  e.printStackTrace();
        }
        return variants;
    }
}
