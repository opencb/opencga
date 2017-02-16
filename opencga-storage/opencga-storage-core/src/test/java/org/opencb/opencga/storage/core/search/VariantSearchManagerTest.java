package org.opencb.opencga.storage.core.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
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
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.search.solr.SolrVariantSearchIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by wasim on 22/11/16.
 */
@Ignore
public class VariantSearchManagerTest extends GenericTest {

    private List<Variant> variantList;
    private JsonFactory factory;
    private InputStream variantsStream;
    private JsonParser variantsParser;
    private ObjectMapper jsonObjectMapper;
    private VariantSearchManager variantSearchManager;
    private int TOTAL_VARIANTS = 97;

    @Before
    public void setUp() throws Exception {
        factory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper();
        initJSONParser(new File(VariantStorageBaseTest.getResourceUri("variant-solr-sample.json.gz")));
        variantList = readNextVariantFromJSON(100);
//        variantSearchManager = new VariantSearchManager("http://localhost:8983/solr/", "biotest_core2");
        variantSearchManager = new VariantSearchManager("http://localhost:8983/solr/", "collection333");
    }

    //    @Test
    public void createCore() {
        try {
            String coreName = "core555";
            String configSet = "myConfSet";
            variantSearchManager.createCore(coreName, configSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void createCollection() {
        try {
            String collectionName = "collection888";
            String configName = "myConfSet";
            int numShards = 2;
            int numReplicas = 2;
            variantSearchManager.createCollection(collectionName, configName, numShards, numReplicas);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void conversionTest() {

        try {
//            String filename = "/home/imedina/Downloads/variation_chr1.full.json.gz";
            String filename = "/home/jtarraga/data150/vcf/variation_chr22.3.json";
            BufferedReader bufferedReader = FileUtils.newBufferedReader(Paths.get(filename));

            VariantSearchToVariantConverter variantSearchToVariantConverter = new VariantSearchToVariantConverter();
            ObjectReader objectReader = jsonObjectMapper.readerFor(Variant.class);
            String line;
            List<Variant> variants = new ArrayList<>(10000);
            int count = 0;
            while ((line = bufferedReader.readLine()) != null) {
                Variant variant = objectReader.readValue(line);
                VariantSearchModel variantSearchModel = variantSearchToVariantConverter.convertToStorageType(variant);
                System.out.println("--------------- variant:");
                System.out.println(variant.toJson());
                System.out.println("--------------- variant search model:");
                System.out.println(variantSearchModel.toString());
                Variant variant2 = variantSearchToVariantConverter.convertToDataModelType(variantSearchModel);
                System.out.println("--------------- variant2:");
                System.out.println(variant2.toJson());
                count++;
            }

            System.out.println("Number of processed variants: " + count);
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    @Test
    public void loadVariantFileIntoSolrTest() {

        String test = "Test_Variant_Insert_";
        try {
//            String filename = "/home/imedina/Downloads/variation_chr1.full.json.gz";
            String filename = "/home/jtarraga/data150/vcf/variation_chr22.3.json";
            BufferedReader bufferedReader = FileUtils.newBufferedReader(Paths.get(filename));

            VariantSearchToVariantConverter variantSearchToVariantConverter = new VariantSearchToVariantConverter();
            ObjectReader objectReader = jsonObjectMapper.readerFor(Variant.class);
            String line;
            List<Variant> variants = new ArrayList<>(10000);
            int count = 0;
            while ((line = bufferedReader.readLine()) != null) {
                Variant variant = objectReader.readValue(line);
//                VariantSearch variantSearch = variantSearchToVariantConverter.create(variant);
                variants.add(variant);
                if (count % 10000 == 0) {
                    variantSearchManager.insert(variants);
                    variants.clear();
                    System.out.println("count = " + count);
                }
                count++;
            }
            if (variants.size() > 0) {
                variantSearchManager.insert(variants);
            }

            System.out.println("Number of processed variants: " + count);

            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insertSingleVariantIntoSolrTest() {

        String test = "Test_Variant_Insert_";
        try {
            List<Variant> variants = modifyVariantsID(test);

            variantSearchManager.insert(variants.get(0));
            Query query = new Query();
            query.append("dbSNP", test + "*");
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.append(QueryOptions.LIMIT, 500);

            SolrVariantSearchIterator iterator = variantSearchManager.iterator(query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertEquals(1, results.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insertMultipleVariantsIntoSolrTest() {

        String test = "Test_Variants_Insert_";
        try {
            List<Variant> variants = modifyVariantsID(test);

            variantSearchManager.insert(variants);
            Query query = new Query();
            query.append("dbSNP", test + "*");
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.append(QueryOptions.LIMIT, 500);

            SolrVariantSearchIterator iterator = variantSearchManager.iterator(query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertEquals(variants.size(), results.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void verifyInsertedVariantTest() {

        String test = "Test_Variant_Verification_";
        try {
            List<Variant> variants = modifyVariantsID(test);

            variantSearchManager.insert(variants.get(0));
            Query query = new Query();
            query.append("dbSNP", test + "*");
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.append(QueryOptions.LIMIT, 500);

            SolrVariantSearchIterator iterator = variantSearchManager.iterator(query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertEquals(1, results.size());
            Assert.assertTrue(variants.get(0).getStart() == results.get(0).getStart());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void queryNonInsertedVariantTest() {

        String test = "Test_Variant_Non_Inserted_";
        try {
            Query query = new Query();
            query.append("dbSNP", test + "*");
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.append(QueryOptions.LIMIT, 500);

            SolrVariantSearchIterator iterator = variantSearchManager.iterator(query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertEquals(0, results.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void variantToVariantSearchConversionTest() {

        Variant variant = variantList.get(0);
        VariantSearchModel variantSearchModel = VariantSearchManager.getVariantSearchToVariantConverter().convertToStorageType(variant);
        Assert.assertEquals(variantSearchModel.getId(), getVariantSolrID(variant));
        Assert.assertEquals(variantSearchModel.getDbSNP(), variant.getId());
        Assert.assertEquals(variantSearchModel.getChromosome(), variant.getChromosome());
        Assert.assertEquals(variantSearchModel.getType().toString(), variant.getType().toString());
    }

    @Test
    public void variantFacetFiledCountTest() {

        try {
            String facetFieldName = "chromosome";
            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();
            query.append("ids", facetFieldName);
            query.append("facet.field", facetFieldName);
            Variant variant = variantList.get(0);
            variant.setId(facetFieldName);
            variantSearchManager.insert(variant);
            VariantSearchFacet variantSearchFacet = variantSearchManager.getFacet(query, queryOptions);

            Assert.assertEquals(variantSearchFacet.getFacetFields().get(0).getName(), facetFieldName);
            Assert.assertEquals(1, variantSearchFacet.getFacetFields().get(0).getValueCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void variantFacetFiledsCountTest() {
        try {
            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();
            query.append("ids", "*");
            query.append("facet.fields", "type,sift");
            variantSearchManager.insert(variantList);

            VariantSearchFacet variantSearchFacet = variantSearchManager.getFacet(query, queryOptions);

            Assert.assertEquals(variantSearchFacet.getFacetFields().get(0).getName(), "type");
            Assert.assertEquals(variantSearchFacet.getFacetFields().get(1).getName(), "sift");

            Assert.assertEquals(TOTAL_VARIANTS, variantSearchFacet.getFacetFields().get(0).getValues().get(0).getCount());
            Assert.assertEquals(TOTAL_VARIANTS, variantSearchFacet.getFacetFields().get(1).getValues().get(0).getCount());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void variantFacetQueryTest() {
        try {
            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();
            query.append("ids", "*");
            query.append("facet.query", "type:SNV");
            variantSearchManager.insert(variantList);

            VariantSearchFacet variantSearchFacet = variantSearchManager.getFacet(query, queryOptions);

            Assert.assertTrue(TOTAL_VARIANTS == variantSearchFacet.getFacetQueries().entrySet().iterator().next().getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void variantFacetRangeTest() {
        try {
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
            variantSearchManager.insert(variantList);

            VariantSearchFacet variantSearchFacet = variantSearchManager.getFacet(query, queryOptions);

            List<RangeFacet.Count> rangeEntries = variantSearchFacet.getFacetRanges().get(0).getCounts();

            Assert.assertNotNull(rangeEntries);
            Assert.assertEquals(0, rangeEntries.get(0).getCount());
            Assert.assertEquals(0, rangeEntries.get(1).getCount());
            Assert.assertEquals(0, rangeEntries.get(2).getCount());
            Assert.assertEquals(0, rangeEntries.get(3).getCount());
            Assert.assertEquals(0, rangeEntries.get(4).getCount());
            Assert.assertEquals(TOTAL_VARIANTS, rangeEntries.get(5).getCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void variantSolrQueryLimitTest() {
        try {
            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();
            query.append("ids", "*");
            queryOptions.append(QueryOptions.LIMIT, 15);
            variantSearchManager.insert(variantList);

            SolrVariantSearchIterator iterator = variantSearchManager.iterator(query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertEquals(15, results.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void queryOptionSortTest() {
        try {
            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();
            query.append("ids", "*");
            queryOptions.append(QueryOptions.LIMIT, 15);
            queryOptions.add(QueryOptions.SORT, "start");
            queryOptions.add(QueryOptions.ORDER, QueryOptions.DESCENDING);
            variantSearchManager.insert(variantList);

            SolrVariantSearchIterator iterator = variantSearchManager.iterator(query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertTrue(results.get(0).getStart() > results.get(14).getStart());
        } catch (Exception e) {
            e.printStackTrace();
        }

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
