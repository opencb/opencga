/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.search.solr.VariantSearchIterator;
import org.opencb.opencga.storage.core.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by wasim on 22/11/16.
 */
@Ignore
public class VariantSearchManagerTest extends GenericTest {

    private String collection;
    private String filename;
    private List<Variant> variantList;
    private JsonFactory factory;
    private InputStream variantsStream;
    private JsonParser variantsParser;
    private ObjectMapper jsonObjectMapper;
    private VariantSearchManager variantSearchManager;
    private int TOTAL_VARIANTS = 97;

    @Before
    public void setUp() throws Exception {
        filename = "/home/imedina/Downloads/variation_chr1.full.json.gz";

        factory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper();
        initJSONParser(new File(VariantStorageBaseTest.getResourceUri(filename)));
        variantList = readNextVariantFromJSON(100);

//        collection = "biotest_core2";
        collection = "biotest_collection_4";
        variantSearchManager = new VariantSearchManager("http://localhost:8983/solr/", collection);
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
    public void existCore() {
        try {
            String name;
            name = "core999";
            System.out.println("exist " + name + "? " + variantSearchManager.existsCore(name));

            name = "core99999";
            System.out.println("exist " + name + "? " + variantSearchManager.existsCore(name));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void createCollection() {
        try {
            String collectionName = "collection888";
            String configName = "myConfSet";
            variantSearchManager.createCollection(collectionName, configName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    @Test
    public void existCollection() {
        try {
            String name;
            name = "collection888";
            System.out.println("exist " + name + "? " + variantSearchManager.existsCollection(name));

            name = "collection888888";
            System.out.println("exist " + name + "? " + variantSearchManager.existsCollection(name));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    @Test
    public void conversionTest() {

        try {
//            filename = "/home/imedina/Downloads/variation_chr1.full.json.gz";
            filename = "/home/jtarraga/data150/vcf/variation_chr22.3.json";
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

    //@Test
    public void loadVariantFileIntoSolrTest() {

        String test = "Test_Variant_Insert_";
        try {
            variantSearchManager.load(collection, Paths.get(filename));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //@Test
    public void verifyInsertedVariantTest() {

        String test = "Test_Variant_Verification_";
        try {
            variantSearchManager.load(collection, Paths.get(filename));
            List<Variant> variants = modifyVariantsID(test);

            Query query = new Query();
            query.append("dbSNP", test + "*");
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.append(QueryOptions.LIMIT, 500);

            VariantSearchIterator iterator = variantSearchManager.nativeIterator(collection, query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

//            Assert.assertEquals(1, results.size());
//            Assert.assertTrue(variants.get(0).getStart() == results.get(0).getStart());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void queryNonInsertedVariantTest() {

        String test = "Test_Variant_Non_Inserted_";
        try {
            Query query = new Query();
            query.append("dbSNP", test + "*");
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.append(QueryOptions.LIMIT, 500);

            VariantSearchIterator iterator = variantSearchManager.nativeIterator(collection, query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertEquals(0, results.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void variantToVariantSearchConversionTest() {

        Variant variant = variantList.get(0);
        VariantSearchToVariantConverter converter = new VariantSearchToVariantConverter();
        VariantSearchModel variantSearchModel = converter.convertToStorageType(variant);
        Assert.assertEquals(variantSearchModel.getId(), getVariantSolrID(variant));
        Assert.assertEquals(variantSearchModel.getVariantId(), variant.getId());
        Assert.assertEquals(variantSearchModel.getChromosome(), variant.getChromosome());
        Assert.assertEquals(variantSearchModel.getType().toString(), variant.getType().toString());
    }

    /*
    // Facet not supported yet !!
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
*/
//    @Test
    public void variantSolrQueryLimitTest() {
        try {
            variantSearchManager.load(collection, Paths.get(filename));
            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();
            query.append("ids", "*");
            queryOptions.append(QueryOptions.LIMIT, 15);

            VariantSearchIterator iterator = variantSearchManager.nativeIterator(collection, query, queryOptions);
            List<VariantSearchModel> results = new ArrayList<>();

            iterator.forEachRemaining(results::add);

            Assert.assertEquals(15, results.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //@Test
    public void queryOptionSortTest() {
        try {
            variantSearchManager.load(collection, Paths.get(filename));

            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();
            query.append("ids", "*");
            queryOptions.append(QueryOptions.LIMIT, 15);
            queryOptions.add(QueryOptions.SORT, "start");
            queryOptions.add(QueryOptions.ORDER, QueryOptions.DESCENDING);

            VariantSearchIterator iterator = variantSearchManager.nativeIterator(collection, query, queryOptions);
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
