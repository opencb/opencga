package org.opencb.opencga.storage.core.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.search.solr.ParseSolrQuery;
import org.opencb.opencga.storage.core.search.solr.SolrVariantSearchIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 09/11/16.
 */
public class SearchManager {

    private SearchConfiguration searchConfiguration;
    //    private SolrClient solrClient;
    private HttpSolrClient solrClient;
    private static VariantSearchToVariantConverter variantSearchToVariantConverter;

    public SearchManager() {
        //TODO remove testing constructor
        if (this.solrClient == null) {
//            this.solrClient = new HttpSolrClient("http://localhost:8983/solr/variants");
            this.solrClient = new HttpSolrClient.Builder("http://localhost:8983/solr/variants").build();
            solrClient.setRequestWriter(new BinaryRequestWriter());

        }
        if (variantSearchToVariantConverter == null) {
            variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        }
    }

    public SearchManager(String host, String collection) {
        if (this.solrClient == null) {
//            this.solrClient = new HttpSolrClient(host + collection);
            this.solrClient = new HttpSolrClient.Builder(host + collection).build();
            solrClient.setRequestWriter(new BinaryRequestWriter());
//            this.solrClient = new ConcurrentUpdateSolrClient.Builder(host+ collection).build();
        }
        if (variantSearchToVariantConverter == null) {
            variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        }
    }

    public SearchManager(StorageConfiguration storageConfiguration) {

        this.searchConfiguration = storageConfiguration.getSearch();

        if (searchConfiguration.getHost() != null && searchConfiguration.getCollection() != null && solrClient == null) {
//            solrClient = new HttpSolrClient(searchConfiguration.getHost() + searchConfiguration.getCollection());
            this.solrClient = new HttpSolrClient.Builder(searchConfiguration.getHost() + searchConfiguration.getCollection()).build();
//            solrClient.setRequestWriter(new BinaryRequestWriter());
//            HttpClientUtil.setBasicAuth((DefaultHttpClient) solrClient.getHttpClient(), searchConfiguration.getUser(),
//                    searchConfiguration.getPassword());
        }

        if (variantSearchToVariantConverter == null) {
            variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        }
    }

    /**
     * Load a Solr core/collection from a Avro or JSON file.
     *
     * @param path      Path to the file to load
     * @throws IOException          IOException
     * @throws SolrServerException  SolrServerException
     * @throws StorageEngineException  SolrServerException
     */
    public void load(Path path) throws IOException, SolrServerException, StorageEngineException {
        // TODO: can we use VariantReaderUtils as implemented in the function load00 below ?
        // TODO: VarriantReaderUtils supports JSON, AVRO and VCF file formats.
        if (path.endsWith(".json")) {
            loadJson(path);
        } else if (path.endsWith(".avro")) {
            loadAvro(path);
        } else {
            throw new IOException("File format " + path + " not supported. Please, use Avro or JSON file formats.");
        }
    }

    // TODO: can we use VariantReaderUtils? It supports JSON, AVRO and VCF file formats.
    // TODO: test !
//    private void load00(Path path) throws IOException, SolrServerException, StorageEngineException {
//        // reader
//        VariantSource source = null;
//        VariantReader reader = VariantReaderUtils.getVariantReader(path, source);
//
//        List<Variant> variants;
//
//        // TODO: get the buffer size from configuration file
//        int bufferSize = 10000;
//
//        do {
//            variants = reader.read(bufferSize);
//            insert(variants);
//        } while (variants.size() == bufferSize);
//
//        reader.close();
//    }

    /**
     * Load a Solr core/collection from a variant DB iterator.
     *
     * @param iterator      Iterator to retrieve the variants to load
     * @throws IOException          IOException
     * @throws SolrServerException  SolrServerException
     */
    public void load(VariantDBIterator iterator) throws IOException, SolrServerException {
        if (iterator != null) {
            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                insert(variant);
            }
        }
    }

    /**
     * Insert a list of variants into solr.
     *
     * @param variants  List of variants to insert
     * @throws IOException          IOException
     * @throws SolrServerException  SolrServerException
     */
    public void insert(List<Variant> variants) throws IOException, SolrServerException {

        List<VariantSearchModel> variantSearchModels = variantSearchToVariantConverter.convertListToStorageType(variants);

        if (!variantSearchModels.isEmpty()) {
            UpdateResponse updateResponse = solrClient.addBeans(variantSearchModels);
            if (0 == updateResponse.getStatus()) {
                solrClient.commit();
            }
        }
    }

    public void insert(Variant variant) throws IOException, SolrServerException {

        VariantSearchModel variantSearchModel = variantSearchToVariantConverter.convertToStorageType(variant);

        if (variantSearchModel != null && variantSearchModel.getId() != null) {
            UpdateResponse updateResponse = solrClient.addBean(variantSearchModel);
            if (0 == updateResponse.getStatus()) {
                solrClient.commit();
            }
        }
    }

    /**
     * Return a Solr variant iterator to retrieve VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param query         Query
     * @param queryOptions  Query options
     * @return              Solr VariantSearch iterator
     */
    public SolrVariantSearchIterator iterator(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = ParseSolrQuery.parse(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrClient.query(solrQuery);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

        return new SolrVariantSearchIterator(response.getBeans(VariantSearchModel.class).iterator());
    }

    /**
     * Return the list of VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param query         Query
     * @param queryOptions  Query options
     * @return              List of VariantSearchModel objects
     */
    public List<VariantSearchModel> query(Query query, QueryOptions queryOptions) {
        List<VariantSearchModel> results = new ArrayList<>();
        SolrVariantSearchIterator iterator = iterator(query, queryOptions);
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }

    /**
     * Return the list of Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param query         Query
     * @param queryOptions  Query options
     * @return              List of Variant objects
     */
    public List<Variant> queryVariant(Query query, QueryOptions queryOptions) {
        List<Variant> results = new ArrayList<>();
        SolrVariantSearchIterator iterator = iterator(query, queryOptions);
        while (iterator.hasNext()) {
            results.add(variantSearchToVariantConverter.convertToDataModelType(iterator.next()));
        }
        return results;
    }

    public VariantSearchFacet getFacet(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = ParseSolrQuery.parse(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrClient.query(solrQuery);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

        return getFacets(response);
    }

    /**-------------------------------------
     *  P R I V A T E    M E T H O D S
     -------------------------------------*/

    /**
     * Load a JSON file into the Solr core/collection.
     *
     * @param path          Path to the JSON file
     * @throws IOException
     * @throws SolrServerException
     */
    private void loadJson(Path path) throws IOException, SolrServerException {
        // reader
        BufferedReader bufferedReader;
        bufferedReader = FileUtils.newBufferedReader(path);
        ObjectReader objectReader = new ObjectMapper().readerFor(Variant.class);

        // TODO: get the buffer size from configuration file
        int bufferSize = 10000;
        List<Variant> variants = new ArrayList<>(bufferSize);

        String line;
        int count = 0;
        while ((line = bufferedReader.readLine()) != null) {
            Variant variant = objectReader.readValue(line);
            variants.add(variant);
            if (count % bufferSize == 0) {
                insert(variants);
                variants.clear();
            }
        }
        if (variants.size() > 0) {
            insert(variants);
        }

        // close
        bufferedReader.close();
    }

    private void loadAvro(Path path) throws IOException, SolrServerException, StorageEngineException {
        // reader
        VariantSource source = null;
        VariantReader reader = VariantReaderUtils.getVariantReader(path, source);

        List<Variant> variants;

        // TODO: get the buffer size from configuration file
        int bufferSize = 10000;

        do {
            variants = reader.read(bufferSize);
            insert(variants);
        } while (variants.size() == bufferSize);

        reader.close();
    }


    /**
     *
     * @param response
     * @return
     */
    private VariantSearchFacet getFacets(QueryResponse response) {

        VariantSearchFacet variantSearchFacet = new VariantSearchFacet();

        if (response.getFacetFields() != null) {
            variantSearchFacet.setFacetFields(response.getFacetFields());
        }
        if (response.getFacetQuery() != null) {
            variantSearchFacet.setFacetQueries(response.getFacetQuery());
        }
        if (response.getFacetRanges() != null) {
            variantSearchFacet.setFacetRanges(response.getFacetRanges());
        }
        if (response.getIntervalFacets() != null) {
            variantSearchFacet.setFacetIntervales(response.getIntervalFacets());
        }

        return variantSearchFacet;
    }



    public static VariantSearchToVariantConverter getVariantSearchToVariantConverter() {
        return variantSearchToVariantConverter;
    }
}
