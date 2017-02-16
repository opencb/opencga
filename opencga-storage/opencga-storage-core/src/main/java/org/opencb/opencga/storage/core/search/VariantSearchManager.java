package org.opencb.opencga.storage.core.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
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
public class VariantSearchManager {

    private String hostName;
    private String collectionName;

    private SearchConfiguration searchConfiguration;
    private HttpSolrClient solrClient;
    private static VariantSearchToVariantConverter variantSearchToVariantConverter;

    public VariantSearchManager(String host, String collection) {
        this.hostName = host;
        this.collectionName = collection;

        this.solrClient = new HttpSolrClient.Builder(host).build();
        this.solrClient.setRequestWriter(new BinaryRequestWriter());
        variantSearchToVariantConverter = new VariantSearchToVariantConverter();

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

    public VariantSearchManager(StorageConfiguration storageConfiguration) {
        this(storageConfiguration.getSearch().getHost(), storageConfiguration.getSearch().getCollection());
        this.searchConfiguration = storageConfiguration.getSearch();
    }

    /**
     * Create a Solr core from a configuration set directory. By default, the configuration set directory is located
     * inside the folder server/solr/configsets.
     *
     * @param coreName      Core name
     * @param configSet     Configuration set name
     */
    public void createCore(String coreName, String configSet) {
        try {
            HttpSolrClient solrClient = new HttpSolrClient.Builder(hostName).build();
            CoreAdminRequest.Create request = new CoreAdminRequest.Create();
            request.setCoreName(coreName);
            request.setConfigSet(configSet);
            request.process(solrClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Check if a given core exists.
     *
     * @param coreName          Core name
     * @return                  True or false
     * @throws Exception        Exception
     */
    public boolean existCore(String coreName) throws Exception {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(hostName).build();
        CoreStatus status = CoreAdminRequest.getCoreStatus(coreName, solrClient);
        try {
            // if the status.response is null, catch the exception
            status.getInstanceDirectory();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Create a Solr collection from a configuration directory. The configuration has to be uploaded to the zookeeper,
     * $ ./bin/solr zk upconfig -n <config name> -d <path to the config dir> -z <host:port zookeeper>.
     * For Solr, collection name, configuration name and number of shards are mandatory in order to create a collection.
     * Number of replicas is optional.
     *
     * @param collectionName     Collection name
     * @param config             Configuration name
     * @param numShards          Number of shards
     * @param numReplicas        Number of replicas
     */
    public void createCollection(String collectionName, String config, int numShards, int numReplicas) {
        try {
            HttpSolrClient solrClient = new HttpSolrClient.Builder(hostName).build();
            CollectionAdminRequest request = CollectionAdminRequest.createCollection(collectionName, config,
                    numShards, numReplicas);
            request.process(solrClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Check if a given collection exists.
     *
     * @param collectionName    Collection name
     * @return                  True or false
     * @throws Exception        Exception
     */
    public boolean existCollection(String collectionName) throws Exception {

        HttpSolrClient solrClient = new HttpSolrClient.Builder(hostName).build();

        List<String> collections = CollectionAdminRequest.listCollections(solrClient);
        for (String collection: collections) {
            System.out.println(collection);
        }
        for (String collection: collections) {
            if (collection.equals(collectionName)) {
                return true;
            }
        }
        return false;
//        .getCoreStatus(coreName, solrClient);
//        try {
//            // if the status.response is null, catch the exception
//            status.getInstanceDirectory();
//        } catch (Exception e) {
//            return false;
//        }
//        return true;
//
////        CollectionAdminResponse request = CollectionAdminRequest.getClusterStatus();
////        System.out.println(response.getCoreStatus(collectionName).size());
////        return (response.getCoreStatus(collectionName).size() == 1);
//
//
//        CollectionAdminRequest.ClusterStatus request = new CollectionAdminRequest.ClusterStatus();
//        request.setCollectionName(collectionName);
//        System.out.println(request.process(solrClient).getCollectionStatus().size());
//        return (request.process(solrClient).getCollectionStatus().size() == 1);
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

        System.out.println("solrQuery.toQueryString = " + solrQuery.toQueryString());
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
