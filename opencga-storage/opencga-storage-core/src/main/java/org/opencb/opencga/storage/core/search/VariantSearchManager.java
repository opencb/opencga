package org.opencb.opencga.storage.core.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.util.NamedList;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResultItem;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.search.solr.SolrQueryParser;
import org.opencb.opencga.storage.core.search.solr.SolrVariantIterator;
import org.opencb.opencga.storage.core.search.solr.SolrVariantSearchIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by wasim on 09/11/16.
 */
public class VariantSearchManager {

    public static final String SUMMARY = "summary";
    private String collection;
    private StorageConfiguration storageConfiguration;

    private SolrQueryParser solrQueryParser;

    private HttpSolrClient solrClient;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;

    private Logger logger;

    private static final int DEFAULT_INSERT_SIZE = 10000;

    @Deprecated
    public VariantSearchManager(String host, String collection) {
//        this.collection = collection;

        this.solrClient = new HttpSolrClient.Builder(host + collection).build();
        this.solrClient.setRequestWriter(new BinaryRequestWriter());
        variantSearchToVariantConverter = new VariantSearchToVariantConverter();
    }

    public VariantSearchManager(StudyConfigurationManager studyConfigurationManager, CellBaseUtils cellbaseUtils,
                                StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;

//        this.solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost() + collection).build();
//        this.solrClient.setRequestWriter(new BinaryRequestWriter());

        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        this.solrQueryParser = new SolrQueryParser(studyConfigurationManager, cellbaseUtils);

        logger = LoggerFactory.getLogger(VariantSearchManager.class);
    }

//    public VariantSearchManager(String collection, StorageConfiguration storageConfiguration) {
////        this.host = storageConfiguration.getSearch().getHost();
////        this.collection = collection;
//        this.storageConfiguration = storageConfiguration;
//
//        this.solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost() + collection).build();
//        this.solrClient.setRequestWriter(new BinaryRequestWriter());
//
//        variantSearchToVariantConverter = new VariantSearchToVariantConverter();
//
//        logger = LoggerFactory.getLogger(VariantSearchManager.class);
//    }

    private void init(String collection) throws VariantSearchException {
        if (this.solrClient == null || StringUtils.isEmpty(this.collection) || !this.collection.equals(collection)) {

            // check if collection exist
            if (!existCollection(collection)) {
                createCollection(collection);
            }

            this.solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost() + collection).build();
            this.solrClient.setRequestWriter(new BinaryRequestWriter());
            this.solrClient.setSoTimeout(storageConfiguration.getSearch().getTimeout()); // ms

            this.collection = collection;
        }
    }

    public boolean isAlive(String collection) {
        try {
            // init collection
            init(collection);

            return solrClient.ping().getResponse().get("status").equals("OK");
        } catch (VariantSearchException | SolrServerException | IOException e) {
            return false;
        }
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
            logger.debug("Creating core: " + storageConfiguration.getSearch().getHost() + ", core=" + coreName
                    + ", configSet=" + configSet);
            HttpSolrClient solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost()).build();
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
     */
    public boolean existCore(String coreName) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost()).build();
        try {
            CoreStatus status = CoreAdminRequest.getCoreStatus(coreName, solrClient);
            // if the status.response is null, catch the exception
            status.getInstanceDirectory();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * * Create a Solr collection with default parameters: configuration, shards and replicas.
     *
     * @param collectionName             Collection name
     * @throws VariantSearchException    Exception
     */
    public void createCollection(String collectionName) throws VariantSearchException {
        createCollection(collectionName, "OpenCGAConfSet", 1, 1);
    }

    /**
     * Create a Solr collection from a configuration directory. The configuration has to be uploaded to the zookeeper,
     * $ ./bin/solr zk upconfig -n <config name> -d <path to the config dir> -z <host:port zookeeper>.
     * For Solr, collection name, configuration name and number of shards are mandatory in order to create a collection.
     * Number of replicas is optional.
     *
     * @param collectionName             Collection name
     * @param config                     Configuration name
     * @param numShards                  Number of shards
     * @param numReplicas                Number of replicas
     * @throws VariantSearchException    Exception
     */
    public void createCollection(String collectionName, String config, int numShards, int numReplicas) throws VariantSearchException {
        logger.debug("Creating collection: " + storageConfiguration.getSearch().getHost() + ", collection=" + collectionName
                + ", config=" + config + ", numShards=" + numShards + ", numReplicas=" + numReplicas);
        try {
            HttpSolrClient solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost()).build();
            CollectionAdminRequest request = CollectionAdminRequest.createCollection(collectionName, config,
                    numShards, numReplicas);
            request.process(solrClient);
        } catch (Exception e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    /**
     * Check if a given collection exists.
     *
     * @param collectionName             Collection name
     * @return                           True or false
     */
    public boolean existCollection(String collectionName) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost()).build();

        try {
            List<String> collections = CollectionAdminRequest.listCollections(solrClient);
//            for (String collection : collections) {
//                System.out.println(collection);
//            }
            for (String collection : collections) {
                if (collection.equals(collectionName)) {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }

        return false;
    }

    /**
     * Load a Solr core/collection from a Avro or JSON file.
     *
     * @param collection               Collection name
     * @param path      Path to the file to load
     * @throws IOException          IOException
     * @throws VariantSearchException  SolrServerException
     * @throws StorageEngineException  SolrServerException
     */
    public void load(String collection, Path path) throws IOException, VariantSearchException, StorageEngineException {
        // TODO: can we use VariantReaderUtils as implemented in the function load00 below ?
        // TODO: VarriantReaderUtils supports JSON, AVRO and VCF file formats.

        // Check path is not null and exists.
        FileUtils.checkFile(path);

        // Init collection if needed
        init(collection);

        File file = path.toFile();
        if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
            loadJson(path);
        } else if (file.getName().endsWith("avro") || file.getName().endsWith("avro.gz")) {
            loadAvro(path);
        } else {
            throw new IOException("File format " + path + " not supported. Please, use Avro or JSON file formats.");
        }
    }

    /**
     * Load a Solr core/collection from a variant DB iterator.
     *
     * @param collection               Collection name
     * @param variantDBIterator        Iterator to retrieve the variants to load
     * @throws IOException             IOException
     * @throws VariantSearchException  VariantSearchException
     */
    public void load(String collection, VariantDBIterator variantDBIterator) throws IOException, VariantSearchException {
        if (variantDBIterator != null) {

            // init collection if needed
            init(collection);

            int count = 0;
            List<Variant> variantList = new ArrayList<>(DEFAULT_INSERT_SIZE);
            while (variantDBIterator.hasNext()) {
                variantList.add(variantDBIterator.next());
                count++;
                if (count % DEFAULT_INSERT_SIZE == 0) {
                    insert(variantList);
                    variantList.clear();
                }
            }
            // insert the remaining variants
            if (variantList.size() > 0) {
                insert(variantList);
            }
            logger.info("Loading done: {} variants.", count);
        }
    }

    /**
     * Return the list of Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param collection    Collection name
     * @param query         Query
     * @param queryOptions  Query options
     * @return              List of Variant objects
     * @throws IOException          IOException
     * @throws VariantSearchException  VariantSearchException
     */
    public VariantQueryResult<Variant> query(String collection, Query query, QueryOptions queryOptions)
            throws IOException, VariantSearchException {
        // we don't initialize here the collection, the iterator does
        StopWatch stopWatch = StopWatch.createStarted();
        List<Variant> results = new ArrayList<>();
        SolrVariantIterator iterator = iterator(collection, query, queryOptions);
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return new VariantQueryResult<>("", (int) stopWatch.getTime(TimeUnit.MILLISECONDS),
                results.size(), iterator.getNumFound(), "Data from Solr", "", results, null);
    }

    /**
     * Return the list of VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param collection    Collection name
     * @param query         Query
     * @param queryOptions  Query options
     * @return              List of VariantSearchModel objects
     * @throws IOException          IOException
     * @throws VariantSearchException  VariantSearchException
     */
    public List<VariantSearchModel> nativeQuery(String collection, Query query, QueryOptions queryOptions) throws IOException,
            VariantSearchException {
        // we don't initialize here the collection, the iterator does
        List<VariantSearchModel> results = new ArrayList<>();
        SolrVariantSearchIterator iterator = nativeIterator(collection, query, queryOptions);
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }

    /**
     * Return a Solr variant iterator to retrieve Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param collection    Collection name
     * @param query         Query
     * @param queryOptions  Query options
     * @return              Solr Variant iterator
     * @throws IOException          IOException
     * @throws VariantSearchException  VariantSearchException
     */
    public SolrVariantIterator iterator(String collection, Query query, QueryOptions queryOptions) throws VariantSearchException,
            IOException {
        // init collection if needed
        init(collection);

        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            QueryResponse response = solrClient.query(solrQuery);
            SolrVariantIterator iterator = new SolrVariantIterator((response.getBeans(VariantSearchModel.class).iterator()));
            iterator.setNumFound(response.getResults().getNumFound());
            return iterator;
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }

    }

    /**
     * Return a Solr variant iterator to retrieve VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param collection    Collection name
     * @param query         Query
     * @param queryOptions  Query options
     * @return              Solr VariantSearch iterator
     * @throws IOException          IOException
     * @throws VariantSearchException  VariantSearchException
     */
    public SolrVariantSearchIterator nativeIterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        // init collection if needed
        init(collection);

        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            QueryResponse response = solrClient.query(solrQuery);
            return new SolrVariantSearchIterator(response.getBeans(VariantSearchModel.class).iterator());
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    /**
     * Return faceted data from a Solr core/collection
     * according a given query.
     *
     * @param collection    Collection name
     * @param query         Query
     * @param queryOptions  Query options (contains the facet and facetRange options)
     * @return              List of Variant objects
     * @throws IOException          IOException
     * @throws VariantSearchException  VariantSearchException
     */
    public FacetedQueryResult facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, VariantSearchException {
        // init collection if needed
        init(collection);

        StopWatch stopWatch = StopWatch.createStarted();
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            QueryResponse response = solrClient.query(solrQuery);
            System.out.println(response);
            FacetedQueryResultItem item = toFacetedQueryResultItem(queryOptions, response);
            return new FacetedQueryResult("", (int) stopWatch.getTime(TimeUnit.MILLISECONDS),
                    1, 1, "Faceted data from Solr", "", item);
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    @Deprecated
    public VariantSearchFacet getFacet(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
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
     * Insert a variant into Solr.
     *
     * @param variant                   Variant to insert
     * @throws IOException              IOException
     * @throws VariantSearchException   VariantSearchException
     */
    private void insert(Variant variant) throws IOException, VariantSearchException {
        VariantSearchModel variantSearchModel = variantSearchToVariantConverter.convertToStorageType(variant);

        if (variantSearchModel != null && variantSearchModel.getId() != null) {
            UpdateResponse updateResponse = null;
            try {
                updateResponse = solrClient.addBean(variantSearchModel);
                if (0 == updateResponse.getStatus()) {
                    solrClient.commit();
                }
            } catch (SolrServerException e) {
                throw new VariantSearchException(e.getMessage(), e);
            }
        }
    }

    /**
     * Insert a list of variants into Solr.
     *
     * @param variants  List of variants to insert
     * @throws IOException          IOException
     * @throws VariantSearchException  VariantSearchException
     */
    private void insert(List<Variant> variants) throws IOException, VariantSearchException {
        if (variants != null && variants.size() > 0) {
            List<VariantSearchModel> variantSearchModels = variantSearchToVariantConverter.convertListToStorageType(variants);

            if (!variantSearchModels.isEmpty()) {
                UpdateResponse updateResponse = null;
                try {
                    updateResponse = solrClient.addBeans(variantSearchModels);
                    if (0 == updateResponse.getStatus()) {
                        solrClient.commit();
                    }
                } catch (SolrServerException e) {
                    throw new VariantSearchException(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Load a JSON file into the Solr core/collection.
     *
     * @param path          Path to the JSON file
     * @throws IOException
     * @throws VariantSearchException
     */
    private void loadJson(Path path) throws IOException, VariantSearchException {
        // This opens json and json.gz files automatically
        BufferedReader bufferedReader = FileUtils.newBufferedReader(path);

        // TODO: get the buffer size from configuration file
        List<Variant> variants = new ArrayList<>(DEFAULT_INSERT_SIZE);

        int count = 0;
        String line;
        ObjectReader objectReader = new ObjectMapper().readerFor(Variant.class);
        while ((line = bufferedReader.readLine()) != null) {
            Variant variant = objectReader.readValue(line);
            variants.add(variant);
            count++;
            if (count % DEFAULT_INSERT_SIZE == 0) {
                insert(variants);
                variants.clear();
            }
        }

        // Insert the remaining variants
        if (variants.size() > 0) {
            insert(variants);
        }

        // close
        bufferedReader.close();
    }

    private void loadAvro(Path path) throws IOException, VariantSearchException, StorageEngineException {
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


    private FacetedQueryResultItem.Field processSolrPivot(String name, int index, Map<String, Set<String>> includes,
                                                          PivotField pivot, String indent) {
        FacetedQueryResultItem.Field field = null;
        if (pivot.getPivot() != null && pivot.getPivot().size() > 0) {
            field = new FacetedQueryResultItem().new Field();
            field.setName(name.split(",")[index]);

            long total = 0;
            List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
            for (PivotField solrPivot : pivot.getPivot()) {
                FacetedQueryResultItem.Field nestedField = processSolrPivot(name, index + 1, includes, solrPivot, "\t");

                // check for includes
                if (toInclude(includes, field.getName(), solrPivot.getValue().toString())) {
//                if (includes.size() == 0 || (!includes.containsKey(field.getName()))
//                        || (includes.containsKey(field.getName())
//                        && includes.get(field.getName()).contains(solrPivot.getValue().toString()))) {
                    FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                            .new Count(solrPivot.getValue().toString(), solrPivot.getCount(), nestedField);
                    counts.add(count);
                }
                total += solrPivot.getCount();
            }
            field.setTotal(total);
            field.setCounts(counts);
        }

        return field;
    }

    Map<String, Set<String>> getIncludeMap(QueryOptions queryOptions) {
        Map<String, Set<String>> includeMap = new HashMap<>();

        if (queryOptions.containsKey(QueryOptions.FACET)) {
            String strFields = queryOptions.getString(QueryOptions.FACET);
            if (StringUtils.isNotEmpty(strFields)) {
                String[] fieldsBySc = strFields.split("[;]");
                for (String fieldSc : fieldsBySc) {
                    String[] fieldsByGt = fieldSc.split(">>");
                    for (String fieldGt : fieldsByGt) {
                        String[] splits1 = fieldGt.split("[\\[\\]]");
                        // first, name
                        String name = splits1[0];

                        // second, includes
                        if (splits1.length >= 2 && StringUtils.isNotEmpty(splits1[1])) {
                            // we have to split by "," to get the includes
                            String[] includes = splits1[1].split(",");
                            for (String include : includes) {
                                if (!includeMap.containsKey(name)) {
                                    includeMap.put(name, new HashSet<>());
                                }
                                includeMap.get(name).add(include);
                            }
                        }
                    }
                }
            }
        }
        return includeMap;
    }

    private boolean toInclude(Map<String, Set<String>> includes, String name, String value) {
        boolean ret = false;
        if (includes.size() == 0
                || !includes.containsKey(name)
                || (includes.containsKey(name) && includes.get(name).contains(value))) {
            ret = true;
        }
        return ret;
    }

    private FacetedQueryResultItem toFacetedQueryResultItem(QueryOptions queryOptions, QueryResponse response) {
        Map<String, Set<String>> includes = getIncludeMap(queryOptions);

        // process Solr facet fields
        List<FacetedQueryResultItem.Field> fields = new ArrayList<>();
        if (response.getFacetFields() != null) {
            for (FacetField solrField: response.getFacetFields()) {
                FacetedQueryResultItem.Field field = new FacetedQueryResultItem().new Field();
                field.setName(solrField.getName());

                long total = 0;
                List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
                for (FacetField.Count solrCount: solrField.getValues()) {
                    // check for includes
                    if (toInclude(includes, field.getName(), solrCount.getName())) {
//                    if (includes.size() == 0 || (!includes.containsKey(field.getName()))
//                            || (includes.containsKey(solrField.getName())
//                            && includes.get(solrField.getName()).contains(solrCount.getName()))) {
                        FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                                .new Count(solrCount.getName(), solrCount.getCount(), null);
                        counts.add(count);
                    }
                    total += solrCount.getCount();
                }
                // initialize field
                field.setTotal(total);
                field.setCounts(counts);

                fields.add(field);
            }
        }

        // process Solr facet pivots
        if (response.getFacetPivot() != null) {
            NamedList<List<PivotField>> facetPivot = response.getFacetPivot();
            for (int i = 0; i < facetPivot.size(); i++) {
                List<PivotField> solrPivots = facetPivot.getVal(i);
                if (solrPivots != null && solrPivots.size() > 0) {
                    // init field
                    FacetedQueryResultItem.Field field = new FacetedQueryResultItem().new Field();
                    field.setName(facetPivot.getName(i).split(",")[0]);

                    long total = 0;
                    List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
                    for (PivotField solrPivot : solrPivots) {
                        FacetedQueryResultItem.Field nestedField = processSolrPivot(facetPivot.getName(i), 1, includes, solrPivot, "\t");

                        // check for includes
                        if (toInclude(includes, field.getName(), solrPivot.getValue().toString())) {
//                        if (includes.size() == 0 || (!includes.containsKey(field.getName()))
//                                || (includes.containsKey(field.getName())
//                                && includes.get(field.getName()).contains(solrPivot.getValue().toString()))) {
                            FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                                    .new Count(solrPivot.getValue().toString(), solrPivot.getCount(), nestedField);
                            counts.add(count);
                        }
                        total += solrPivot.getCount();
                    }
                    // update field
                    field.setTotal(total);
                    field.setCounts(counts);

                    fields.add(field);
                }
            }
        }

        // process Solr facet range
        List<FacetedQueryResultItem.Range> ranges = new ArrayList<>();
        if (response.getFacetRanges() != null) {
            for (RangeFacet solrRange: response.getFacetRanges()) {
                List<Long> counts = new ArrayList<>();
                long total = 0;
                for (Object objCount: solrRange.getCounts()) {
                    long count = ((RangeFacet.Count) objCount).getCount();
                    total += count;
                    counts.add(count);
                }
                ranges.add(new FacetedQueryResultItem().new Range(solrRange.getName(),
                        (Number) solrRange.getStart(), (Number) solrRange.getEnd(),
                        (Number) solrRange.getGap(), total, counts));
            }
        }

        // process Solr facet range
        List<FacetedQueryResultItem.Intersection> intersections = new ArrayList<>();
        Map<String, List<List<String>>> intersectionMap = getInputIntersections(queryOptions);
        if (intersectionMap.size() > 0) {
            if (response.getFacetQuery() != null && response.getFacetQuery().size() > 0) {
                for (String key: intersectionMap.keySet()) {
                    List<List<String>> intersectionLists = intersectionMap.get(key);
                    for (List<String> list: intersectionLists) {
                        FacetedQueryResultItem.Intersection intersection = new FacetedQueryResultItem().new Intersection();
                        intersection.setName(key);
                        intersection.setSize(list.size());
                        if (list.size() == 2) {
                            Map<String, Long> counts = new LinkedHashMap<>();
                            String name = list.get(0);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            intersection.setCounts(counts);

                            // add to the list
                            intersections.add(intersection);
                        } else if (list.size() == 3) {
                            Map<String, Long> map = new LinkedHashMap<>();
                            Map<String, Long> counts = new LinkedHashMap<>();
                            String name = list.get(0);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(1);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(1) + "__" + list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            name = list.get(0) + "__" + list.get(1) + "__" + list.get(2);
                            counts.put(name, (long) response.getFacetQuery().get(name));
                            intersection.setCounts(counts);

                            // add to the list
                            intersections.add(intersection);
                        } else {
                            logger.warn("Facet intersection '" + intersection + "' malformed. The expected intersection format"
                                    + " is 'name:value1:value2[:value3]', value3 is optional");
                        }
                    }
                }
            } else {
                logger.warn("Something wrong happened (intersection input and output mismatch).");
            }
        }

        return new FacetedQueryResultItem(fields, ranges, intersections);
    }

    Map<String, List<List<String>>> getInputIntersections(QueryOptions queryOptions) {
        Map<String, List<List<String>>> inputIntersections = new HashMap<>();
        if (queryOptions.containsKey(QueryOptions.FACET_INTERSECTION)
                && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET_INTERSECTION))) {
            String[] intersections = queryOptions.getString(QueryOptions.FACET_INTERSECTION).split("[;]");

            for (String intersection : intersections) {
                String[] splits = intersection.split(":");
                if (splits.length != 3 && splits.length != 4) {
                    logger.warn("Facet intersection '" + intersection + "' malformed. The expected intersection format"
                            + " is 'name:value1:value2[:value3]', value3 is optional");
                } else {
                    if (!inputIntersections.containsKey(splits[0])) {
                        inputIntersections.put(splits[0], new LinkedList<>());
                    }
                    List<String> values = new LinkedList<>();
                    for (int i = 1; i < splits.length; i++) {
                        values.add(splits[i]);
                    }
                    inputIntersections.get(splits[0]).add(values);
                }
            }
        }
        return inputIntersections;
    }

    /**
     *
     * @param response
     * @return
     */
    @Deprecated
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
}
