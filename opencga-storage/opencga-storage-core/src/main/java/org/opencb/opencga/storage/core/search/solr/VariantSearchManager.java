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

package org.opencb.opencga.storage.core.search.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
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
import org.opencb.opencga.storage.core.search.VariantSearchModel;
import org.opencb.opencga.storage.core.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
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

    public static final String CONF_SET = "OpenCGAConfSet";

    private SolrClient solrClient;
    private StorageConfiguration storageConfiguration;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private SolrQueryParser solrQueryParser;

    private Logger logger;

    public static final String SKIP_SEARCH = "skipSearch";
    public static final String QUERY_INTERSECT = "queryIntersect";
    private static final int DEFAULT_INSERT_SIZE = 10000;

    @Deprecated
    public VariantSearchManager(String host, String collection) {
        this.solrClient = new HttpSolrClient.Builder(host + collection).build();
        variantSearchToVariantConverter = new VariantSearchToVariantConverter();
    }

    public VariantSearchManager(StudyConfigurationManager studyConfigurationManager, CellBaseUtils cellbaseUtils,
                                StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;

        logger = LoggerFactory.getLogger(VariantSearchManager.class);

        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        this.solrQueryParser = new SolrQueryParser(studyConfigurationManager, cellbaseUtils);

        init();
    }

    private void init() {
        this.solrClient = new HttpSolrClient.Builder(storageConfiguration.getSearch().getHost()).build();

        // The default implementation is HttpSolrClient and we can set up some parameters
        ((HttpSolrClient)this.solrClient).setRequestWriter(new BinaryRequestWriter());
        ((HttpSolrClient)this.solrClient).setSoTimeout(storageConfiguration.getSearch().getTimeout());
    }

    public boolean isAlive(String collection) {
        try {
            SolrPing solrPing = new SolrPing();
            SolrPingResponse response = solrPing.process(solrClient, collection);
            return response.getResponse().get("status").equals("OK");
        } catch (SolrServerException | IOException | SolrException e) {
            return false;
        }
    }

    public void create(String coreName) throws VariantSearchException {
        create(coreName, "OpenCGAConfSet");
    }

    public void create(String dbName, String configSet) throws VariantSearchException {
        String mode = storageConfiguration.getSearch().getMode();
        if (StringUtils.isEmpty(mode)) {
            logger.warn("Solr 'mode' is empty, setting default 'cloud'");
            mode = "cloud";
        }

        if (StringUtils.isEmpty(dbName)) {
            throw new VariantSearchException("We cannot create a Solr for the empty database '" + dbName + "'");
        }

        if (StringUtils.isEmpty(configSet)) {
            logger.warn("Solr 'configSet' is empty, setting default 'OpenCGAConfSet'");
            configSet = "OpenCGAConfSet";
        }

        switch (mode.toLowerCase()) {
            case "core":
            case "standalone": {
                if (existsCore(dbName)) {
                    logger.warn("Solr standalone core {} already exists", dbName);
                } else {
                    createCore(dbName, configSet);
                }
                break;
            }
            case "collection":
            case "cloud": {
                if (existsCollection(dbName)) {
                    logger.warn("Solr cloud collection {} already exists", dbName);
                } else {
                    createCollection(dbName, configSet);
                }
                break;
            }
            default: {
                throw new IllegalArgumentException("Invalid Solr mode '" + mode + "'. Valid values are 'standalone' or 'cloud'");
            }
        }
    }

    /**
     * Create a Solr core from a configuration set directory. By default, the configuration set directory is located
     * inside the folder server/solr/configsets.
     *
     * @param coreName      Core name
     * @param configSet     Configuration set name
     * @throws VariantSearchException    Exception
     */
    public void createCore(String coreName, String configSet) throws VariantSearchException {
        try {
            logger.debug("Creating core={}, core={}, configSet={}", storageConfiguration.getSearch().getHost(), coreName, configSet);
            CoreAdminRequest.Create request = new CoreAdminRequest.Create();
            request.setCoreName(coreName);
            request.setConfigSet(configSet);
            request.process(solrClient);
        } catch (Exception e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    /**
     * Create a Solr collection from a configuration directory. The configuration has to be uploaded to the zookeeper,
     * $ ./bin/solr zk upconfig -n <config name> -d <path to the config dir> -z <host:port zookeeper>.
     * For Solr, collection name, configuration name and number of shards are mandatory in order to create a collection.
     * Number of replicas is optional.
     *
     * @param collectionName             Collection name
     * @param configSet                  Configuration name
     * @throws VariantSearchException    Exception
     */
    public void createCollection(String collectionName, String configSet) throws VariantSearchException {
        logger.debug("Creating collection: {}, collection={}, config={}, numShards={}, numReplicas={}",
                storageConfiguration.getSearch().getHost(), collectionName, configSet, 1, 1);
        try {
            CollectionAdminRequest request = CollectionAdminRequest.createCollection(collectionName, configSet, 1, 1);
            request.process(solrClient);
        } catch (Exception e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    public boolean exists(String dbName) throws VariantSearchException {
        String mode = storageConfiguration.getSearch().getMode();
        if (StringUtils.isNotEmpty(mode)) {
            logger.warn("Solr 'mode' is empty, setting default 'cloud'");
            mode = "cloud";
        }

        if (StringUtils.isEmpty(dbName)) {
            throw new VariantSearchException("We cannot check if Solr database exists '" + dbName + "'");
        }

        switch (mode.toLowerCase()) {
            case "core":
            case "standalone": {
                return existsCore(dbName);
            }
            case "collection":
            case "cloud": {
                return existsCollection(dbName);
            }
            default: {
                throw new IllegalArgumentException("Invalid Solr mode '" + mode + "'. Valid values are 'standalone' or 'cloud'");
            }
        }
    }

    /**
     * Check if a given core exists.
     *
     * @param coreName          Core name
     * @return                  True or false
     */
    public boolean existsCore(String coreName) {
        try {
            CoreStatus status = CoreAdminRequest.getCoreStatus(coreName, solrClient);
            status.getInstanceDirectory();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Check if a given collection exists.
     *
     * @param collectionName            Collection name
     * @return                          True or false
     * @throws VariantSearchException   VariantSearchException
     */
    public boolean existsCollection(String collectionName) throws VariantSearchException {
        try {
            List<String> collections = CollectionAdminRequest.listCollections(solrClient);
            for (String collection : collections) {
                if (collection.equals(collectionName)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
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

        File file = path.toFile();
        if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
            loadJson(collection, path);
        } else if (file.getName().endsWith("avro") || file.getName().endsWith("avro.gz")) {
            loadAvro(collection, path);
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
            int count = 0;
            List<Variant> variantList = new ArrayList<>(DEFAULT_INSERT_SIZE);
            while (variantDBIterator.hasNext()) {
                variantList.add(variantDBIterator.next());
                count++;
                if (count % DEFAULT_INSERT_SIZE == 0) {
                    insert(collection, variantList);
                    variantList.clear();
                }
            }

            // insert the remaining variants
            if (variantList.size() > 0) {
                insert(collection, variantList);
            }

            logger.debug("Variant search loading done: {} variants.", count);
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
        StopWatch stopWatch = StopWatch.createStarted();
        List<Variant> results;
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        try {
            QueryResponse solrResponse = solrClient.query(collection, solrQuery);
            List<VariantSearchModel> solrResponseBeans = solrResponse.getBeans(VariantSearchModel.class);
            int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

            results = new ArrayList<>(solrResponseBeans.size());
            for (VariantSearchModel variantSearchModel: solrResponseBeans) {
                results.add(variantSearchToVariantConverter.convertToDataModelType(variantSearchModel));
            }
            return new VariantQueryResult<>("", dbTime,
                    results.size(), solrResponse.getResults().getNumFound(), "Data from Solr", "", results, null);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error fetching from Solr", e);
        }
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
    public VariantQueryResult<VariantSearchModel> nativeQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, VariantSearchException {
        StopWatch stopWatch = StopWatch.createStarted();
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        try {
            QueryResponse solrResponse = solrClient.query(collection, solrQuery);
            List<VariantSearchModel> solrResponseBeans = solrResponse.getBeans(VariantSearchModel.class);
            int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

            return new VariantQueryResult<>("", dbTime,
                    solrResponseBeans.size(), solrResponse.getResults().getNumFound(), "Data from Solr", "", solrResponseBeans, null);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error fetching from Solr", e);
        }
    }

    public VariantIterator iterator(String collection, Query query, QueryOptions queryOptions) throws VariantSearchException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantIterator(solrClient, collection, solrQuery);
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
    public VariantSearchIterator nativeIterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantSearchIterator(solrClient, collection, solrQuery);
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
        StopWatch stopWatch = StopWatch.createStarted();
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            QueryResponse response = solrClient.query(collection, solrQuery);
            FacetedQueryResultItem item = toFacetedQueryResultItem(queryOptions, response);
            return new FacetedQueryResult("", (int) stopWatch.getTime(TimeUnit.MILLISECONDS),
                    1, 1, "Faceted data from Solr", "", item);
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
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
    private void insert(String collection, Variant variant) throws IOException, VariantSearchException {
        VariantSearchModel variantSearchModel = variantSearchToVariantConverter.convertToStorageType(variant);

        if (variantSearchModel != null && variantSearchModel.getId() != null) {
            UpdateResponse updateResponse;
            try {
                updateResponse = solrClient.addBean(collection, variantSearchModel);
                if (updateResponse.getStatus() == 0) {
                    solrClient.commit(collection);
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
    private void insert(String collection, List<Variant> variants) throws IOException, VariantSearchException {
        if (variants != null && variants.size() > 0) {
            List<VariantSearchModel> variantSearchModels = variantSearchToVariantConverter.convertListToStorageType(variants);

            if (!variantSearchModels.isEmpty()) {
                UpdateResponse updateResponse;
                try {
                    updateResponse = solrClient.addBeans(collection, variantSearchModels);
                    if (updateResponse.getStatus() == 0) {
                        solrClient.commit(collection);
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
    private void loadJson(String collection, Path path) throws IOException, VariantSearchException {
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
                insert(collection, variants);
                variants.clear();
            }
        }

        // Insert the remaining variants
        if (variants.size() > 0) {
            insert(collection, variants);
        }

        // close
        bufferedReader.close();
    }

    private void loadAvro(String collection, Path path) throws IOException, VariantSearchException, StorageEngineException {
        // reader
        VariantSource source = null;
        VariantReader reader = VariantReaderUtils.getVariantReader(path, source);

        List<Variant> variants;

        // TODO: get the buffer size from configuration file
        int bufferSize = 10000;

        do {
            variants = reader.read(bufferSize);
            insert(collection, variants);
        } while (variants.size() == bufferSize);

        reader.close();
    }


    private FacetedQueryResultItem.Field processSolrPivot(String name, int index, Map<String, Set<String>> includes,
                                                          PivotField pivot) {
        String countName;
        FacetedQueryResultItem.Field field = null;
        if (pivot.getPivot() != null && pivot.getPivot().size() > 0) {
            field = new FacetedQueryResultItem().new Field();
            field.setName(name.split(",")[index]);

            long total = 0;
            List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
            for (PivotField solrPivot : pivot.getPivot()) {
                FacetedQueryResultItem.Field nestedField = processSolrPivot(name, index + 1, includes, solrPivot);

                countName = solrPivot.getValue().toString();
                // Discard Ensembl genes and transcripts
                if (!field.getName().equals("genes")
                        || (!countName.startsWith("ENSG0") && !countName.startsWith("ENST0"))) {
                    // and then check if this has to be include
                    if (toInclude(includes, field.getName(), solrPivot.getValue().toString())) {
                        FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                                .new Count(updateValueIfSoAcc(field.getName(), countName),
                                solrPivot.getCount(), nestedField);
                        counts.add(count);
                    }
                    total += solrPivot.getCount();
                }
            }
            field.setTotal(total);
            field.setCounts(counts);
        }

        return field;
    }

    private Map<String, Set<String>> getIncludeMap(QueryOptions queryOptions) {
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

    private String updateValueIfSoAcc(String fieldName, String fieldValue) {
        String value = fieldValue;
        if (fieldName.equals("soAcc")) {
            int so = Integer.parseInt(fieldValue);
            value = ConsequenceTypeMappings.accessionToTerm.get(so) + String.format(" (SO:%07d)", so);
        }
        return value;
    }

    private FacetedQueryResultItem toFacetedQueryResultItem(QueryOptions queryOptions, QueryResponse response) {
        Map<String, Set<String>> includes = getIncludeMap(queryOptions);

        String countName;

        // process Solr facet fields
        List<FacetedQueryResultItem.Field> fields = new ArrayList<>();
        if (response.getFacetFields() != null) {
            for (FacetField solrField: response.getFacetFields()) {
                FacetedQueryResultItem.Field field = new FacetedQueryResultItem().new Field();
                field.setName(solrField.getName());

                long total = 0;
                List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
                for (FacetField.Count solrCount: solrField.getValues()) {
                    countName = solrCount.getName();
                    // discard Ensembl genes and trascripts
                    if (!field.getName().equals("genes")
                            || (!countName.startsWith("ENSG0") && !countName.startsWith("ENST0"))) {
                        // and then check if this has to be include
                        if (toInclude(includes, field.getName(), solrCount.getName())) {
                            FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                                    .new Count(updateValueIfSoAcc(field.getName(), countName), solrCount.getCount(), null);
                            counts.add(count);
                        }
                        total += solrCount.getCount();
                    }
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
                        FacetedQueryResultItem.Field nestedField = processSolrPivot(facetPivot.getName(i), 1, includes, solrPivot);

                        countName = solrPivot.getValue().toString();
                        // discard Ensembl genes and trascripts
                        if (!field.getName().equals("genes")
                                || (!countName.startsWith("ENSG0") && !countName.startsWith("ENST0"))) {
                            // and then check if this has to be include
                            if (toInclude(includes, field.getName(), solrPivot.getValue().toString())) {
                                FacetedQueryResultItem.Count count = new FacetedQueryResultItem()
                                        .new Count(updateValueIfSoAcc(field.getName(), solrPivot.getValue().toString()),
                                        solrPivot.getCount(), nestedField);
                                counts.add(count);
                            }
                            total += solrPivot.getCount();
                        }
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

        // process Solr facet intersections
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
        if (queryOptions.containsKey(QueryOptions.FACET)
                && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String[] intersections = queryOptions.getString(QueryOptions.FACET).split("[;]");

            for (String intersection : intersections) {
                String[] splitA = intersection.split(":");
                if (splitA.length == 2) {
                    String[] splitB = splitA[1].split("\\^");
                    if (splitB.length == 2 || splitB.length == 3) {
                        if (!inputIntersections.containsKey(splitA[0])) {
                            inputIntersections.put(splitA[0], new LinkedList<>());
                        }
                        List<String> values = new LinkedList<>();
                        for (int i = 0; i < splitB.length; i++) {
                            values.add(splitB[i]);
                        }
                        inputIntersections.get(splitA[0]).add(values);
                    }
                }
            }
        }
        return inputIntersections;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantSearchManager{");
        sb.append("solrClient=").append(solrClient);
        sb.append(", storageConfiguration=").append(storageConfiguration);
        sb.append(", variantSearchToVariantConverter=").append(variantSearchToVariantConverter);
        sb.append(", solrQueryParser=").append(solrQueryParser);
        sb.append('}');
        return sb.toString();
    }

    public SolrClient getSolrClient() {
        return solrClient;
    }

    public VariantSearchManager setSolrClient(SolrClient solrClient) {
        this.solrClient = solrClient;
        return this;
    }
}
