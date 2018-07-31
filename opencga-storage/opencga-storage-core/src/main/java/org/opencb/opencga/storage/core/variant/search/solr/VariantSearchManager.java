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

package org.opencb.opencga.storage.core.variant.search.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.util.NamedList;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResultItem;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by imedina on 09/11/16.
 * Created by wasim on 09/11/16.
 */
public class VariantSearchManager {

    private SolrManager solrManager;

    private SolrQueryParser solrQueryParser;
    private StorageConfiguration storageConfiguration;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private int insertBatchSize;

    private Logger logger;

    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;
    public static final String CONF_SET = "OpenCGAConfSet";
    public static final String SEARCH_ENGINE_ID = "solr";
    public static final String USE_SEARCH_INDEX = "useSearchIndex";

    public enum UseSearchIndex {
        YES, NO, AUTO;
        public static UseSearchIndex from(Map<String, Object> options) {
            return options == null || !options.containsKey(USE_SEARCH_INDEX)
                    ? AUTO
                    : UseSearchIndex.valueOf(options.get(USE_SEARCH_INDEX).toString().toUpperCase());
        }
    }


    @Deprecated
    public VariantSearchManager(String host, String collection) {
        throw new UnsupportedOperationException("Not supported!!");
    }

    public VariantSearchManager(StudyConfigurationManager studyConfigurationManager, StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;

        this.solrQueryParser = new SolrQueryParser(studyConfigurationManager);
        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();

        this.solrManager = new SolrManager(storageConfiguration.getSearch().getHost(), storageConfiguration.getSearch().getMode(),
                storageConfiguration.getSearch().getTimeout());

        // Set internal insert batch size from configuration and default value
        insertBatchSize = storageConfiguration.getSearch().getInsertBatchSize() > 0
                ? storageConfiguration.getSearch().getInsertBatchSize()
                : DEFAULT_INSERT_BATCH_SIZE;

        logger = LoggerFactory.getLogger(VariantSearchManager.class);
    }

    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
    }

    public void create(String coreName) throws VariantSearchException {
        solrManager.create(coreName, "OpenCGAConfSet");
    }

    public void create(String dbName, String configSet) throws VariantSearchException {
        solrManager.create(dbName, configSet);
    }
    public void createCore(String coreName, String configSet) throws VariantSearchException {
        solrManager.createCore(coreName, configSet);
    }

    public void createCollection(String collectionName, String configSet) throws VariantSearchException {
        solrManager.createCollection(collectionName, configSet);
    }

    public boolean exists(String dbName) throws VariantSearchException {
        return solrManager.exists(dbName);
    }

    public boolean existsCore(String coreName) {
        return solrManager.existsCore(coreName);
    }

    public boolean existsCollection(String collectionName) throws VariantSearchException {
        return solrManager.existsCollection(collectionName);
    }

    /**
     * Load a Solr core/collection from a Avro or JSON file.
     *
     * @param collection Collection name
     * @param path       Path to the file to load
     * @throws IOException            IOException
     * @throws VariantSearchException SolrServerException
     * @throws StorageEngineException SolrServerException
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
     * @param collection        Collection name
     * @param variantDBIterator Iterator to retrieve the variants to load
     * @param progressLogger    Progress logger
     * @throws IOException            IOException
     * @throws VariantSearchException VariantSearchException
     */
    public void load(String collection, VariantDBIterator variantDBIterator, ProgressLogger progressLogger)
            throws IOException, VariantSearchException {
        if (variantDBIterator == null) {
            throw new VariantSearchException("VariantDBIterator parameter is null");
        }

        int count = 0;
        List<Variant> variantList = new ArrayList<>(insertBatchSize);
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            progressLogger.increment(1, () -> "up to position " + variant.toString());
            variantList.add(variant);
            count++;
            if (count % insertBatchSize == 0) {
                insert(collection, variantList);
                variantList.clear();
            }
        }

        // Insert the remaining variants
        if (CollectionUtils.isNotEmpty(variantList)) {
            insert(collection, variantList);
        }

        logger.debug("Variant Search loading done: {} variants indexed", count);
    }

    /**
     * Return the list of Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of Variant objects
     * @throws IOException            IOException
     * @throws VariantSearchException VariantSearchException
     */
    public VariantQueryResult<Variant> query(String collection, Query query, QueryOptions queryOptions)
            throws IOException, VariantSearchException {
        StopWatch stopWatch = StopWatch.createStarted();
        List<Variant> results;
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        try {
            QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
            List<VariantSearchModel> solrResponseBeans = solrResponse.getBeans(VariantSearchModel.class);
            int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

            results = new ArrayList<>(solrResponseBeans.size());
            for (VariantSearchModel variantSearchModel : solrResponseBeans) {
                results.add(variantSearchToVariantConverter.convertToDataModelType(variantSearchModel));
            }
            return new VariantQueryResult<>("", dbTime,
                    results.size(), solrResponse.getResults().getNumFound(), "", "", results, null, SEARCH_ENGINE_ID);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error fetching from Solr", e);
        }
    }

    /**
     * Return the list of VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of VariantSearchModel objects
     * @throws IOException            IOException
     * @throws VariantSearchException VariantSearchException
     */
    public VariantQueryResult<VariantSearchModel> nativeQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, VariantSearchException {
        StopWatch stopWatch = StopWatch.createStarted();
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        try {
            QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
            List<VariantSearchModel> solrResponseBeans = solrResponse.getBeans(VariantSearchModel.class);
            int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

            return new VariantQueryResult<>("", dbTime,
                    solrResponseBeans.size(), solrResponse.getResults().getNumFound(), "", "", solrResponseBeans, null, SEARCH_ENGINE_ID);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error fetching from Solr", e);
        }
    }

    public VariantSolrIterator iterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    /**
     * Return a Solr variant iterator to retrieve VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return Solr VariantSearch iterator
     * @throws IOException            IOException
     * @throws VariantSearchException VariantSearchException
     */
    public VariantSearchSolrIterator nativeIterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantSearchSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    /**
     * Return faceted data from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options (contains the facet and facetRange options)
     * @return List of Variant objects
     * @throws IOException            IOException
     * @throws VariantSearchException VariantSearchException
     */
    public FacetedQueryResult facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, VariantSearchException {
        StopWatch stopWatch = StopWatch.createStarted();
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            QueryResponse response = solrManager.getSolrClient().query(collection, solrQuery);
            FacetedQueryResultItem item = toFacetedQueryResultItem(queryOptions, response);
            return new FacetedQueryResult("", (int) stopWatch.getTime(), 1, 1, "Faceted data from Solr", "", item);
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }


    /**-------------------------------------
     *  P R I V A T E    M E T H O D S
     -------------------------------------*/
    /**
     * Insert a list of variants into Solr.
     *
     * @param variants List of variants to insert
     * @throws IOException            IOException
     * @throws VariantSearchException VariantSearchException
     */
    private void insert(String collection, List<Variant> variants) throws IOException, VariantSearchException {
        if (variants != null && CollectionUtils.isNotEmpty(variants)) {
            List<VariantSearchModel> variantSearchModels = variantSearchToVariantConverter.convertListToStorageType(variants);

            if (!variantSearchModels.isEmpty()) {
                UpdateResponse updateResponse;
                try {
                    updateResponse = solrManager.getSolrClient().addBeans(collection, variantSearchModels);
                    if (updateResponse.getStatus() == 0) {
                        solrManager.getSolrClient().commit(collection);
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
     * @param path Path to the JSON file
     * @throws IOException
     * @throws VariantSearchException
     */
    private void loadJson(String collection, Path path) throws IOException, VariantSearchException {
        // This opens json and json.gz files automatically
        try (BufferedReader bufferedReader = FileUtils.newBufferedReader(path)) {
            // TODO: get the buffer size from configuration file
            List<Variant> variants = new ArrayList<>(insertBatchSize);
            int count = 0;
            String line;
            ObjectReader objectReader = new ObjectMapper().readerFor(Variant.class);
            while ((line = bufferedReader.readLine()) != null) {
                Variant variant = objectReader.readValue(line);
                variants.add(variant);
                count++;
                if (count % insertBatchSize == 0) {
                    logger.debug("Loading variants from '{}', {} variants loaded", path.toString(), count);
                    insert(collection, variants);
                    variants.clear();
                }
            }

            // Insert the remaining variants
            if (CollectionUtils.isNotEmpty(variants)) {
                logger.debug("Loading remaining variants from '{}', {} variants loaded", path.toString(), count);
                insert(collection, variants);
            }
        }
    }

    private void loadAvro(String collection, Path path) throws IOException, VariantSearchException, StorageEngineException {
        // reader
        VariantReader reader = VariantReaderUtils.getVariantReader(path, null);

        // TODO: get the buffer size from configuration file
        int bufferSize = 10000;

        List<Variant> variants;
        do {
            variants = reader.read(bufferSize);
            insert(collection, variants);
        } while (CollectionUtils.isNotEmpty(variants));

        reader.close();
    }


    private FacetedQueryResultItem.Field processSolrPivot(String name, int index, Map<String, Set<String>> includes, PivotField pivot) {
        String countName;
        FacetedQueryResultItem.Field field = null;
        if (pivot.getPivot() != null && CollectionUtils.isNotEmpty(pivot.getPivot())) {
            field = new FacetedQueryResultItem().new Field();
            field.setName(name.split(",")[index]);

            long total = 0;
            List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
            for (PivotField solrPivot : pivot.getPivot()) {
                FacetedQueryResultItem.Field nestedField = processSolrPivot(name, index + 1, includes, solrPivot);

                countName = solrPivot.getValue().toString();
                // Discard Ensembl genes and transcripts
                if (!("genes").equals(field.getName())
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
        if (("soAcc").equals(fieldName)) {
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
            for (FacetField solrField : response.getFacetFields()) {
                FacetedQueryResultItem.Field field = new FacetedQueryResultItem().new Field();
                field.setName(solrField.getName());

                long total = 0;
                List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
                for (FacetField.Count solrCount : solrField.getValues()) {
                    countName = solrCount.getName();
                    // discard Ensembl genes and trascripts
                    if (!("genes").equals(field.getName())
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
                if (solrPivots != null && CollectionUtils.isNotEmpty(solrPivots)) {
                    // init field
                    FacetedQueryResultItem.Field field = new FacetedQueryResultItem().new Field();
                    field.setName(facetPivot.getName(i).split(",")[0]);

                    long total = 0;
                    List<FacetedQueryResultItem.Count> counts = new ArrayList<>();
                    for (PivotField solrPivot : solrPivots) {
                        FacetedQueryResultItem.Field nestedField = processSolrPivot(facetPivot.getName(i), 1, includes, solrPivot);

                        countName = solrPivot.getValue().toString();
                        // discard Ensembl genes and trascripts
                        if (!("genes").equals(field.getName())
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
            for (RangeFacet solrRange : response.getFacetRanges()) {
                List<Long> counts = new ArrayList<>();
                long total = 0;
                for (Object objCount : solrRange.getCounts()) {
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
                for (String key : intersectionMap.keySet()) {
                    List<List<String>> intersectionLists = intersectionMap.get(key);
                    for (List<String> list : intersectionLists) {
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

    private Map<String, List<List<String>>> getInputIntersections(QueryOptions queryOptions) {
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

    public void close() throws IOException {
        solrManager.close();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantSearchManager{");
        sb.append("solrManager=").append(solrManager);
        sb.append(", solrQueryParser=").append(solrQueryParser);
        sb.append(", storageConfiguration=").append(storageConfiguration);
        sb.append(", variantSearchToVariantConverter=").append(variantSearchToVariantConverter);
        sb.append(", insertBatchSize=").append(insertBatchSize);
        sb.append('}');
        return sb.toString();
    }

    public SolrManager getSolrManager() {
        return solrManager;
    }

    public VariantSearchManager setSolrManager(SolrManager solrManager) {
        this.solrManager = solrManager;
        return this;
    }

    public SolrClient getSolrClient() {
        return solrManager.getSolrClient();
    }

    public VariantSearchManager setSolrClient(SolrClient solrClient) {
        this.solrManager.setSolrClient(solrClient);
        return this;
    }

    public SolrQueryParser getSolrQueryParser() {
        return solrQueryParser;
    }

    public VariantSearchManager setSolrQueryParser(SolrQueryParser solrQueryParser) {
        this.solrQueryParser = solrQueryParser;
        return this;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public VariantSearchManager setStorageConfiguration(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
        return this;
    }

    public VariantSearchToVariantConverter getVariantSearchToVariantConverter() {
        return variantSearchToVariantConverter;
    }

    public VariantSearchManager setVariantSearchToVariantConverter(VariantSearchToVariantConverter variantSearchToVariantConverter) {
        this.variantSearchToVariantConverter = variantSearchToVariantConverter;
        return this;
    }

    public int getInsertBatchSize() {
        return insertBatchSize;
    }

    public VariantSearchManager setInsertBatchSize(int insertBatchSize) {
        this.insertBatchSize = insertBatchSize;
        return this;
    }

}
