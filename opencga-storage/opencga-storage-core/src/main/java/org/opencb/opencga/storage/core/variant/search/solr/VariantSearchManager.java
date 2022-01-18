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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.search.VariantToSolrBeanConverterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;


/**
 * Created by imedina on 09/11/16.
 * Created by wasim on 09/11/16.
 */
public class VariantSearchManager {

    private SolrManager solrManager;
    private CellBaseClient cellBaseClient;
    private final ObjectMap options;
    private SolrQueryParser solrQueryParser;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private final int insertBatchSize;
    private final String configSet;

    private Logger logger;

//    public static final String CONF_SET = "OpenCGAConfSet-1.4.0";
    public static final String SEARCH_ENGINE_ID = "solr";
    public static final String USE_SEARCH_INDEX = "useSearchIndex";
    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;

    public VariantSearchManager(VariantStorageMetadataManager variantStorageMetadataManager,
                                StorageConfiguration storageConfiguration, ObjectMap options) {

        this.solrQueryParser = new SolrQueryParser(variantStorageMetadataManager);
        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.options = options;
        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        this.configSet = storageConfiguration.getSearch().getConfigSet();

        this.solrManager = new SolrManager(storageConfiguration.getSearch().getHosts(), storageConfiguration.getSearch().getMode(),
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

    public void create(String dbName) throws VariantSearchException {
        try {
            solrManager.create(dbName, configSet);
        } catch (SolrException e) {
            throw new VariantSearchException("Error creating Solr collection '" + dbName + "'", e);
        }
    }

    public void create(String dbName, String configSet) throws VariantSearchException {
        try {
            solrManager.create(dbName, configSet);
        } catch (SolrException e) {
            throw new VariantSearchException("Error creating Solr collection '" + dbName + "'", e);
        }
    }

//    public void createCore(String coreName, String configSet) throws VariantSearchException {
//        try {
//            solrManager.createCore(coreName, configSet);
//        } catch (SolrException e) {
//            throw new VariantSearchException("Error creating Solr core '" + coreName + "'", e);
//        }
//    }
//
//    public void createCollection(String collectionName, String configSet) throws VariantSearchException {
//        try {
//            solrManager.createCollection(collectionName, configSet);
//        } catch (SolrException e) {
//            throw new VariantSearchException("Error creating Solr collection '" + collectionName + "'", e);
//        }
//    }

    public boolean exists(String dbName) throws VariantSearchException {
        try {
            return solrManager.exists(dbName);
        } catch (SolrException e) {
            throw new VariantSearchException("Error asking if Solr collection '" + dbName + "' exists", e);
        }
    }

    public boolean existsCore(String coreName) throws VariantSearchException {
        try {
            return solrManager.existsCore(coreName);
        } catch (SolrException e) {
            throw new VariantSearchException("Error asking if Solr core '" + coreName + "' exists", e);
        }
    }

    public boolean existsCollection(String collectionName) throws VariantSearchException {
        try {
            return solrManager.existsCollection(collectionName);
        } catch (SolrException e) {
            throw new VariantSearchException("Error asking if Solr collection '" + collectionName + "' exists", e);
        }
    }

    /**
     * Insert a list of variants into the given Solr collection.
     *
     * @param collection Solr collection where to insert
     * @param variants List of variants to insert
     * @throws IOException   IOException
     * @throws SolrServerException SolrServerException
     */
    public void insert(String collection, List<Variant> variants) throws IOException, SolrServerException {
        if (CollectionUtils.isNotEmpty(variants)) {
            List<VariantSearchModel> variantSearchModels = variantSearchToVariantConverter.convertListToStorageType(variants);

            if (!variantSearchModels.isEmpty()) {
                UpdateResponse updateResponse;
                updateResponse = solrManager.getSolrClient().addBeans(collection, variantSearchModels);
                if (updateResponse.getStatus() == 0) {
                    solrManager.getSolrClient().commit(collection);
                }
            }
        }
    }

    /**
     * Load a Solr core/collection from a variant DB iterator.
     *
     * @param collection        Collection name
     * @param variantDBIterator Iterator to retrieve the variants to load
     * @param writer Data Writer
     * @return VariantSearchLoadResult
     * @throws VariantSearchException VariantSearchException
     */
    public VariantSearchLoadResult load(String collection,
                                        VariantDBIterator variantDBIterator,
                                        SolrInputDocumentDataWriter writer)
            throws VariantSearchException {
        if (variantDBIterator == null) {
            throw new VariantSearchException("Missing variant DB iterator when loading Solr variant collection");
        }
        getSolrManager().checkExists(collection);

        int batchSize = options.getInt(
                VariantStorageOptions.SEARCH_LOAD_BATCH_SIZE.key(),
                VariantStorageOptions.SEARCH_LOAD_BATCH_SIZE.defaultValue());
        int numThreads = options.getInt(
                VariantStorageOptions.SEARCH_LOAD_THREADS.key(),
                VariantStorageOptions.SEARCH_LOAD_THREADS.defaultValue());

        ProgressLogger progressLogger = new ProgressLogger("Variants loaded in Solr:");

        ParallelTaskRunner<Variant, SolrInputDocument> ptr = new ParallelTaskRunner<>(
                new VariantDBReader(variantDBIterator),
                progressLogger
                        .<Variant>asTask(d -> "up to position " + d)
                        .then(new VariantToSolrBeanConverterTask(solrManager.getSolrClient().getBinder())),
                writer,
                ParallelTaskRunner.Config.builder()
                        .setBatchSize(batchSize)
                        .setCapacity(2)
                        .setNumTasks(numThreads)
                        .build());

        StopWatch stopWatch = StopWatch.createStarted();
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new VariantSearchException("Error loading secondary index", e);
        }

        int count = variantDBIterator.getCount();
        logger.info("Variant Search loading done. " + count + " variants indexed in " + TimeUtils.durationToString(stopWatch));
        return new VariantSearchLoadResult(count, count, 0);
    }


    /**
     * Delete variants a Solr core/collection from a variant DB iterator.
     *
     * @param collection        Collection name
     * @param variantDBIterator Iterator to retrieve the variants to remove
     * @param progressLogger    Progress logger
     * @return VariantSearchLoadResult
     * @throws VariantSearchException VariantSearchException
     * @throws IOException IOException
     */
    public int delete(String collection, VariantDBIterator variantDBIterator, ProgressLogger progressLogger)
            throws VariantSearchException, IOException {
        if (variantDBIterator == null) {
            throw new VariantSearchException("Missing variant DB iterator when deleting variants");
        }

        int count = 0;
        List<String> variantList = new ArrayList<>(insertBatchSize);
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            progressLogger.increment(1, () -> "up to position " + variant.toString());
            variantList.add(variant.toString());
            count++;
            if (count % insertBatchSize == 0 || !variantDBIterator.hasNext()) {
                try {
                    delete(collection, variantList);
                } catch (SolrServerException e) {
                    throw new VariantSearchException("Error deleting variants.", e);
                }
                variantList.clear();
            }
        }
        logger.debug("Variant Search delete done: {} variants removed", count);
        return count;
    }

    /**
     * Return the list of Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of Variant objects
     * @throws VariantSearchException VariantSearchException
     * @throws IOException   IOException
     */
    public VariantQueryResult<Variant> query(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<Variant> queryResult;
        try {
            queryResult = solrCollection.query(solrQuery, VariantSearchModel.class,
                    new VariantSearchToVariantConverter(VariantField.getIncludeFields(queryOptions)));
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error executing variant query", e);
        }

        return new VariantQueryResult<>(queryResult, null, SEARCH_ENGINE_ID);
    }

    /**
     * Return the list of VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of VariantSearchModel objects
     * @throws VariantSearchException VariantSearchException
     * @throws IOException   IOException
     */
    public VariantQueryResult<VariantSearchModel> nativeQuery(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<VariantSearchModel> queryResult;
        try {
            queryResult = solrCollection.query(solrQuery, VariantSearchModel.class);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error executing variant query (nativeQuery)", e);
        }

        return new VariantQueryResult<>(queryResult, null, SEARCH_ENGINE_ID);
    }

    /**
     * Return a Solr variant iterator to retrieve Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return Solr VariantSearch iterator
     * @throws VariantSearchException VariantSearchException
     * @throws IOException   IOException
     */
    public SolrVariantDBIterator iterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new SolrVariantDBIterator(solrManager.getSolrClient(), collection, solrQuery,
                    new VariantSearchToVariantConverter(VariantField.getIncludeFields(queryOptions)));
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error getting variant iterator", e);
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
     * @throws VariantSearchException VariantSearchException
     */
    public SolrNativeIterator nativeIterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new SolrNativeIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error getting variant iterator (native)", e);
        }
    }

    /**
     *
     * @param collection Collection name
     * @param query      Query
     * @return Number of results
     * @throws VariantSearchException VariantSearchException
     * @throws IOException IOException
     */
    public long count(String collection, Query query) throws VariantSearchException, IOException {
        SolrQuery solrQuery = solrQueryParser.parse(query, QueryOptions.empty());
        SolrCollection solrCollection = solrManager.getCollection(collection);

        try {
            return solrCollection.count(solrQuery).getResults().get(0);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error executing count for a given query", e);
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
     * @throws VariantSearchException VariantSearchException
     * @throws IOException IOException
     */
    public DataResult<FacetField> facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        // Pre-processing
        //   - As "genes" contains, for each gene: gene names, Ensembl gene ID and all its Ensembl transcript IDs,
        //     we do not have to repeat counts for all of them, by default, only for gene names
        //   - consequenceType is replaced by soAcc (i.e., by the field name in the Solr schema)
        boolean replaceSoAcc = false;
        boolean replaceGenes = false;
        Map<String, Set<String>> includingValuesMap = new HashMap<>();
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String facetQuery = queryOptions.getString(QueryOptions.FACET);

            // Gene management
            if (facetQuery.contains("genes[")
                    && (facetQuery.contains("genes;") || facetQuery.contains("genes>>") || facetQuery.endsWith("genes"))) {
                throw new VariantSearchException("Invalid gene facet query: " + facetQuery);
            }

            try {
                includingValuesMap = new FacetQueryParser().getIncludingValuesMap(facetQuery);
            } catch (Exception e) {
                throw new VariantSearchException("Error parsing faceted query", e);
            }

            if (!facetQuery.contains("genes[") && facetQuery.contains("genes")) {
                // Force to query genes by prefix ENSG
                queryOptions.put(QueryOptions.FACET, facetQuery.replace("genes", "genes[ENSG0*]"));
                replaceGenes = true;
            }

            // Consequence type management
            facetQuery = queryOptions.getString(QueryOptions.FACET);
            if (facetQuery.contains("consequenceType")) {
                replaceSoAcc = true;

                facetQuery = facetQuery.replace("consequenceType", "soAcc");
                queryOptions.put(QueryOptions.FACET, facetQuery);

                String[] split = facetQuery.split("soAcc\\[");
                if (split.length > 1 || facetQuery.startsWith("soAcc[")) {
                    int start = 0;
                    StringBuilder newFacetQuery = new StringBuilder();
                    if (!facetQuery.startsWith("soAcc[")) {
                        newFacetQuery.append(split[0]);
                        start = 1;
                    }
                    for (int i = start; i < split.length; i++) {
                        newFacetQuery.append("soAcc");

                        // Manage values to include
                        int index = split[i].indexOf("]");
                        String strValues = split[i].substring(0, index);
                        String[] arrValues = strValues.split(",");
                        List<String> soAccs = new ArrayList<>();
                        for (String value: arrValues) {
                            String val = value.replace("SO:", "");
                            try {
                                // Try to get SO accession, and if it is a valid SO accession
                                int soAcc = Integer.parseInt(val);
                                if (ConsequenceTypeMappings.accessionToTerm.containsKey(soAcc)) {
                                    soAccs.add(String.valueOf(soAcc));
                                }
                            } catch (NumberFormatException e) {
                                // Otherwise, it is treated as a SO term, and check if it is a valid SO term
                                if (ConsequenceTypeMappings.termToAccession.containsKey(val)) {
                                    soAccs.add(String.valueOf(ConsequenceTypeMappings.termToAccession.get(val)));
                                }
                            }
                        }
                        if (ListUtils.isNotEmpty(soAccs)) {
                            newFacetQuery.append("[").append(StringUtils.join(soAccs, ",")).append("]");
                        }
                    }
                    queryOptions.put(QueryOptions.FACET, newFacetQuery.toString());
                }
            }
        }

        // Query
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        Postprocessing postprocessing = null;
        String jsonFacet = solrQuery.get("json.facet");
        if (StringUtils.isNotEmpty(jsonFacet) && jsonFacet.contains(SolrQueryParser.CHROM_DENSITY)) {
            postprocessing = new Postprocessing().setFacet(jsonFacet);
        }
        SolrCollection solrCollection = solrManager.getCollection(collection);

        DataResult<FacetField> facetResult;
        try {
            facetResult = solrCollection.facet(solrQuery, null, postprocessing);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }

        // Post-processing
        Map<String, String> ensemblGeneIdToGeneName = null;
        if (replaceGenes) {
            List<String> ensemblGeneIds = getEnsemblGeneIds(facetResult.getResults());
            CellBaseDataResponse<Gene> geneCellBaseDataResponse = cellBaseClient.getGeneClient().get(ensemblGeneIds, QueryOptions.empty());
            ensemblGeneIdToGeneName = new HashMap<>();
            for (Gene gene: geneCellBaseDataResponse.allResults()) {
                String name = StringUtils.isEmpty(gene.getName()) ? gene.getId() : gene.getName();
                ensemblGeneIdToGeneName.put(gene.getId(), name);
            }
        }

        facetPostProcessing(facetResult.getResults(), includingValuesMap, ensemblGeneIdToGeneName, replaceSoAcc);

        return facetResult;
    }

    public void close() throws IOException {
        solrManager.close();
    }

    /*-------------------------------------
     *  P R I V A T E    M E T H O D S
     -------------------------------------*/

    private void delete(String collection, List<String> variants) throws IOException, SolrServerException {
        if (CollectionUtils.isNotEmpty(variants)) {
            UpdateResponse updateResponse = solrManager.getSolrClient().deleteById(collection, variants);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(collection);
            }
        }
    }

    private List<String> getEnsemblGeneIds(List<FacetField> results) {
        Set<String> ensemblGeneIds = new HashSet<>();
        Queue<FacetField> queue = new LinkedList<>();
        for (FacetField facetField: results) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetField facet = queue.remove();
            for (FacetField.Bucket bucket: facet.getBuckets()) {
                if (bucket.getValue().startsWith("ENSG0")) {
                    ensemblGeneIds.add(bucket.getValue());
                }
                if (ListUtils.isNotEmpty(bucket.getFacetFields())) {
                    for (FacetField facetField: bucket.getFacetFields()) {
                        queue.add(facetField);
                    }
                }

            }
        }
        return new ArrayList<>(ensemblGeneIds);
    }

    private void facetPostProcessing(List<FacetField> results, Map<String, Set<String>> includingValuesMap,
                                     Map<String, String> ensemblGeneIdToGeneName, boolean replaceSoAcc) {
        Queue<FacetField> queue = new LinkedList<>();
        for (FacetField facetField: results) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetField facet = queue.remove();
            String facetName = facet.getName();

            boolean toSoTerm = false;
            boolean isGene = false;
            if ("genes".equals(facetName)) {
                isGene = true;
            } else if (replaceSoAcc && "soAcc".equals(facetName)) {
                facet.setName("consequenceType");
                toSoTerm = true;
            }

            List<FacetField.Bucket> validBuckets =  new ArrayList<>();
            Map<String, Set<String>> presentValues = new HashMap<>();
            if (MapUtils.isNotEmpty(includingValuesMap) && CollectionUtils.isNotEmpty(includingValuesMap.get(facetName))) {
                presentValues.put(facetName, new HashSet<>());
            }

            for (FacetField.Bucket bucket : facet.getBuckets()) {
                // We save values for a field name with including values
                if (presentValues.containsKey(facetName)) {
                    presentValues.get(facetName).add(bucket.getValue());
                }

                if (toSoTerm) {
                    bucket.setValue(ConsequenceTypeMappings.accessionToTerm.get(Integer.parseInt(bucket.getValue())));
                } else if (isGene) {
                    if (MapUtils.isNotEmpty(includingValuesMap) && CollectionUtils.isNotEmpty(includingValuesMap.get(facetName))
                            && includingValuesMap.get(facetName).contains(bucket.getValue())) {
                        validBuckets.add(bucket);
                    } else if (ensemblGeneIdToGeneName != null && bucket.getValue().startsWith("ENSG0")) {
                        bucket.setValue(ensemblGeneIdToGeneName.getOrDefault(bucket.getValue(), bucket.getValue()));
                    }
                }

                // Add next fields
                if (ListUtils.isNotEmpty(bucket.getFacetFields())) {
                    for (FacetField facetField: bucket.getFacetFields()) {
                        queue.add(facetField);
                    }
                }
            }
            // For field name 'genes', we have to overwrite the valid buckets (removing Ensembl gene and transcript IDs
            if (CollectionUtils.isNotEmpty(validBuckets)) {
                facet.setBuckets(validBuckets);
            }
            // Check for including values with count equalts to 0, then we include it
            // We save values for a field name with including values
            if (presentValues.containsKey(facetName)) {
                for (String value : includingValuesMap.get(facetName)) {
                    if (!presentValues.get(facetName).contains(value)) {
                        facet.getBuckets().add(new FacetField.Bucket(value, 0, null));
                    }
                }
            }

        }
    }

    private class Postprocessing implements SolrCollection.FacetPostprocessing {
        private String facet;

        public Postprocessing setFacet(String facet) {
            this.facet = facet;
            return this;
        }

        @Override
        public org.apache.solr.client.solrj.response.QueryResponse apply(
                org.apache.solr.client.solrj.response.QueryResponse response) {

            // Check buckets for chromosome length
            SimpleOrderedMap solrFacets = (SimpleOrderedMap) response.getResponse().get("facets");
            Iterator iterator = solrFacets.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, SimpleOrderedMap> next = (Map.Entry<String, SimpleOrderedMap>) iterator.next();
                if (next.getValue() instanceof SimpleOrderedMap) {
                    if (next.getKey().startsWith(SolrQueryParser.CHROM_DENSITY)) {
                        SimpleOrderedMap value = next.getValue();
                        if (value.get("chromosome") != null) {
                            List<SimpleOrderedMap<Object>> chromBuckets = (List<SimpleOrderedMap<Object>>)
                                    ((SimpleOrderedMap) value.get("chromosome")).get("buckets");
                            for (SimpleOrderedMap<Object> chromBucket: chromBuckets) {
                                String chrom = chromBucket.get("val").toString();
                                SimpleOrderedMap startMap = (SimpleOrderedMap) chromBucket.get("start");
                                if (startMap != null) {
                                    List<SimpleOrderedMap<Object>> startBuckets =
                                            (List<SimpleOrderedMap<Object>>) startMap.get("buckets");
                                    for (int i = startBuckets.size() - 1; i >= 0; i--) {
                                        int pos = (int) startBuckets.get(i).get("val");
                                        if (pos > SolrQueryParser.getChromosomeMap().get(chrom)) {
                                            startBuckets.remove(i);
                                        } else {
                                            // Should we update "val" to the chromosome length?
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Check for chromosome density range
            return response;
        }
    }

    /*--------------------------------------
     *  toString and GETTERS and SETTERS
     -------------------------------------*/

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

}
