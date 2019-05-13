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
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by imedina on 09/11/16.
 * Created by wasim on 09/11/16.
 */
public class VariantSearchManager {

    private SolrManager solrManager;
    private CellBaseClient cellBaseClient;
    private SolrQueryParser solrQueryParser;
    private StorageConfiguration storageConfiguration;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private int insertBatchSize;

    private Logger logger;

    public static final String CONF_SET = "OpenCGAConfSet-1.4.0";
    public static final String SEARCH_ENGINE_ID = "solr";
    public static final String USE_SEARCH_INDEX = "useSearchIndex";
    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;

    @Deprecated
    public VariantSearchManager(String host, String collection) {
        throw new UnsupportedOperationException("Not supported!!");
    }

    public VariantSearchManager(VariantStorageMetadataManager variantStorageMetadataManager, StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;

        this.solrQueryParser = new SolrQueryParser(variantStorageMetadataManager);
        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();

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
            solrManager.create(dbName, CONF_SET);
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

    public void createCore(String coreName, String configSet) throws VariantSearchException {
        try {
            solrManager.createCore(coreName, configSet);
        } catch (SolrException e) {
            throw new VariantSearchException("Error creating Solr core '" + coreName + "'", e);
        }
    }

    public void createCollection(String collectionName, String configSet) throws VariantSearchException {
        try {
            solrManager.createCollection(collectionName, configSet);
        } catch (SolrException e) {
            throw new VariantSearchException("Error creating Solr collection '" + collectionName + "'", e);
        }
    }

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
     * Load a Solr core/collection from a Avro or JSON file.
     *
     * @param collection Collection name
     * @param uri        Path to the file to load
     * @param variantReaderUtils Variant reader utils
     * @throws VariantSearchException VariantSearchException
     * @throws IOException            IOException
     */
    public void load(String collection, URI uri, VariantReaderUtils variantReaderUtils) throws VariantSearchException, IOException {
        // TODO: can we use VariantReaderUtils as implemented in the function load00 below ?
        // TODO: VarriantReaderUtils supports JSON, AVRO and VCF file formats.

        String fileName = UriUtils.fileName(uri);
        if (fileName.endsWith("json") || fileName.endsWith("json.gz")) {
            try {
                loadJson(collection, uri, variantReaderUtils);
            } catch (SolrServerException e) {
                throw new VariantSearchException("Error loading variants from JSON file.", e);
            }
        } else if (fileName.endsWith("avro") || fileName.endsWith("avro.gz")) {
            try {
                loadAvro(collection, uri, variantReaderUtils);
            } catch (StorageEngineException | SolrServerException e) {
                throw new VariantSearchException("Error loading variants from AVRO file.", e);
            }
        } else {
            throw new VariantSearchException("File format " + uri + " not supported. Please, use Avro or JSON file formats.");
        }
    }

    /**
     * Load a Solr core/collection from a variant DB iterator.
     *
     * @param collection        Collection name
     * @param variantDBIterator Iterator to retrieve the variants to load
     * @param progressLogger    Progress logger
     * @param loadListener      Load listener
     * @return VariantSearchLoadResult
     * @throws VariantSearchException VariantSearchException
     */
    public VariantSearchLoadResult load(String collection, VariantDBIterator variantDBIterator, ProgressLogger progressLogger,
                                        VariantSearchLoadListener loadListener) throws VariantSearchException {
        if (variantDBIterator == null) {
            throw new VariantSearchException("Missing variant DB iterator when loading Solr variant collection");
        }

        AtomicInteger count = new AtomicInteger();
        AtomicInteger numLoadedVariants = new AtomicInteger();

        ParallelTaskRunner<Variant, Variant> ptr = new ParallelTaskRunner<>((n) -> {
            List<Variant> batch = new ArrayList<>(n);
            while (batch.size() < n && variantDBIterator.hasNext()) {
                batch.add(variantDBIterator.next());
            }
            count.addAndGet(batch.size());
            return batch;
        }, batch -> {
            progressLogger.increment(batch.size(), () -> "up to position " + batch.get(batch.size() - 1).toString());
            return batch;
        }, batch -> {
            try {
                loadListener.preLoad(batch);
                numLoadedVariants.addAndGet(batch.size());
                insert(collection, batch);
                loadListener.postLoad(batch);
            } catch (SolrServerException | IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }, ParallelTaskRunner.Config.builder()
                .setBatchSize(insertBatchSize)
                .setCapacity(2)
                .setNumTasks(1)
                .build());

        StopWatch stopWatch = StopWatch.createStarted();
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new VariantSearchException("Error loading secondary index", e);
        }

        loadListener.close();

        logger.info("Variant Search loading done. " + numLoadedVariants + " variants indexed in " + TimeUtils.durationToString(stopWatch));
        return new VariantSearchLoadResult(count.get(), numLoadedVariants.get(), 0);
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
        QueryResult<Variant> queryResult;
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
        QueryResult<VariantSearchModel> queryResult;
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
    public VariantSolrIterator iterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantSolrIterator(solrManager.getSolrClient(), collection, solrQuery,
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
    public VariantSearchSolrIterator nativeIterator(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantSearchSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
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
            return solrCollection.count(solrQuery).getResult().get(0);
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
    public FacetQueryResult facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        // Pre-processing
        //   - As "genes" contains, for each gene: gene names, Ensembl gene ID and all its Ensembl transcript IDs,
        //     we do not have to repeat counts for all of them, by default, only for gene names
        //   - consequenceType is replaced by soAcc (i.e., by the field name in the Solr schema)
        boolean replaceSoAcc = false;
        boolean replaceGenes = false;
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String facetQuery = queryOptions.getString(QueryOptions.FACET);

            // Gene management
            if (facetQuery.contains("genes[")
                    && (facetQuery.contains("genes;") || facetQuery.contains("genes>>") || facetQuery.endsWith("genes"))) {
                throw new VariantSearchException("Invalid gene facet query: " + facetQuery);
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

        FacetQueryResult facetResult;
        try {
            facetResult = solrCollection.facet(solrQuery, null, postprocessing);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }

        // Post-processing
        Map<String, String> ensemblGeneIdToGeneName = null;
        if (replaceGenes) {
            List<String> ensemblGeneIds = getEnsemblGeneIds(facetResult.getResults());
            QueryResponse<Gene> geneQueryResponse = cellBaseClient.getGeneClient().get(ensemblGeneIds, QueryOptions.empty());
            ensemblGeneIdToGeneName = new HashMap<>();
            for (Gene gene: geneQueryResponse.allResults()) {
                ensemblGeneIdToGeneName.put(gene.getId(), gene.getName());
            }
//            replaceEnsemblGeneIds(facetResult.getResults(), ensemblGeneIdToGeneName);
        }

        if (replaceGenes || replaceSoAcc) {
            facetPostProcessing(facetResult.getResults(), ensemblGeneIdToGeneName, replaceSoAcc);
        }

        return facetResult;
    }

    public void close() throws IOException {
        solrManager.close();
    }

    /*-------------------------------------
     *  P R I V A T E    M E T H O D S
     -------------------------------------*/

    /**
     * Load a JSON file into the Solr core/collection.
     *
     * @param uri Path to the JSON file
     * @throws IOException
     * @throws SolrException
     */
    private void loadJson(String collection, URI uri, VariantReaderUtils utils) throws IOException, SolrServerException {
        // This opens json and json.gz files automatically
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(utils.getIOManagerProvider().newInputStream(uri)))) {
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
                    logger.debug("Loading variants from '{}', {} variants loaded", uri.toString(), count);
                    insert(collection, variants);
                    variants.clear();
                }
            }

            // Insert the remaining variants
            if (CollectionUtils.isNotEmpty(variants)) {
                logger.debug("Loading remaining variants from '{}', {} variants loaded", uri.toString(), count);
                insert(collection, variants);
            }
        }
    }

    private void loadAvro(String collection, URI uri, VariantReaderUtils variantReaderUtils)
            throws StorageEngineException, IOException, SolrServerException {
        // reader
        VariantReader reader = variantReaderUtils.getVariantReader(uri, null);

        // TODO: get the buffer size from configuration file
        int bufferSize = 10000;

        List<Variant> variants;
        do {
            variants = reader.read(bufferSize);
            insert(collection, variants);
        } while (CollectionUtils.isNotEmpty(variants));

        reader.close();
    }

    private void delete(String collection, List<String> variants) throws IOException, SolrServerException {
        if (CollectionUtils.isNotEmpty(variants)) {
            UpdateResponse updateResponse = solrManager.getSolrClient().deleteById(collection, variants);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(collection);
            }
        }
    }

    private List<String> getEnsemblGeneIds(List<FacetQueryResult.Field> results) {
        Set<String> ensemblGeneIds = new HashSet<>();
        Queue<FacetQueryResult.Field> queue = new LinkedList<>();
        for (FacetQueryResult.Field facetField: results) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetQueryResult.Field facet = queue.remove();
            for (FacetQueryResult.Bucket bucket: facet.getBuckets()) {
                if (bucket.getValue().startsWith("ENSG0")) {
                    ensemblGeneIds.add(bucket.getValue());
                }
                if (ListUtils.isNotEmpty(bucket.getFields())) {
                    for (FacetQueryResult.Field facetField: bucket.getFields()) {
                        queue.add(facetField);
                    }
                }

            }
        }
        return new ArrayList<>(ensemblGeneIds);
    }

    private void facetPostProcessing(List<FacetQueryResult.Field> results, Map<String, String> ensemblGeneIdToGeneName,
                                     boolean replaceSoAcc) {
        Queue<FacetQueryResult.Field> queue = new LinkedList<>();
        for (FacetQueryResult.Field facetField: results) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetQueryResult.Field facet = queue.remove();
            boolean toSoTerm = false;
            if (replaceSoAcc && "soAcc".equals(facet.getName())) {
                facet.setName("consequenceType");
                toSoTerm = true;
            }
            for (FacetQueryResult.Bucket bucket: facet.getBuckets()) {
                if (toSoTerm) {
                    bucket.setValue(ConsequenceTypeMappings.accessionToTerm.get(Integer.parseInt(bucket.getValue())));
                } else if (ensemblGeneIdToGeneName != null && bucket.getValue().startsWith("ENSG0")) {
                    bucket.setValue(ensemblGeneIdToGeneName.getOrDefault(bucket.getValue(), bucket.getValue()));
                }

                // Add next fields
                if (ListUtils.isNotEmpty(bucket.getFields())) {
                    for (FacetQueryResult.Field facetField: bucket.getFields()) {
                        queue.add(facetField);
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
