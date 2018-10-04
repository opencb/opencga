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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.commons.datastore.core.result.FacetQueryResultItem;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
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

    public static final String CONF_SET = "OpenCGAConfSet-1.4.x";
    public static final String SEARCH_ENGINE_ID = "solr";
    public static final String USE_SEARCH_INDEX = "useSearchIndex";
    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;

    @Deprecated
    public VariantSearchManager(String host, String collection) {
        throw new UnsupportedOperationException("Not supported!!");
    }

    public VariantSearchManager(StudyConfigurationManager studyConfigurationManager, StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;

        this.solrQueryParser = new SolrQueryParser(studyConfigurationManager);
        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
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

    public void create(String coreName) throws SolrException {
        solrManager.create(coreName, CONF_SET);
    }

    public void create(String dbName, String configSet) throws SolrException {
        solrManager.create(dbName, configSet);
    }

    public void createCore(String coreName, String configSet) throws SolrException {
        solrManager.createCore(coreName, configSet);
    }

    public void createCollection(String collectionName, String configSet) throws SolrException {
        solrManager.createCollection(collectionName, configSet);
    }

    public boolean exists(String dbName) throws SolrException {
        return solrManager.exists(dbName);
    }

    public boolean existsCore(String coreName) {
        return solrManager.existsCore(coreName);
    }

    public boolean existsCollection(String collectionName) throws SolrException {
        return solrManager.existsCollection(collectionName);
    }

    /**
     * Load a Solr core/collection from a Avro or JSON file.
     *
     * @param collection Collection name
     * @param path       Path to the file to load
     * @throws IOException            IOException
     * @throws SolrException          SolrException
     * @throws StorageEngineException StorageEngineException
     */
    public void load(String collection, Path path) throws IOException, SolrException, StorageEngineException {
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
     * @param loadListener      Load listener
     * @return VariantSearchLoadResult
     * @throws IOException            IOException
     * @throws SolrException SolrException
     */
    public VariantSearchLoadResult load(String collection, VariantDBIterator variantDBIterator, ProgressLogger progressLogger,
                                        VariantSearchLoadListener loadListener)
            throws IOException, SolrException {
        if (variantDBIterator == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "VariantDBIterator parameter is null");
        }

        int count = 0;
        int numLoadedVariants = 0;
        List<Variant> variantList = new ArrayList<>(insertBatchSize);
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            progressLogger.increment(1, () -> "up to position " + variant.toString());
            variantList.add(variant);
            count++;
            if (count % insertBatchSize == 0) {
                loadListener.preLoad(variantList);
                numLoadedVariants += variantList.size();
                insert(collection, variantList);
                loadListener.postLoad(variantList);
                variantList.clear();
            }
        }

        // Insert the remaining variants
        if (CollectionUtils.isNotEmpty(variantList)) {
            loadListener.preLoad(variantList);
            numLoadedVariants += variantList.size();
            insert(collection, variantList);
            loadListener.postLoad(variantList);
        }
        loadListener.close();

        logger.debug("Variant Search loading done: {} variants indexed", count);
        return new VariantSearchLoadResult(count, numLoadedVariants, 0);
    }

    /**
     * Delete variants a Solr core/collection from a variant DB iterator.
     *
     * @param collection        Collection name
     * @param variantDBIterator Iterator to retrieve the variants to remove
     * @param progressLogger    Progress logger
     * @return VariantSearchLoadResult
     * @throws IOException            IOException
     * @throws SolrException SolrException
     */
    public int delete(String collection, VariantDBIterator variantDBIterator, ProgressLogger progressLogger)
            throws IOException, SolrException {
        if (variantDBIterator == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "VariantDBIterator parameter is null");
        }

        int count = 0;
        List<String> variantList = new ArrayList<>(insertBatchSize);
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            progressLogger.increment(1, () -> "up to position " + variant.toString());
            variantList.add(variant.toString());
            count++;
            if (count % insertBatchSize == 0 || !variantDBIterator.hasNext()) {
                delete(collection, variantList);
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
     * @throws IOException   IOException
     * @throws SolrException SolrException
     */
    public VariantQueryResult<Variant> query(String collection, Query query, QueryOptions queryOptions)
            throws IOException {
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        QueryResult<Variant> queryResult;
        try {
            queryResult = solrCollection.query(solrQuery, VariantSearchModel.class, variantSearchToVariantConverter);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }

        return new VariantQueryResult<>("", queryResult.getDbTime(), queryResult.getNumResults(),
                queryResult.getNumTotalResults(), "", "", queryResult.getResult(), null, SEARCH_ENGINE_ID);
    }

    /**
     * Return the list of VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of VariantSearchModel objects
     * @throws IOException   IOException
     * @throws SolrException SolrException
     */
    public VariantQueryResult<VariantSearchModel> nativeQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, SolrException {
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        QueryResult<VariantSearchModel> queryResult;
        try {
            queryResult = solrCollection.query(solrQuery, VariantSearchModel.class);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }

        return new VariantQueryResult<>("", queryResult.getDbTime(), queryResult.getNumResults(),
                queryResult.getNumTotalResults(), "", "", queryResult.getResult(), null, SEARCH_ENGINE_ID);
    }

    public VariantSolrIterator iterator(String collection, Query query, QueryOptions queryOptions)
            throws SolrException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
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
     * @throws IOException   IOException
     * @throws SolrException SolrException
     */
    public VariantSearchSolrIterator nativeIterator(String collection, Query query, QueryOptions queryOptions)
            throws SolrException, IOException {
        try {
            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
            return new VariantSearchSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }
    }

    public long count(String collection, Query query) throws IOException {
        SolrQuery solrQuery = solrQueryParser.parse(query, QueryOptions.empty());
        SolrCollection solrCollection = solrManager.getCollection(collection);

        try {
            return solrCollection.count(solrQuery).getResult().get(0);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
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
     * @throws IOException IOException
     * @throws VariantSearchException VariantSearchException
     */
    public FacetQueryResult facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, VariantSearchException {
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);

        // As "genes" contains, for each gene: gene names, Ensembl gene ID and all its Ensembl transcript IDs,
        // we do not have to repeat counts for all of them, usually, only for gene names
        boolean replaceGenes = false;
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String facetQuery = queryOptions.getString(QueryOptions.FACET);
            if (facetQuery.contains("genes[")
                    && (facetQuery.contains("genes;") || facetQuery.contains("genes>>") || facetQuery.endsWith("genes"))) {
                throw new VariantSearchException("Invalid gene facet query: " + facetQuery);
            }
            if (!facetQuery.contains("genes[") && facetQuery.contains("genes")) {
                // Force to query genes by prefix ENSG
                queryOptions.put(QueryOptions.FACET, facetQuery.replace("genes", "genes[\"ENSG0\"]"));
                replaceGenes = true;
            }
        }

        FacetQueryResult facetResult;
        try {
            facetResult = solrCollection.facet(solrQuery);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }

        if (replaceGenes) {
            List<String> ensemblGeneIds = getEnsemblGeneIds(facetResult.getResult());
            QueryResponse<Gene> geneQueryResponse = cellBaseClient.getGeneClient().get(ensemblGeneIds, QueryOptions.empty());
            Map<String, String> ensemblGeneIdToGeneName = new HashMap<>();
            for (Gene gene: geneQueryResponse.allResults()) {
                ensemblGeneIdToGeneName.put(gene.getId(), gene.getName());
            }
            replaceEnsemblGeneIds(facetResult.getResult(), ensemblGeneIdToGeneName);
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
     * Insert a list of variants into Solr.
     *
     * @param variants List of variants to insert
     * @throws IOException   IOException
     * @throws SolrException SolrException
     */
    private void insert(String collection, List<Variant> variants) throws IOException, SolrException {
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
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Load a JSON file into the Solr core/collection.
     *
     * @param path Path to the JSON file
     * @throws IOException
     * @throws SolrException
     */
    private void loadJson(String collection, Path path) throws IOException, SolrException {
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

    private void loadAvro(String collection, Path path) throws IOException, SolrException, StorageEngineException {
        // reader
        VariantReader reader = VariantReaderUtils.getVariantReader(path);

        // TODO: get the buffer size from configuration file
        int bufferSize = 10000;

        List<Variant> variants;
        do {
            variants = reader.read(bufferSize);
            insert(collection, variants);
        } while (CollectionUtils.isNotEmpty(variants));

        reader.close();
    }

    private void delete(String collection, List<String> variants) throws IOException, SolrException {
        if (CollectionUtils.isNotEmpty(variants)) {
            try {
                UpdateResponse updateResponse = solrManager.getSolrClient().deleteById(collection, variants);
                if (updateResponse.getStatus() == 0) {
                    solrManager.getSolrClient().commit(collection);
                }
            } catch (SolrServerException e) {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
            }
        }
    }

    private List<String> getEnsemblGeneIds(FacetQueryResultItem result) {
        Set<String> ensemblGeneIds = new HashSet<>();
        Queue<FacetQueryResultItem.FacetField> queue = new LinkedList<>();
        for (FacetQueryResultItem.FacetField facetField: result.getFacetFields()) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetQueryResultItem.FacetField facet = queue.remove();
            for (FacetQueryResultItem.Bucket bucket: facet.getBuckets()) {
                if (bucket.getValue().startsWith("ENSG0")) {
                    ensemblGeneIds.add(bucket.getValue());
                }
                if (ListUtils.isNotEmpty(bucket.getFacetFields())) {
                    for (FacetQueryResultItem.FacetField facetField: bucket.getFacetFields()) {
                        queue.add(facetField);
                    }
                }

            }
        }
        return new ArrayList<>(ensemblGeneIds);
    }

    private void replaceEnsemblGeneIds(FacetQueryResultItem result, Map<String, String> ensemblGeneIdToGeneName) {
        Queue<FacetQueryResultItem.FacetField> queue = new LinkedList<>();
        for (FacetQueryResultItem.FacetField facetField: result.getFacetFields()) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetQueryResultItem.FacetField facet = queue.remove();
            for (FacetQueryResultItem.Bucket bucket: facet.getBuckets()) {
                if (bucket.getValue().startsWith("ENSG0")) {
                    bucket.setValue(ensemblGeneIdToGeneName.getOrDefault(bucket.getValue(), bucket.getValue()));
                }
                if (ListUtils.isNotEmpty(bucket.getFacetFields())) {
                    for (FacetQueryResultItem.FacetField facetField: bucket.getFacetFields()) {
                        queue.add(facetField);
                    }
                }
            }
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
