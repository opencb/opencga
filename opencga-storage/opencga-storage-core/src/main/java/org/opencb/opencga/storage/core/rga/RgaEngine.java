package org.opencb.opencga.storage.core.rga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.variant.search.solr.SolrNativeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class RgaEngine implements Closeable {

    private SolrManager solrManager;
    private RgaQueryParser queryParser;
    private IndividualRgaConverter individualRgaConverter;
    private GeneRgaConverter geneConverter;
    private StorageConfiguration storageConfiguration;
    private int insertBatchSize;

    private Logger logger;

    public static final String USE_SEARCH_INDEX = "useSearchIndex";
    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;

    public RgaEngine(StorageConfiguration storageConfiguration) {
        this.individualRgaConverter = new IndividualRgaConverter();
        this.geneConverter = new GeneRgaConverter();
        this.queryParser = new RgaQueryParser();
        this.storageConfiguration = storageConfiguration;

        this.solrManager = new SolrManager(storageConfiguration.getSearch().getHosts(), storageConfiguration.getSearch().getMode(),
                storageConfiguration.getSearch().getTimeout());

        // Set internal insert batch size from configuration and default value
        insertBatchSize = storageConfiguration.getSearch().getInsertBatchSize() > 0
                ? storageConfiguration.getSearch().getInsertBatchSize()
                : DEFAULT_INSERT_BATCH_SIZE;

        logger = LoggerFactory.getLogger(RgaEngine.class);
    }

    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
    }

    public void create(String dbName) throws VariantSearchException {
        try {
            solrManager.create(dbName, this.storageConfiguration.getSearch().getConfigSet());
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
     * Insert a list of RGA models into the given Solr collection.
     *
     * @param collection Solr collection where to insert
     * @param knockoutByIndividualList List of knockoutByIndividual to insert
     * @throws IOException   IOException
     * @throws SolrServerException SolrServerException
     */
    public void insert(String collection, List<KnockoutByIndividual> knockoutByIndividualList) throws IOException, SolrServerException {
        if (CollectionUtils.isNotEmpty(knockoutByIndividualList)) {
            List<RgaDataModel> rgaDataModelList = individualRgaConverter.convertToStorageType(knockoutByIndividualList);

            if (!rgaDataModelList.isEmpty()) {
                UpdateResponse updateResponse;
                updateResponse = solrManager.getSolrClient().addBeans(collection, rgaDataModelList);
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
     * @param path        Path to the file to load
     * @throws IOException            IOException
     * @throws RgaException RgaException
     */
    public void load(String collection, Path path) throws IOException, RgaException {
        String fileName = path.getFileName().toString();
        if (fileName.endsWith("json") || fileName.endsWith("json.gz")) {
            try {
                loadJson(collection, path);
            } catch (SolrServerException e) {
                throw new RgaException("Error loading KnockoutIndividual from JSON file.", e);
            }
        } else {
            throw new RgaException("File format " + path + " not supported. Please, use JSON file format.");
        }
    }


    /**
     * Return the list of KnockoutByIndividual objects from a Solr core/collection given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of KnockoutByIndividual objects
     * @throws RgaException RgaException
     * @throws IOException   IOException
     */
    public OpenCGAResult<KnockoutByIndividual> individualQuery(String collection, Query query, QueryOptions queryOptions)
            throws RgaException, IOException {
        SolrQuery solrQuery = queryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<KnockoutByIndividual> queryResult;
        try {
            DataResult<RgaDataModel> result = solrCollection.query(solrQuery, RgaDataModel.class);
            List<KnockoutByIndividual> knockoutByIndividuals = individualRgaConverter.convertToDataModelType(result.getResults());
            queryResult = new OpenCGAResult<>(result.getTime(), result.getEvents(), knockoutByIndividuals.size(), knockoutByIndividuals,
                    result.getNumMatches());
        } catch (SolrServerException e) {
            throw new RgaException("Error executing KnockoutByIndividual query", e);
        }

        return new OpenCGAResult<>(queryResult);
    }

    /**
     * Return the list of KnockoutByGene objects from a Solr core/collection given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of KnockoutByGene objects
     * @throws RgaException RgaException
     * @throws IOException   IOException
     */
    public OpenCGAResult<KnockoutByGene> geneQuery(String collection, Query query, QueryOptions queryOptions)
            throws RgaException, IOException {
        SolrQuery solrQuery = queryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<KnockoutByGene> queryResult;
        try {
            DataResult<RgaDataModel> result = solrCollection.query(solrQuery, RgaDataModel.class);
            List<KnockoutByGene> knockoutByGeneList = geneConverter.convertToDataModelType(result.getResults());
            queryResult = new OpenCGAResult<>(result.getTime(), result.getEvents(), knockoutByGeneList.size(), knockoutByGeneList,
                    result.getNumMatches());
        } catch (SolrServerException e) {
            throw new RgaException("Error executing KnockoutByGene query", e);
        }

        return new OpenCGAResult<>(queryResult);
    }

    /**
     * Return the list of RgaDataModel objects from a Solr core/collection given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of RgaDataModel objects
     * @throws RgaException RgaException
     * @throws IOException   IOException
     */
    public OpenCGAResult<RgaDataModel> nativeQuery(String collection, Query query, QueryOptions queryOptions)
            throws RgaException, IOException {
        SolrQuery solrQuery = queryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<RgaDataModel> queryResult;
        try {
            queryResult = solrCollection.query(solrQuery, RgaDataModel.class);
        } catch (SolrServerException e) {
            throw new RgaException("Error executing KnockoutByIndividual query (nativeQuery)", e);
        }

        return new OpenCGAResult<>(queryResult);
    }

//    /**
//     * Return a Solr variant iterator to retrieve KnockoutByIndividual objects from a Solr core/collection given a query.
//     *
//     * @param collection   Collection name
//     * @param query        Query
//     * @param queryOptions Query options
//     * @return Solr VariantSearch iterator
//     * @throws VariantSearchException VariantSearchException
//     * @throws IOException   IOException
//     */
//    public SolrVariantDBIterator iterator(String collection, Query query, QueryOptions queryOptions)
//            throws VariantSearchException, IOException {
//        try {
//            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
//            return new SolrVariantDBIterator(solrManager.getSolrClient(), collection, solrQuery,
//                    new VariantSearchToVariantConverter(VariantField.getIncludeFields(queryOptions)));
//        } catch (SolrServerException e) {
//            throw new VariantSearchException("Error getting variant iterator", e);
//        }
//    }

    /**
     * Return a Solr KnockoutByIndividual iterator to retrieve RgaDataModel objects from a Solr core/collection given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return Solr SolrNativeIterator iterator
     * @throws RgaException RgaException
     */
    public SolrNativeIterator nativeIterator(String collection, Query query, QueryOptions queryOptions)
            throws RgaException {
        try {
            SolrQuery solrQuery = queryParser.parse(query, queryOptions);
            return new SolrNativeIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new RgaException("Error getting KnockoutByIndividual iterator (native)", e);
        }
    }

    /**
     *
     * @param collection Collection name
     * @param query      Query
     * @return Number of results
     * @throws RgaException RgaException
     * @throws IOException IOException
     */
    public long count(String collection, Query query) throws RgaException, IOException {
        SolrQuery solrQuery = queryParser.parse(query, QueryOptions.empty());
        SolrCollection solrCollection = solrManager.getCollection(collection);

        try {
            return solrCollection.count(solrQuery).getResults().get(0);
        } catch (SolrServerException e) {
            throw new RgaException("Error executing count for a given query", e);
        }
    }

    /**
     * Return faceted data from a Solr core/collection given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options (contains the facet and facetRange options)
     * @return List of KnockoutByIndividual objects
     * @throws RgaException RgaException
     * @throws IOException IOException
     */
    public DataResult<FacetField> facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws RgaException, IOException {
        // Pre-processing
        //   - As "genes" contains, for each gene: gene names, Ensembl gene ID and all its Ensembl transcript IDs,
        //     we do not have to repeat counts for all of them, by default, only for gene names
        //   - consequenceType is replaced by soAcc (i.e., by the field name in the Solr schema)
        boolean replaceSoAcc = false;
        boolean replaceGenes = false;
        Map<String, Set<String>> includingValuesMap = new HashMap<>();
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String facetQuery = queryOptions.getString(QueryOptions.FACET);

//            // Gene management
//            if (facetQuery.contains("genes[")
//                    && (facetQuery.contains("genes;") || facetQuery.contains("genes>>") || facetQuery.endsWith("genes"))) {
//                throw new VariantSearchException("Invalid gene facet query: " + facetQuery);
//            }

            try {
                includingValuesMap = new FacetQueryParser().getIncludingValuesMap(facetQuery);
            } catch (Exception e) {
                throw new RgaException("Error parsing faceted query", e);
            }

//            if (!facetQuery.contains("genes[") && facetQuery.contains("genes")) {
//                // Force to query genes by prefix ENSG
//                queryOptions.put(QueryOptions.FACET, facetQuery.replace("genes", "genes[ENSG0*]"));
//                replaceGenes = true;
//            }

            // Consequence type management
//            facetQuery = queryOptions.getString(QueryOptions.FACET);
//            if (facetQuery.contains("consequenceType")) {
//                replaceSoAcc = true;
//
//                facetQuery = facetQuery.replace("consequenceType", "soAcc");
//                queryOptions.put(QueryOptions.FACET, facetQuery);
//
//                String[] split = facetQuery.split("soAcc\\[");
//                if (split.length > 1 || facetQuery.startsWith("soAcc[")) {
//                    int start = 0;
//                    StringBuilder newFacetQuery = new StringBuilder();
//                    if (!facetQuery.startsWith("soAcc[")) {
//                        newFacetQuery.append(split[0]);
//                        start = 1;
//                    }
//                    for (int i = start; i < split.length; i++) {
//                        newFacetQuery.append("soAcc");
//
//                        // Manage values to include
//                        int index = split[i].indexOf("]");
//                        String strValues = split[i].substring(0, index);
//                        String[] arrValues = strValues.split(",");
//                        List<String> soAccs = new ArrayList<>();
//                        for (String value: arrValues) {
//                            String val = value.replace("SO:", "");
//                            try {
//                                // Try to get SO accession, and if it is a valid SO accession
//                                int soAcc = Integer.parseInt(val);
//                                if (ConsequenceTypeMappings.accessionToTerm.containsKey(soAcc)) {
//                                    soAccs.add(String.valueOf(soAcc));
//                                }
//                            } catch (NumberFormatException e) {
//                                // Otherwise, it is treated as a SO term, and check if it is a valid SO term
//                                if (ConsequenceTypeMappings.termToAccession.containsKey(val)) {
//                                    soAccs.add(String.valueOf(ConsequenceTypeMappings.termToAccession.get(val)));
//                                }
//                            }
//                        }
//                        if (ListUtils.isNotEmpty(soAccs)) {
//                            newFacetQuery.append("[").append(StringUtils.join(soAccs, ",")).append("]");
//                        }
//                    }
//                    queryOptions.put(QueryOptions.FACET, newFacetQuery.toString());
//                }
//            }
//        }
        }

        // Query
        SolrQuery solrQuery = queryParser.parse(query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);

        /*
        *    try {
                FacetQueryParser facetQueryParser = new FacetQueryParser();

                String facetQuery = parseFacet(queryOptions.getString(QueryOptions.FACET));
                String jsonFacet = facetQueryParser.parse(facetQuery);

                solrQuery.set("json.facet", jsonFacet);
                solrQuery.setRows(0);
                solrQuery.setStart(0);
                solrQuery.setFields();

                logger.debug(">>>>>> Solr Facet: " + solrQuery.toString());
            } catch (Exception e) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Solr parse exception: " + e.getMessage(), e);
            }
        * */

        DataResult<FacetField> facetResult;
        try {
            facetResult = solrCollection.facet(solrQuery, null);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }

        return facetResult;
    }


    @Override
    public void close() throws IOException {
        solrManager.close();
    }

    /*-------------------------------------
     *  P R I V A T E    M E T H O D S
     -------------------------------------*/

    /**
     * Load a JSON file into the Solr core/collection.
     *
     * @param path Path to the JSON file
     * @throws IOException
     * @throws SolrException
     */
    private void loadJson(String collection, Path path) throws IOException, SolrServerException {
        // This opens json and json.gz files automatically
        try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
            List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(insertBatchSize);
            int count = 0;
            String line;
            ObjectReader objectReader = new ObjectMapper().readerFor(KnockoutByIndividual.class);
            while ((line = bufferedReader.readLine()) != null) {
                KnockoutByIndividual knockoutByIndividual = objectReader.readValue(line);
                knockoutByIndividualList.add(knockoutByIndividual);
                count++;
                if (count % insertBatchSize == 0) {
                    logger.debug("Loading knockoutByIndividual from '{}', {} entries loaded", path, count);
                    insert(collection, knockoutByIndividualList);
                    knockoutByIndividualList.clear();
                }
            }

            // Insert the remaining entries
            if (CollectionUtils.isNotEmpty(knockoutByIndividualList)) {
                logger.debug("Loading remaining knockoutByIndividual from '{}', {} entries loaded", path, count);
                insert(collection, knockoutByIndividualList);
            }
        }
    }

    public SolrManager getSolrManager() {
        return solrManager;
    }

    public RgaEngine setSolrManager(SolrManager solrManager) {
        this.solrManager = solrManager;
        return this;
    }
}
