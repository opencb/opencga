package org.opencb.opencga.storage.core.rga;

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
import org.opencb.opencga.core.models.analysis.knockout.RgaKnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.variant.search.solr.SolrNativeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class RgaEngine implements Closeable {

    private SolrManager solrManager;
    private RgaQueryParser parser;
    private IndividualRgaConverter individualRgaConverter;
    private GeneRgaConverter geneConverter;
    private VariantRgaConverter variantConverter;
    private StorageConfiguration storageConfiguration;

    private Logger logger;

    public static final String USE_SEARCH_INDEX = "useSearchIndex";

    public RgaEngine(StorageConfiguration storageConfiguration) {
        this.individualRgaConverter = new IndividualRgaConverter();
        this.geneConverter = new GeneRgaConverter();
        this.variantConverter = new VariantRgaConverter();
        this.parser = new RgaQueryParser();
        this.storageConfiguration = storageConfiguration;

        this.solrManager = new SolrManager(storageConfiguration.getRga().getHosts(), storageConfiguration.getRga().getMode(),
                storageConfiguration.getRga().getTimeout());

        logger = LoggerFactory.getLogger(RgaEngine.class);
    }

    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
    }

    public void create(String dbName) throws RgaException {
        try {
            solrManager.create(dbName, this.storageConfiguration.getRga().getConfigSet());
        } catch (SolrException e) {
            throw new RgaException("Error creating Solr collection '" + dbName + "'", e);
        }
    }

    public void create(String dbName, String configSet) throws RgaException {
        try {
            solrManager.create(dbName, configSet);
        } catch (SolrException e) {
            throw new RgaException("Error creating Solr collection '" + dbName + "'", e);
        }
    }


    public boolean exists(String dbName) throws RgaException {
        try {
            return solrManager.exists(dbName);
        } catch (SolrException e) {
            throw new RgaException("Error asking if Solr collection '" + dbName + "' exists", e);
        }
    }

    public boolean existsCore(String coreName) throws RgaException {
        try {
            return solrManager.existsCore(coreName);
        } catch (SolrException e) {
            throw new RgaException("Error asking if Solr core '" + coreName + "' exists", e);
        }
    }

    public boolean existsCollection(String collectionName) throws RgaException {
        try {
            return solrManager.exists(collectionName);
        } catch (SolrException e) {
            throw new RgaException("Error asking if Solr collection '" + collectionName + "' exists", e);
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
        SolrQuery solrQuery = parser.parseQuery(query);
        fixIndividualOptions(queryOptions, solrQuery);
        solrQuery.setRows(Integer.MAX_VALUE);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<KnockoutByIndividual> queryResult;
        try {
            DataResult<RgaDataModel> result = solrCollection.query(solrQuery, RgaDataModel.class);
            List<KnockoutByIndividual> knockoutByIndividuals = individualRgaConverter.convertToDataModelType(result.getResults());
            queryResult = new OpenCGAResult<>(result.getTime(), result.getEvents(), knockoutByIndividuals.size(), knockoutByIndividuals,
                    -1);
        } catch (SolrServerException e) {
            throw new RgaException("Error executing KnockoutByIndividual query", e);
        }

        return new OpenCGAResult<>(queryResult);
    }

    private void fixIndividualOptions(QueryOptions queryOptions, SolrQuery solrQuery) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            for (String include : individualRgaConverter.getIncludeFields(queryOptions.getAsStringList(QueryOptions.INCLUDE))) {
                solrQuery.addField(include);
            }
        } else if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            for (String include : individualRgaConverter.getIncludeFromExcludeFields(queryOptions.getAsStringList(QueryOptions.EXCLUDE))) {
                solrQuery.addField(include);
            }
        }
    }

    private void fixGeneOptions(QueryOptions queryOptions, SolrQuery solrQuery) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            for (String include : geneConverter.getIncludeFields(queryOptions.getAsStringList(QueryOptions.INCLUDE))) {
                solrQuery.addField(include);
            }
        } else if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            for (String include : geneConverter.getIncludeFromExcludeFields(queryOptions.getAsStringList(QueryOptions.EXCLUDE))) {
                solrQuery.addField(include);
            }
        }
    }

    private void fixVariantOptions(QueryOptions queryOptions, SolrQuery solrQuery) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            for (String include : variantConverter.getIncludeFields(queryOptions.getAsStringList(QueryOptions.INCLUDE))) {
                solrQuery.addField(include);
            }
        } else if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            for (String include : variantConverter.getIncludeFromExcludeFields(queryOptions.getAsStringList(QueryOptions.EXCLUDE))) {
                solrQuery.addField(include);
            }
        }
    }

    @Deprecated
    private SolrQuery fixQuery(String collection, Query query, QueryOptions queryOptions) throws IOException, RgaException {
        int limit = queryOptions.getInt(QueryOptions.LIMIT);
        int skip = queryOptions.getInt(QueryOptions.SKIP);
        if (limit > 0 || skip > 0) {
            // Perform first a facet to obtain all the different sample ids there are available
            QueryOptions facetOptions = new QueryOptions()
                    .append(QueryOptions.SKIP, skip)
                    .append(QueryOptions.LIMIT, limit)
                    .append(QueryOptions.FACET, RgaQueryParams.SAMPLE_ID.key());
            DataResult<FacetField> facetFieldDataResult = facetedQuery(collection, query, facetOptions);

            // Add only the samples obtained after the facet
            List<String> sampleIds = facetFieldDataResult.first().getBuckets().stream().map(FacetField.Bucket::getValue)
                    .collect(Collectors.toList());
            query.append(RgaQueryParams.SAMPLE_ID.key(), sampleIds);
        }

        SolrQuery solrQuery = parser.parseQuery(query);
        return solrQuery;
    }

    /**
     * Return the list of RgaKnockoutByGene objects from a Solr core/collection given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of RgaKnockoutByGene objects
     * @throws RgaException RgaException
     * @throws IOException   IOException
     */
    public OpenCGAResult<RgaKnockoutByGene> geneQuery(String collection, Query query, QueryOptions queryOptions)
            throws RgaException, IOException {
        SolrQuery solrQuery = parser.parseQuery(query);
        fixGeneOptions(queryOptions, solrQuery);
        solrQuery.setRows(Integer.MAX_VALUE);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<RgaKnockoutByGene> queryResult;
        try {
            DataResult<RgaDataModel> result = solrCollection.query(solrQuery, RgaDataModel.class);
            List<RgaKnockoutByGene> knockoutByGeneList = geneConverter.convertToDataModelType(result.getResults());
            queryResult = new OpenCGAResult<>(result.getTime(), result.getEvents(), knockoutByGeneList.size(), knockoutByGeneList,
                    result.getNumMatches());
        } catch (SolrServerException e) {
            throw new RgaException("Error executing RgaKnockoutByGene query", e);
        }

        return new OpenCGAResult<>(queryResult);
    }

    /**
     * Return the list of KnockoutByVariant objects from a Solr core/collection given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return List of KnockoutByVariant objects
     * @throws RgaException RgaException
     * @throws IOException   IOException
     */
    public OpenCGAResult<KnockoutByVariant> variantQuery(String collection, Query query, QueryOptions queryOptions)
            throws RgaException, IOException {
        SolrQuery solrQuery = parser.parseQuery(query);
        fixVariantOptions(queryOptions, solrQuery);
        solrQuery.setRows(Integer.MAX_VALUE);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<KnockoutByVariant> queryResult;
        try {
            DataResult<RgaDataModel> result = solrCollection.query(solrQuery, RgaDataModel.class);
            List<KnockoutByVariant> knockoutByVariants = variantConverter.convertToDataModelType(result.getResults(),
                    query.getAsStringList(RgaQueryParams.VARIANTS.key()));
            queryResult = new OpenCGAResult<>(result.getTime(), result.getEvents(), knockoutByVariants.size(), knockoutByVariants,
                    -1);
        } catch (SolrServerException e) {
            throw new RgaException("Error executing KnockoutByVariant query", e);
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
        SolrQuery solrQuery = fixQuery(collection, query, queryOptions);
        solrQuery.setRows(Integer.MAX_VALUE);
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
//     * @throws RgaException RgaException
//     * @throws IOException   IOException
//     */
//    public SolrVariantDBIterator iterator(String collection, Query query, QueryOptions queryOptions)
//            throws RgaException, IOException {
//        try {
//            SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
//            return new SolrVariantDBIterator(solrManager.getSolrClient(), collection, solrQuery,
//                    new VariantSearchToVariantConverter(VariantField.getIncludeFields(queryOptions)));
//        } catch (SolrServerException e) {
//            throw new RgaException("Error getting variant iterator", e);
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
            SolrQuery solrQuery = fixQuery(collection, query, queryOptions);
            solrQuery.setRows(Integer.MAX_VALUE);
            return new SolrNativeIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException | IOException e) {
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
        SolrQuery solrQuery = parser.parseQuery(query);
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
        SolrQuery solrQuery = parser.parseQuery(query);

        if (queryOptions.containsKey(QueryOptions.FACET)
                && org.apache.commons.lang3.StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            try {
                FacetQueryParser facetQueryParser = new FacetQueryParser();

                String facetQuery = parser.parseFacet(queryOptions.getString(QueryOptions.FACET));
                String jsonFacet = facetQueryParser.parse(facetQuery, queryOptions);

                solrQuery.set("json.facet", jsonFacet);
                solrQuery.setRows(0);
                solrQuery.setStart(0);
                solrQuery.setFields();

                logger.debug(">>>>>> Solr Facet: " + solrQuery.toString());
            } catch (Exception e) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Solr parse exception: " + e.getMessage(), e);
            }
        }

        SolrCollection solrCollection = solrManager.getCollection(collection);
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

    public SolrManager getSolrManager() {
        return solrManager;
    }

    public RgaEngine setSolrManager(SolrManager solrManager) {
        this.solrManager = solrManager;
        return this;
    }
}
