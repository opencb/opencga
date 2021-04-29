package org.opencb.opencga.analysis.rga;

import javafx.scene.paint.Stop;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
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
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.analysis.rga.iterators.RgaIterator;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RgaEngine implements Closeable {

    private SolrManager solrManager;
    private RgaQueryParser parser;
    private IndividualRgaConverter individualRgaConverter;
    private GeneRgaConverter geneConverter;
    private VariantRgaConverter variantConverter;
    private StorageConfiguration storageConfiguration;
    private static Map<String, SolrCollection> solrCollectionMap;

    private Logger logger;

    static {
        solrCollectionMap = new HashMap<>();
    }

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
     * @param rgaDataModelList List of RgaDataModel to insert
     * @throws IOException   IOException
     * @throws SolrServerException SolrServerException
     */
    public void insert(String collection, List<RgaDataModel> rgaDataModelList) throws IOException, SolrServerException {
        if (CollectionUtils.isNotEmpty(rgaDataModelList)) {
            UpdateResponse updateResponse;
            updateResponse = solrManager.getSolrClient().addBeans(collection, rgaDataModelList);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(collection);
            }
        }
    }

    /**
     * Return an RgaDataModel iterator given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return RgaIterator.
     * @throws RgaException RgaException
     */
    public RgaIterator individualQuery(String collection, Query query, QueryOptions queryOptions) throws RgaException{
        SolrQuery solrQuery = parser.parseQuery(query);
        fixIndividualOptions(queryOptions, solrQuery);
//        solrQuery.setRows(Integer.MAX_VALUE);
        solrQuery.setRows(queryOptions.getInt(QueryOptions.LIMIT, Integer.MAX_VALUE));
        try {
            return new RgaIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new RgaException("Error executing KnockoutByIndividual query", e);
        }
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

    /**
     * Return an RgaDataModel iterator given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return RgaIterator.
     * @throws RgaException RgaException
     */
    public RgaIterator geneQuery(String collection, Query query, QueryOptions queryOptions) throws RgaException {
        SolrQuery solrQuery = parser.parseQuery(query);
        fixGeneOptions(queryOptions, solrQuery);
        solrQuery.setRows(Integer.MAX_VALUE);
        try {
            return new RgaIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new RgaException("Error executing RgaKnockoutByGene query", e);
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

    /**
     * Return an RgaDataModel iterator given a query.
     *
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options
     * @return RgaIterator object.
     * @throws RgaException RgaException
     */
    public RgaIterator variantQuery(String collection, Query query, QueryOptions queryOptions) throws RgaException {
        SolrQuery solrQuery = parser.parseQuery(query);
        fixVariantOptions(queryOptions, solrQuery);
        solrQuery.setRows(Integer.MAX_VALUE);
        try {
            return new RgaIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new RgaException("Error executing KnockoutByVariant query", e);
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
        SolrCollection solrCollection = getSolrCollection(collection);

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
        StopWatch stopWatch = StopWatch.createStarted();

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

        SolrCollection solrCollection = getSolrCollection(collection);
        DataResult<FacetField> facetResult;
        try {
            facetResult = solrCollection.facet(solrQuery, null);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }
        logger.info("Facet '{}': {} milliseconds", solrQuery.toString(), stopWatch.getTime(TimeUnit.MILLISECONDS));

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

    private SolrCollection getSolrCollection(String collection) {
        if (solrCollectionMap.containsKey(collection)) {
            return solrCollectionMap.get(collection);
        } else {
            SolrCollection solrCollection = solrManager.getCollection(collection);
            solrCollectionMap.put(collection, solrCollection);
            return solrCollection;
        }
    }
}
