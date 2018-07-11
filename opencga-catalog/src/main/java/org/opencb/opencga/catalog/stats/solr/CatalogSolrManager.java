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

package org.opencb.opencga.catalog.stats.solr;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResultItem;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.stats.solr.converters.SolrFacetUtil;
import org.opencb.opencga.core.SolrManager;
import org.opencb.opencga.core.config.SearchConfiguration;
import org.opencb.opencga.core.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSolrManager {

    private CatalogManager catalogManager;
    private SolrManager solrManager;
    private int insertBatchSize;
    private String DATABASE_PREFIX = "opencga";

    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;
    public static final String COHORT_SOLR_COLLECTION = "Catalog_Cohort_Collection";
    public static final String FILE_SOLR_COLLECTION = "Catalog_File_Collection";
    public static final String FAMILY_SOLR_COLLECTION = "Catalog_Family_Collection";
    public static final String INDIVIDUAL_SOLR_COLLECTION = "Catalog_Individual_Collection";
    public static final String SAMPLES_SOLR_COLLECTION = "Catalog_Sample_Collection";

    public static final String COHORT_CONF_SET = "OpenCGACatalogCohortConfSet";
    public static final String FILE_CONF_SET = "OpenCGACatalogFileConfSet";
    public static final String FAMILY_CONF_SET = "OpenCGACatalogFamilyConfSet";
    public static final String INDIVIDUAL_CONF_SET = "OpenCGACatalogIndividualConfSet";
    public static final String SAMPLE_CONF_SET = "OpenCGACatalogSampleConfSet";
    public static final Map<String, String> CONFIGS_COLLECTION = new HashMap();
    private Map<Long, String> STUDIES_UID_TO_ID = new HashMap();

    private Logger logger;

    public CatalogSolrManager(CatalogManager catalogManager) throws SolrException, CatalogDBException {
        this.catalogManager = catalogManager;
        SearchConfiguration searchConfiguration = catalogManager.getConfiguration().getCatalog().getSearch();
        this.solrManager = new SolrManager(searchConfiguration.getHost(), searchConfiguration.getMode(), searchConfiguration.getTimeout());
        insertBatchSize = searchConfiguration.getInsertBatchSize() > 0
                ? searchConfiguration.getInsertBatchSize() : DEFAULT_INSERT_BATCH_SIZE;

        DATABASE_PREFIX = catalogManager.getConfiguration().getDatabasePrefix() + "_";

        populateConfigCollectionMap();

        if (searchConfiguration.getMode().equals("cloud")) {
            createCatalogSolrCollections();
        } else {
            createCatalogSolrCores();
        }

        STUDIES_UID_TO_ID = catalogManager.getStudyManager().getAllStudiesIdAndUid();

        logger = LoggerFactory.getLogger(CatalogSolrManager.class);
    }

    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
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

    public void createCatalogSolrCollections() throws SolrException {

        for (String key : CONFIGS_COLLECTION.keySet()) {
            if (!existsCollection(key)) {
                createCollection(key, CONFIGS_COLLECTION.get(key));
            }
        }
    }

    public void createCatalogSolrCores() throws SolrException {

        for (String key : CONFIGS_COLLECTION.keySet()) {
            if (!existsCore(key)) {
                createCore(key, CONFIGS_COLLECTION.get(key));
            }
        }
    }

    public <T> void insertCatalogCollection(DBIterator<T> iterator, ComplexTypeConverter converter,
                                            String collectionName) throws CatalogException, IOException, SolrException {

        int count = 0;
        List<T> records = new ArrayList<>(insertBatchSize);
        while (iterator.hasNext()) {
            T record = iterator.next();
            records.add(record);
            count++;
            if (count % insertBatchSize == 0) {
                insertCatalogCollection(records, converter, collectionName);
                records.clear();
            }
        }

        if (CollectionUtils.isNotEmpty(records)) {
            insertCatalogCollection(records, converter, collectionName);
        }
    }

    public <T, M> void insertCatalogCollection(List<T> records, ComplexTypeConverter converter,
                                               String collectionName) throws IOException, SolrException {
        List<M> solrModels = new ArrayList<>();

        for (T record : records) {
            M result = (M) converter.convertToStorageType(record);
            solrModels.add(setStudyId(record, result));
        }

        UpdateResponse updateResponse;
        try {
            updateResponse = solrManager.getSolrClient().addBeans(DATABASE_PREFIX + collectionName, solrModels);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(DATABASE_PREFIX + collectionName);
            }
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
     * @throws IOException   IOException
     * @throws SolrException SolrException
     */
    public FacetedQueryResult facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, SolrException {
        StopWatch stopWatch = StopWatch.createStarted();
        try {
            CatalogSolrQueryParser catalogSolrQueryParser = new CatalogSolrQueryParser();
            SolrQuery solrQuery = catalogSolrQueryParser.parse(query, queryOptions);
            QueryResponse response = solrManager.getSolrClient().query(collection, solrQuery);
            FacetedQueryResultItem item = SolrFacetUtil.toFacetedQueryResultItem(queryOptions, response);
            return new FacetedQueryResult("", (int) stopWatch.getTime(), 1, 1, "Faceted data from Solr", "", item);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }
    }

    //***************** PRIVATE ****************/

    private void populateConfigCollectionMap() {
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + COHORT_SOLR_COLLECTION, COHORT_CONF_SET);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + FILE_SOLR_COLLECTION, FILE_CONF_SET);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + FAMILY_SOLR_COLLECTION, FAMILY_CONF_SET);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + INDIVIDUAL_SOLR_COLLECTION, INDIVIDUAL_CONF_SET);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + SAMPLES_SOLR_COLLECTION, SAMPLE_CONF_SET);
    }

    private <T, M> M setStudyId(T record, M result) {

        String studyId;
        if (record instanceof Cohort) {
            studyId = STUDIES_UID_TO_ID.get(((Cohort) record).getStudyUid());
            return (M) ((CohortSolrModel) result).setStudyId(studyId);
        } else if (record instanceof File) {
            studyId = STUDIES_UID_TO_ID.get(((File) record).getStudyUid());
            return (M) ((FileSolrModel) result).setStudyId(studyId);
        } else if (record instanceof Sample) {
            studyId = STUDIES_UID_TO_ID.get(((Sample) record).getStudyUid());
            return (M) ((SampleSolrModel) result).setStudyId(studyId);
        } else if (record instanceof Individual) {
            studyId = STUDIES_UID_TO_ID.get(((Individual) record).getStudyUid());
            return (M) ((IndividualSolrModel) result).setStudyId(studyId);
        } else if (record instanceof Family) {
            studyId = STUDIES_UID_TO_ID.get(((Family) record).getStudyUid());
            return (M) ((FamilySolrModel) result).setStudyId(studyId);
        }
        return result;
    }
}

