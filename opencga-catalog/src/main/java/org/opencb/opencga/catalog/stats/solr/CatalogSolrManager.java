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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.SolrManager;
import org.opencb.opencga.core.config.SearchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSolrManager {

    private CatalogManager catalogManager;
    private SolrManager solrManager;
    private int insertBatchSize;

    public static final int DEFAULT_INSERT_BATCH_SIZE = 2000;

    public static final String COHORT_SOLR_COLLECTION = "Catalog_Cohort_Collection";
    public static final String FILE_SOLR_COLLECTION = "Catalog_FILE_Collection";
    public static final String FAMILY_SOLR_COLLECTION = "Catalog_Family_Collection";
    public static final String INDIVIDUAL_SOLR_COLLECTION = "Catalog_Individual_Collection";
    public static final String SAMPLES_SOLR_COLLECTION = "Catalog_Sample_Collection";

    public static final String COHORT_CONF_SET = "OpenCGACatalogCohortConfSet";
    public static final String FILE_CONF_SET = "OpenCGACatalogFileConfSet";
    public static final String SAMPLE_CONF_SET = "OpenCGACatalogSampleConfSet";
    public static final String FAMILY_CONF_SET = "OpenCGACatalogFamilyConfSet";
    public static final String INDIVIDUAL_CONF_SET = "OpenCGACatalogIndividualConfSet";

    private Logger logger;

    public CatalogSolrManager(CatalogManager catalogManager) throws SolrException {
        this.catalogManager = catalogManager;
        SearchConfiguration searchConfiguration = catalogManager.getConfiguration().getCatalog().getSearch();
        this.solrManager = new SolrManager(searchConfiguration.getHost(), searchConfiguration.getMode(), searchConfiguration.getTimeout());
        insertBatchSize = searchConfiguration.getInsertBatchSize() > 0
                ? searchConfiguration.getInsertBatchSize() : DEFAULT_INSERT_BATCH_SIZE;

        if (searchConfiguration.getMode().equals("cloud")) {
            createCatalogSolrCollections();
        } else {
            createCatalogSolrCores();
        }

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
        if (!existsCollection(COHORT_SOLR_COLLECTION)) {
            createCollection(COHORT_SOLR_COLLECTION, COHORT_CONF_SET);
        }
        if (!existsCollection(FAMILY_SOLR_COLLECTION)) {
            createCollection(FAMILY_SOLR_COLLECTION, FAMILY_CONF_SET);
        }
        if (!existsCollection(FILE_SOLR_COLLECTION)) {
            createCollection(FILE_SOLR_COLLECTION, FILE_CONF_SET);
        }
        if (!existsCollection(INDIVIDUAL_SOLR_COLLECTION)) {
            createCollection(INDIVIDUAL_SOLR_COLLECTION, INDIVIDUAL_CONF_SET);
        }
        if (!existsCollection(SAMPLES_SOLR_COLLECTION)) {
            createCollection(SAMPLES_SOLR_COLLECTION, SAMPLE_CONF_SET);
        }
    }

    public void createCatalogSolrCores() throws SolrException {
        if (!existsCore(COHORT_SOLR_COLLECTION)) {
            createCore(COHORT_SOLR_COLLECTION, COHORT_CONF_SET);
        }
        if (!existsCore(FAMILY_SOLR_COLLECTION)) {
            createCore(FAMILY_SOLR_COLLECTION, FAMILY_CONF_SET);
        }
        if (!existsCore(FILE_SOLR_COLLECTION)) {
            createCore(FILE_SOLR_COLLECTION, FILE_CONF_SET);
        }
        if (!existsCore(INDIVIDUAL_SOLR_COLLECTION)) {
            createCore(INDIVIDUAL_SOLR_COLLECTION, INDIVIDUAL_CONF_SET);
        }
        if (!existsCore(SAMPLES_SOLR_COLLECTION)) {
            createCore(SAMPLES_SOLR_COLLECTION, SAMPLE_CONF_SET);
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
            solrModels.add((M) converter.convertToStorageType(record));
        }

        UpdateResponse updateResponse;
        try {
            updateResponse = solrManager.getSolrClient().addBeans(collectionName, solrModels);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(collectionName);
            }
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }
    }
}

