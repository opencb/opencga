/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.DatabaseCredentials;
import org.opencb.opencga.core.models.study.Study;
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
public class CatalogSolrManager implements AutoCloseable {

    private CatalogManager catalogManager;
    private SolrManager solrManager;
    private int insertBatchSize;
    private String DATABASE_PREFIX;

    public static final int DEFAULT_INSERT_BATCH_SIZE = 2000;
    public static final String COHORT_SOLR_COLLECTION = "catalog-cohort";
    public static final String FILE_SOLR_COLLECTION = "catalog-file";
    public static final String FAMILY_SOLR_COLLECTION = "catalog-family";
    public static final String INDIVIDUAL_SOLR_COLLECTION = "catalog-individual";
    public static final String SAMPLE_SOLR_COLLECTION = "catalog-sample";
    public static final String JOB_SOLR_COLLECTION = "catalog-job";

    public static final String COHORT_CONF_SET = "opencga-cohort-configset";
    public static final String FILE_CONF_SET = "opencga-file-configset";
    public static final String FAMILY_CONF_SET = "opencga-family-configset";
    public static final String INDIVIDUAL_CONF_SET = "opencga-individual-configset";
    public static final String SAMPLE_CONF_SET = "opencga-sample-configset";
    public static final String JOB_CONF_SET = "opencga-job-configset";
    private final Map<String, String> CONFIGS_COLLECTION = new HashMap<>();

    private Logger logger;

    public CatalogSolrManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        DatabaseCredentials searchConfiguration = catalogManager.getConfiguration().getCatalog().getSearchEngine();
        String mode = searchConfiguration.getOptions().getOrDefault("mode", "cloud");
        int timeout = Integer.parseInt(searchConfiguration.getOptions().getOrDefault("timeout", "30000"));
        int tmpInsertBatchSize = Integer.parseInt(searchConfiguration.getOptions().getOrDefault("insertBatchSize",
                String.valueOf(DEFAULT_INSERT_BATCH_SIZE)));
        insertBatchSize = tmpInsertBatchSize > 0 ? tmpInsertBatchSize : DEFAULT_INSERT_BATCH_SIZE;
        this.solrManager = new SolrManager(searchConfiguration.getHosts(), mode, timeout);

        DATABASE_PREFIX = catalogManager.getConfiguration().getDatabasePrefix() + "-";

        populateConfigCollectionMap();

        logger = LoggerFactory.getLogger(CatalogSolrManager.class);
    }

    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
    }

    public void create(String dbName, String configSet) {
        solrManager.create(dbName, configSet);
    }

    public void createCore(String coreName, String configSet) {
        solrManager.createCore(coreName, configSet);
    }

    public void createCollection(String collectionName, String configSet) {
        solrManager.createCollection(collectionName, configSet);
    }

    public boolean exists(String dbName) {
        return solrManager.exists(dbName);
    }

    public boolean existsCore(String coreName) {
        return solrManager.existsCore(coreName);
    }

    public boolean existsCollection(String collectionName) {
        return solrManager.existsCollection(collectionName);
    }

    public void createSolrCollections(String solrCollection) throws CatalogException {
        if (!CONFIGS_COLLECTION.containsKey(DATABASE_PREFIX + solrCollection)) {
            throw new CatalogException("Cannot create solr collection '" + solrCollection + "'. Solr collection does not exist.");
        }

        if (catalogManager.getConfiguration().getCatalog().getSearchEngine().getOptions().getOrDefault("mode", "cloud").equals("cloud")) {
            createCatalogSolrCollections(solrCollection);
        } else {
            createCatalogSolrCores(solrCollection);
        }
    }

    private void createCatalogSolrCollections(String solrCollection) {
        String key = DATABASE_PREFIX + solrCollection;
        if (!existsCollection(key)) {
            createCollection(key, CONFIGS_COLLECTION.get(key));
        }
    }

    private void createCatalogSolrCores(String solrCollection) {
        String key = DATABASE_PREFIX + solrCollection;
        if (!existsCore(key)) {
            createCore(key, CONFIGS_COLLECTION.get(key));
        }
    }

    public <T> void insertCatalogCollection(DBIterator<T> iterator, ComplexTypeConverter converter, String collectionName)
            throws CatalogException {

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

    public <T, M> void insertCatalogCollection(List<T> records, ComplexTypeConverter converter, String collectionName)
            throws CatalogException {
        List<M> solrModels = new ArrayList<>();

        for (T record : records) {
            solrModels.add((M) converter.convertToStorageType(record));
        }

        UpdateResponse updateResponse;
        try {
            updateResponse = solrManager.getSolrClient().addBeans(DATABASE_PREFIX + collectionName, solrModels);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(DATABASE_PREFIX + collectionName);
            } else {
                throw new CatalogException(updateResponse.getException());
            }
        } catch (IOException | SolrServerException e) {
            throw new CatalogException(e.getMessage(), e);
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
     * @throws CatalogException CatalogException
     */
    @Deprecated
    public DataResult<FacetField> facetedQuery(String collection, Query query, QueryOptions queryOptions)
            throws IOException, CatalogException {
        CatalogSolrQueryParser catalogSolrQueryParser = new CatalogSolrQueryParser();
        SolrQuery solrQuery = catalogSolrQueryParser.parse(query, queryOptions, null);

        SolrCollection solrCollection = solrManager.getCollection(DATABASE_PREFIX + collection);
        try {
            return solrCollection.facet(solrQuery, catalogSolrQueryParser.getAliasMap());
        } catch (SolrServerException e) {
            throw new CatalogException(e.getMessage(), e);
        }
    }

    /**
     * Return faceted data from a Solr core/collection according to a given query.
     *
     * @param study        Study
     * @param collection   Collection name
     * @param query        Query
     * @param queryOptions Query options (contains the facet and facetRange options)
     * @param userId       User performing the facet query.
     * @return Facet results
     * @throws CatalogException CatalogException
     */
    public DataResult<FacetField> facetedQuery(Study study, String collection, Query query, QueryOptions queryOptions, String userId)
            throws CatalogException {
        Query queryCopy = query == null ? new Query() : new Query(query);
        QueryOptions queryOptionsCopy = queryOptions == null ? new QueryOptions() : new QueryOptions(queryOptions);

        queryCopy.put(CatalogSolrQueryParser.QueryParams.STUDY.key(), study.getFqn());

        if (!catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId)) {
            // We need to add an acl query to perform the facet query
            List<String> groups = new ArrayList<>();
            study.getGroups().forEach(group -> {
                if (group.getUserIds().contains(userId) || group.getUserIds().contains(AbstractManager.ANONYMOUS)) {
                    groups.add(group.getId());
                }
            });

            final String suffixPermission;
            if (queryCopy.containsKey(CatalogSolrQueryParser.QueryParams.ANNOTATIONS.key())) {
                suffixPermission = "__VIEW_ANNOTATIONS";
            } else {
                suffixPermission = "__VIEW";
            }
            List<String> aclList = new ArrayList<>();
            if (AbstractManager.ANONYMOUS.equals(userId)) {
                // We need to escape that user ( * )
                aclList.add("\\" + userId + suffixPermission);
                groups.forEach(group -> aclList.add("(*:* -\\" + userId + "__NONE AND " + group + suffixPermission + ")"));
            } else {
                aclList.add(userId + suffixPermission);
                groups.forEach(group -> aclList.add("(*:* -" + userId + "__NONE AND " + group + suffixPermission + ")"));
                // Add anonymous user as well. If anonymous user can see, userId should be able to see
                aclList.add("\\" + AbstractManager.ANONYMOUS + suffixPermission);
            }

            queryCopy.put(CatalogSolrQueryParser.QueryParams.ACL.key(), "(" + StringUtils.join(aclList, " OR ") + ")");
        }

        CatalogSolrQueryParser catalogSolrQueryParser = new CatalogSolrQueryParser();
        SolrQuery solrQuery = catalogSolrQueryParser.parse(queryCopy, queryOptionsCopy, study.getVariableSets());

        SolrCollection solrCollection = solrManager.getCollection(DATABASE_PREFIX + collection);
        try {
            return solrCollection.facet(solrQuery, catalogSolrQueryParser.getAliasMap());
        } catch (SolrServerException | IOException e) {
            throw new CatalogException(e.getMessage(), e);
        }
    }

    //***************** PRIVATE ****************/

    private void populateConfigCollectionMap() {
        String version = GitRepositoryState.getInstance().getBuildVersion();

        CONFIGS_COLLECTION.put(DATABASE_PREFIX + COHORT_SOLR_COLLECTION, COHORT_CONF_SET + "-" + version);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + FILE_SOLR_COLLECTION, FILE_CONF_SET + "-" + version);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + FAMILY_SOLR_COLLECTION, FAMILY_CONF_SET + "-" + version);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + INDIVIDUAL_SOLR_COLLECTION, INDIVIDUAL_CONF_SET + "-" + version);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + SAMPLE_SOLR_COLLECTION, SAMPLE_CONF_SET + "-" + version);
        CONFIGS_COLLECTION.put(DATABASE_PREFIX + JOB_SOLR_COLLECTION, JOB_CONF_SET + "-" + version);
    }

    protected void setSolrClient(SolrClient solrClient) {
        solrManager.setSolrClient(solrClient);
    }

    @Override
    public void close() throws CatalogException {
        try {
            solrManager.close();
        } catch (IOException e) {
            throw new CatalogException(e);
        }
    }
}

