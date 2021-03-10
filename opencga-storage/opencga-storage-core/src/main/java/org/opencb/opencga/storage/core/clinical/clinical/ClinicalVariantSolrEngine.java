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

package org.opencb.opencga.storage.core.clinical.clinical;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantIterator;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jtarraga on 11/11/17.
 */
public class ClinicalVariantSolrEngine implements ClinicalVariantEngine {

    private SolrManager solrManager;
    private StorageConfiguration storageConfiguration;

    private InterpretationConverter interpretaionConverter;
    private ClinicalQueryParser queryParser;

    private Logger logger;

    private static final String CONF_SET = "ClinicalConfSet";
    private static final int DEFAULT_LIMIT = 1000000;

    public ClinicalVariantSolrEngine(VariantStorageMetadataManager storageMedataManager, StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
        solrManager = new SolrManager(storageConfiguration.getSearch().getHost(), storageConfiguration.getSearch().getMode(),
                storageConfiguration.getSearch().getTimeout());

        this.interpretaionConverter = new InterpretationConverter();
        this.queryParser = new ClinicalQueryParser(storageMedataManager);

        this.logger = LoggerFactory.getLogger(ClinicalVariantSolrEngine.class);
    }

    @Override
    public void create(String dbName) throws ClinicalVariantException {
        try {
            solrManager.create(dbName, CONF_SET);
        } catch (SolrException e) {
            throw new ClinicalVariantException("", e);
        }
    }

    @Override
    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
    }

    @Override
    public boolean exists(String dbName) throws ClinicalVariantException {
        try {
            return solrManager.exists(dbName);
        } catch (SolrException e) {
            throw new ClinicalVariantException("", e);
        }
    }


    /**
     * Insert an Interpretation object into Solr: previously the Interpretation object is
     * converted to multiple ClinicalVariantSearchModel objects and they will be stored in Solr.
     *
     * @param interpretation    Interpretation object to insert
     * @param collection        Solr collection where to insert
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
    @Override
    public void insert(Interpretation interpretation, String collection) throws IOException, ClinicalVariantException {
        List<ClinicalVariantSearchModel> clinicalVariantSearchModels;
        clinicalVariantSearchModels = interpretaionConverter.toReportedVariantSearchList(interpretation);

        if (ListUtils.isNotEmpty(clinicalVariantSearchModels)) {
            UpdateResponse updateResponse;
            try {
                updateResponse = solrManager.getSolrClient().addBeans(collection, clinicalVariantSearchModels);
                if (updateResponse.getStatus() == 0) {
                    solrManager.getSolrClient().commit(collection);
                }
            } catch (SolrServerException e) {
                throw new ClinicalVariantException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void insert(List<Interpretation> interpretations, String collection) throws IOException, ClinicalVariantException {

    }

    @Override
    public void insert(Path interpretationJsonPath, String collection) throws IOException, ClinicalVariantException {

    }


    /**
     * Return the list of ClinicalVariant objects from a Solr core/collection according a given query.
     *
     * @param query        Query
     * @param options Query options
     * @param collection   Collection name
     * @return             List of ClinicalVariant objects
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   VariantSearchException
     */
    @Override
    public DataResult<ClinicalVariant> query(Query query, QueryOptions options, String collection)
            throws IOException, ClinicalVariantException {
        int limit = options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
        if (limit > DEFAULT_LIMIT) {
            limit = DEFAULT_LIMIT;
        }
        options.put(QueryOptions.LIMIT, limit);

        List<ClinicalVariant> results = new ArrayList<>(limit);

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalVariantIterator iterator = iterator(query, options, collection);
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new DataResult<>(dbTime, null, results.size(), results, results.size());
    }

    /**
     * Return the list of Interpretation objects from a Solr core/collection according a given query.
     *
     * @param query         Query
     * @param options  Query options
     * @param collection    Collection name
     * @return              List of Interpretation objects
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   VariantSearchException
     */
    @Override
    public DataResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String collection)
            throws IOException, ClinicalVariantException {
        int limit = options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
        if (limit > DEFAULT_LIMIT) {
            limit = DEFAULT_LIMIT;
        }
        options.put(QueryOptions.LIMIT, limit);

        // Make sure that query is sorted by Interpretation ID in order to group ReportedVariant objects
        // belonging to the same Interpretation
        options.put(QueryOptions.SORT, "intId");
        options.put(QueryOptions.ORDER, QueryOptions.ASCENDING);

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalVariantNativeSolrIterator iterator = nativeIterator(query, options, collection);

        List<Interpretation> results = new ArrayList<>(limit);
        String currentIntId = null;
        List<ClinicalVariantSearchModel> clinicalVariantSearchModels = new ArrayList<>();
        while (iterator.hasNext()) {
            ClinicalVariantSearchModel clinicalVariantSearchModel = iterator.next();
            if (currentIntId != null && clinicalVariantSearchModel.getIntId() != currentIntId) {
                Interpretation interpretation = interpretaionConverter.toInterpretation(clinicalVariantSearchModels);
                results.add(interpretation);
                clinicalVariantSearchModels.clear();
            }
            clinicalVariantSearchModels.add(clinicalVariantSearchModel);
            currentIntId = clinicalVariantSearchModel.getIntId();
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        return new DataResult<>(dbTime, null, results.size(), results, results.size());
    }

    @Override
    public DataResult<FacetField> facet(Query query, QueryOptions queryOptions, String s) throws IOException, ClinicalVariantException {
        return null;
    }

    /**
     * Return the list of ClinicalVariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param query        Query
     * @param queryOptions Query options
     * @param collection   Collection name
     * @return List of VariantSearchModel objects
     * @throws IOException            IOException
     * @throws ClinicalVariantException ClinicalVariantException
     */
    public DataResult<ClinicalVariantSearchModel> nativeQuery(Query query, QueryOptions queryOptions, String collection)
            throws IOException, ClinicalVariantException {
        int limit = queryOptions.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
        if (limit > DEFAULT_LIMIT) {
            limit = DEFAULT_LIMIT;
        }
        queryOptions.put(QueryOptions.LIMIT, limit);

        List<ClinicalVariantSearchModel> results = new ArrayList<>(limit);

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalVariantNativeSolrIterator iterator = nativeIterator(query, queryOptions, collection);
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new DataResult<>(dbTime, null, results.size(), results, results.size());
    }

    /**
     * Return a Solr ClinicalVariant iterator to retrieve ReportedVariant objects from a Solr
     * core/collection according a given query.
     *
     * @param query        Query
     * @param options Query options
     * @param collection   Collection name
     * @return Solr ClinicalVariant iterator
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
    @Override
    public ClinicalVariantIterator iterator(Query query, QueryOptions options, String collection)
            throws ClinicalVariantException, IOException {
        try {
            SolrQuery solrQuery = queryParser.parse(query, options);
            return new ClinicalVariantSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new ClinicalVariantException(e.getMessage(), e);
        }
    }

    /**
     * Return a Solr ReportedVariantSearch iterator to retrieve ClinicalVariantSearchModel objects from a Solr
     * core/collection according a given query.
     *
     * @param query        Query
     * @param queryOptions Query options
     * @param collection   Collection name
     * @return Solr ReportedVariantSearch iterator
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
    public ClinicalVariantNativeSolrIterator nativeIterator(Query query, QueryOptions queryOptions, String collection)
            throws ClinicalVariantException, IOException {
        try {
            SolrQuery solrQuery = queryParser.parse(query, queryOptions);
            return new ClinicalVariantNativeSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new ClinicalVariantException(e.getMessage(), e);
        }
    }


    @Override
    public void addInterpretationComment(long interpretationId, ClinicalComment comment, String collection) throws IOException,
            ClinicalVariantException {
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();

        query.put("intId", interpretationId);
        SolrQuery solrQuery = queryParser.parse(query, queryOptions);
        try {
            org.apache.solr.client.solrj.response.QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
            if (ListUtils.isNotEmpty(solrResponse.getResults())) {
                for (SolrDocument solrDocument: solrResponse.getResults()) {
                    SolrInputDocument solrInputDocument = new SolrInputDocument();
                    solrInputDocument.addField("id", solrDocument.getFieldValue("id"));
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("add", InterpretationConverter.encodeComent(comment));
                    solrInputDocument.addField("intComments", map);
                    solrManager.getSolrClient().add(collection, solrInputDocument);
                    solrManager.getSolrClient().commit(collection);
                }
            } else {
                throw new ClinicalVariantException("Error adding Interpretation comment: Interpretation with ID "
                        + interpretationId + " does not exist");
            }
        } catch (SolrServerException e) {
            throw new ClinicalVariantException("Error adding Interpretation comment: " + e);
        }
    }

    @Override
    public void addClinicalVariantComment(long interpretationId, String variantId, ClinicalComment comment, String collection)
            throws IOException, ClinicalVariantException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        solrQuery.addFilterQuery("(intId:\"" + interpretationId + "\" AND id:\"" + variantId + "\")");
        try {
            org.apache.solr.client.solrj.response.QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
            if (CollectionUtils.isNotEmpty(solrResponse.getResults())) {
                for (SolrDocument solrDocument: solrResponse.getResults()) {
                    SolrInputDocument solrInputDocument = new SolrInputDocument();
                    solrInputDocument.addField("id", solrDocument.getFieldValue("id"));
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("add", InterpretationConverter.encodeComent(comment));
                    solrInputDocument.addField("comments", map);
                    solrManager.getSolrClient().add(collection, solrInputDocument);
                    solrManager.getSolrClient().commit(collection);
                }
            } else {
                throw new ClinicalVariantException("Error adding ReportedVariant comment: reported variant for "
                        + " variant ID = " + variantId + " and interpretation ID = " + interpretationId
                        + " does not exit");
            }
        } catch (SolrServerException e) {
            throw new ClinicalVariantException("Error adding Interpretation comment: " + e);
        }
    }

    @Override
    public void setStorageConfiguration(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public SolrManager getSolrManager() {
        return solrManager;
    }

    public ClinicalVariantSolrEngine setSolrManager(SolrManager solrManager) {
        this.solrManager = solrManager;
        return this;
    }

    public SolrClient getSolrClient() {
        return solrManager.getSolrClient();
    }

    public ClinicalVariantSolrEngine setSolrClient(SolrClient solrClient) {
        this.solrManager.setSolrClient(solrClient);
        return this;
    }
}

