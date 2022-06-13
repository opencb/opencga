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

package org.opencb.opencga.clinical;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.clinical.Comment;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.ReportedVariant;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ReportedVariantIterator;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
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

    private InterpretationConverter interpretaionConverter;
    private ClinicalQueryParser queryParser;

    private Logger logger;

    private static final String CONF_SET = "ClinicalConfSet";
    private static final int DEFAULT_LIMIT = 1000000;

    public ClinicalVariantSolrEngine(StudyConfigurationManager studyConfigurationManager, StorageConfiguration storageConfig) {
        solrManager = new SolrManager(storageConfig.getSearch().getHost(), storageConfig.getSearch().getMode(),
                storageConfig.getSearch().getTimeout());

        this.interpretaionConverter = new InterpretationConverter();
        this.queryParser = new ClinicalQueryParser(studyConfigurationManager);

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
     * converted to multiple ReportedVariantSearchModel objects and they will be stored in Solr.
     *
     * @param interpretation    Interpretation object to insert
     * @param collection        Solr collection where to insert
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
//    @Override
    public void insert(Interpretation interpretation, String collection) throws IOException, ClinicalVariantException {
        List<ReportedVariantSearchModel> reportedVariantSearchModels;
        reportedVariantSearchModels = interpretaionConverter.toReportedVariantSearchList(interpretation);

        if (ListUtils.isNotEmpty(reportedVariantSearchModels)) {
            UpdateResponse updateResponse;
            try {
                updateResponse = solrManager.getSolrClient().addBeans(collection, reportedVariantSearchModels);
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
     * Return the list of ReportedVariant objects from a Solr core/collection according a given query.
     *
     * @param query        Query
     * @param options Query options
     * @param collection   Collection name
     * @return             List of ReportedVariant objects
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   VariantSearchException
     */
    @Override
    public QueryResult<ReportedVariant> query(Query query, QueryOptions options, String collection)
            throws IOException, ClinicalVariantException {
        int limit = options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
        if (limit > DEFAULT_LIMIT) {
            limit = DEFAULT_LIMIT;
        }
        options.put(QueryOptions.LIMIT, limit);

        List<ReportedVariant> results = new ArrayList<>(limit);

        StopWatch stopWatch = StopWatch.createStarted();
        ReportedVariantIterator iterator = iterator(query, options, collection);
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new QueryResult<>("", dbTime, results.size(), results.size(), "Data from Solr", "", results);
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
    public QueryResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String collection)
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
        ReportedVariantNativeSolrIterator iterator = nativeIterator(query, options, collection);

        List<Interpretation> results = new ArrayList<>(limit);
        String currentIntId = null;
        List<ReportedVariantSearchModel> reportedVariantSearchModels = new ArrayList<>();
        while (iterator.hasNext()) {
            ReportedVariantSearchModel reportedVariantSearchModel = iterator.next();
            if (currentIntId != null && reportedVariantSearchModel.getIntId() != currentIntId) {
                Interpretation interpretation = interpretaionConverter.toInterpretation(reportedVariantSearchModels);
                results.add(interpretation);
                reportedVariantSearchModels.clear();
            }
            reportedVariantSearchModels.add(reportedVariantSearchModel);
            currentIntId = reportedVariantSearchModel.getIntId();
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        return new QueryResult<>("", dbTime, results.size(), results.size(), "Data from Solr", "", results);
    }

    @Override
    public FacetedQueryResult facet(Query query, QueryOptions queryOptions, String s) throws IOException, ClinicalVariantException {
        return null;
    }

    /**
     * Return the list of ReportedVariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param query        Query
     * @param queryOptions Query options
     * @param collection   Collection name
     * @return List of VariantSearchModel objects
     * @throws IOException            IOException
     * @throws ClinicalVariantException ClinicalVariantException
     */
    public QueryResult<ReportedVariantSearchModel> nativeQuery(Query query, QueryOptions queryOptions, String collection)
            throws IOException, ClinicalVariantException {
        int limit = queryOptions.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
        if (limit > DEFAULT_LIMIT) {
            limit = DEFAULT_LIMIT;
        }
        queryOptions.put(QueryOptions.LIMIT, limit);

        List<ReportedVariantSearchModel> results = new ArrayList<>(limit);

        StopWatch stopWatch = StopWatch.createStarted();
        ReportedVariantNativeSolrIterator iterator = nativeIterator(query, queryOptions, collection);
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new QueryResult<>("", dbTime, results.size(), results.size(), "Data from Solr", "", results);
    }

    /**
     * Return a Solr ReportedVariant iterator to retrieve ReportedVariant objects from a Solr
     * core/collection according a given query.
     *
     * @param query        Query
     * @param options Query options
     * @param collection   Collection name
     * @return Solr ReportedVariant iterator
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
    @Override
    public ReportedVariantIterator iterator(Query query, QueryOptions options, String collection)
            throws ClinicalVariantException, IOException {
        try {
            SolrQuery solrQuery = queryParser.parse(query, options);
            return new ReportedVariantSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new ClinicalVariantException(e.getMessage(), e);
        }
    }

    /**
     * Return a Solr ReportedVariantSearch iterator to retrieve ReportedVariantSearchModel objects from a Solr
     * core/collection according a given query.
     *
     * @param query        Query
     * @param queryOptions Query options
     * @param collection   Collection name
     * @return Solr ReportedVariantSearch iterator
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
    public ReportedVariantNativeSolrIterator nativeIterator(Query query, QueryOptions queryOptions, String collection)
            throws ClinicalVariantException, IOException {
        try {
            SolrQuery solrQuery = queryParser.parse(query, queryOptions);
            return new ReportedVariantNativeSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new ClinicalVariantException(e.getMessage(), e);
        }
    }


    @Override
    public void addInterpretationComment(long interpretationId, Comment comment, String collection)
            throws IOException, ClinicalVariantException {
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();

        query.put("intId", interpretationId);
        SolrQuery solrQuery = queryParser.parse(query, queryOptions);
        try {
            QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
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
    public void addReportedVariantComment(long interpretationId, String variantId, Comment comment, String collection)
            throws IOException, ClinicalVariantException {

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        solrQuery.addFilterQuery("(intId:\"" + interpretationId + "\" AND id:\"" + variantId + "\")");
        try {
            QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
            if (ListUtils.isNotEmpty(solrResponse.getResults())) {
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

