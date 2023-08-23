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

package com.zettagenomics.opencga.enterprise.cva;

import com.zettagenomics.opencga.enterprise.cva.converters.*;
import com.zettagenomics.opencga.enterprise.cva.exceptions.CvaException;
import com.zettagenomics.opencga.enterprise.cva.iterators.ClinicalVariantNativeSolrIterator;
import com.zettagenomics.opencga.enterprise.cva.iterators.ClinicalVariantSolrIterator;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalAnalysisSearch;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalVariantEvidenceSearch;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalVariantSearch;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalInterpretationSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantIterator;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jtarraga on 11/11/17.
 */
public class ClinicalVariantSolrEngine implements ClinicalVariantEngine {

    private SolrManager solrManager;

    private ClinicalAnalysisConverter caConverter;
    private ClinicalInterpretationConverter ciConverter;
    private ClinicalVariantConverter cvConverter;
    private ClinicalVariantEvidenceConverter cveConverter;

    private ClinicalQueryParser queryParser;

    private Logger logger;

    public static final String ANALYSIS_COLLECTION = "analysis";
    public static final String INTERPRETATIONS_COLLECTION = "interpretations";
    public static final String CLINICAL_VARIANTS_COLLECTION = "clinical_variants";
    public static final String CLINICAL_VARIANT_EVIDENCES_COLLECTION = "clinical_variant_evidences";

    public static final String ANALYSIS_CONFIGSET = "opencga-ca-configset-" + GitRepositoryState.getInstance().getBuildVersion();
    public static final String INTERPRETATION_CONFIGSET = "opencga-ci-configset-" + GitRepositoryState.getInstance().getBuildVersion();
    public static final String CLINICAL_VARIANT_CONFIGSET = "opencga-cv-configset-" + GitRepositoryState.getInstance().getBuildVersion();
    public static final String CLINICAL_VARIANT_EVIDENCE_CONFIGSET = "opencga-cve-configset-" + GitRepositoryState.getInstance().getBuildVersion();

    private static final String CONF_SET = "ClinicalConfSet";
    private static final int DEFAULT_LIMIT = 1000000;

    public ClinicalVariantSolrEngine() {
        init();
    }

    public ClinicalVariantSolrEngine(VariantStorageMetadataManager variantStorageMetadataManager, StorageConfiguration storageConfig) {
        solrManager = new SolrManager(storageConfig.getSearch().getHost(), storageConfig.getSearch().getMode(),
                storageConfig.getSearch().getTimeout());

        this.queryParser = new ClinicalQueryParser(variantStorageMetadataManager);

        init();
    }

    private void init() {
        this.caConverter = new ClinicalAnalysisConverter();
        this.ciConverter = new ClinicalInterpretationConverter();
        this.cvConverter = new ClinicalVariantConverter();
        this.cveConverter = new ClinicalVariantEvidenceConverter();

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

    public void insert(ClinicalAnalysis clinicalAnalysis) throws CvaException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            UpdateResponse updateResponse;

            // Clinical analysis
            ClinicalAnalysisSearch clinicalAnalysisSearch = caConverter.toClinicalAnalysisSearch(clinicalAnalysis);
            updateResponse = solrClient.addBean(ANALYSIS_COLLECTION, clinicalAnalysisSearch);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Interpretations
            if (clinicalAnalysis.getInterpretation() != null) {
                insert(clinicalAnalysis.getInterpretation(), true, solrClient);
            }
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                    insert(secondaryInterpretation, false, solrClient);
                }
            }

            // Commit
            solrClient.commit(ANALYSIS_COLLECTION);
            solrClient.commit(INTERPRETATIONS_COLLECTION);
            solrClient.commit(CLINICAL_VARIANTS_COLLECTION);
            solrClient.commit(CLINICAL_VARIANT_EVIDENCES_COLLECTION);
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    public void insert(Interpretation interpretation, boolean primary) throws CvaException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            UpdateResponse updateResponse;

            // Interpretation
            ClinicalInterpretationSearch clinicalInterpretationSearch = ciConverter.toInterpretationSearch(interpretation, primary);
            updateResponse = solrClient.addBean(INTERPRETATIONS_COLLECTION, clinicalInterpretationSearch);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                insert(interpretation.getPrimaryFindings(), true, interpretation.getId(), interpretation.getClinicalAnalysisId(),
                        solrClient);
            }

            // Commit
            solrClient.commit(INTERPRETATIONS_COLLECTION);
            solrClient.commit(CLINICAL_VARIANTS_COLLECTION);
            solrClient.commit(CLINICAL_VARIANT_EVIDENCES_COLLECTION);
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    public void insert(ClinicalVariant clinicalVariant, boolean primary, String interpretationId, String clinicalAnalysisId)
            throws CvaException {
        insert(Collections.singletonList(clinicalVariant), primary, interpretationId, clinicalAnalysisId);
    }

    public void insert(List<ClinicalVariant> clinicalVariants, boolean primary, String interpretationId, String clinicalAnalysisId)
            throws CvaException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            UpdateResponse updateResponse;

            // Clinical variants
            List<ClinicalVariantSearch> cvsList = cvConverter.toClinicalVariantSearch(clinicalVariants, primary, interpretationId,
                    clinicalAnalysisId);
            updateResponse = solrClient.addBeans(CLINICAL_VARIANTS_COLLECTION, cvsList);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Clinical variant evidences
            for (ClinicalVariant clinicalVariant : clinicalVariants) {
                if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                    insert(clinicalVariant.getEvidences(), clinicalVariant.getId(), interpretationId, clinicalAnalysisId, solrClient);
                }
            }

            // Commit
            solrClient.commit(CLINICAL_VARIANTS_COLLECTION);
            solrClient.commit(CLINICAL_VARIANT_EVIDENCES_COLLECTION);
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    public void insert(ClinicalVariantEvidence clinicalVariantEvidence, String variantId, String interpretationId,
                       String clinicalAnalysisId) throws CvaException {
        insert(Collections.singletonList(clinicalVariantEvidence), variantId, interpretationId, clinicalAnalysisId);
    }

    public void insert(List<ClinicalVariantEvidence> cveList, String variantId, String interpretationId, String clinicalAnalysisId)
            throws CvaException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            UpdateResponse updateResponse;

            // Clinical variant evidences
            List<ClinicalVariantEvidenceSearch> cvesList = cveConverter.toClinicalVariantEvidenceSearch(cveList, variantId,
                    interpretationId, clinicalAnalysisId);
            updateResponse = solrClient.addBeans(CLINICAL_VARIANT_EVIDENCES_COLLECTION, cvesList);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Commit
            solrClient.commit(CLINICAL_VARIANT_EVIDENCES_COLLECTION);
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void insert(Interpretation interpretation, boolean isPrimary, SolrClient solrClient)
            throws CvaException {
        try {
            UpdateResponse updateResponse;

            // Interpretation
            ClinicalInterpretationSearch clinicalInterpretationSearch = ciConverter.toInterpretationSearch(interpretation, isPrimary);
            updateResponse = solrClient.addBean(INTERPRETATIONS_COLLECTION, clinicalInterpretationSearch);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Clinical variants
            if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                insert(interpretation.getPrimaryFindings(), true, interpretation.getId(), interpretation.getClinicalAnalysisId(),
                        solrClient);
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void insert(List<ClinicalVariant> clinicalVariants, boolean primary, String interpretationId, String clinicalAnalysisId,
                        SolrClient solrClient) throws CvaException {
        try {
            UpdateResponse updateResponse;

            // Clinical variants
            List<ClinicalVariantSearch> cvsList = cvConverter.toClinicalVariantSearch(clinicalVariants, primary, interpretationId,
                    clinicalAnalysisId);
            updateResponse = solrClient.addBeans(CLINICAL_VARIANTS_COLLECTION, cvsList);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }

            for (ClinicalVariant clinicalVariant : clinicalVariants) {
                if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                    insert(clinicalVariant.getEvidences(), clinicalVariant.getId(), interpretationId, clinicalAnalysisId, solrClient);
                }
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void insert(List<ClinicalVariantEvidence> clinicalVariantEvidences, String variantId, String interpretationId,
                        String clinicalAnalysisId, SolrClient solrClient) throws CvaException {
        try {
            UpdateResponse updateResponse;

            // Clinical variants
            List<ClinicalVariantEvidenceSearch> cveList = cveConverter.toClinicalVariantEvidenceSearch(clinicalVariantEvidences,
                    variantId, interpretationId, clinicalAnalysisId);
            updateResponse = solrClient.addBeans(CLINICAL_VARIANT_EVIDENCES_COLLECTION, cveList);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void rollback(SolrClient solrClient, int status) throws CvaException {
        try {
            solrClient.rollback();
            throw new CvaException("Error when adding Solr documents (status = " + status + ")");
        } catch (SolrServerException | IOException e) {
            throw new CvaException("Error when rollingback Solr after adding documents (status = " + status + ")", e);
        }
    }

    private void rollback(SolrClient solrClient, Exception exception) throws CvaException {
        try {
            solrClient.rollback();
            throw new CvaException("Solr exception", exception);
        } catch (SolrServerException | IOException e) {
            throw new CvaException("Error when rollingback after Solr exception (" + exception.getMessage() + ")", e);
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
    @Override
    public void insert(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation, String collection)
            throws IOException, ClinicalVariantException {
//        List<ClinicalVariantSearch> clinicalVariantSearches;
//        clinicalVariantSearches = interpretaionConverter.toClinicalVariantSearchList(interpretation);
//
//        if (CollectionUtils.isNotEmpty(clinicalVariantSearches)) {
//            UpdateResponse updateResponse;
//            try {
//                updateResponse = solrManager.getSolrClient().addBeans(collection, clinicalVariantSearches);
//                if (updateResponse.getStatus() == 0) {
//                    solrManager.getSolrClient().commit(collection);
//                }
//            } catch (SolrServerException e) {
//                throw new ClinicalVariantException(e.getMessage(), e);
//            }
//        }
    }

    @Override
    public void insert(List<org.opencb.biodata.models.clinical.interpretation.Interpretation> interpretations, String collection)
            throws IOException, ClinicalVariantException {
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
    public DataResult<ClinicalVariant> query(Query query, QueryOptions options, String collection)
            throws IOException, ClinicalVariantException {
        return  null;
//        int limit = options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
//        if (limit > DEFAULT_LIMIT) {
//            limit = DEFAULT_LIMIT;
//        }
//        options.put(QueryOptions.LIMIT, limit);
//
//        List<ClinicalVariant> results = new ArrayList<>(limit);
//
//        StopWatch stopWatch = StopWatch.createStarted();
//        ClinicalVariantIterator iterator = iterator(query, options, collection);
//        while (iterator.hasNext()) {
//            results.add(iterator.next());
//        }
//        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
//
//        return new DataResult<>(dbTime, null, results.size(), results, results.size());
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
    public DataResult<org.opencb.biodata.models.clinical.interpretation.Interpretation> interpretationQuery(Query query,
                                                                                                            QueryOptions options,
                                                                                                            String collection)
            throws IOException, ClinicalVariantException {
        return null;
//        int limit = options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
//        if (limit > DEFAULT_LIMIT) {
//            limit = DEFAULT_LIMIT;
//        }
//        options.put(QueryOptions.LIMIT, limit);
//
//        // Make sure that query is sorted by Interpretation ID in order to group ReportedVariant objects
//        // belonging to the same Interpretation
//        options.put(QueryOptions.SORT, "intId");
//        options.put(QueryOptions.ORDER, QueryOptions.ASCENDING);
//
//        StopWatch stopWatch = StopWatch.createStarted();
//        ClinicalVariantNativeSolrIterator iterator = nativeIterator(query, options, collection);
//
//        List<org.opencb.biodata.models.clinical.interpretation.Interpretation> results = new ArrayList<>(limit);
//        String currentIntId = null;
//        List<ClinicalVariantSearch> clinicalVariantSearches = new ArrayList<>();
//        while (iterator.hasNext()) {
//            ClinicalVariantSearch clinicalVariantSearch = iterator.next();
//            if (currentIntId != null && clinicalVariantSearch.getCiId() != currentIntId) {
//                Interpretation interpretation = interpretaionConverter.toInterpretation(clinicalVariantSearches);
//                results.add(interpretation);
//                clinicalVariantSearches.clear();
//            }
//            clinicalVariantSearches.add(clinicalVariantSearch);
//            currentIntId = clinicalVariantSearch.getCiId();
//        }
//        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
//        return new DataResult<>(dbTime, null, results.size(), results, results.size());
    }

    @Override
    public DataResult<FacetField> facet(Query query, QueryOptions queryOptions, String s) throws IOException, ClinicalVariantException {
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
    public QueryResult<ClinicalVariantSearch> nativeQuery(Query query, QueryOptions queryOptions, String collection)
            throws IOException, ClinicalVariantException {
        int limit = queryOptions.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
        if (limit > DEFAULT_LIMIT) {
            limit = DEFAULT_LIMIT;
        }
        queryOptions.put(QueryOptions.LIMIT, limit);

        List<ClinicalVariantSearch> results = new ArrayList<>(limit);

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalVariantNativeSolrIterator iterator = nativeIterator(query, queryOptions, collection);
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
    public void addInterpretationComment(long interpretationId, ClinicalComment comment, String collection)
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
                    map.put("add", ConverterUtils.encodeComent(comment));
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
    public void addClinicalVariantComment(long interpretationId, String variantId, ClinicalComment comment, String collection) {

    }

    @Override
    public void setStorageConfiguration(StorageConfiguration storageConfiguration) {

    }

    public void addReportedVariantComment(long interpretationId, String variantId, ClinicalComment comment, String collection)
            throws IOException, ClinicalVariantException {

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        solrQuery.addFilterQuery("(intId:\"" + interpretationId + "\" AND id:\"" + variantId + "\")");
        try {
            QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
            if (CollectionUtils.isNotEmpty(solrResponse.getResults())) {
                for (SolrDocument solrDocument: solrResponse.getResults()) {
                    SolrInputDocument solrInputDocument = new SolrInputDocument();
                    solrInputDocument.addField("id", solrDocument.getFieldValue("id"));
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("add", ConverterUtils.encodeComent(comment));
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

