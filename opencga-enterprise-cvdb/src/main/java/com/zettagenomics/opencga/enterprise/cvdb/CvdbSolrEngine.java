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

package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.core.configuration.CvdbConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalAnalysisConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalInterpretationConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantEvidenceConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.analysis.rga.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by jtarraga on 11/11/17.
 */
public class CvdbSolrEngine {

    private SolrManager solrManager;

    private ClinicalAnalysisConverter caConverter;
    private ClinicalInterpretationConverter ciConverter;
    private ClinicalVariantConverter cvConverter;
    private ClinicalVariantEvidenceConverter cveConverter;

    private ClinicalQueryParser queryParser;

    private Logger logger;

    public static final String CLINICAL_ANALYSES_COLLECTION_SUFFIX = "_cvdb_analyses";
    public static final String INTERPRETATIONS_COLLECTION_SUFFIX = "_cvdb_interpretations";
    public static final String CLINICAL_VARIANTS_COLLECTION_SUFFIX = "_cvdb_variants";
    public static final String CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX = "_cvdb_evidences";

    public static final List<String> COLLECTION_SUFFIXES = Arrays.asList(CLINICAL_ANALYSES_COLLECTION_SUFFIX,
            INTERPRETATIONS_COLLECTION_SUFFIX,
            CLINICAL_VARIANTS_COLLECTION_SUFFIX,
            CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);

    public static final String CLINICAL_ANALYSIS_CONFIGSET = "opencga-ca-configset-"
            + GitRepositoryState.load("com/zettagenomics/opencga/enterprise/git-enterprise.properties").getBuildVersion();
    public static final String INTERPRETATION_CONFIGSET = "opencga-ci-configset-"
            + GitRepositoryState.load("com/zettagenomics/opencga/enterprise/git-enterprise.properties").getBuildVersion();
    public static final String CLINICAL_VARIANT_CONFIGSET = "opencga-cv-configset-"
            + GitRepositoryState.load("com/zettagenomics/opencga/enterprise/git-enterprise.properties").getBuildVersion();
    public static final String CLINICAL_VARIANT_EVIDENCE_CONFIGSET = "opencga-cve-configset-"
            + GitRepositoryState.load("com/zettagenomics/opencga/enterprise/git-enterprise.properties").getBuildVersion();

    public static final List<String> COLLECTION_CONFIGSETS = Arrays.asList(CLINICAL_ANALYSIS_CONFIGSET,
            INTERPRETATION_CONFIGSET,
            CLINICAL_VARIANT_CONFIGSET,
            CLINICAL_VARIANT_EVIDENCE_CONFIGSET);

    public static final String WITHOUT_ID = "-234";

    public CvdbSolrEngine() {
        init();
    }

    public CvdbSolrEngine(CvdbConfiguration cvdbConfig, VariantStorageMetadataManager variantStorageMetadataManager) {
        solrManager = new SolrManager(cvdbConfig.getDatabase().getHosts(), cvdbConfig.getDatabase().getMode(),
                cvdbConfig.getDatabase().getTimeout());

        this.queryParser = new ClinicalQueryParser(variantStorageMetadataManager);

        init();
    }

    private void init() {
        this.caConverter = new ClinicalAnalysisConverter();
        this.ciConverter = new ClinicalInterpretationConverter();
        this.cvConverter = new ClinicalVariantConverter();
        this.cveConverter = new ClinicalVariantEvidenceConverter();

        this.logger = LoggerFactory.getLogger(CvdbSolrEngine.class);
    }

    //----------------------------------------------------------------------
    // P U B L I C      M E T H O D S
    //----------------------------------------------------------------------

    public static String getCollectionName(String projectId, String suffix) {
        return "opencga_" + projectId + suffix;
    }

    public CvdbIndexResult index(String projectId, CatalogManager catalogManager, boolean overwrite, String sessionIdUser)
            throws CatalogException, CvdbException {

        int numIndexed = 0;
        Map<String, String> failures = new HashMap<>();
        StopWatch stopWatch = StopWatch.createStarted();

        OpenCGAResult<Study> studyResults = catalogManager.getStudyManager().search(projectId, new Query(), QueryOptions.empty(),
                sessionIdUser);
        List<String> studyIds = studyResults.getResults().stream().map(s -> s.getId()).collect(Collectors.toList());
        for (String studyId : studyIds) {
            OpenCGAResult<ClinicalAnalysis> caResults = catalogManager.getClinicalAnalysisManager().search(studyId, new Query(),
                    QueryOptions.empty(), sessionIdUser);
            for (ClinicalAnalysis clinicalAnalysis : caResults.getResults()) {
                try {
                    if (index(clinicalAnalysis, projectId, overwrite)) {
                        numIndexed++;
                    } else {
                        String key = clinicalAnalysis.getId() + "(" + projectId + ":" + studyId + ")";
                        failures.put(key, "Skipping index (overwrite is set to false)");
                    }
                } catch (Exception e) {
                    String key = clinicalAnalysis.getId() + "(" + projectId + ":" + studyId + ")";
                    failures.put(key, e.getMessage());
                }
            }
        }
        stopWatch.stop();

        return new CvdbIndexResult(numIndexed, failures, (int) stopWatch.getTime(TimeUnit.SECONDS));
    }

    public boolean index(ClinicalAnalysis clinicalAnalysis, String projectId, boolean overwrite) throws CvdbException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            // Index
            if (index(clinicalAnalysis, projectId, overwrite, solrClient)) {
                // Commit
                solrClient.commit(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
                return true;
            }
        } catch (SolrServerException | IOException e) {
            logger.warn("Clinical analysis {}: {}", clinicalAnalysis.getInterpretation(), e.getMessage());
            rollback(solrClient, e);
            return false;
        }
        return false;
    }

    //    private void index(List<ClinicalAnalysis> clinicalAnalysisList, String projectId) throws CvdbException {
//        SolrClient solrClient = solrManager.getSolrClient();
//
//        // Index
//        for (ClinicalAnalysis clinicalAnalysis : clinicalAnalysisList) {
//            index(clinicalAnalysis, projectId, solrClient);
//        }
//    }

//    public void index(List<ClinicalVariant> clinicalVariants, boolean primary, String projectId) throws CvdbException {
//        SolrClient solrClient = solrManager.getSolrClient();
//
//        try {
//            UpdateResponse updateResponse;
//
//            // Clinical variants
//            List<ClinicalVariantSearch> cvsList = cvConverter.toClinicalVariantSearch(clinicalVariants, primary, WITHOUT_ID, WITHOUT_ID);
//            updateResponse = solrClient.addBeans(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX), cvsList);
//            if (updateResponse.getStatus() != 0) {
//                rollback(solrClient, updateResponse.getStatus());
//            }
//
//            // Clinical variant evidences
//            for (ClinicalVariant clinicalVariant : clinicalVariants) {
//                if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
//                    index(clinicalVariant.getEvidences(), clinicalVariant.getId(), WITHOUT_ID, WITHOUT_ID, projectId, solrClient);
//                }
//            }
//
//            // Commit
//            solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
//            solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
//        } catch (SolrServerException | IOException e) {
//            rollback(solrClient, e);
//        }
//    }

    //----------------------------------------------------------------------
    // P R I V A T E      M E T H O D S
    //----------------------------------------------------------------------

    private boolean index(ClinicalAnalysis clinicalAnalysis, String projectId, boolean overwrite, SolrClient solrClient)
            throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical analysis
            ClinicalAnalysisSearch clinicalAnalysisSearch = caConverter.toClinicalAnalysisSearch(clinicalAnalysis);

            boolean exists;
            if (overwrite) {
                exists = false;
            } else {
                try {
                    exists = clinicalAnalysisExists(clinicalAnalysis.getId(), getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                            solrClient);
                } catch (SolrServerException | IOException e) {
                    logger.warn("Could not index clinical analysis {}: {}", clinicalAnalysis.getId(), e.getMessage());
                    return false;
                }
            }

            if (!exists) {
                    // Index
                    updateResponse = solrClient.addBean(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX), clinicalAnalysisSearch);
                    if (updateResponse.getStatus() != 0) {
                        rollback(solrClient, updateResponse.getStatus());
                    }

                    // Interpretations
                    if (clinicalAnalysis.getInterpretation() != null) {
                        index(clinicalAnalysis.getInterpretation(), true, projectId, solrClient);
                    }
                    if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                        for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                            index(secondaryInterpretation, false, projectId, solrClient);
                        }
                    }
                } else {
                    logger.warn("Skipping clinical analysis {}, it was already indexed", clinicalAnalysis.getId());
                    return false;
                }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
            return false;
        }

        return true;
    }

//    private void index(Interpretation interpretation, boolean primary, String projectId) throws CvdbException {
//        SolrClient solrClient = solrManager.getSolrClient();
//
//        try {
//            UpdateResponse updateResponse;
//
//            // Interpretation
//            ClinicalInterpretationSearch clinicalInterpretationSearch = ciConverter.toInterpretationSearch(interpretation, primary);
//            updateResponse = solrClient.addBean(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX),
//                    clinicalInterpretationSearch);
//            if (updateResponse.getStatus() != 0) {
//                rollback(solrClient, updateResponse.getStatus());
//            }
//
//            if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
//                index(interpretation.getPrimaryFindings(), true, interpretation.getId(), interpretation.getClinicalAnalysisId(), projectId,
//                        solrClient);
//            }
//
//            // Commit
//            solrClient.commit(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX));
//            solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
//            solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
//        } catch (SolrServerException | IOException e) {
//            rollback(solrClient, e);
//        }
//    }

    private void index(ClinicalVariantEvidence clinicalVariantEvidence, String variantId, String interpretationId,
                       String clinicalAnalysisId, String projectId) throws CvdbException {
        index(Collections.singletonList(clinicalVariantEvidence), variantId, interpretationId, clinicalAnalysisId, projectId);
    }

    private void index(List<ClinicalVariantEvidence> cveList, String variantId, String interpretationId, String clinicalAnalysisId,
                       String projectId) throws CvdbException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            UpdateResponse updateResponse;

            // Clinical variant evidences
            List<ClinicalVariantEvidenceSearch> cvesList = cveConverter.toClinicalVariantEvidenceSearch(cveList, variantId,
                    interpretationId, clinicalAnalysisId);
            updateResponse = solrClient.addBeans(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX), cvesList);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Commit
            solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(Interpretation interpretation, boolean isPrimary, String projectId, SolrClient solrClient) throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Interpretation
            ClinicalInterpretationSearch clinicalInterpretationSearch = ciConverter.toInterpretationSearch(interpretation, isPrimary);
            updateResponse = solrClient.addBean(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX),
                    clinicalInterpretationSearch);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Clinical variants
            if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                index(interpretation.getPrimaryFindings(), true, interpretation.getId(), interpretation.getClinicalAnalysisId(), projectId,
                        solrClient);
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(List<ClinicalVariant> clinicalVariants, boolean primary, String interpretationId, String clinicalAnalysisId,
                       String projectId, SolrClient solrClient) throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical variants
            List<ClinicalVariantSearch> cvsList = cvConverter.toClinicalVariantSearch(clinicalVariants, primary, interpretationId,
                    clinicalAnalysisId);
            updateResponse = solrClient.addBeans(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX), cvsList);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }

            for (ClinicalVariant clinicalVariant : clinicalVariants) {
                if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                    index(clinicalVariant.getEvidences(), clinicalVariant.getId(), interpretationId, clinicalAnalysisId, projectId,
                            solrClient);
                }
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(List<ClinicalVariantEvidence> clinicalVariantEvidences, String variantId, String interpretationId,
                       String clinicalAnalysisId, String projectId, SolrClient solrClient) throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical variants
            List<ClinicalVariantEvidenceSearch> cveList = cveConverter.toClinicalVariantEvidenceSearch(clinicalVariantEvidences,
                    variantId, interpretationId, clinicalAnalysisId);
            updateResponse = solrClient.addBeans(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX), cveList);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void rollback(SolrClient solrClient, int status) throws CvdbException {
        try {
            solrClient.rollback();
            throw new CvdbException("Error when adding Solr documents (status = " + status + ")");
        } catch (SolrServerException | IOException e) {
            throw new CvdbException("Error when rollingback Solr after adding documents (status = " + status + ")", e);
        }
    }

    private void rollback(SolrClient solrClient, Exception exception) throws CvdbException {
        try {
            solrClient.rollback();
            throw new CvdbException("Solr exception", exception);
        } catch (SolrServerException | IOException e) {
            throw new CvdbException("Error when rollingback after Solr exception (" + exception.getMessage() + ")", e);
        }
    }


//    /**
//     * Return the list of ReportedVariant objects from a Solr core/collection according a given query.
//     *
//     * @param query       Query
//     * @param options     Query options
//     * @param collection  Collection name
//     * @return List of ReportedVariant objects
//     * @throws IOException   IOException
//     * @throws CvdbException CvdbException
//     */
//    public DataResult<ClinicalVariant> query(Query query, QueryOptions options, String collection)
//            throws IOException, CvdbException {
//        return  null;
////        int limit = options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
////        if (limit > DEFAULT_LIMIT) {
////            limit = DEFAULT_LIMIT;
////        }
////        options.put(QueryOptions.LIMIT, limit);
////
////        List<ClinicalVariant> results = new ArrayList<>(limit);
////
////        StopWatch stopWatch = StopWatch.createStarted();
////        ClinicalVariantIterator iterator = iterator(query, options, collection);
////        while (iterator.hasNext()) {
////            results.add(iterator.next());
////        }
////        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
////
////        return new DataResult<>(dbTime, null, results.size(), results, results.size());
//    }
//
//    /**
//     * Return the list of Interpretation objects from a Solr core/collection according a given query.
//     *
//     * @param query      Query
//     * @param options    Query options
//     * @param collection Collection name
//     * @return List of Interpretation objects
//     * @throws IOException   IOException
//     * @throws CvdbException CvdbException
//     */
//    public DataResult<org.opencb.biodata.models.clinical.interpretation.Interpretation> interpretationQuery(Query query,
//                                                                                                            QueryOptions options,
//                                                                                                            String collection)
//            throws CvdbException, IOException {
//        return interpretationQuery(query, options);
//    }
//    public DataResult<org.opencb.biodata.models.clinical.interpretation.Interpretation> interpretationQuery(Query query,
//                                                                                                            QueryOptions options)
//            throws CvdbException, IOException {
//        int limit = options.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
//        if (limit > DEFAULT_LIMIT) {
//            limit = DEFAULT_LIMIT;
//        }
//        options.put(QueryOptions.LIMIT, limit);
//
//        StopWatch stopWatch = StopWatch.createStarted();
//        InterpretationNativeSolrIterator iterator = interpreationNativeIterator(query, options);
//
//        List<org.opencb.biodata.models.clinical.interpretation.Interpretation> results = new ArrayList<>(limit);
//        while (iterator.hasNext()) {
//            ClinicalInterpretationSearch next = iterator.next();
//            results.add(ciConverter.toInterpretation(next));
//        }
//        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
//        return new DataResult<>(dbTime, null, results.size(), results, results.size());
//    }
//
//    public DataResult<FacetField> facet(Query query, QueryOptions queryOptions, String s) throws IOException, CvdbException {
//        return null;
//    }
//
//    /**
//     * Return the list of ReportedVariantSearchModel objects from a Solr core/collection
//     * according a given query.
//     *
//     * @param query        Query
//     * @param queryOptions Query options
//     * @param collection   Collection name
//     * @return List of VariantSearchModel objects
//     * @throws IOException   IOException
//     * @throws CvdbException CvdbException
//     */
//    public QueryResult<ClinicalVariantSearch> nativeQuery(Query query, QueryOptions queryOptions, String collection)
//            throws IOException, CvdbException {
//        int limit = queryOptions.getInt(QueryOptions.LIMIT, DEFAULT_LIMIT);
//        if (limit > DEFAULT_LIMIT) {
//            limit = DEFAULT_LIMIT;
//        }
//        queryOptions.put(QueryOptions.LIMIT, limit);
//
//        List<ClinicalVariantSearch> results = new ArrayList<>(limit);
//
//        StopWatch stopWatch = StopWatch.createStarted();
//        ClinicalVariantNativeSolrIterator iterator = nativeIterator(query, queryOptions, collection);
//        while (iterator.hasNext()) {
//            results.add(iterator.next());
//        }
//        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
//
//        return new QueryResult<>("", dbTime, results.size(), results.size(), "Data from Solr", "", results);
//    }
//
//    /**
//     * Return a Solr ReportedVariant iterator to retrieve ReportedVariant objects from a Solr
//     * core/collection according a given query.
//     *
//     * @param query      Query
//     * @param options    Query options
//     * @param collection Collection name
//     * @return Solr ReportedVariant iterator
//     * @throws IOException   IOException
//     * @throws CvdbException CvdbException
//     */
//    public ClinicalVariantIterator iterator(Query query, QueryOptions options, String collection)
//            throws CvdbException, IOException {
//        try {
//            SolrQuery solrQuery = queryParser.parse(query, options);
//            return new ClinicalVariantSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
//        } catch (SolrServerException e) {
//            throw new CvdbException(e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Return a Solr ReportedVariantSearch iterator to retrieve ReportedVariantSearchModel objects from a Solr
//     * core/collection according a given query.
//     *
//     * @param query        Query
//     * @param queryOptions Query options
//     * @param collection   Collection name
//     * @return Solr ReportedVariantSearch iterator
//     * @throws IOException   IOException
//     * @throws CvdbException CvdbException
//     */
//    public ClinicalVariantNativeSolrIterator nativeIterator(Query query, QueryOptions queryOptions, String collection)
//            throws CvdbException, IOException {
//        try {
//            SolrQuery solrQuery = queryParser.parse(query, queryOptions);
//            return new ClinicalVariantNativeSolrIterator(solrManager.getSolrClient(), collection, solrQuery);
//        } catch (SolrServerException e) {
//            throw new CvdbException(e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Return a Solr ClinicalInterpretationSearch iterator to retrieve ClinicalInterpreationSearch objects according a given query.
//     *
//     * @param query        Query
//     * @param queryOptions Query options
//     * @return Solr ReportedVariantSearch iterator
//     * @throws IOException   IOException
//     * @throws CvdbException CvdbException
//     */
//    public InterpretationNativeSolrIterator interpreationNativeIterator(Query query, QueryOptions queryOptions)
//            throws CvdbException, IOException {
//        try {
//            SolrQuery solrQuery = queryParser.parse(query, queryOptions);
//            return new InterpretationNativeSolrIterator(solrManager.getSolrClient(), solrQuery);
//        } catch (SolrServerException e) {
//            throw new CvdbException(e.getMessage(), e);
//        }
//    }
//
//    @Override
//    public void addInterpretationComment(long interpretationId, ClinicalComment comment, String collection)
//            throws IOException, CvdbException {
//        Query query = new Query();
//        QueryOptions queryOptions = new QueryOptions();
//
//        query.put("intId", interpretationId);
//        SolrQuery solrQuery = queryParser.parse(query, queryOptions);
//        try {
//            QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
//            if (ListUtils.isNotEmpty(solrResponse.getResults())) {
//                for (SolrDocument solrDocument: solrResponse.getResults()) {
//                    SolrInputDocument solrInputDocument = new SolrInputDocument();
//                    solrInputDocument.addField("id", solrDocument.getFieldValue("id"));
//                    HashMap<String, Object> map = new HashMap<>();
//                    map.put("add", ConverterUtils.encodeComent(comment));
//                    solrInputDocument.addField("intComments", map);
//                    solrManager.getSolrClient().add(collection, solrInputDocument);
//                    solrManager.getSolrClient().commit(collection);
//                }
//            } else {
//                throw new CvdbException("Error adding Interpretation comment: Interpretation with ID "
//                        + interpretationId + " does not exist");
//            }
//        } catch (SolrServerException e) {
//            throw new CvdbException("Error adding Interpretation comment: " + e);
//        }
//    }
//
//    public void addClinicalVariantComment(long interpretationId, String variantId, ClinicalComment comment, String collection) {
//
//    }
//
//    public void setStorageConfiguration(StorageConfiguration storageConfiguration) {
//
//    }
//
//    public void addReportedVariantComment(long interpretationId, String variantId, ClinicalComment comment, String collection)
//            throws IOException, CvdbException {
//
//        SolrQuery solrQuery = new SolrQuery();
//        solrQuery.setQuery("*:*");
//        solrQuery.addFilterQuery("(intId:\"" + interpretationId + "\" AND id:\"" + variantId + "\")");
//        try {
//            QueryResponse solrResponse = solrManager.getSolrClient().query(collection, solrQuery);
//            if (CollectionUtils.isNotEmpty(solrResponse.getResults())) {
//                for (SolrDocument solrDocument: solrResponse.getResults()) {
//                    SolrInputDocument solrInputDocument = new SolrInputDocument();
//                    solrInputDocument.addField("id", solrDocument.getFieldValue("id"));
//                    HashMap<String, Object> map = new HashMap<>();
//                    map.put("add", ConverterUtils.encodeComent(comment));
//                    solrInputDocument.addField("comments", map);
//                    solrManager.getSolrClient().add(collection, solrInputDocument);
//                    solrManager.getSolrClient().commit(collection);
//                }
//            } else {
//                throw new CvdbException("Error adding ReportedVariant comment: reported variant for "
//                        + " variant ID = " + variantId + " and interpretation ID = " + interpretationId
//                        + " does not exit");
//            }
//        } catch (SolrServerException e) {
//            throw new CvdbException("Error adding Interpretation comment: " + e);
//        }
//    }
//
//    public void create(String dbName) throws CvdbException {
//        try {
//            solrManager.create(dbName, CONF_SET);
//        } catch (SolrException e) {
//            throw new CvdbException("", e);
//        }
//    }
//
//    public boolean isAlive(String collection) {
//        return solrManager.isAlive(collection);
//    }
//
//    public boolean exists(String dbName) throws CvdbException {
//        try {
//            return solrManager.exists(dbName);
//        } catch (SolrException e) {
//            throw new CvdbException("", e);
//        }
//    }


    public boolean existCollections(String projectId) throws CvdbException {
        try {
            for (String suffix : COLLECTION_SUFFIXES) {
                if (!solrManager.exists(getCollectionName(projectId, suffix))) {
                    return false;
                }
            }
            return true;
        } catch (SolrException e) {
            throw new CvdbException("Checking if Solr collections exist for project " + projectId, e);
        }
    }

    public void createCollections(String projectId) throws CvdbException {
        try {
            for (int i = 0 ; i < COLLECTION_SUFFIXES.size() ; i++) {
                String name = getCollectionName(projectId, COLLECTION_SUFFIXES.get(i));
                if (!solrManager.exists(name)) {
                    solrManager.create(name, COLLECTION_CONFIGSETS.get(i));
                }
            }
        } catch (SolrException e) {
            throw new CvdbException("Creating Solr collections for project " + projectId, e);
        }
    }

    private boolean clinicalAnalysisExists(String clinicalAnalysisId, String collectionName, SolrClient solrClient)
            throws SolrServerException, IOException {
        // Build and execute the Solr query
        SolrQuery solrQuery = new SolrQuery("id:" + clinicalAnalysisId);
        solrQuery.setFields("id");
        QueryResponse response = solrClient.query(collectionName, solrQuery);

        return (response.getResults().getNumFound() == 1);
    }

    //----------------------------------------------------------------------
    // G E T T E R S     A N D      S E T T E R S
    //----------------------------------------------------------------------
    public SolrManager getSolrManager() {
        return solrManager;
    }

    public CvdbSolrEngine setSolrManager(SolrManager solrManager) {
        this.solrManager = solrManager;
        return this;
    }

    public SolrClient getSolrClient() {
        return solrManager.getSolrClient();
    }

    public CvdbSolrEngine setSolrClient(SolrClient solrClient) {
        this.solrManager.setSolrClient(solrClient);
        return this;
    }
}

