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
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalIterator;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalSolrIterator;
import com.zettagenomics.opencga.enterprise.cvdb.models.*;
import com.zettagenomics.opencga.enterprise.cvdb.models.mappings.*;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.clinical.CvdbIndexStatus;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.opencb.commons.datastore.core.QueryOptions.*;
import static org.opencb.opencga.core.api.ParamConstants.ANONYMOUS_USER_ID;
import static org.opencb.opencga.core.models.clinical.CvdbIndexStatus.ERROR;
import static org.opencb.opencga.core.models.common.InternalStatus.READY;

/**
 * Created by jtarraga on 11/11/17.
 */
public class CvdbSolrEngine {

    private SolrManager solrManager;
    private CatalogManager catalogManager;
    private VariantStorageMetadataManager variantStorageMetadataManager;

    private Configuration configuration;
    private CvdbConfiguration cvdbConfiguration;

    private ClinicalAnalysisConverter caConverter;
    private ClinicalInterpretationConverter ciConverter;
    private ClinicalVariantConverter cvConverter;
    private ClinicalVariantEvidenceConverter cveConverter;

    private static final String GIT_ENTERPRISE_PROPERTIES = "com/zettagenomics/opencga/enterprise/git-enterprise.properties";
    private Logger logger;

    public static final String NO_ACCESS_FOR_ANONYMOUS_USERS_MSG = "Access to CVDB is restricted for anonymous users. Please log in to"
            + " proceed.";

    public static final String CLINICAL_ANALYSES_COLLECTION_SUFFIX = "_analyses";
    public static final String INTERPRETATIONS_COLLECTION_SUFFIX = "_interpretations";
    public static final String CLINICAL_VARIANTS_COLLECTION_SUFFIX = "_variants";
    public static final String CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX = "_evidences";

    protected static final List<String> COLLECTION_SUFFIXES = Arrays.asList(CLINICAL_ANALYSES_COLLECTION_SUFFIX,
            INTERPRETATIONS_COLLECTION_SUFFIX,
            CLINICAL_VARIANTS_COLLECTION_SUFFIX,
            CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);

    public static final String CLINICAL_ANALYSIS_CONFIGSET = "opencga-ca-configset-"
            + GitRepositoryState.load(GIT_ENTERPRISE_PROPERTIES).getBuildVersion();
    public static final String INTERPRETATION_CONFIGSET = "opencga-ci-configset-"
            + GitRepositoryState.load(GIT_ENTERPRISE_PROPERTIES).getBuildVersion();
    public static final String CLINICAL_VARIANT_CONFIGSET = "opencga-cv-configset-"
            + GitRepositoryState.load(GIT_ENTERPRISE_PROPERTIES).getBuildVersion();
    public static final String CLINICAL_VARIANT_EVIDENCE_CONFIGSET = "opencga-cve-configset-"
            + GitRepositoryState.load(GIT_ENTERPRISE_PROPERTIES).getBuildVersion();

    protected static final List<String> COLLECTION_CONFIGSETS = Arrays.asList(CLINICAL_ANALYSIS_CONFIGSET,
            INTERPRETATION_CONFIGSET,
            CLINICAL_VARIANT_CONFIGSET,
            CLINICAL_VARIANT_EVIDENCE_CONFIGSET);

    public static final String WITHOUT_ID = "-234";

    public CvdbSolrEngine(Configuration configuration, CvdbConfiguration cvdbConfiguration) {
        this.configuration = configuration;
        this.cvdbConfiguration = cvdbConfiguration;

        init();
    }

    public CvdbSolrEngine(CvdbConfiguration cvdbConfig, CatalogManager catalogManager, VariantStorageMetadataManager variantStorageMetadataManager) {
        this.configuration = catalogManager.getConfiguration();
        this.cvdbConfiguration = cvdbConfig;

        this.solrManager = new SolrManager(cvdbConfig.getDatabase().getHosts(), cvdbConfig.getDatabase().getMode(),
                cvdbConfig.getDatabase().getTimeout());
        this.catalogManager = catalogManager;
        this.variantStorageMetadataManager = variantStorageMetadataManager;

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

    public String getCollectionName(String organizationId, String projectId, String suffix) {
        return CvdbUtils.getCollectionName(configuration.getDatabasePrefix(), organizationId, projectId, suffix);
    }

    public CvdbIndexResult indexProject(String projectId, CatalogManager catalogManager, boolean overwrite, String token)
            throws CatalogException {
        logger.info("Loading all clinical analyses from project: '{}'", projectId);

        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromProject(projectId, jwtPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        CvdbIndexResult result = new CvdbIndexResult();

        // Start time
        StopWatch stopWatch = StopWatch.createStarted();

        // Main loop
        OpenCGAResult<Study> studyResults = catalogManager.getStudyManager().search(projectId, new Query(), QueryOptions.empty(),
                token);
        List<String> studyFqns = studyResults.getResults().stream().map(Study::getFqn).collect(Collectors.toList());
        for (String studyFqn : studyFqns) {
            CvdbIndexResult tmpResult = indexStudy(studyFqn, catalogManager, overwrite, token);
            result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
            result.getFailures().putAll(tmpResult.getFailures());
        }

        // Stop time
        stopWatch.stop();
        result.setTime((int) stopWatch.getTime(TimeUnit.SECONDS));

        return result;
    }

    public CvdbIndexResult indexStudy(String studyFqn, CatalogManager catalogManager, boolean overwrite, String token)
            throws CatalogException {
        logger.info("Loading all clinical analyses from study: '{}'", studyFqn);

        CvdbIndexResult result = new CvdbIndexResult();
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyFqn, jwtPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        // Start time
        StopWatch stopWatch = StopWatch.createStarted();

        // Get project for that study
        OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), token);
        Study study = studyResult.first();

        Query projectQuery = new Query();
        projectQuery.put(ProjectDBAdaptor.QueryParams.STUDY.key(), study.getFqn());
        catalogManager.getProjectManager().search(organizationId, projectQuery, QueryOptions.empty(), token);

        // Get all clinical analyses for that study
        QueryOptions queryOptions = new QueryOptions(INCLUDE, "id");
        DBIterator<ClinicalAnalysis> iterator = catalogManager.getClinicalAnalysisManager().iterator(study.getFqn(), new Query(),
                queryOptions, token);

        int listSize = 100;
        List<String> caIds = new ArrayList<>(listSize);
        while (iterator.hasNext()) {
            caIds.add(iterator.next().getId());
            if (caIds.size() == listSize) {
                CvdbIndexResult tmpResult = indexClinicalAnalyses(caIds, studyFqn, catalogManager, overwrite, token);
                result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
                result.getFailures().putAll(tmpResult.getFailures());

                // Reset list
                caIds.clear();
            }
        }

        // Check if there are still clinical analyses to index
        if (CollectionUtils.isNotEmpty(caIds)) {
            CvdbIndexResult tmpResult = indexClinicalAnalyses(caIds, studyFqn, catalogManager, overwrite, token);
            result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
            result.getFailures().putAll(tmpResult.getFailures());
        }

        // Stop time
        stopWatch.stop();
        result.setTime((int) stopWatch.getTime(TimeUnit.SECONDS));

        return result;
    }

    public CvdbIndexResult indexClinicalAnalyses(List<String> clinicalAnalysisIds, String studyFqn, CatalogManager catalogManager,
                                                 boolean overwrite, String token) throws CatalogException {
        logger.info("Loading {} clinical analyses from the input list", clinicalAnalysisIds.size());

        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyFqn, jwtPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        // Build caId-userId map, i.e., Map<Clinical Analysis ID, List<User ID>>
        Map<String, List<String>> caIdUserIdsMap = new HashMap<>();
        OpenCGAResult<Acl> aclResult = catalogManager.getAdminManager().getEffectivePermissions(studyFqn, clinicalAnalysisIds,
                Collections.singletonList(ClinicalAnalysisPermissions.VIEW.name()), Enums.Resource.CLINICAL_ANALYSIS.name(), token);
        for (Acl acl : aclResult.getResults()) {
            // Only one permission (VIEW) has been queried, so the first item has to be taken
            caIdUserIdsMap.put(acl.getId(), acl.getPermissions().get(0).getUserIds());
        }

        int numIndexed = 0;
        Map<String, String> failures = new HashMap<>();
        StopWatch stopWatch = StopWatch.createStarted();

        OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), token);
        Study study = studyResult.first();

        // Get project for that study
        Query projectQuery = new Query();
        projectQuery.put(ProjectDBAdaptor.QueryParams.STUDY.key(), study.getFqn());
        OpenCGAResult<Project> projectResult = catalogManager.getProjectManager().search(organizationId, projectQuery, QueryOptions.empty(),
                token);
        String projectId = projectResult.first().getId();

        // Get the input clinical analyses
        Query caQuery = new Query();
        for (String caId : clinicalAnalysisIds) {
            caQuery.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), caId);
            OpenCGAResult<ClinicalAnalysis> caResult = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), caQuery,
                    QueryOptions.empty(), token);
            if (caResult.getNumResults() == 1) {
                ClinicalAnalysis clinicalAnalysis = caResult.first();
                try {
                    if (index(clinicalAnalysis, organizationId, projectId, study.getFqn(), caIdUserIdsMap.get(caId), overwrite)) {
                        numIndexed++;
                        updateClinicalAnalysisCvdbIndexStatus(study.getFqn(), clinicalAnalysis, new CvdbIndexStatus(READY), token);
                    } else {
                        String key = caId + " (" + study.getFqn() + ")";
                        failures.put(key, "Skipping index (overwrite is set to false)");
                        logger.warn("{}: {}", key, failures.get(key));
                    }
                } catch (Exception e) {
                    String key = caId + " (" + study.getFqn() + ")";
                    failures.put(key, e.getMessage());
                    try {
                        updateClinicalAnalysisCvdbIndexStatus(study.getFqn(), clinicalAnalysis, new CvdbIndexStatus(ERROR, e.getMessage()),
                                token);
                    } catch (CvdbException ex) {
                        logger.warn("Error when indexing clinical analysis " + clinicalAnalysis.getId(), ex);
                    }
                }
            } else {
                String key = caId + " (" + study.getFqn() + ")";
                failures.put(key, "Num. results = " + caResult.getNumResults() + " when searching for clinical analysis: " + caId);
                logger.warn("{}: {}", key, failures.get(key));
            }
        }

        return new CvdbIndexResult(numIndexed, failures, (int) stopWatch.getTime(TimeUnit.SECONDS));
    }

    //----------------------------------------------------------------------
    // CLINICAL ANALYSIS: SEARCH, ITERATOR, FACET
    //----------------------------------------------------------------------

    public DataResult<ClinicalAnalysis> searchClinicalAnalyses(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
//        int limit = queryOptions.getInt(LIMIT);
        List<ClinicalAnalysis> results = new ArrayList<>();

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalIterator<ClinicalAnalysis, ClinicalAnalysisSearch, ClinicalAnalysisConverter> iterator = clinicalAnalysisIterator(query,
                queryOptions, token);
        while (iterator.hasNext()) {
            results.add(iterator.next());
//            if (results.size() == limit) {
//                break;
//            }
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new DataResult<>(dbTime, null, results.size(), results, iterator.getNumFound());
    }

    public ClinicalIterator<ClinicalAnalysis, ClinicalAnalysisSearch, ClinicalAnalysisConverter> clinicalAnalysisIterator(
            Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Parse query
        SolrQuery solrQuery = parseClinicalAnalysisQuery(query, queryOptions, token);

        // Execute query
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_ANALYSES_COLLECTION_SUFFIX);
            return new ClinicalIterator(solrManager.getSolrClient(), collection, solrQuery, queryOptions, ClinicalAnalysisSearch.class,
                    ClinicalAnalysisConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public ClinicalSolrIterator<ClinicalAnalysisSearch> clinicalAnalysisNativeIterator(
            Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Parse query
        SolrQuery solrQuery = parseClinicalAnalysisQuery(query, queryOptions, token);

        // Execute query
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_ANALYSES_COLLECTION_SUFFIX);
            return new ClinicalSolrIterator(getSolrClient(), collection, solrQuery, ClinicalAnalysisSearch.class);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<Long> clinicalAnalysisCount(Query query, String token)
            throws CvdbException, IOException, CatalogException {
        // Parse query
        SolrQuery solrQuery = parseClinicalAnalysisQuery(query, QueryOptions.empty(), token);

        String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                CLINICAL_ANALYSES_COLLECTION_SUFFIX);
        SolrCollection solrCollection = getSolrManager().getCollection(collection);
        // Execute query
        try {
            return solrCollection.count(solrQuery);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalAnalyses(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check
        checkFacet(query, queryOptions, token);

        // Parse query
        ClinicalAnalysisQueryParser parser = new ClinicalAnalysisQueryParser(catalogManager.getConfiguration().getDatabasePrefix(),
                variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_ANALYSES_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
//            postProcessing(facetResult, new CaFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    private SolrQuery parseClinicalAnalysisQuery(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, CvdbException {
        // Check
        checkQuery(query, queryOptions, token);

        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        ClinicalAnalysisQueryParser parser = new ClinicalAnalysisQueryParser(catalogManager.getConfiguration().getDatabasePrefix(),
                variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);
        if (queryOptions.containsKey(INCLUDE)) {
            List<String> includeList = new ArrayList<>();
            for (String include : queryOptions.getAsStringList(INCLUDE, ",")) {
                String[] split = include.split("\\.");
                if (split.length > 2 && (split[1].equals("primaryFindings") || split[1].equals("secondaryFindings"))) {
                    if (isImplClinicalVariantField(split[2])) {
                        StringBuilder sb = new StringBuilder(split[0]).append(".").append(split[1]).append(".impl");
                        for (int i = 2; i < split.length; i++) {
                            sb.append(".").append(split[i]);
                        }
                        includeList.add(sb.toString());
                    } else {
                        includeList.add(include);
                    }
                } else {
                    includeList.add(include);
                }
            }
            queryOptions.put(INCLUDE, StringUtils.join(includeList, ","));
        }
        return solrQuery;
    }

    //----------------------------------------------------------------------
    // CLINICAL INTERPRETATION: SEARCH, ITERATOR, FACET
    //----------------------------------------------------------------------

    public DataResult<Interpretation> searchClinicalInterpretations(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
//        int limit = queryOptions.getInt(LIMIT);
        List<Interpretation> results = new ArrayList<>();

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalIterator<Interpretation, ClinicalInterpretationSearch, ClinicalInterpretationConverter> iterator =
                clinicalInterpretationIterator(query, queryOptions, token);
        while (iterator.hasNext()) {
            results.add(iterator.next());
//            if (results.size() == limit) {
//                break;
//            }
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new DataResult<>(dbTime, null, results.size(), results, iterator.getNumFound());
    }

    public ClinicalIterator<Interpretation, ClinicalInterpretationSearch, ClinicalInterpretationConverter> clinicalInterpretationIterator(
            Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Check
        checkQuery(query, queryOptions, token);

        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        ClinicalInterpretationQueryParser parser = new ClinicalInterpretationQueryParser(
                catalogManager.getConfiguration().getDatabasePrefix(), variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);
        if (queryOptions.containsKey(INCLUDE)) {
            List<String> includeList = new ArrayList<>();
            for (String include : queryOptions.getAsStringList(INCLUDE, ",")) {
                String[] split = include.split("\\.");
                if (split.length > 1 && (split[0].equals("primaryFindings") || split[0].equals("secondaryFindings"))) {
                    if (isImplClinicalVariantField(split[1])) {
                        StringBuilder sb = new StringBuilder(split[0]).append(".impl");
                        for (int i = 1; i < split.length; i++) {
                            sb.append(".").append(split[i]);
                        }
                        includeList.add(sb.toString());
                    } else {
                        includeList.add(include);
                    }
                } else {
                    includeList.add(include);
                }
            }
            queryOptions.put(INCLUDE, StringUtils.join(includeList, ","));
        }

        // Execute query
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    INTERPRETATIONS_COLLECTION_SUFFIX);
            return new ClinicalIterator(solrManager.getSolrClient(), collection, solrQuery, queryOptions, ClinicalInterpretationSearch.class,
                    ClinicalInterpretationConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalInterpretations(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check
        checkFacet(query, queryOptions, token);

        // Parse query
        ClinicalInterpretationQueryParser parser = new ClinicalInterpretationQueryParser(
                catalogManager.getConfiguration().getDatabasePrefix(), variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    INTERPRETATIONS_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
//            postProcessing(facetResult, new CiFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    //----------------------------------------------------------------------
    // CLINICAL VARIANT: SEARCH, ITERATOR, FACET
    //----------------------------------------------------------------------

    public DataResult<ClinicalVariant> searchClinicalVariants(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
//        int limit = queryOptions.getInt(LIMIT);
        List<ClinicalVariant> results = new ArrayList<>();

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalIterator<ClinicalVariant, ClinicalVariantSearch, ClinicalVariantConverter> iterator = clinicalVariantIterator(query,
                queryOptions, token);
        while (iterator.hasNext()) {
            results.add(iterator.next());
//            if (results.size() == limit) {
//                break;
//            }
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new DataResult<>(dbTime, null, results.size(), results, iterator.getNumFound());
    }

    public ClinicalIterator<ClinicalVariant, ClinicalVariantSearch, ClinicalVariantConverter> clinicalVariantIterator(
            Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Parse query
        SolrQuery solrQuery = parseClinicalVariantQuery(query, queryOptions, token);

        // Execute query
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_VARIANTS_COLLECTION_SUFFIX);
            return new ClinicalIterator(solrManager.getSolrClient(), collection, solrQuery, queryOptions, ClinicalVariantSearch.class,
                    ClinicalVariantConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public ClinicalSolrIterator<ClinicalVariantSearch> clinicalVariantNativeIterator(
            Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Parse query
        SolrQuery solrQuery = parseClinicalVariantQuery(query, queryOptions, token);

        // Execute query
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_VARIANTS_COLLECTION_SUFFIX);
            return new ClinicalSolrIterator<>(getSolrClient(), collection, solrQuery, ClinicalVariantSearch.class);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    private SolrQuery parseClinicalVariantQuery(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, CvdbException {
        // Check
        checkQuery(query, queryOptions, token);

        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        ClinicalVariantQueryParser parser = new ClinicalVariantQueryParser(catalogManager.getConfiguration().getDatabasePrefix(),
                variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);
        if (queryOptions.containsKey(INCLUDE)) {
            List<String> includeList = new ArrayList<>();
            for (String include : queryOptions.getAsStringList(INCLUDE, ",")) {
                String[] split = include.split("\\.");
                if (isImplClinicalVariantField(split[0])) {
                    includeList.add("impl." + include);
                } else {
                    includeList.add(include);
                }
            }
            queryOptions.put(INCLUDE, StringUtils.join(includeList, ","));
        }

        return solrQuery;
    }

    public DataResult<FacetField> facetClinicalVariants(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check
        checkFacet(query, queryOptions, token);

        // Parse query
        ClinicalVariantQueryParser parser = new ClinicalVariantQueryParser(catalogManager.getConfiguration().getDatabasePrefix(),
                variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_VARIANTS_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
//            postProcessing(facetResult, new CvFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    //----------------------------------------------------------------------
    // CLINICAL VARIANT EVIDENCE: SEARCH, ITERATOR, FACET
    //----------------------------------------------------------------------

    public DataResult<ClinicalVariantEvidence> searchClinicalVariantEvidences(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
//        int limit = queryOptions.getInt(LIMIT);
        List<ClinicalVariantEvidence> results = new ArrayList<>();

        StopWatch stopWatch = StopWatch.createStarted();
        ClinicalIterator<ClinicalVariantEvidence, ClinicalVariantEvidenceSearch, ClinicalVariantEvidenceConverter> iterator =
                clinicalVariantEvidenceIterator(query, queryOptions, token);
        while (iterator.hasNext()) {
            results.add(iterator.next());
//            if (results.size() == limit) {
//                break;
//            }
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new DataResult<>(dbTime, null, results.size(), results, iterator.getNumFound());
    }

    public ClinicalIterator<ClinicalVariantEvidence, ClinicalVariantEvidenceSearch, ClinicalVariantEvidenceConverter>
    clinicalVariantEvidenceIterator(Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Parse query
        SolrQuery solrQuery = parseClinicalVariantEvidenceQuery(query, queryOptions, token);

        // Execute query
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
            return new ClinicalIterator(solrManager.getSolrClient(), collection, solrQuery, queryOptions,
                    ClinicalVariantEvidenceSearch.class, ClinicalVariantEvidenceConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public ClinicalSolrIterator<ClinicalVariantEvidenceSearch> clinicalVariantEvidenceNativeIterator(Query query, QueryOptions queryOptions,
                                                                                                     String token)
            throws CvdbException, IOException, CatalogException {
        // Parse query
        SolrQuery solrQuery = parseClinicalVariantEvidenceQuery(query, queryOptions, token);

        // Execute query
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
            return new ClinicalSolrIterator<>(getSolrClient(), collection, solrQuery, ClinicalVariantEvidenceSearch.class);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    private SolrQuery parseClinicalVariantEvidenceQuery(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, CvdbException {
        // Check
        checkQuery(query, queryOptions, token);

        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        ClinicalVariantEvidenceQueryParser parser = new ClinicalVariantEvidenceQueryParser(configuration.getDatabasePrefix(),
                variantStorageMetadataManager);
        return parser.parse(query, queryOptions);
    }

    public DataResult<FacetField> facetClinicalVariantEvidences(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check
        checkFacet(query, queryOptions, token);

        // Parse query
        ClinicalVariantEvidenceQueryParser parser = new ClinicalVariantEvidenceQueryParser(
                catalogManager.getConfiguration().getDatabasePrefix(), variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(ORGANIZATION_PARAM_NAME), query.getString(PROJECT_PARAM_NAME),
                    CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
//            postProcessing(facetResult, new CveFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    //----------------------------------------------------------------------
    // CLINICAL VARIANT SUMMARY
    //----------------------------------------------------------------------

    public DataResult<ClinicalVariantSummaryStats> getClinicalVariantSummaryStats(List<String> variantIds, String interpretationStatusId,
                                                                                  String organizationId, String projectId, String token)
            throws CatalogException, IOException, CvdbException {
        // Sanity check
        if (CollectionUtils.isEmpty(variantIds)) {
            throw new CvdbException("Missing variant ID(s) when running clinical variant summary");
        }

        if (variantIds.size() > DEFAULT_LIMIT) {
            throw new CvdbException("The maximum number of variants (" + DEFAULT_LIMIT + ")has been exceeded (" + variantIds.size() + ")");
        }

        // Get project from study
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        if (StringUtils.isEmpty(organizationId)) {
            organizationId = jwtPayload.getOrganization();
        } else if (!organizationId.equals(jwtPayload.getOrganization())) {
            throw new CvdbException("Access to CVDB from other organizations ('" + organizationId + "') is not yet implemented. You"
                    + " can only access your own organization CVDB ('" + jwtPayload.getOrganization() + "')");
        }

        List<String> targetProjectIds = new ArrayList<>();
        if (StringUtils.isEmpty(projectId)) {
            OpenCGAResult<Project> allProjects = catalogManager.getProjectManager().search(organizationId, new Query(),
                    new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key()), token);
            for (Project project : allProjects.getResults()) {
                if (existCollections(organizationId, project.getId())) {
                    targetProjectIds.add(project.getId());
                }
            }
        } else {
            CatalogFqn catalogFqn = CatalogFqn.extractFqnFromProject(projectId, jwtPayload);
            if (existCollections(catalogFqn.getOrganizationId(), catalogFqn.getProjectId())) {
                targetProjectIds.add(catalogFqn.getProjectId());
            }
        }
        if (CollectionUtils.isEmpty(targetProjectIds)) {
            throw new CvdbException("No CVDB found!");
        }

        logger.info("Computing variant summary stats for projects: {}", targetProjectIds);

        StopWatch stopWatch = StopWatch.createStarted();
        List<ClinicalVariantSummaryStats> variantStatsList = new ArrayList<>(variantIds.size());

        Query query;
        Map<String, Map<String, Long>> facetMap = new HashMap<>();

        for (String variantId : variantIds) {
            ClinicalVariantSummaryStats variantStats = new ClinicalVariantSummaryStats();

            for (String targetProjectId : targetProjectIds) {
                query = new Query()
                        .append(ORGANIZATION_PARAM_NAME, organizationId)
                        .append(PROJECT_PARAM_NAME, targetProjectId)
                        .append(CV_VARIANT_ID_NAME, variantId);

                ClinicalVariantSummaryStats projectStats = new ClinicalVariantSummaryStats();

                // Clinical analysis stats: num. cases, disorder IDs, proband disorder IDs and phenotype names
                facetMap.clear();
                facetMap.put("disorderId", projectStats.getClinicalAnalysis().getDisorders());
                facetMap.put("probandDisorderIds", projectStats.getClinicalAnalysis().getProbandDisorders());
                facetMap.put("probandPhenotypeNames", projectStats.getClinicalAnalysis().getProbandPhenotypes());
                performFacet(query, facetMap, "case", projectStats, token);

                // Clinical interpretation stats: num. primary and secondary interpretations; panel IDs and method names
                facetMap.clear();
                facetMap.put("primary", null);
                facetMap.put("panelIds", projectStats.getInterpretation().getPanels());
                facetMap.put("methodName", projectStats.getInterpretation().getMethods());
                performFacet(query, facetMap, "interpretation", projectStats, token);

                // Clinical variant stats: status and confidence values
                facetMap.clear();
                facetMap.put("status", projectStats.getVariant().getStatus());
                facetMap.put("confidenceValue", projectStats.getVariant().getConfidences());
                performFacet(query, facetMap, "variant", projectStats, token);

                // Clinical variant evidence stats: gene names, transcript IDs, SO term accessions, panel IDs, MoIs, ACMGs, and for review
                // tiers, ACMGs and clinical significances
                facetMap.clear();
                facetMap.put("geneName", projectStats.getEvidence().getGenes());
                facetMap.put("transcriptId", projectStats.getEvidence().getTranscripts());
                facetMap.put("soTermAccessions", projectStats.getEvidence().getSoTerms());
                facetMap.put("panelId", projectStats.getEvidence().getPanels());
                facetMap.put("mois", projectStats.getEvidence().getMois());
                facetMap.put("acmgs", projectStats.getEvidence().getAcmgs());
                facetMap.put("reviewAcmgs", projectStats.getEvidence().getReviewAcmgs());
                facetMap.put("reviewTier", projectStats.getEvidence().getReviewTiers());
                facetMap.put("reviewClinicalSignificance", projectStats.getEvidence().getReviewClinicalSignificances());
                performFacet(query, facetMap, "evidence", projectStats, token);

                updateSummaryStats(projectStats, variantStats);
            }

            variantStatsList.add(variantStats);
        }

        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        return new DataResult<>(dbTime, null, variantStatsList.size(), variantStatsList, variantStatsList.size());
    }

    private void performFacet(Query query, Map<String, Map<String, Long>> facetMap, String type, ClinicalVariantSummaryStats stats,
                              String token) throws IOException, CvdbException, CatalogException {
        StopWatch watch = StopWatch.createStarted();
        DataResult<FacetField> facetResult;
        List<String> facetNames = new ArrayList<>(facetMap.keySet());
        QueryOptions queryOptions = new QueryOptions(FACET, StringUtils.join(facetNames, FacetQueryParser.FACET_SEPARATOR));
        switch (type) {
            case "case": {
                facetResult = facetClinicalAnalyses(query, queryOptions, token);
                break;
            }
            case "interpretation": {
                facetResult = facetClinicalInterpretations(query, queryOptions, token);
                break;
            }
            case "variant": {
                facetResult = facetClinicalVariants(query, queryOptions, token);
                break;
            }
            case "evidence": {
                facetResult = facetClinicalVariantEvidences(query, queryOptions, token);
                break;
            }
            default: {
                throw new CvdbException("Invalid type: " + type);
            }
        }
        for (FacetField facetField : facetResult.getResults()) {
            if ("primary".equals(facetField.getName())) {
                for (FacetField.Bucket bucket : facetField.getBuckets()) {
                    if (Boolean.TRUE.equals(Boolean.valueOf(bucket.getValue()))) {
                        stats.setNumPrimaryInterpretations(bucket.getCount());
                    } else if (Boolean.FALSE.equals(Boolean.valueOf(bucket.getValue()))) {
                        stats.setNumSecondaryInterpretations(bucket.getCount());
                    }
                }
            } else {
                if ("disorderId".equals(facetField.getName())) {
                    stats.setNumClinicalAnalyses(facetField.getCount());
                }

                Map<String, Long> counts = facetMap.get(facetField.getName());
                for (FacetField.Bucket bucket : facetField.getBuckets()) {
                    counts.put(bucket.getValue(), bucket.getCount());
                }
            }
        }
    }

    public DataResult<ClinicalVariantSummaryStats> getClinicalVariantSummaryStats(String variantId, String interpretationStatusId,
                                                                                  String organizationId, String projectId, String token)
            throws CatalogException, IOException, CvdbException {
        // Checking parameter
        if (StringUtils.isEmpty(variantId)) {
            throw new CvdbException("Missing variant ID(s) when running clinical variant summary");
        }
        List<String> ids = new ArrayList<>();
        ids.addAll(Arrays.asList(variantId.split(",")));
        return getClinicalVariantSummaryStats(ids, interpretationStatusId, organizationId, projectId, token);
    }

    //----------------------------------------------------------------------
    // P R I V A T E      M E T H O D S
    //----------------------------------------------------------------------

    private void checkQuery(Query query, QueryOptions queryOptions, String token) throws CvdbException, CatalogException {
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);

        // Check organization
        String organizationId;
        if (query.containsKey(ORGANIZATION_PARAM_NAME) && StringUtils.isNotEmpty(query.getString(ORGANIZATION_PARAM_NAME))) {
            organizationId = query.getString(ORGANIZATION_PARAM_NAME);
            if (!organizationId.equals(jwtPayload.getOrganization())) {
                throw new CvdbException("Access to CVDB from other organizations ('" + organizationId + "') is not yet implemented. You"
                        + " can only access your own organization CVDB ('" + jwtPayload.getOrganization() + "')");
            }
        } else {
            organizationId = jwtPayload.getOrganization();
            query.put(ORGANIZATION_PARAM_NAME, organizationId);
        }

        // Check project and study
        if ((!query.containsKey(PROJECT_PARAM_NAME) || StringUtils.isEmpty(query.getString(PROJECT_PARAM_NAME)))
                && (!query.containsKey(STUDY_PARAM_NAME) || StringUtils.isEmpty(query.getString(STUDY_PARAM_NAME)))) {
            throw new CvdbException("Missing project ID and study ID");
        }

        String projectInputValue = query.getString(PROJECT_PARAM_NAME);
        String studyInputValue = query.getString(STUDY_PARAM_NAME);

        if (StringUtils.isEmpty(studyInputValue)) {
            // Study is undefined, only project is provided
            OpenCGAResult<Project> projectResult = catalogManager.getProjectManager().get(projectInputValue, empty(), token);
            query.put(PROJECT_PARAM_NAME, projectResult.first().getId());
        } else if (StringUtils.isEmpty(projectInputValue)) {
            // Project is undefined, only study is provided
            // Check study ID
            OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(studyInputValue, empty(), token);
            query.put(PROJECT_PARAM_NAME, FqnUtils.getProject(studyResult.first().getFqn()));
            query.put(STUDY_PARAM_NAME, studyResult.first().getId());
        } else {
            // Both project and study are defined, check congruency
            OpenCGAResult<Project> projectResult = catalogManager.getProjectManager().get(projectInputValue, empty(), token);
            OpenCGAResult<Study> studytResult = catalogManager.getStudyManager().get(studyInputValue, empty(), token);
            if (!projectResult.first().getId().equals(FqnUtils.getProject(studytResult.first().getFqn()))) {
                throw new CvdbException("Project ID '" + projectResult.first().getFqn() + "' and study ID '"
                        + studytResult.first().getFqn() + "' mismatch. Please ensure that the study belongs to the specified project");
            }
            query.put(PROJECT_PARAM_NAME, projectResult.first().getId());
            query.put(STUDY_PARAM_NAME, studytResult.first().getId());
        }
    }

    private void checkFacet(Query query, QueryOptions queryOptions, String token) throws CvdbException, CatalogException {
        checkQuery(query, queryOptions, token);

        if (!queryOptions.containsKey(FACET) || StringUtils.isEmpty(queryOptions.getString(FACET))) {
            throw new CvdbException("Missing facet field to aggregation stats");
        }
    }

    private boolean index(ClinicalAnalysis clinicalAnalysis, String organizationId, String projectId, String studyId, List<String> viewers,
                          boolean overwrite) throws CvdbException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            // Index
            if (index(clinicalAnalysis, organizationId, projectId, studyId, viewers, overwrite, solrClient)) {
                // Commit
                solrClient.commit(getCollectionName(organizationId, projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(organizationId, projectId, INTERPRETATIONS_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(organizationId, projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(organizationId, projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
                return true;
            }
        } catch (SolrServerException | IOException e) {
            logger.warn("Error indexing clinical analysis {}: {}", clinicalAnalysis.getId(), e.getMessage());
            rollback(solrClient, e);
            return false;
        }
        return false;
    }

    private boolean index(ClinicalAnalysis clinicalAnalysis, String organizationId, String projectId, String studyId, List<String> viewers,
                          boolean overwrite, SolrClient solrClient) throws CvdbException {
        try {
            logger.info("Indexing clinical analysis {} ...", clinicalAnalysis.getId());
            UpdateResponse updateResponse;

            boolean exists;
            if (overwrite) {
                exists = false;
            } else {
                try {
                    exists = clinicalAnalysisExists(clinicalAnalysis.getId(), getCollectionName(organizationId, projectId,
                                    CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                            solrClient);
                } catch (SolrServerException | IOException e) {
                    logger.warn("Something wrong happened, clinical analysis {} could not be indexed: {}", clinicalAnalysis.getId(), e.getMessage());
                    return false;
                }
            }

            if (!exists) {
                // Clinical analysis
                ClinicalAnalysisSearch cas = caConverter.toClinicalAnalysisSearch(clinicalAnalysis, FqnUtils.getStudy(studyId), viewers);

                // Index
                updateResponse = solrClient.addBean(getCollectionName(organizationId, projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX), cas);
                if (updateResponse.getStatus() != 0) {
                    rollback(solrClient, updateResponse.getStatus());
                }

                // Primary interpretation
                if (clinicalAnalysis.getInterpretation() != null) {
                    index(clinicalAnalysis.getInterpretation(), true, organizationId, projectId, studyId, viewers, solrClient);
                }

                // Secondary interpretations
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                    for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                        index(secondaryInterpretation, false, organizationId, projectId, studyId, viewers, solrClient);
                    }
                }
                logger.info("Done! Indexed clinical analysis {}", clinicalAnalysis.getId());
            } else {
                logger.warn("Skipping clinical analysis {}: it was already indexed", clinicalAnalysis.getId());
                return false;
            }
        } catch (SolrServerException | IOException e) {
            logger.warn("Something wrong happened, clinical analysis {} could not be indexed: {}",
                    clinicalAnalysis.getId(), e.getMessage());
            rollback(solrClient, e);
            return false;
        }

        return true;
    }

    private void index(Interpretation interpretation, boolean isPrimary, String organizationId, String projectId, String studyId,
                       List<String> viewers, SolrClient solrClient) throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Interpretation
            ClinicalInterpretationSearch cis = ciConverter.toInterpretationSearch(interpretation, isPrimary, FqnUtils.getStudy(studyId),
                    viewers);

            updateResponse = solrClient.addBean(getCollectionName(organizationId, projectId, INTERPRETATIONS_COLLECTION_SUFFIX), cis);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Clinical variants
            if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                for (ClinicalVariant primaryFinding : interpretation.getPrimaryFindings()) {
                    index(primaryFinding, true, interpretation.getId(), interpretation.getClinicalAnalysisId(), organizationId, projectId,
                            studyId, viewers, solrClient);
                }
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(ClinicalVariant clinicalVariant, boolean primary, String interpretationId, String clinicalAnalysisId,
                       String organizationId, String projectId, String studyId, List<String> viewers, SolrClient solrClient)
            throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical variant search
            if (clinicalVariant.getAttributes() == null) {
                clinicalVariant.setAttributes(new HashMap<>());
            }
            clinicalVariant.getAttributes().put(CA_ID_NAME, clinicalAnalysisId);
            clinicalVariant.getAttributes().put(CI_ID_NAME, interpretationId);
            ClinicalVariantSearch cvs = cvConverter.toClinicalVariantSearch(clinicalVariant, primary, interpretationId,
                    clinicalAnalysisId, FqnUtils.getStudy(studyId), viewers);

            updateResponse = solrClient.addBean(getCollectionName(organizationId, projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX), cvs);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }

            for (ClinicalVariantEvidence evidence : clinicalVariant.getEvidences()) {
                index(evidence, clinicalVariant.getId(), interpretationId, clinicalAnalysisId, organizationId, projectId, studyId, viewers,
                        solrClient);
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(ClinicalVariantEvidence clinicalVariantEvidence, String variantId, String interpretationId,
                       String clinicalAnalysisId, String organizationId, String projectId, String studyId, List<String> viewers,
                       SolrClient solrClient)
            throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical variant evidences
            if (clinicalVariantEvidence.getAttributes() == null) {
                clinicalVariantEvidence.setAttributes(new HashMap<>());
            }
            clinicalVariantEvidence.getAttributes().put(CA_ID_NAME, clinicalAnalysisId);
            clinicalVariantEvidence.getAttributes().put(CI_ID_NAME, interpretationId);
            clinicalVariantEvidence.getAttributes().put(CV_VARIANT_ID_NAME, variantId);
            ClinicalVariantEvidenceSearch cves = cveConverter.toClinicalVariantEvidenceSearch(clinicalVariantEvidence, variantId,
                    interpretationId, clinicalAnalysisId, FqnUtils.getStudy(studyId), viewers);

            updateResponse = solrClient.addBean(getCollectionName(organizationId, projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
                    cves);
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

    public boolean existCollections(String organizationId, String projectId) throws CvdbException {
        try {
            for (String suffix : COLLECTION_SUFFIXES) {
                if (!solrManager.exists(getCollectionName(organizationId, projectId, suffix))) {
                    return false;
                }
            }
            return true;
        } catch (SolrException e) {
            throw new CvdbException("Checking if Solr CVDB collections exist for organization '" + organizationId + " and 'project '"
                    + projectId + "'", e);
        }
    }

    public void createCollections(String organizationId, String projectId) throws CvdbException {
        try {
            for (int i = 0 ; i < COLLECTION_SUFFIXES.size() ; i++) {
                String name = getCollectionName(organizationId, projectId, COLLECTION_SUFFIXES.get(i));
                if (!solrManager.exists(name)) {
                    logger.info("collection name = {}, config set = {}", name, COLLECTION_CONFIGSETS.get(i));
                    solrManager.create(name, COLLECTION_CONFIGSETS.get(i));
                }
            }
        } catch (SolrException e) {
            throw new CvdbException("Creating Solr CVDB collections for organization '" + organizationId + " and 'project '"
                    + projectId + "'", e);
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

    private void updateClinicalAnalysisCvdbIndexStatus(String studyFqn, ClinicalAnalysis clinicalAnalysis, CvdbIndexStatus indexStatus,
                                                       String token) throws CvdbException {
        try {
            catalogManager.getClinicalAnalysisManager().updateCvdbIndex(studyFqn, clinicalAnalysis, indexStatus, token);
        } catch (CatalogException e) {
            throw new CvdbException("Error updating clinical anslysis CVBD index status", e);
        }
    }

    //----------------------------------------------------------------------

    private boolean isImplClinicalVariantField(String field) {
        switch (field) {
            case "evidences":
            case "comments":
            case "filters":
            case "discussion":
            case "confidence":
            case "tags":
            case "status":
            case "attributes": {
                return false;
            }
            default: {
                return true;
            }
        }
    }

    //----------------------------------------------------------------------

    private void postProcessing(DataResult<FacetField> dataResult, FieldMapping fieldMapping) {
        for (FacetField facetField : dataResult.getResults()) {
            postProcessingRecursive(facetField, fieldMapping);
        }
    }

    private void postProcessingRecursive(FacetField facetField, FieldMapping fieldMapping) {
        try {
            String newName = fieldMapping.toModelField(facetField.getName());
            facetField.setName(newName);
            for (FacetField.Bucket bucket : facetField.getBuckets()) {
                if (CollectionUtils.isNotEmpty(bucket.getFacetFields())) {
                    for (FacetField field : bucket.getFacetFields()) {
                        postProcessingRecursive(field, fieldMapping);
                    }
                }
            }
        } catch (Exception e) {
            // Nothing to do
        }
    }

    private void setViewerInQuery(Query query, String token) throws CvdbException {
        try {
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            String userId = jwtPayload.getUserId();
            if (StringUtils.isEmpty(userId)) {
                throw new CvdbException("No user found for the input token");
            }
            if (ANONYMOUS_USER_ID.equals(userId)) {
                throw new CvdbException(NO_ACCESS_FOR_ANONYMOUS_USERS_MSG);
            }
            query.put(VIEWER_NAME, userId);
        } catch (CatalogException e) {
            throw new CvdbException("Checking user query permissions", e);
        }
    }


    public void updateSummaryStats(ClinicalVariantSummaryStats srcStats, ClinicalVariantSummaryStats destStats) {
        // Clinical analysis stats: num. cases, disorder IDs, proband disorder IDs and phenotype names
        destStats.setNumClinicalAnalyses(destStats.getNumClinicalAnalyses() + srcStats.getNumClinicalAnalyses());
        updateStatsMap(srcStats.getClinicalAnalysis().getDisorders(), destStats.getClinicalAnalysis().getDisorders());
        updateStatsMap(srcStats.getClinicalAnalysis().getProbandDisorders(), destStats.getClinicalAnalysis().getProbandDisorders());
        updateStatsMap(srcStats.getClinicalAnalysis().getProbandPhenotypes(), destStats.getClinicalAnalysis().getProbandPhenotypes());

        // Clinical interpretation stats: num. primary and secondary interpretations; panel IDs and method names
        destStats.setNumPrimaryInterpretations(destStats.getNumPrimaryInterpretations() + srcStats.getNumPrimaryInterpretations());
        destStats.setNumSecondaryInterpretations(destStats.getNumSecondaryInterpretations() + srcStats.getNumSecondaryInterpretations());
        updateStatsMap(srcStats.getInterpretation().getPanels(), destStats.getInterpretation().getPanels());
        updateStatsMap(srcStats.getInterpretation().getMethods(), destStats.getInterpretation().getMethods());

        // Clinical variant stats: status and confidence values
        updateStatsMap(srcStats.getVariant().getStatus(), destStats.getVariant().getStatus());
        updateStatsMap(srcStats.getVariant().getConfidences(), destStats.getVariant().getConfidences());

        // Clinical variant evidence stats: gene names, transcript IDs, SO term accessions, panel IDs, MoIs, ACMGs, and for review
        // tiers, ACMGs and clinical significances
        updateStatsMap(srcStats.getEvidence().getGenes(), destStats.getEvidence().getGenes());
        updateStatsMap(srcStats.getEvidence().getTranscripts(), destStats.getEvidence().getTranscripts());
        updateStatsMap(srcStats.getEvidence().getSoTerms(), destStats.getEvidence().getSoTerms());
        updateStatsMap(srcStats.getEvidence().getPanels(), destStats.getEvidence().getPanels());
        updateStatsMap(srcStats.getEvidence().getMois(), destStats.getEvidence().getMois());
        updateStatsMap(srcStats.getEvidence().getAcmgs(), destStats.getEvidence().getAcmgs());
        updateStatsMap(srcStats.getEvidence().getReviewTiers(), destStats.getEvidence().getReviewTiers());
        updateStatsMap(srcStats.getEvidence().getReviewAcmgs(), destStats.getEvidence().getReviewAcmgs());
        updateStatsMap(srcStats.getEvidence().getReviewClinicalSignificances(), destStats.getEvidence().getReviewClinicalSignificances());
    }

    private void updateStatsMap(Map<String, Long> srcMap, Map<String, Long> destMap) {
        for (Map.Entry<String, Long> entry : srcMap.entrySet()) {
            if (destMap.containsKey(entry.getKey())) {
                destMap.put(entry.getKey(), destMap.get(entry.getKey()) + entry.getValue());
            } else {
                destMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    //----------------------------------------------------------------------
    // G E T T E R S     A N D      S E T T E R S
    //----------------------------------------------------------------------

    public CvdbConfiguration getCvdbConfiguration() {
        return cvdbConfiguration;
    }

    public CvdbSolrEngine setCvdbConfiguration(CvdbConfiguration cvdbConfiguration) {
        this.cvdbConfiguration = cvdbConfiguration;
        return this;
    }

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

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public CvdbSolrEngine setCatalogManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        return this;
    }

    public VariantStorageMetadataManager getVariantStorageMetadataManager() {
        return variantStorageMetadataManager;
    }

    public CvdbSolrEngine setVariantStorageMetadataManager(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
        return this;
    }
}
