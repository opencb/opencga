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
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalAnalysisQueryParser;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalInterpretationQueryParser;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalVariantEvidenceQueryParser;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalVariantQueryParser;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.biodata.models.clinical.interpretation.stats.InterpretationSummaryStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
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
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
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
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParser.*;
import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.commons.datastore.core.QueryOptions.LIMIT;
import static org.opencb.opencga.core.api.ParamConstants.ANONYMOUS_USER_ID;

/**
 * Created by jtarraga on 11/11/17.
 */
public class CvdbSolrEngine {

    private SolrManager solrManager;
    private CatalogManager catalogManager;
    private VariantStorageMetadataManager variantStorageMetadataManager;

    private ClinicalAnalysisConverter caConverter;
    private ClinicalInterpretationConverter ciConverter;
    private ClinicalVariantConverter cvConverter;
    private ClinicalVariantEvidenceConverter cveConverter;

    private static final String GIT_ENTERPRISE_PROPERTIES = "com/zettagenomics/opencga/enterprise/git-enterprise.properties";
    private Logger logger;

    public static final String NO_ACCESS_FOR_ANONYMOUS_USERS_MSG = "Access to CVDB is restricted for anonymous users. Please log in to"
            + " proceed.";

    public static final String CLINICAL_ANALYSES_COLLECTION_SUFFIX = "_cvdb_analyses";
    public static final String INTERPRETATIONS_COLLECTION_SUFFIX = "_cvdb_interpretations";
    public static final String CLINICAL_VARIANTS_COLLECTION_SUFFIX = "_cvdb_variants";
    public static final String CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX = "_cvdb_evidences";

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

    public CvdbSolrEngine() {
        init();
    }

    public CvdbSolrEngine(CvdbConfiguration cvdbConfig, CatalogManager catalogManager, VariantStorageMetadataManager variantStorageMetadataManager) {
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

    public static String getCollectionName(String projectId, String suffix) {
        return "opencga_" + projectId + suffix;
    }

    public CvdbIndexResult indexProject(String projectId, CatalogManager catalogManager, boolean overwrite, String sessionIdUser)
            throws CatalogException {
        logger.info("Loading all clinical analyses from project: '{}'", projectId);

        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(sessionIdUser);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromProject(projectId, jwtPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        CvdbIndexResult result = new CvdbIndexResult();

        // Start time
        StopWatch stopWatch = StopWatch.createStarted();

        // Main loop
        OpenCGAResult<Study> studyResults = catalogManager.getStudyManager().search(projectId, new Query(), QueryOptions.empty(),
                sessionIdUser);
        List<String> studyFqns = studyResults.getResults().stream().map(Study::getFqn).collect(Collectors.toList());
        for (String studyFqn : studyFqns) {
            CvdbIndexResult tmpResult = indexStudy(studyFqn, catalogManager, overwrite, sessionIdUser);
            result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
            result.getFailures().putAll(tmpResult.getFailures());
        }

        // Stop time
        stopWatch.stop();
        result.setTime((int) stopWatch.getTime(TimeUnit.SECONDS));

        return result;
    }

    public CvdbIndexResult indexStudy(String studyId, CatalogManager catalogManager, boolean overwrite, String sessionIdUser)
            throws CatalogException {
        logger.info("Loading all clinical analyses from study: '{}'", studyId);

        CvdbIndexResult result = new CvdbIndexResult();
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(sessionIdUser);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyId, jwtPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        // Start time
        StopWatch stopWatch = StopWatch.createStarted();

        // Get project for that study
        OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), sessionIdUser);
        Study study = studyResult.first();

        Query projectQuery = new Query();
        projectQuery.put(ProjectDBAdaptor.QueryParams.STUDY.key(), study.getFqn());
        catalogManager.getProjectManager().search(organizationId, projectQuery, QueryOptions.empty(), sessionIdUser);

        // Get all clinical analyses for that study
        QueryOptions queryOptions = new QueryOptions(INCLUDE, "id");
        DBIterator<ClinicalAnalysis> iterator = catalogManager.getClinicalAnalysisManager().iterator(study.getFqn(), new Query(),
                queryOptions, sessionIdUser);

        int listSize = 100;
        List<String> caIds = new ArrayList<>(listSize);
        while (iterator.hasNext()) {
            caIds.add(iterator.next().getId());
            if (caIds.size() == listSize) {
                CvdbIndexResult tmpResult = indexClinicalAnalyses(caIds, studyId, catalogManager, overwrite, sessionIdUser);
                result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
                result.getFailures().putAll(tmpResult.getFailures());

                // Reset list
                caIds.clear();
            }
        }

        // Check if there are still clinical analyses to index
        if (CollectionUtils.isNotEmpty(caIds)) {
            CvdbIndexResult tmpResult = indexClinicalAnalyses(caIds, studyId, catalogManager, overwrite, sessionIdUser);
            result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
            result.getFailures().putAll(tmpResult.getFailures());
        }

        // Stop time
        stopWatch.stop();
        result.setTime((int) stopWatch.getTime(TimeUnit.SECONDS));

        return result;
    }

    public CvdbIndexResult indexClinicalAnalyses(List<String> clinicalAnalysisIds, String studyId, CatalogManager catalogManager,
                                                 boolean overwrite, String sessionIdUser) throws CatalogException {
        logger.info("Loading {} clinical analyses from the input list", clinicalAnalysisIds.size());

        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(sessionIdUser);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyId, jwtPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        // Build caId-userId map, i.e., Map<Clinical Analysis ID, List<User ID>>
        Map<String, List<String>> caIdUserIdsMap = new HashMap<>();
        OpenCGAResult<Acl> aclResult = catalogManager.getAdminManager().getEffectivePermissions(studyId, clinicalAnalysisIds,
                Collections.singletonList(ClinicalAnalysisPermissions.VIEW.name()), Enums.Resource.CLINICAL_ANALYSIS.name(), sessionIdUser);
        for (Acl acl : aclResult.getResults()) {
            // Only one permission (VIEW) has been queried, so the first item has to be taken
            caIdUserIdsMap.put(acl.getId(), acl.getPermissions().get(0).getUserIds());
        }

        int numIndexed = 0;
        Map<String, String> failures = new HashMap<>();
        StopWatch stopWatch = StopWatch.createStarted();

        OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), sessionIdUser);
        Study study = studyResult.first();

        // Get project for that study
        Query projectQuery = new Query();
        projectQuery.put(ProjectDBAdaptor.QueryParams.STUDY.key(), study.getFqn());
        OpenCGAResult<Project> projectResult = catalogManager.getProjectManager().search(organizationId, projectQuery, QueryOptions.empty(), sessionIdUser);
        String projectId = projectResult.first().getId();

        // Get the input clinical analyses
        Query caQuery = new Query();
        for (String caId : clinicalAnalysisIds) {
            caQuery.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), caId);
            OpenCGAResult<ClinicalAnalysis> caResult = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), caQuery,
                    QueryOptions.empty(), sessionIdUser);
            if (caResult.getNumResults() == 1) {
                ClinicalAnalysis clinicalAnalysis = caResult.first();
                try {
                    if (index(clinicalAnalysis, projectId, study.getFqn(), caIdUserIdsMap.get(caId), overwrite)) {
                        numIndexed++;
                    } else {
                        String key = caId + " (" + study.getFqn() + ")";
                        failures.put(key, "Skipping index (overwrite is set to false)");
                        logger.warn("{}: {}", key, failures.get(key));
                    }
                } catch (Exception e) {
                    String key = caId + " (" + study.getFqn() + ")";
                    failures.put(key, e.getMessage());
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
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_ANALYSES_COLLECTION_SUFFIX);
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
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_ANALYSES_COLLECTION_SUFFIX);
            return new ClinicalSolrIterator(getSolrClient(), collection, solrQuery, ClinicalAnalysisSearch.class);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalAnalyses(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException {
        // Check
        checkFacet(query, queryOptions, CA_FACET_FIELD_SET);

        // Parse query
        ClinicalAnalysisQueryParser parser = new ClinicalAnalysisQueryParser(variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_ANALYSES_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
            postProcessing(facetResult, new CaFieldMapping());
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
        ClinicalAnalysisQueryParser parser = new ClinicalAnalysisQueryParser(variantStorageMetadataManager);
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
        ClinicalInterpretationQueryParser parser = new ClinicalInterpretationQueryParser(variantStorageMetadataManager);
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
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), INTERPRETATIONS_COLLECTION_SUFFIX);
            return new ClinicalIterator(solrManager.getSolrClient(), collection, solrQuery, queryOptions, ClinicalInterpretationSearch.class,
                    ClinicalInterpretationConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalInterpretations(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException {
        // Check
        checkFacet(query, queryOptions, CI_FACET_FIELD_SET);

        // Parse query
        ClinicalInterpretationQueryParser parser = new ClinicalInterpretationQueryParser(variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), INTERPRETATIONS_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
            postProcessing(facetResult, new CiFieldMapping());
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
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_VARIANTS_COLLECTION_SUFFIX);
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
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_VARIANTS_COLLECTION_SUFFIX);
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
        ClinicalVariantQueryParser parser = new ClinicalVariantQueryParser(variantStorageMetadataManager);
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
            throws IOException, CvdbException {
        // Check
        checkFacet(query, queryOptions, CV_FACET_FIELD_SET);

        // Parse query
        ClinicalVariantQueryParser parser = new ClinicalVariantQueryParser(variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_VARIANTS_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
            postProcessing(facetResult, new CvFieldMapping());
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
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
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
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
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
        ClinicalVariantEvidenceQueryParser parser = new ClinicalVariantEvidenceQueryParser(variantStorageMetadataManager);
        return parser.parse(query, queryOptions);
    }

    public DataResult<FacetField> facetClinicalVariantEvidences(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException {
        // Check
        checkFacet(query, queryOptions, CVE_FACET_FIELD_SET);

        // Parse query
        ClinicalVariantEvidenceQueryParser parser = new ClinicalVariantEvidenceQueryParser(variantStorageMetadataManager);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = getCollectionName(query.getString(PROJECT_PARAM_NAME), CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
            postProcessing(facetResult, new CveFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    //----------------------------------------------------------------------
    // CLINICAL VARIANT SUMMARY
    //----------------------------------------------------------------------

    public DataResult<ClinicalVariantSummaryStats> getClinicalVariantSummaryStats(List<String> variantIds, String interpretationStatusId,
                                                                                  String projectId, String token)
            throws CatalogException, IOException, CvdbException {
        // Sanity check
        if (CollectionUtils.isEmpty(variantIds)) {
            throw new CvdbException("Missing variant ID(s) when running clinical variant summary");
        }

//        if (variantIds.size() > DEFAULT_LIMIT) {
//            throw new CvdbException("The maximum number of variants (" + DEFAULT_LIMIT + ")has been exceeded (" + variantIds.size() + ")");
//        }

        // Get project from study
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromProject(projectId, jwtPayload);

        List<String> projectIds;
        if (StringUtils.isEmpty(projectId)) {
            OpenCGAResult<Project> allProjects = catalogManager.getProjectManager().search(catalogFqn.getOrganizationId(), new Query(),
                    new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key()), token);
            projectIds = allProjects.getResults().stream().map(p -> p.getId()).collect(Collectors.toList());
        } else {
            projectIds = Collections.singletonList(catalogFqn.getProjectId());
        }

        logger.info("Computing variant summary stats for projects: {}", projectIds);

        StopWatch stopWatch = StopWatch.createStarted();
        List<ClinicalVariantSummaryStats> variantStatsList = new ArrayList<>(variantIds.size());

        QueryOptions caQueryOptions = new QueryOptions(INCLUDE, "id,disorder.id");//,interpretation.id,secondaryInterpretations.id");
        QueryOptions cvQueryOptions = new QueryOptions(INCLUDE, "status,confidence.value"); //,evidences");
        QueryOptions cveQueryOptions = new QueryOptions(INCLUDE, "genomicFeature.geneName,modeOfInheritances,panelId,classification.acmg,"
                + "classification.tier,classification.clinicalSignificance");
        for (String variantId : variantIds) {
            ClinicalVariantSummaryStats variantStats = new ClinicalVariantSummaryStats();
            for (String targetProjectId : projectIds) {
                // Check CVDB collections
                if (!existCollections(targetProjectId)) {
                    logger.warn("No CVDB collections were found for project ID {}, so no summary statistics will be returned",
                            targetProjectId);
                } else {
                    ClinicalVariantSummaryStats projectVariantStats = new ClinicalVariantSummaryStats();

                    // Clinical analysis
                    Query query = new Query()
                            .append(PROJECT_PARAM_NAME, targetProjectId)
                            .append(STUDY_PARAM_NAME, ALL_STUDIES_VALUE)
                            .append(CV_ID_NAME, variantId);
                    if (StringUtils.isNotEmpty(interpretationStatusId)) {
                        // Set clinical interpretation status ID
                        query.append(CI_STATUS_ID_NAME, interpretationStatusId);
                    }


                    // Clinical analysis
                    ClinicalSolrIterator<ClinicalAnalysisSearch> caIterator = clinicalAnalysisNativeIterator(query, caQueryOptions, token);
                    int numCases = 0;
                    while (caIterator.hasNext()) {
                        ClinicalAnalysisSearch cas = caIterator.next();
                        // Disorder counts
                        updateCount(cas.getDisorderId(), projectVariantStats.getClinicalAnalysisDisorderCounts());

                        numCases++;
                    }
                    projectVariantStats.setNumCases(numCases);

//                    DataResult<ClinicalAnalysis> caDataResult = searchClinicalAnalyses(query, caQueryOptions, token);
//
//                    // Num. clinical analysis
//                    projectVariantStats.setNumCases(caDataResult.getNumResults());
//                    for (ClinicalAnalysis ca : caDataResult.getResults()) {
//                        // Disorder counts
//                        if (ca.getDisorder() != null && StringUtils.isNotEmpty(ca.getDisorder().getId())) {
//                            updateCount(ca.getDisorder().getId(), projectVariantStats.getClinicalAnalysisDisorderCounts());
//                        }
//
////                        // Num. primary interpretations
////                        if (ca.getInterpretation() != null) {
////                            projectVariantStats.setNumPrimaryInterpretations(1 + projectVariantStats.getNumPrimaryInterpretations());
////                        }
////
////                        // Num. secondary interpretations
////                        if (CollectionUtils.isNotEmpty(ca.getSecondaryInterpretations())) {
////                            projectVariantStats.setNumSecondaryInterpretations(ca.getSecondaryInterpretations().size()
////                                    + projectVariantStats.getNumSecondaryInterpretations());
////                        }
//                    }

                    // Clinical variants
                    ClinicalSolrIterator<ClinicalVariantSearch> cvsIterator = clinicalVariantNativeIterator(query, cvQueryOptions, token);
                    while (cvsIterator.hasNext()) {
                        ClinicalVariantSearch cvs = cvsIterator.next();
                        // Variant status counts
                        updateCount(cvs.getStatus(), projectVariantStats.getVariantStatusCounts());

                        // Variant confidence counts
                        updateCount(cvs.getConfidenceValue(), projectVariantStats.getVariantConfidenceCounts());
                    }

                    // Clinical variant evidences
                    ClinicalSolrIterator<ClinicalVariantEvidenceSearch> cvesIterator = clinicalVariantEvidenceNativeIterator(query,
                            cveQueryOptions, token);
                    while (cvesIterator.hasNext()) {
                        ClinicalVariantEvidenceSearch cves = cvesIterator.next();

                        // Genomic feature (gene name) counts
                        updateCount(cves.getGeneName(), projectVariantStats.getInterpretationSummaryStats().getEvidenceGeneNameCounts());

                        // Genomic feature (transcript id) counts
                        // updateCount(cves.get, projectVariantStats.getInterpretationSummaryStats().getEvidenceTranscriptCounts());

                        // Moi counts
                        updateCount(cves.getMois(), projectVariantStats.getInterpretationSummaryStats().getEvidenceModeOfInheritanceCounts());

                        // Panel counts
                        updateCount(cves.getPanelId(), projectVariantStats.getInterpretationSummaryStats().getEvidencePanelCounts());

                        // ACMG counts
                        updateCount(cves.getAcmgs(), projectVariantStats.getInterpretationSummaryStats().getEvidenceClassificationAcmgCounts());

                        // Review tier counts
                        updateCount(cves.getTier(), projectVariantStats.getInterpretationSummaryStats().getEvidenceReviewTierCounts());

                        // Review ACMG counts
                        //updateCount(cves.getAcmgs(), projectVariantStats.getInterpretationSummaryStats().getEvidenceClassificationAcmgCounts());

                        // Review clinical significance counts
                        // updateCount(cves.getClinicalSignificance(), projectVariantStats.getInterpretationSummaryStats().getEvidenceReviewClinicalSignificanceCounts());
                    }

                    updateSummaryStats(projectVariantStats, variantStats);
                }
            }

            // Add summary to the list
            variantStatsList.add(variantStats);
        }

        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        return new DataResult<>(dbTime, null, variantStatsList.size(), variantStatsList, variantStatsList.size());
    }

    public DataResult<ClinicalVariantSummaryStats> getClinicalVariantSummaryStats(String variantId, String interpretationStatusId,
                                                                                  String projectId, String token)
            throws CatalogException, IOException, CvdbException {
        // Checking parameter
        if (StringUtils.isEmpty(variantId)) {
            throw new CvdbException("Missing variant ID(s) when running clinical variant summary");
        }
        List<String> ids = new ArrayList<>();
        ids.addAll(Arrays.asList(variantId.split(",")));
        return getClinicalVariantSummaryStats(ids, interpretationStatusId, projectId, token);
    }

    //----------------------------------------------------------------------
    // P R I V A T E      M E T H O D S
    //----------------------------------------------------------------------

    private void checkQuery(Query query, QueryOptions queryOptions, String token) throws CvdbException, CatalogException {
        String projectInputValue = null;
        if (query.containsKey(PROJECT_PARAM_NAME) && StringUtils.isNotEmpty(query.getString(PROJECT_PARAM_NAME))) {
            projectInputValue = query.getString(PROJECT_PARAM_NAME);
        }

        String studyInputValue = null;
        if (query.containsKey(STUDY_PARAM_NAME) || StringUtils.isNotEmpty(query.getString(STUDY_PARAM_NAME))) {
            studyInputValue = query.getString(STUDY_PARAM_NAME);
        }

        // Project and study are not defined
        if (StringUtils.isEmpty(projectInputValue) && StringUtils.isEmpty(studyInputValue)) {
            throw new CvdbException("Missing project ID and/or study ID");
        }

        // Only project is defined (study is undefined)
        if (StringUtils.isEmpty(studyInputValue)) {
            OpenCGAResult<Project> projectResult = catalogManager.getProjectManager().get(projectInputValue, QueryOptions.empty(), token);
            if (projectResult.getNumResults() <= 0) {
                throw new CvdbException("No project found for ID '" + projectInputValue + "'");
            } else if (projectResult.getNumResults() == 1) {
                // Setting project ID in query
                query.put(PROJECT_PARAM_NAME, projectResult.first().getId());
            } else {
                // This should never happen
                throw new CvdbException("More than one project found for ID '" + projectInputValue + "'");
            }

            OpenCGAResult<Study> studyResults = catalogManager.getStudyManager().search(projectInputValue, new Query(),
                    QueryOptions.empty(), token);
            if (studyResults.getNumResults() <= 0) {
                // No studies found for that project
                throw new CvdbException("No studies found for project '" + projectInputValue + "'");
            } else if (studyResults.getNumResults() == 1) {
                // Setting study ID in the query
                query.put(STUDY_PARAM_NAME, studyResults.first().getId());
            } else {
                // More than one study found for project
                throw new CvdbException("More than one study found for project '" + projectInputValue + "'. Please, enter which studies to"
                        + " use or the value '" + ALL_STUDIES_VALUE + "' to use all studies");
            }
            return;
        }

        // Study is defined, check if there are multiple studies
        List<String> studyInputValues = Arrays.asList(studyInputValue.split(","));

        // Get project, in case project is defined as well
        Project project = null;
        if (StringUtils.isNotEmpty(projectInputValue)) {
            OpenCGAResult<Project> projectResult = catalogManager.getProjectManager().get(projectInputValue, QueryOptions.empty(), token);
            if (projectResult.getNumResults() <= 0) {
                throw new CvdbException("No project found for ID '" + projectInputValue + "'");
            } else if (projectResult.getNumResults() == 1) {
                project = projectResult.first();
            } else {
                // This should never happen
                throw new CvdbException("More than one project found for ID '" + projectInputValue + "'");
            }
        }

        Set<String> studyIds = new HashSet<>();
        List<String> projectIds = new ArrayList<>();
        for (String studyValue : studyInputValues) {
            if (ALL_STUDIES_VALUE.equals(studyValue)) {
                if (project != null) {
                    OpenCGAResult<Study> studyResults = catalogManager.getStudyManager().search(project.getId(), new Query(),
                            QueryOptions.empty(), token);
                    for (Study study : studyResults.getResults()) {
                        if (!FqnUtils.getProject(study.getFqn()).equals(project.getId())) {
                            throw new CvdbException("Invalid study ID '" + study.getId() + "' not found in project '" + project.getId()
                                    + "'");
                        }
                        studyIds.add(study.getId());
                    }
                } else {
                    throw new CvdbException("Invalid use of '" + ALL_STUDIES_VALUE + "' (to indicate all studies) because no project has"
                            + " been specified");
                }
            } else {
                OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(studyValue, QueryOptions.empty(), token);
                if (studyResult.getNumResults() == 0) {
                    throw new CvdbException("Study not found for ID '" + studyInputValue + "'");
                }
                Study study = studyResult.first();
                if (project != null) {
                    // Project was defined
                    if (!project.getId().equals(FqnUtils.getProject(study.getFqn()))) {
                        throw new CvdbException("Mismatch project ID: from input study ID '" + studyValue + ", got project ID '"
                                + FqnUtils.getProject(study.getFqn()) + "', but the project ID parameter '" + project.getId() + "'");
                    }
                } else {
                    if (CollectionUtils.isEmpty(projectIds)) {
                        projectIds.add(FqnUtils.getProject(study.getFqn()));
                    } else {
                        if (!projectIds.contains(FqnUtils.getProject(study.getFqn()))) {
                            throw new CvdbException("Study IDs belong to different projects: '" + projectIds.get(0) + "' and '"
                                    + FqnUtils.getProject(study.getFqn()) + "'");
                        }
                    }
                }
                studyIds.add(study.getId());
            }
        }

        // Set project ID in query
        if (project != null) {
            query.put(PROJECT_PARAM_NAME, project.getId());
        } else {
            query.put(PROJECT_PARAM_NAME, projectIds.get(0));
        }

        // Set study IDs in query
        query.put(STUDY_PARAM_NAME, StringUtils.join(new ArrayList<>(studyIds), ","));

        // Check limit
        if (queryOptions.containsKey(LIMIT)) {
            int limit = queryOptions.getInt(LIMIT);
            if (limit < 1) {
                throw new CvdbException("Invalid query limit: " + limit);
            }
//            if (limit < 1 || limit > ParamConstants.DEFAULT_LIMIT) {
//                logger.warn("Invalid query limit {}, the default limit {} will be used", limit, DEFAULT_LIMIT);
//                queryOptions.put(LIMIT, DEFAULT_LIMIT);
//            }
//        } else {
//            logger.warn("Query limit not defined, the default limit {} will be used", DEFAULT_LIMIT);
//            queryOptions.put(LIMIT, DEFAULT_LIMIT);
        }
    }

    private void checkFacet(Query query, QueryOptions queryOptions, Set<String> fieldSet) throws CvdbException {
        if (!query.containsKey(PROJECT_PARAM_NAME) || StringUtils.isEmpty(query.getString(PROJECT_PARAM_NAME))) {
            throw new CvdbException("Missing project ID");
        }

        if (!queryOptions.containsKey(QueryOptions.FACET) || StringUtils.isEmpty(queryOptions.getString(QueryOptions.FACET))) {
            throw new CvdbException("Missing facet field to aggregation stats");
        }
    }

    private boolean index(ClinicalAnalysis clinicalAnalysis, String projectId, String studyId, List<String> viewers, boolean overwrite)
            throws CvdbException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            // Index
            if (index(clinicalAnalysis, projectId, studyId, viewers, overwrite, solrClient)) {
                // Commit
                solrClient.commit(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
                solrClient.commit(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
                return true;
            }
        } catch (SolrServerException | IOException e) {
            logger.warn("Error indexing clinical analysis {}: {}", clinicalAnalysis.getId(), e.getMessage());
            rollback(solrClient, e);
            return false;
        }
        return false;
    }

    private boolean index(ClinicalAnalysis clinicalAnalysis, String projectId, String studyId, List<String> viewers, boolean overwrite,
                          SolrClient solrClient) throws CvdbException {
        try {
            logger.info("Indexing clinical analysis {} ...", clinicalAnalysis.getId());
            UpdateResponse updateResponse;

            boolean exists;
            if (overwrite) {
                exists = false;
            } else {
                try {
                    exists = clinicalAnalysisExists(clinicalAnalysis.getId(), getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
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
                updateResponse = solrClient.addBean(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX), cas);
                if (updateResponse.getStatus() != 0) {
                    rollback(solrClient, updateResponse.getStatus());
                }

                // Primary interpretation
                if (clinicalAnalysis.getInterpretation() != null) {
                    index(clinicalAnalysis.getInterpretation(), true, projectId, studyId, viewers, solrClient);
                }

                // Secondary interpretations
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                    for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                        index(secondaryInterpretation, false, projectId, studyId, viewers, solrClient);
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

    private void index(Interpretation interpretation, boolean isPrimary, String projectId, String studyId, List<String> viewers,
                       SolrClient solrClient) throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Interpretation
            ClinicalInterpretationSearch cis = ciConverter.toInterpretationSearch(interpretation, isPrimary, FqnUtils.getStudy(studyId),
                    viewers);

            updateResponse = solrClient.addBean(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX), cis);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Clinical variants
            if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                for (ClinicalVariant primaryFinding : interpretation.getPrimaryFindings()) {
                    index(primaryFinding, true, interpretation.getId(), interpretation.getClinicalAnalysisId(), projectId,
                            studyId, viewers, solrClient);
                }
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(ClinicalVariant clinicalVariant, boolean primary, String interpretationId, String clinicalAnalysisId,
                       String projectId, String studyId, List<String> viewers, SolrClient solrClient) throws CvdbException {
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

            updateResponse = solrClient.addBean(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX), cvs);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }

            for (ClinicalVariantEvidence evidence : clinicalVariant.getEvidences()) {
                index(evidence, clinicalVariant.getId(), interpretationId, clinicalAnalysisId, projectId, studyId, viewers, solrClient);
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(ClinicalVariantEvidence clinicalVariantEvidence, String variantId, String interpretationId,
                       String clinicalAnalysisId, String projectId, String studyId, List<String> viewers, SolrClient solrClient)
            throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical variant evidences
            if (clinicalVariantEvidence.getAttributes() == null) {
                clinicalVariantEvidence.setAttributes(new HashMap<>());
            }
            clinicalVariantEvidence.getAttributes().put(CA_ID_NAME, clinicalAnalysisId);
            clinicalVariantEvidence.getAttributes().put(CI_ID_NAME, interpretationId);
            clinicalVariantEvidence.getAttributes().put(CV_ID_NAME, variantId);
            ClinicalVariantEvidenceSearch cves = cveConverter.toClinicalVariantEvidenceSearch(clinicalVariantEvidence, variantId,
                    interpretationId, clinicalAnalysisId, FqnUtils.getStudy(studyId), viewers);

            updateResponse = solrClient.addBean(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX), cves);
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
                    logger.info("collection name = {}, config set = {}", name, COLLECTION_CONFIGSETS.get(i));
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

    private void updateGenomicFeatureCounts(GenomicFeature genomicFeature, InterpretationSummaryStats stats) {
        if (genomicFeature != null) {
            // Gene name counts
            if (StringUtils.isNotEmpty(genomicFeature.getGeneName())) {
                updateCount(genomicFeature.getGeneName(), stats.getEvidenceGeneNameCounts());
            }

            // Transcript ID counts
            if (StringUtils.isNotEmpty(genomicFeature.getTranscriptId())) {
                updateCount(genomicFeature.getTranscriptId(), stats.getEvidenceTranscriptCounts());
            }
        }
    }

    private void updateModeOfInheritanceCounts(List<ClinicalProperty.ModeOfInheritance> mois, InterpretationSummaryStats stats) {
        if (CollectionUtils.isNotEmpty(mois)) {
            for (ClinicalProperty.ModeOfInheritance moi : mois) {
                updateCount(moi.name(), stats.getEvidenceModeOfInheritanceCounts());
            }
        }
    }

    private void updatePanelCounts(String panelId, InterpretationSummaryStats stats) {
        if (StringUtils.isNotEmpty(panelId)) {
            updateCount(panelId, stats.getEvidencePanelCounts());
        }
    }

    private void updateReviewCounts(ClinicalEvidenceReview review, InterpretationSummaryStats stats) {
        if (review != null) {
            // Review tier counts
            updateCount(review.getTier(), stats.getEvidenceReviewTierCounts());

            // Review ACMG counts
            updateAcmgCounts(review.getAcmg(), stats.getEvidenceReviewAcmgCounts());

            // Review clinical significance counts
            if (review.getClinicalSignificance() != null) {
                updateCount(review.getClinicalSignificance().name(), stats.getEvidenceReviewClinicalSignificanceCounts());
            }
        }
    }

    private void updateClassificationCounts(VariantClassification variantClassification, InterpretationSummaryStats stats) {
        if (variantClassification != null) {
            // Classification ACMG counts
            updateAcmgCounts(variantClassification.getAcmg(), stats.getEvidenceClassificationAcmgCounts());
        }
    }

    private void updateAcmgCounts(List<ClinicalAcmg> acmgs, Map<String, Integer> counts) {
        if (CollectionUtils.isNotEmpty(acmgs)) {
            for (ClinicalAcmg acmg : acmgs) {
                if (StringUtils.isNotEmpty(acmg.getClassification())) {
                    updateCount(acmg.getClassification(), counts);
                }
            }
        }
    }

    private void updateCount(String key, Map<String, Integer> counts) {
        if (StringUtils.isNotEmpty(key)) {
            if (!counts.containsKey(key)) {
                counts.put(key, 1);
            } else {
                counts.put(key, 1 + counts.get(key));
            }
        }
    }

    private void updateCount(List<String> keys, Map<String, Integer> counts) {
        if (CollectionUtils.isNotEmpty(keys)) {
            for (String key : keys) {
                if (!counts.containsKey(key)) {
                    counts.put(key, 1);
                } else {
                    counts.put(key, 1 + counts.get(key));
                }
            }
        }
    }

    public void updateSummaryStats(ClinicalVariantSummaryStats srcStats, ClinicalVariantSummaryStats destStats) {
        // Num. cases
        destStats.setNumCases(destStats.getNumCases() + srcStats.getNumCases());

        // Variant status counts
        updateStatsMap(srcStats.getVariantStatusCounts(), destStats.getVariantStatusCounts());

        // Variant confidence counts
        updateStatsMap(srcStats.getVariantConfidenceCounts(), destStats.getVariantConfidenceCounts());

        // Num. primary interpretations
        destStats.setNumPrimaryInterpretations(destStats.getNumPrimaryInterpretations() + srcStats.getNumPrimaryInterpretations());

        // Num. secondary interpretations
        destStats.setNumSecondaryInterpretations(destStats.getNumSecondaryInterpretations() + srcStats.getNumSecondaryInterpretations());

        // Interpretation summary stats
        updateInterpretationSummaryStats(srcStats.getInterpretationSummaryStats(), destStats.getInterpretationSummaryStats());

        // Clinical analysis disorder counts
        updateStatsMap(srcStats.getClinicalAnalysisDisorderCounts(), destStats.getClinicalAnalysisDisorderCounts());
    }

    private void updateInterpretationSummaryStats(InterpretationSummaryStats srcStats, InterpretationSummaryStats destStats) {
        updateStatsMap(srcStats.getEvidenceTranscriptCounts(), destStats.getEvidenceTranscriptCounts());
        updateStatsMap(srcStats.getEvidenceGeneNameCounts(), destStats.getEvidenceGeneNameCounts());
        updateStatsMap(srcStats.getEvidenceModeOfInheritanceCounts(), destStats.getEvidenceModeOfInheritanceCounts());
        updateStatsMap(srcStats.getEvidencePanelCounts(), destStats.getEvidencePanelCounts());
        updateStatsMap(srcStats.getEvidenceReviewTierCounts(), destStats.getEvidenceReviewTierCounts());
        updateStatsMap(srcStats.getEvidenceReviewAcmgCounts(), destStats.getEvidenceReviewAcmgCounts());
        updateStatsMap(srcStats.getEvidenceReviewClinicalSignificanceCounts(), destStats.getEvidenceReviewClinicalSignificanceCounts());
        updateStatsMap(srcStats.getEvidenceClassificationAcmgCounts(), destStats.getEvidenceClassificationAcmgCounts());
    }

    private void updateStatsMap(Map<String, Integer> srcMap, Map<String, Integer> destMap) {
        for (Map.Entry<String, Integer> entry : srcMap.entrySet()) {
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
