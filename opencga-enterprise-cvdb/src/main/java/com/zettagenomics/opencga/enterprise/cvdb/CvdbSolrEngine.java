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

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import com.zettagenomics.opencga.enterprise.catalog.utils.FederationUtils;
import com.zettagenomics.opencga.enterprise.core.GitUtils;
import com.zettagenomics.opencga.enterprise.core.configuration.CvdbConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalAnalysisConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalInterpretationConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantEvidenceConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalIterator;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalSolrIterator;
import com.zettagenomics.opencga.enterprise.cvdb.models.*;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalAnalysisQueryParser;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalInterpretationQueryParser;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalVariantEvidenceQueryParser;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalVariantQueryParser;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ProjectManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.core.client.GenericClient;
import org.opencb.opencga.core.client.ParentClient;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.clinical.CvdbIndexStatus;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.CollectionNameGenerator.*;
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
    private SearchIndexMetadata searchIndexMetadata;
    private CollectionNameGenerator collectionNameGenerator;

    private Configuration configuration;
    private CvdbConfiguration cvdbConfiguration;

    private ClinicalAnalysisConverter caConverter;
    private ClinicalInterpretationConverter ciConverter;
    private ClinicalVariantConverter cvConverter;
    private ClinicalVariantEvidenceConverter cveConverter;

    private Logger logger;

    public static final String NO_ACCESS_FOR_ANONYMOUS_USERS_MSG = "Access to CVDB is restricted for anonymous users. Please log in to"
            + " proceed.";

    public static final String CLINICAL_ANALYSIS_CONFIGSET = "opencga-ca-configset-"
            + GitUtils.getEnterprise().getBuildVersion();
    public static final String INTERPRETATION_CONFIGSET = "opencga-ci-configset-"
            + GitUtils.getEnterprise().getBuildVersion();
    public static final String CLINICAL_VARIANT_CONFIGSET = "opencga-cv-configset-"
            + GitUtils.getEnterprise().getBuildVersion();
    public static final String CLINICAL_VARIANT_EVIDENCE_CONFIGSET = "opencga-cve-configset-"
            + GitUtils.getEnterprise().getBuildVersion();
    public static final String CLINICAL_VIEWERS_CONFIGSET = "opencga-viewers-configset-"
            + GitUtils.getEnterprise().getBuildVersion();

    private static final Map<String, String> COLLECTION_CONFIGSETS_MAP;

    static {
        COLLECTION_CONFIGSETS_MAP = new HashMap<>();
        COLLECTION_CONFIGSETS_MAP.put(CLINICAL_ANALYSES_COLLECTION_SUFFIX, CLINICAL_ANALYSIS_CONFIGSET);
        COLLECTION_CONFIGSETS_MAP.put(INTERPRETATIONS_COLLECTION_SUFFIX, INTERPRETATION_CONFIGSET);
        COLLECTION_CONFIGSETS_MAP.put(CLINICAL_VARIANTS_COLLECTION_SUFFIX, CLINICAL_VARIANT_CONFIGSET);
        COLLECTION_CONFIGSETS_MAP.put(CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX, CLINICAL_VARIANT_EVIDENCE_CONFIGSET);
        COLLECTION_CONFIGSETS_MAP.put(CLINICAL_VIEWERS_COLLECTION_SUFFIX, CLINICAL_VIEWERS_CONFIGSET);
    }

    private static final QueryOptions ORGANIZATION_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            OrganizationDBAdaptor.QueryParams.ID.key(), OrganizationDBAdaptor.QueryParams.OWNER.key(),
            OrganizationDBAdaptor.QueryParams.ADMINS.key(), OrganizationDBAdaptor.QueryParams.FEDERATION.key()));

    private static final QueryOptions INCLUDE_PROJECT_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.FQN.key(), ProjectDBAdaptor.QueryParams.UID.key(),
            ProjectDBAdaptor.QueryParams.FEDERATION.key(), ProjectDBAdaptor.QueryParams.INTERNAL.key()));

    public CvdbSolrEngine(Configuration configuration, CvdbConfiguration cvdbConfiguration) {
        this.configuration = configuration;
        this.cvdbConfiguration = cvdbConfiguration;

        init();
    }

    public CvdbSolrEngine(CvdbConfiguration cvdbConfig, CatalogManager catalogManager) {
        this.configuration = catalogManager.getConfiguration();
        this.cvdbConfiguration = cvdbConfig;

        this.solrManager = new SolrManager(cvdbConfig.getDatabase().getHosts(), cvdbConfig.getDatabase().getMode(),
                cvdbConfig.getDatabase().getTimeout());
        this.catalogManager = catalogManager;
        this.collectionNameGenerator = new CollectionNameGenerator(catalogManager);

        init();
    }

    static SearchIndexMetadata getDefaultSearchIndexMetadata() {
        return new SearchIndexMetadata(
                0, Date.from(Instant.now()), Date.from(Instant.now()), SearchIndexMetadata.Status.ACTIVE,
                SearchIndexMetadata.DataStatus.READY, CLINICAL_VARIANT_CONFIGSET, "",
                new ObjectMap()
                        .append(VariantStorageOptions.SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED.key(), false)
                        .append(VariantStorageOptions.SEARCH_STATS_VARIANT_ID_VERSION.key(), "v1")
        );
    }

    private void init() {
        // Ideally, the SearchIndexMetadata should be loaded from some persistent storage, but for now we will use a default one.
        this.searchIndexMetadata = getDefaultSearchIndexMetadata();

        this.caConverter = new ClinicalAnalysisConverter();
        this.ciConverter = new ClinicalInterpretationConverter();
        this.cvConverter = new ClinicalVariantConverter(searchIndexMetadata);
        this.cveConverter = new ClinicalVariantEvidenceConverter();

        this.logger = LoggerFactory.getLogger(CvdbSolrEngine.class);
    }

    //----------------------------------------------------------------------
    // P U B L I C      M E T H O D S
    //----------------------------------------------------------------------

    public CvdbIndexResult indexProject(String projectId, boolean overwrite, String token)
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
            CvdbIndexResult tmpResult = indexStudy(studyFqn, overwrite, token);
            result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
            result.getFailures().putAll(tmpResult.getFailures());
        }

        // Stop time
        stopWatch.stop();
        result.setTime((int) stopWatch.getTime(TimeUnit.SECONDS));

        return result;
    }

    public CvdbIndexResult indexStudy(String studyFqn, boolean overwrite, String token)
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
                CvdbIndexResult tmpResult = indexClinicalAnalyses(caIds, studyFqn, overwrite, token);
                result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
                result.getFailures().putAll(tmpResult.getFailures());

                // Reset list
                caIds.clear();
            }
        }

        // Check if there are still clinical analyses to index
        if (CollectionUtils.isNotEmpty(caIds)) {
            CvdbIndexResult tmpResult = indexClinicalAnalyses(caIds, studyFqn, overwrite, token);
            result.setNumIndexed(result.getNumIndexed() + tmpResult.getNumIndexed());
            result.getFailures().putAll(tmpResult.getFailures());
        }

        // Stop time
        stopWatch.stop();
        result.setTime((int) stopWatch.getTime(TimeUnit.SECONDS));

        return result;
    }

    public CvdbIndexResult indexClinicalAnalyses(List<String> clinicalAnalysisIds, String studyFqn, boolean overwrite, String token)
            throws CatalogException {
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

        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId, token);

        // Get the input clinical analyses
        Query caQuery = new Query();
        for (String caId : clinicalAnalysisIds) {
            caQuery.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), caId);
            OpenCGAResult<ClinicalAnalysis> caResult = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), caQuery,
                    QueryOptions.empty(), token);
            if (caResult.getNumResults() == 1) {
                ClinicalAnalysis clinicalAnalysis = caResult.first();

                // If overwrite, we need to remove interpretations, clinical variants and evidences for that clinical analysis
                if (overwrite) {
                    try {
                        removeClinicalAnalysis(caId, organizationId, projectId, study.getId(), collectionPrefix);
                    } catch (CvdbException e) {
                        String key = caId + " (" + study.getFqn() + ")";
                        failures.put(key, e.getMessage());
                        try {
                            updateClinicalAnalysisCvdbIndexStatus(study.getFqn(), clinicalAnalysis,
                                    new CvdbIndexStatus(ERROR, e.getMessage()), token);
                        } catch (CvdbException ex) {
                            logger.warn("Error when indexing clinical analysis " + clinicalAnalysis.getId(), ex);
                        }

                        // next
                        continue;
                    }
                }

                try {
                    if (index(clinicalAnalysis, organizationId, projectId, study.getFqn(), collectionPrefix)) {
                        List<String> viewers = caIdUserIdsMap.get(caId);
                        if (CollectionUtils.isNotEmpty(viewers)) {
                            // Add the user IDs to the clinical viewers collection
                            indexViewers(clinicalAnalysis.getId(), study.getId(), viewers, collectionPrefix);
                        }
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

    public void indexViewers(String caId, String studyId, List<String> viewers, String collectionPrefix)
            throws CvdbException, SolrServerException, IOException {
        SolrClient solrClient = getSolrClient();

        String viewerCollectionName = collectionNameGenerator.getClinicalViewerCollectionName(collectionPrefix);

        // Index viewers for that clinical analysis ID
        UpdateResponse updateResponse = solrClient.addBean(viewerCollectionName, new ClinicalViewersSearch(caId, studyId, viewers));
        if (updateResponse.getStatus() != 0) {
            rollback(solrClient, updateResponse.getStatus());
        }

        // Commit
        solrClient.commit(viewerCollectionName);
    }

    private void removeClinicalAnalysis(String caId, String organizationId, String projectId, String studyId, String collectionPrefix)
            throws CvdbException {
        logger.info("Removing clinical analysis {} and its interpretations, clinical variants and evidences", caId);

        SolrClient solrClient = getSolrClient();

        try {
            UpdateResponse updateResponse;

            // Delete clinical analysis
            String query = "id:" + ClientUtils.escapeQueryChars(caId) + " AND " + "studyId:" + ClientUtils.escapeQueryChars(studyId);

            String caCollectionName = collectionNameGenerator.getClinicalAnalysisCollectionName(collectionPrefix);
            updateResponse = solrClient.deleteByQuery(caCollectionName, query);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Change query for the other collections
            query = CA_ID_NAME + ":" + ClientUtils.escapeQueryChars(caId) + " AND " + "studyId:" + ClientUtils.escapeQueryChars(studyId);

            // Delete interpretations for that clinical analysis
            String ciCollectionName = collectionNameGenerator.getClinicalInterpretationCollectionName(collectionPrefix);
            updateResponse = solrClient.deleteByQuery(ciCollectionName, query);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }
            // Delete clinical variants for that clinical analysis
            String cvCollectionName = collectionNameGenerator.getClinicalVariantCollectionName(collectionPrefix);
            updateResponse = solrClient.deleteByQuery(cvCollectionName, query);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Delete clinical evidences for that clinical analysis
            String cveCollectionName = collectionNameGenerator.getClinicalVariantEvidenceCollectionName(collectionPrefix);
            updateResponse = solrClient.deleteByQuery(cveCollectionName, query);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Commit
            solrClient.commit(caCollectionName);
            solrClient.commit(ciCollectionName);
            solrClient.commit(cvCollectionName);
            solrClient.commit(cveCollectionName);
        } catch (SolrServerException | IOException e) {
            logger.warn("Error removing interpretations, clinical variants and evidences for clinical analysis {}: {}", caId,
                    e.getMessage());
            rollback(solrClient, e);
        }
    }

    //----------------------------------------------------------------------
    // CLINICAL ANALYSIS: SEARCH, ITERATOR, FACET
    //----------------------------------------------------------------------

    public DataResult<ClinicalAnalysis> searchClinicalAnalyses(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

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
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return null;
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalAnalysisQuery(query, queryOptions, token);

        // Execute query
        try {
            String collectionName = collectionNameGenerator.getClinicalAnalysisCollectionName(query.getString(PROJECT_PARAM_NAME), token);
            return new ClinicalIterator<>(solrManager.getSolrClient(), collectionName, solrQuery, queryOptions, ClinicalAnalysisSearch.class,
                    ClinicalAnalysisConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                 | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public ClinicalSolrIterator<ClinicalAnalysisSearch> clinicalAnalysisNativeIterator(
            Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return null;
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalAnalysisQuery(query, queryOptions, token);

        // Execute query
        try {
            String collectionName = collectionNameGenerator.getClinicalAnalysisCollectionName(query.getString(PROJECT_PARAM_NAME), token);
            return new ClinicalSolrIterator<>(getSolrClient(), collectionName, solrQuery, ClinicalAnalysisSearch.class);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<Long> clinicalAnalysisCount(Query query, String token)
            throws CvdbException, IOException, CatalogException {
        // Check CVDB data store availability
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalAnalysisQuery(query, QueryOptions.empty(), token);

        String collectionName = collectionNameGenerator.getClinicalAnalysisCollectionName(query.getString(PROJECT_PARAM_NAME), token);
        SolrCollection solrCollection = getSolrManager().getCollection(collectionName);
        // Execute query
        try {
            return solrCollection.count(solrQuery);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalAnalyses(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

        // Check
        checkFacet(query, queryOptions, token);
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);

        // Parse query
        ClinicalAnalysisQueryParser parser = new ClinicalAnalysisQueryParser(collectionPrefix, searchIndexMetadata);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collectionName = collectionNameGenerator.getClinicalAnalysisCollectionName(collectionPrefix);
            SolrCollection solrCollection = solrManager.getCollection(collectionName);
            facetResult = solrCollection.facet(solrQuery);
//            postProcessing(facetResult, new CaFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    private SolrQuery parseClinicalAnalysisQuery(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, CvdbException {
        // Get collection prefix
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);

        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        ClinicalAnalysisQueryParser parser = new ClinicalAnalysisQueryParser(collectionPrefix, searchIndexMetadata);
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
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

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
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return null;
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalInterpretationQuery(query, queryOptions, token);

        // Execute query
        try {
            String collectionName = collectionNameGenerator.getClinicalInterpretationCollectionName(query.getString(PROJECT_PARAM_NAME),
                    token);
            return new ClinicalIterator<>(solrManager.getSolrClient(), collectionName, solrQuery, queryOptions,
                    ClinicalInterpretationSearch.class, ClinicalInterpretationConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                 | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalInterpretations(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

        // Check
        checkFacet(query, queryOptions, token);
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);

        // Parse query
        ClinicalInterpretationQueryParser parser = new ClinicalInterpretationQueryParser(collectionPrefix, searchIndexMetadata);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collectionName = collectionNameGenerator.getClinicalInterpretationCollectionName(collectionPrefix);
            SolrCollection solrCollection = solrManager.getCollection(collectionName);
            facetResult = solrCollection.facet(solrQuery);
//            postProcessing(facetResult, new CiFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    private SolrQuery parseClinicalInterpretationQuery(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, CvdbException {
        // Get collection prefix
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);

        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        ClinicalInterpretationQueryParser parser = new ClinicalInterpretationQueryParser(collectionPrefix, searchIndexMetadata);
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
        return solrQuery;
    }

    //----------------------------------------------------------------------
    // CLINICAL VARIANT: SEARCH, ITERATOR, FACET
    //----------------------------------------------------------------------

    public DataResult<ClinicalVariant> searchClinicalVariants(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

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
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return null;
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalVariantQuery(query, queryOptions, token);

        // Execute query
        try {
            String collectionName = collectionNameGenerator.getClinicalVariantCollectionName(query.getString(PROJECT_PARAM_NAME), token);
            ClinicalIterator<ClinicalVariant, ClinicalVariantSearch, ClinicalVariantConverter> iterator = new ClinicalIterator<>(
                    solrManager.getSolrClient(), collectionName, solrQuery, queryOptions, ClinicalVariantSearch.class,
                    ClinicalVariantConverter.class);

            ClinicalVariantConverter converter = iterator.getConverter();
            converter.setVariantSearchToVariantConverter(VariantSearchToVariantConverter.converterSimpleStats(searchIndexMetadata));

            return iterator;
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                 | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public ClinicalSolrIterator<ClinicalVariantSearch> clinicalVariantNativeIterator(
            Query query, QueryOptions queryOptions, String token) throws CvdbException, IOException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return null;
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalVariantQuery(query, queryOptions, token);

        // Execute query
        try {
            String collectionName = collectionNameGenerator.getClinicalVariantCollectionName(query.getString(PROJECT_PARAM_NAME), token);
            return new ClinicalSolrIterator<>(getSolrClient(), collectionName, solrQuery, ClinicalVariantSearch.class);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalVariants(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

        // Check
        checkFacet(query, queryOptions, token);

        // Parse query
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);
        ClinicalVariantQueryParser parser = new ClinicalVariantQueryParser(collectionPrefix, searchIndexMetadata);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collectionName = collectionNameGenerator.getClinicalVariantCollectionName(collectionPrefix);
            SolrCollection solrCollection = solrManager.getCollection(collectionName);
            facetResult = solrCollection.facet(solrQuery);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    private SolrQuery parseClinicalVariantQuery(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, CvdbException {
        // Get collection prefix
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);

        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        ClinicalVariantQueryParser parser = new ClinicalVariantQueryParser(collectionPrefix, searchIndexMetadata);
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

    //----------------------------------------------------------------------
    // CLINICAL VARIANT EVIDENCE: SEARCH, ITERATOR, FACET
    //----------------------------------------------------------------------

    public DataResult<ClinicalVariantEvidence> searchClinicalVariantEvidences(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

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
    clinicalVariantEvidenceIterator(Query query, QueryOptions queryOptions, String token)
            throws CvdbException, IOException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return null;
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalVariantEvidenceQuery(query, queryOptions, token);

        // Execute query
        try {
            String collectionName = collectionNameGenerator.getClinicalVariantEvidenceCollectionName(query.getString(PROJECT_PARAM_NAME),
                    token);
            return new ClinicalIterator<>(solrManager.getSolrClient(), collectionName, solrQuery, queryOptions,
                    ClinicalVariantEvidenceSearch.class, ClinicalVariantEvidenceConverter.class);
        } catch (SolrServerException | NoSuchMethodException | InvocationTargetException | InstantiationException
                 | IllegalAccessException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public ClinicalSolrIterator<ClinicalVariantEvidenceSearch> clinicalVariantEvidenceNativeIterator(Query query, QueryOptions queryOptions,
                                                                                                     String token)
            throws CvdbException, IOException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return null;
        }

        // Parse query
        SolrQuery solrQuery = parseClinicalVariantEvidenceQuery(query, queryOptions, token);

        // Execute query
        try {
            String collection = collectionNameGenerator.getClinicalVariantEvidenceCollectionName(query.getString(PROJECT_PARAM_NAME),
                    token);
            return new ClinicalSolrIterator<>(getSolrClient(), collection, solrQuery, ClinicalVariantEvidenceSearch.class);
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }
    }

    public DataResult<FacetField> facetClinicalVariantEvidences(Query query, QueryOptions queryOptions, String token)
            throws IOException, CvdbException, CatalogException {
        // Check CVDB data store availability
        checkQuery(query, queryOptions, token);
        if (!isAvailableCvdbDataStore(query.getString(PROJECT_PARAM_NAME), token)) {
            return new DataResult<>();
        }

        // Check
        checkFacet(query, queryOptions, token);
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);

        // Parse query
        ClinicalVariantEvidenceQueryParser parser = new ClinicalVariantEvidenceQueryParser(collectionPrefix, searchIndexMetadata);
        SolrQuery solrQuery = parser.parse(query, queryOptions);

        // Execute query
        DataResult<FacetField> facetResult;
        try {
            String collection = collectionNameGenerator.getClinicalVariantEvidenceCollectionName(collectionPrefix);
            SolrCollection solrCollection = solrManager.getCollection(collection);
            facetResult = solrCollection.facet(solrQuery);
//            postProcessing(facetResult, new CveFieldMapping());
        } catch (SolrServerException e) {
            throw new CvdbException(e.getMessage(), e);
        }

        return facetResult;
    }

    private SolrQuery parseClinicalVariantEvidenceQuery(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, CvdbException {
        // Update query with user from token
        setViewerInQuery(query, token);

        // Parse query
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(query.getString(PROJECT_PARAM_NAME), token);
        ClinicalVariantEvidenceQueryParser parser = new ClinicalVariantEvidenceQueryParser(collectionPrefix, searchIndexMetadata);
        return parser.parse(query, queryOptions);
    }

    //----------------------------------------------------------------------
    // CLINICAL VARIANT SUMMARY
    //----------------------------------------------------------------------

    public DataResult<ClinicalVariantSummaryStats> getClinicalVariantSummaryStats(List<String> variantIds, List<String> projectIds,
                                                                                  String token)
            throws CatalogException, IOException, CvdbException {
        // Sanity check
        if (CollectionUtils.isEmpty(variantIds)) {
            throw new CvdbException("Missing variant ID(s) when running clinical variant summary");
        }

        if (variantIds.size() > DEFAULT_LIMIT) {
            throw new CvdbException("The maximum number of variants (" + DEFAULT_LIMIT + ") has been exceeded (" + variantIds.size() + ")");
        }

        String order = STATS_DEFAULT_ORDER;
        int limit = STATS_DEFAULT_LIMIT;

        StopWatch stopWatch = StopWatch.createStarted();
        List<ClinicalVariantSummaryStats> variantStatsList = new ArrayList<>(variantIds.size());

        // Get organization
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = jwtPayload.getOrganization();

        DBAdaptorFactory dbAdaptorFactory = EnterpriseFactory.getCatalogDBAdaptorFactory();
        Organization organization = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(ORGANIZATION_OPTIONS).first();

        OpenCGAResult<Project> projectResult;
        if (CollectionUtils.isEmpty(projectIds)) {
            projectResult = catalogManager.getProjectManager().search(organizationId, new Query(), INCLUDE_PROJECT_OPTIONS, token);
        } else {
            projectResult = catalogManager.getProjectManager().get(projectIds, INCLUDE_PROJECT_OPTIONS, false, token);
        }

        // Get local and federated projects in two different list
        List<Project> localProjects = new LinkedList<>();
        Map<String, List<Project>> federatedProjects = new HashMap<>();
        for (Project project : projectResult.getResults()) {
            if (project.getInternal().isFederated()) {
                federatedProjects.putIfAbsent(project.getFederation().getId(), new LinkedList<>());
                federatedProjects.get(project.getFederation().getId()).add(project);
            } else {
                String collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, project.getId(), token);
                if (existCollections(collectionPrefix)) {
                    localProjects.add(project);
                }
            }
        }

        // Get summary stats from federated projects
        ExecutorService executor;
        List<Future<RestResponse<ClinicalVariantSummaryStats>>> federatedSummaryFutureList = new ArrayList<>(federatedProjects.size());
        if (!federatedProjects.isEmpty()) {
            String variantId = StringUtils.join(variantIds, ",");

            executor = Executors.newFixedThreadPool(federatedProjects.size());
            for (Entry<String, List<Project>> entry : federatedProjects.entrySet()) {
                String federationId = entry.getKey();
                List<Project> projectList = entry.getValue();
                List<String> federatedProjectIds = projectList.stream().map(Project::getFqn).collect(Collectors.toList());
                ObjectMap params = new ObjectMap(PROJECT_PARAM_NAME, federatedProjectIds);

                logger.info("Computing variant summary stats from federation {} for projects: {}", federationId,
                        StringUtils.join(federatedProjectIds, ", "));

                FederationClientParams federationClient = FederationUtils.findFederationClient(organization, federationId);
                Future<RestResponse<ClinicalVariantSummaryStats>> future = executor.submit(() -> {
                    try {
                        GenericClient client = FederationUtils.getClientInstance(federationClient);
                        return client.execute("analysis", null, "cvdb/variant", variantId, "stats", params, ParentClient.GET,
                                ClinicalVariantSummaryStats.class);
                    } catch (ClientException e) {
                        throw new RuntimeException(e);
                    }
                });

                federatedSummaryFutureList.add(future);
            }
        }

        // Process summary stats from local projects
        List<ClinicalVariantSummaryStats> localStatsList = new ArrayList<>(variantIds.size());
        if (CollectionUtils.isNotEmpty(localProjects)) {
            logger.info("Computing variant summary stats for local projects: {}",
                    StringUtils.join(localProjects.stream().map(Project::getFqn).collect(Collectors.toList()), ", "));

            Query query;
            Map<String, Map<String, Long>> facetMap = new HashMap<>();

            for (String variantId : variantIds) {
                for (Project project : localProjects) {
                    // Check CVDB data store availability for that project
                    if (!isAvailableCvdbDataStore(project)) {
                        continue;
                    }

                    query = new Query()
                            .append(PROJECT_PARAM_NAME, project.getId())
                            .append(CV_VARIANT_ID_NAME, variantId);

                    ClinicalVariantSummaryStats variantStats = new ClinicalVariantSummaryStats();
                    variantStats.setId(organizationId + "@" + project.getId());
                    variantStats.setVariantId(variantId);

                    // Clinical analysis stats: num. cases, disorder IDs, proband disorder IDs and phenotype names
                    facetMap.clear();
                    facetMap.put("disorderId", variantStats.getClinicalAnalysis().getDisorders());
                    facetMap.put("probandDisorderIds", variantStats.getClinicalAnalysis().getProbandDisorders());
                    facetMap.put("probandPhenotypeNames", variantStats.getClinicalAnalysis().getProbandPhenotypes());
                    performFacet(query, facetMap, "analysis", variantStats, order, limit, token);

                    if (variantStats.getNumClinicalAnalyses() > 0) {
                        // Clinical interpretation stats: num. primary and secondary interpretations; panel IDs and method names
                        facetMap.clear();
                        facetMap.put("primary", null);
                        facetMap.put("panelIds", variantStats.getInterpretation().getPanels());
                        facetMap.put("methodName", variantStats.getInterpretation().getMethods());
                        performFacet(query, facetMap, "interpretation", variantStats, order, limit, token);

                        // Clinical variant stats: status and confidence values
                        facetMap.clear();
                        facetMap.put("status", variantStats.getVariant().getStatus());
                        facetMap.put("confidenceValue", variantStats.getVariant().getConfidences());
                        performFacet(query, facetMap, "variant", variantStats, order, limit, token);

                        // Clinical variant evidence stats: gene names, transcript IDs, SO term accessions, panel IDs, MoIs, ACMGs, and for review
                        // tiers, ACMGs and clinical significances
                        facetMap.clear();
                        facetMap.put("geneName", variantStats.getEvidence().getGenes());
                        facetMap.put("transcriptId", variantStats.getEvidence().getTranscripts());
                        facetMap.put("soTermNames", variantStats.getEvidence().getSoTerms());
                        facetMap.put("panelId", variantStats.getEvidence().getPanels());
                        facetMap.put("mois", variantStats.getEvidence().getMois());
                        facetMap.put("acmgs", variantStats.getEvidence().getAcmgs());
                        facetMap.put("reviewAcmgs", variantStats.getEvidence().getReviewAcmgs());
                        facetMap.put("reviewTier", variantStats.getEvidence().getReviewTiers());
                        facetMap.put("reviewClinicalSignificance", variantStats.getEvidence().getReviewClinicalSignificances());
                        performFacet(query, facetMap, "evidence", variantStats, order, limit, token);

                        // Add to the list
                        localStatsList.add(variantStats);
                    }
                }
            }
        }

        // Prepare aggregated stats ALL from the federated ones: stats for a given variant that is present in multiple projects
        Map<String, ClinicalVariantSummaryStats> aggMap = new HashMap<>();

        // Get (and wait if necessary for) summary stats from federated projects
        if (CollectionUtils.isNotEmpty(federatedSummaryFutureList)) {
            for (Future<RestResponse<ClinicalVariantSummaryStats>> restResponseFuture : federatedSummaryFutureList) {
                try {
                    for (ClinicalVariantSummaryStats stats : restResponseFuture.get().allResults()) {
                        if ("ALL".equals(stats.getId())) {
                            aggMap.put(stats.getVariantId(), stats);
                        } else {
                            variantStatsList.add(stats);
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Error computing summary stats from federated projects", e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        // Compute the summary stats ALL for each variant (if necessary)
        for (ClinicalVariantSummaryStats variantStats : localStatsList) {
            // Add the local stats to the list to return
            variantStatsList.add(variantStats);

            if (!aggMap.containsKey(variantStats.getVariantId())) {
                ClinicalVariantSummaryStats aggVariantStats = new ClinicalVariantSummaryStats();
                aggVariantStats.setId("ALL");
                aggVariantStats.setVariantId(variantStats.getVariantId());
                aggMap.put(variantStats.getVariantId(), aggVariantStats);
            }
            // Update aggregated variant stats
            updateSummaryStats(variantStats, aggMap.get(variantStats.getVariantId()));
        }

        // Add the aggregated stats ALL to the list to return
        variantStatsList.addAll(aggMap.values().stream().filter(s -> s.getNumClinicalAnalyses() > 1).collect(Collectors.toList()));

        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        return new DataResult<>(dbTime, null, variantStatsList.size(), variantStatsList, variantStatsList.size());
    }

    private void performFacet(Query query, Map<String, Map<String, Long>> facetMap, String type, ClinicalVariantSummaryStats stats,
                              String order, int limit, String token) throws IOException, CvdbException, CatalogException {
        DataResult<FacetField> facetResult;
        List<String> facetNames = new ArrayList<>(facetMap.keySet());
        QueryOptions queryOptions = new QueryOptions(FACET, StringUtils.join(facetNames, FacetQueryParser.FACET_SEPARATOR));
        queryOptions.put(LIMIT, limit);
        queryOptions.put(ORDER, order);
        switch (type) {
            case "analysis": {
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

    public DataResult<ClinicalVariantSummaryStats> getClinicalVariantSummaryStats(String variantId, String projectId, String token)
            throws CatalogException, IOException, CvdbException {
        // Checking parameter
        if (StringUtils.isEmpty(variantId)) {
            throw new CvdbException("Missing variant ID(s) when running clinical variant summary");
        }
        List<String> variantIds = new ArrayList<>();
        if (StringUtils.isNotEmpty(variantId)) {
            variantIds.addAll(Arrays.asList(variantId.split(",")));
        }
        List<String> projectIds = new ArrayList<>();
        if (StringUtils.isNotEmpty(projectId)) {
            projectIds.addAll(Arrays.asList(projectId.split(",")));
        }
        return getClinicalVariantSummaryStats(variantIds, projectIds, token);
    }

    //----------------------------------------------------------------------
    // P R I V A T E      M E T H O D S
    //----------------------------------------------------------------------

    private void checkQuery(Query query, QueryOptions queryOptions, String token) throws CvdbException, CatalogException {
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
            OpenCGAResult<Study> studyResult = catalogManager.getStudyManager().get(studyInputValue, empty(), token);
            if (!projectResult.first().getId().equals(FqnUtils.getProject(studyResult.first().getFqn()))) {
                throw new CvdbException("Project ID '" + projectResult.first().getFqn() + "' and study ID '"
                        + studyResult.first().getFqn() + "' mismatch. Please ensure that the study belongs to the specified project");
            }
            query.put(PROJECT_PARAM_NAME, projectResult.first().getId());
            query.put(STUDY_PARAM_NAME, studyResult.first().getId());
        }
    }

    private void checkFacet(Query query, QueryOptions queryOptions, String token) throws CvdbException, CatalogException {
        if (!queryOptions.containsKey(FACET) || StringUtils.isEmpty(queryOptions.getString(FACET))) {
            throw new CvdbException("Missing facet field to aggregation stats");
        }
    }

    private boolean index(ClinicalAnalysis clinicalAnalysis, String organizationId, String projectId, String studyFqn,
                          String collectionPrefix ) throws CvdbException {
        SolrClient solrClient = solrManager.getSolrClient();

        try {
            // Index
            if (index(clinicalAnalysis, organizationId, projectId, studyFqn, collectionPrefix, solrClient)) {
                // Commit
                solrClient.commit(collectionNameGenerator.getClinicalAnalysisCollectionName(collectionPrefix));
                solrClient.commit(collectionNameGenerator.getClinicalInterpretationCollectionName(collectionPrefix));
                solrClient.commit(collectionNameGenerator.getClinicalVariantCollectionName(collectionPrefix));
                solrClient.commit(collectionNameGenerator.getClinicalVariantEvidenceCollectionName(collectionPrefix));
                return true;
            }
        } catch (SolrServerException | IOException e) {
            logger.warn("Error indexing clinical analysis {}: {}", clinicalAnalysis.getId(), e.getMessage());
            rollback(solrClient, e);
            return false;
        }
        return false;
    }

    private boolean index(ClinicalAnalysis clinicalAnalysis, String organizationId, String projectId, String studyFqn,
                          String collectionPrefix, SolrClient solrClient) throws CvdbException {
        try {
            logger.info("Indexing clinical analysis {} ...", clinicalAnalysis.getId());
            UpdateResponse updateResponse;

            boolean exists;
            try {
                exists = clinicalAnalysisExists(clinicalAnalysis.getId(),
                        collectionNameGenerator.getClinicalAnalysisCollectionName(collectionPrefix), solrClient);
            } catch (SolrServerException | IOException e) {
                logger.warn("Something wrong happened, clinical analysis {} could not be indexed: {}", clinicalAnalysis.getId(),
                        e.getMessage());
                return false;
            }

            if (!exists) {
                // Clinical analysis
                ClinicalAnalysisSearch cas = caConverter.toClinicalAnalysisSearch(clinicalAnalysis, FqnUtils.getStudy(studyFqn));

                // Index
                updateResponse = solrClient.addBean(collectionNameGenerator.getClinicalAnalysisCollectionName(collectionPrefix), cas);
                if (updateResponse.getStatus() != 0) {
                    rollback(solrClient, updateResponse.getStatus());
                }

                // Primary interpretation
                if (clinicalAnalysis.getInterpretation() != null) {
                    index(clinicalAnalysis.getInterpretation(), true, organizationId, projectId, studyFqn, collectionPrefix, solrClient);
                }

                // Secondary interpretations
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                    for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                        index(secondaryInterpretation, false, organizationId, projectId, studyFqn, collectionPrefix, solrClient);
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

    private void index(Interpretation interpretation, boolean isPrimaryInterpretation, String organizationId, String projectId,
                       String studyFqn, String collectionPrefix, SolrClient solrClient) throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Interpretation
            ClinicalInterpretationSearch cis = ciConverter.toInterpretationSearch(interpretation, isPrimaryInterpretation,
                    FqnUtils.getStudy(studyFqn));

            updateResponse = solrClient.addBean(collectionNameGenerator.getClinicalInterpretationCollectionName(collectionPrefix), cis);
            if (updateResponse.getStatus() != 0) {
                rollback(solrClient, updateResponse.getStatus());
            }

            // Primary findings
            if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                for (ClinicalVariant primaryFinding : interpretation.getPrimaryFindings()) {
                    index(primaryFinding, true, interpretation.getId(), isPrimaryInterpretation, interpretation.getClinicalAnalysisId(),
                            organizationId, projectId, studyFqn, collectionPrefix, solrClient);
                }
            }
            // Secondary findings
            if (CollectionUtils.isNotEmpty(interpretation.getSecondaryFindings())) {
                for (ClinicalVariant secondaryFinding : interpretation.getSecondaryFindings()) {
                    index(secondaryFinding, false, interpretation.getId(), isPrimaryInterpretation, interpretation.getClinicalAnalysisId(),
                            organizationId, projectId, studyFqn, collectionPrefix, solrClient);
                }
            }
        } catch (SolrServerException | IOException e) {
            rollback(solrClient, e);
        }
    }

    private void index(ClinicalVariant clinicalVariant, boolean isPrimaryFinding, String interpretationId, boolean isPrimaryInterpretation,
                       String clinicalAnalysisId, String organizationId, String projectId, String studyFqn, String collectionPrefix,
                       SolrClient solrClient) throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical variant search
            if (clinicalVariant.getAttributes() == null) {
                clinicalVariant.setAttributes(new HashMap<>());
            }
            clinicalVariant.getAttributes().put(OPENCGA_STUDY_ID, studyFqn);
            clinicalVariant.getAttributes().put(OPENCGA_CLINICAL_ANALYSIS_ID, clinicalAnalysisId);
            clinicalVariant.getAttributes().put(OPENCGA_INTERPRETATION_ID, interpretationId);
            clinicalVariant.getAttributes().put(OPENCGA_PRIMARY_INTERPRETATION, isPrimaryInterpretation);
            clinicalVariant.getAttributes().put(OPENCGA_PRIMARY_FINDING, isPrimaryFinding);

            ClinicalVariantSearch cvs = cvConverter.toClinicalVariantSearch(clinicalVariant, isPrimaryFinding, interpretationId,
                    isPrimaryInterpretation, clinicalAnalysisId, FqnUtils.getStudy(studyFqn));

            updateResponse = solrClient.addBean(collectionNameGenerator.getClinicalVariantCollectionName(collectionPrefix), cvs);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }

            int evidenceIndex = 0;
            for (ClinicalVariantEvidence evidence : clinicalVariant.getEvidences()) {
                index(evidence, evidenceIndex++, clinicalVariant.getId(), isPrimaryFinding, interpretationId, isPrimaryInterpretation,
                        clinicalAnalysisId, organizationId, projectId, studyFqn, collectionPrefix, solrClient);
            }
        } catch (SolrServerException | IOException e) {
            logger.warn("Indexing clinical variant", e);
            rollback(solrClient, e);
        }
    }

    private void index(ClinicalVariantEvidence clinicalVariantEvidence, int evidenceIndex, String variantId, boolean isPrimaryFinding,
                       String interpretationId, boolean isPrimaryInterpretation, String clinicalAnalysisId, String organizationId,
                       String projectId, String studyFqn, String collectionPrefix, SolrClient solrClient)
            throws CvdbException {
        try {
            UpdateResponse updateResponse;

            // Clinical variant evidences
            if (clinicalVariantEvidence.getAttributes() == null) {
                clinicalVariantEvidence.setAttributes(new HashMap<>());
            }
            clinicalVariantEvidence.getAttributes().put(OPENCGA_STUDY_ID, studyFqn);
            clinicalVariantEvidence.getAttributes().put(OPENCGA_CLINICAL_ANALYSIS_ID, clinicalAnalysisId);
            clinicalVariantEvidence.getAttributes().put(OPENCGA_INTERPRETATION_ID, interpretationId);
            clinicalVariantEvidence.getAttributes().put(OPENCGA_PRIMARY_INTERPRETATION, isPrimaryInterpretation);
            clinicalVariantEvidence.getAttributes().put(OPENCGA_VARIANT_ID, variantId);
            clinicalVariantEvidence.getAttributes().put(OPENCGA_PRIMARY_FINDING, isPrimaryFinding);

            ClinicalVariantEvidenceSearch cves = cveConverter.toClinicalVariantEvidenceSearch(clinicalVariantEvidence, evidenceIndex,
                    variantId, isPrimaryFinding, interpretationId, isPrimaryInterpretation, clinicalAnalysisId,
                    FqnUtils.getStudy(studyFqn));

            updateResponse = solrClient.addBean(collectionNameGenerator.getClinicalVariantEvidenceCollectionName(collectionPrefix), cves);
            if (updateResponse.getStatus() != 0) {
                solrClient.rollback();
            }
        } catch (SolrServerException | IOException e) {
            logger.warn("Indexing clinical variant evidence", e);
            rollback(solrClient, e);
        }
    }

    private void rollback(SolrClient solrClient, int status) throws CvdbException {
        try {
            solrClient.rollback();
            throw new CvdbException("Error when adding Solr documents (status = " + status + ")");
        } catch (SolrServerException | IOException e) {
            String msg = "Error when rollingback Solr after adding documents (status = " + status + ")";
            throw new CvdbException(msg, e);
        }
    }

    private void rollback(SolrClient solrClient, Exception exception) throws CvdbException {
        try {
            solrClient.rollback();
            throw new CvdbException("Solr exception", exception);
        } catch (SolrServerException | IOException e) {
            String msg = "Error when rollingback after Solr exception (" + exception.getMessage() + ")";
            throw new CvdbException(msg, e);
        }
    }

    public boolean existCollections(String collectionPrefix) throws CvdbException {
        try {
            List<String> collectionNames = collectionNameGenerator.getCollectionNames(collectionPrefix);
            for (String collectionName : collectionNames) {
                if (!solrManager.exists(collectionName)) {
                    logger.info("Collection '{}' does not exist", collectionName);
                    return false;
                }
            }
            return true;
        } catch (SolrException e) {
            String msg = "Checking if Solr CVDB collections exist (collection prefix = '" + collectionPrefix + "')";
            throw new CvdbException(msg, e);
        }
    }

    public List<String> getCollectionNames(String collectionPrefix) throws CvdbException {
        try {
            List<String> collectionNames = collectionNameGenerator.getCollectionNames(collectionPrefix);
            for (String collectionName : collectionNames) {
                if (!solrManager.exists(collectionName)) {
                    String msg = "Error getting CVDB collection names: not all CVDB collections found for prefix '" + collectionPrefix
                            + "'. Collection '" + collectionName + "' does not exist";
                    throw new CvdbException(msg);
                }
            }
            return collectionNames;
        } catch (SolrException e) {
            String msg = "Getting Solr CVDB collection names (collection prefix = '" + collectionPrefix + "')";
            throw new CvdbException(msg, e);
        }
    }

    public void createCollections(String projectFqn, String collectionPrefix, String token) throws CvdbException {
        // Sanity check
        Project project;
        ProjectManager projectManager = catalogManager.getProjectManager();
        QueryOptions queryOptions = new QueryOptions().append(INCLUDE, ProjectDBAdaptor.QueryParams.INTERNAL_DATASTORES_CVDB.key());
        try {
            project = projectManager.get(projectFqn, queryOptions, token).first();
        } catch (CatalogException e) {
            String msg = "Error checking project '" + projectFqn + "' when creating CVDB collections.";
            throw new CvdbException(msg, e);
        }
        if (project == null) {
            String msg = "Project '" + projectFqn + "' not found when creating CVDB collections.";
            logger.error(msg);
            throw new CvdbException(msg);
        }

        // Create collections
        logger.info("Creating CVDB collections for project '{}', collection prefix '{}'", projectFqn, collectionPrefix);
        List<String> collectionNames = createCollections(collectionPrefix);

        // If created successfully, set the CVDB datastore in the project
        DataStore cvdbDatastore = new DataStore("solr", collectionPrefix, new ObjectMap("collections", collectionNames));
        logger.info("Setting CVDB datastore in project '{}': {}", projectFqn, cvdbDatastore);
        try {
            projectManager.setDatastoreCvdb(project.getFqn(), cvdbDatastore, token);
        } catch (CatalogException e) {
            throw new CvdbException("Error setting CVDB datastore after creating CVDB collections", e);
        }
    }

    private List<String> createCollections(String collectionPrefix) throws CvdbException {
        List<String> collectionNames = new ArrayList<>();
        try {
            for (String collectionSuffix : collectionNameGenerator.getCollectionSuffixes()) {
                String collectionName = collectionNameGenerator.getCollectionName(collectionPrefix, collectionSuffix);
                if (!solrManager.exists(collectionName)) {
                    String configSet = COLLECTION_CONFIGSETS_MAP.get(collectionSuffix);
                    logger.info("Creating collection name = {}, config set = {}", collectionName, configSet);
                    solrManager.create(collectionName, configSet);
                }
                collectionNames.add(collectionName);
            }
        } catch (SolrException e) {
            throw new CvdbException("Creating Solr CVDB collections; collection prefix = '" + collectionPrefix + "'", e);
        }
        return collectionNames;
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

    public List<String> getCvdbProjects(List<String> organizationIds, String token) throws CvdbException, CatalogException {
        List<String> projectFqns = new ArrayList<>();

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions(INCLUDE, Arrays.asList(ProjectDBAdaptor.QueryParams.INTERNAL_DATASTORES_CVDB.key(),
                ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.FQN.key()));
        for (String organizationId : organizationIds) {
            OpenCGAResult<Project> projectResults = catalogManager.getProjectManager().search(organizationId, query, queryOptions, token);
            if (CollectionUtils.isNotEmpty(projectResults.getResults())) {
                for (Project project : projectResults.getResults()) {
                    String dbPrefix = getCvdbPrefix(organizationId, project);
                    if (existCollections(dbPrefix)) {
                        projectFqns.add(project.getFqn());
                    }
                }
            }
        }

        return  projectFqns;
    }

    public DataStore getCvdbDatastore(String projectFqn, String token) throws CatalogException, CvdbException {
        QueryOptions queryOptions = new QueryOptions(INCLUDE, Arrays.asList(ProjectDBAdaptor.QueryParams.INTERNAL_DATASTORES_CVDB.key(),
                ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.FQN.key()));
        Project project = catalogManager.getProjectManager().get(projectFqn, queryOptions, token).first();
        String dbPrefix = getCvdbPrefix(FqnUtils.getOrganization(projectFqn), project);

        return new DataStore("solr", dbPrefix, new ObjectMap("collections", collectionNameGenerator.getCollectionNames(dbPrefix)));
    }

    private String getCvdbPrefix(String organizationId, Project project) {
        String dbPrefix;
        if (project.getInternal() != null && project.getInternal().getDatastores() != null
                && project.getInternal().getDatastores().getCvdb() != null
                && StringUtils.isNotEmpty(project.getInternal().getDatastores().getCvdb().getDbName())) {
            dbPrefix = project.getInternal().getDatastores().getCvdb().getDbName();
        } else {
            dbPrefix = VariantStorageManager.buildDatabaseName(catalogManager.getConfiguration().getDatabasePrefix(), "cvdb",
                    organizationId, project.getId());
        }
        logger.info("Checking CVDB database prefix '{}' for project '{}'", dbPrefix, project.getFqn());
        return dbPrefix;
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

    public ClinicalVariantSummaryStats sortSummaryStats(ClinicalVariantSummaryStats srcStats, String order, int limit) {
        ClinicalVariantSummaryStats sortedStats = new ClinicalVariantSummaryStats();
        sortedStats.setId(srcStats.getId());
        sortedStats.setVariantId(srcStats.getVariantId());

        // Clinical analysis stats: num. cases, disorder IDs, proband disorder IDs and phenotype names
        sortedStats.setNumClinicalAnalyses(srcStats.getNumClinicalAnalyses());
        sortedStats.getClinicalAnalysis().setDisorders(sortMap(srcStats.getClinicalAnalysis().getDisorders(), order, limit));
        sortedStats.getClinicalAnalysis().setProbandDisorders(sortMap(srcStats.getClinicalAnalysis().getProbandDisorders(), order, limit));
        sortedStats.getClinicalAnalysis().setProbandPhenotypes(sortMap(srcStats.getClinicalAnalysis().getProbandPhenotypes(), order,
                limit));

        // Clinical interpretation stats: num. primary and secondary interpretations; panel IDs and method names
        sortedStats.setNumPrimaryInterpretations(srcStats.getNumPrimaryInterpretations());
        sortedStats.setNumSecondaryInterpretations(srcStats.getNumSecondaryInterpretations());
        sortedStats.getInterpretation().setPanels(sortMap(srcStats.getInterpretation().getPanels(), order, limit));
        sortedStats.getInterpretation().setMethods(sortMap(srcStats.getInterpretation().getMethods(), order, limit));

        // Clinical variant stats: status and confidence values
        sortedStats.getVariant().setStatus(sortMap(srcStats.getVariant().getStatus(), order, limit));
        sortedStats.getVariant().setConfidences(sortMap(srcStats.getVariant().getConfidences(), order, limit));

        // Clinical variant evidence stats: gene names, transcript IDs, SO term accessions, panel IDs, MoIs, ACMGs, and for review
        // tiers, ACMGs and clinical significances
        sortedStats.getEvidence().setGenes(sortMap(srcStats.getEvidence().getGenes(), order, limit));
        sortedStats.getEvidence().setTranscripts(sortMap(srcStats.getEvidence().getTranscripts(), order, limit));
        sortedStats.getEvidence().setSoTerms(sortMap(srcStats.getEvidence().getSoTerms(), order, limit));
        sortedStats.getEvidence().setPanels(sortMap(srcStats.getEvidence().getPanels(), order, limit));
        sortedStats.getEvidence().setMois(sortMap(srcStats.getEvidence().getMois(), order, limit));
        sortedStats.getEvidence().setAcmgs(sortMap(srcStats.getEvidence().getAcmgs(), order, limit));
        sortedStats.getEvidence().setReviewTiers(sortMap(srcStats.getEvidence().getReviewTiers(), order, limit));
        sortedStats.getEvidence().setReviewAcmgs(sortMap(srcStats.getEvidence().getReviewAcmgs(), order, limit));
        sortedStats.getEvidence().setReviewClinicalSignificances(sortMap(srcStats.getEvidence().getReviewClinicalSignificances(), order,
                limit));

        return sortedStats;
    }

    private Map<String, Long> sortMap(Map<String, Long> srcMap, String order, int limit) {
        if (MapUtils.isEmpty(srcMap)) {
            return srcMap;
        }

        Comparator<Entry<String, Long>> comparator;
        if (order.equals(DESC) || order.equals(DESCENDING)) {
            comparator = Entry.<String, Long>comparingByValue().reversed();
        } else {
            comparator = Entry.<String, Long>comparingByValue();
        }

        return srcMap.entrySet()
                .stream()
                .sorted(comparator) // Sort by value in descending order
                .limit(limit)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, // Handle duplicate keys
                        LinkedHashMap::new // Maintain insertion order
                ));
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

    private boolean isAvailableCvdbDataStore(String projectId, String token) throws CatalogException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "internal.datastores");
        Project project = catalogManager.getProjectManager().get(projectId, queryOptions, token).first();
        return isAvailableCvdbDataStore(project);
    }

    private boolean isAvailableCvdbDataStore(Project project) throws CatalogException {
        return (project != null
                && project.getInternal() != null
                && project.getInternal().getDatastores() != null
                && project.getInternal().getDatastores().getCvdb() != null
                && StringUtils.isNotEmpty(project.getInternal().getDatastores().getCvdb().getDbName()));
    }

    //----------------------------------------------------------------------

    public void close() {
        logger.debug("Closing CVDB engine (i.e., Solr client connection used by the solrManager = {})", solrManager);
        if (solrManager != null) {
            try {
                solrManager.close();
                logger.debug("CVDB engine closed successfully (i.e., the Solr client used by SolrManager)");
            } catch (IOException e) {
                logger.error("Error closing Solr manager", e);
            }
        }
        if (catalogManager != null) {
            catalogManager = null;
        }
        if (searchIndexMetadata != null) {
            searchIndexMetadata = null;
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

    public CollectionNameGenerator getCollectionNameGenerator() {
        return collectionNameGenerator;
    }

    public CvdbSolrEngine setCollectionNameGenerator(CollectionNameGenerator collectionNameGenerator) {
        this.collectionNameGenerator = collectionNameGenerator;
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

    public SearchIndexMetadata getSearchIndexMetadata() {
        return searchIndexMetadata;
    }
}
