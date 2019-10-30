package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetQueryResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.ClinicalAnalysisAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public class InterpretationManager extends ResourceManager<Interpretation> {

    protected static Logger logger = LoggerFactory.getLogger(InterpretationManager.class);

    private UserManager userManager;
    private StudyManager studyManager;

    public static final QueryOptions INCLUDE_INTERPRETATION_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            InterpretationDBAdaptor.QueryParams.ID.key(), InterpretationDBAdaptor.QueryParams.UID.key(),
            InterpretationDBAdaptor.QueryParams.UUID.key(), InterpretationDBAdaptor.QueryParams.VERSION.key()));

    public InterpretationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                                 DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                                 Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    QueryResult<Interpretation> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            queryCopy.put(InterpretationDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(InterpretationDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//                InterpretationDBAdaptor.QueryParams.UUID.key(), InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS.key(),
//                InterpretationDBAdaptor.QueryParams.UID.key(), InterpretationDBAdaptor.QueryParams.STUDY_UID.key(),
//                InterpretationDBAdaptor.QueryParams.ID.key(), InterpretationDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<Interpretation> interpretationQueryResult = interpretationDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
        if (interpretationQueryResult.getNumResults() == 0) {
            interpretationQueryResult = interpretationDBAdaptor.get(queryCopy, queryOptions);
            if (interpretationQueryResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the interpretation "
                        + entry);
            }
        } else if (interpretationQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one interpretation found based on " + entry);
        } else {
            // We perform this query to check permissions because interpretations doesn't have ACLs
            try {
                catalogManager.getClinicalAnalysisManager().internalGet(studyUid,
                        interpretationQueryResult.first().getClinicalAnalysisId(),
                        ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, user);
            } catch (CatalogException e) {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the interpretation "
                        + entry);
            }

            return interpretationQueryResult;
        }
    }

    @Override
    InternalGetQueryResult<Interpretation> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                                       String user, boolean silent) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing interpretation entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<Interpretation, String> interpretationStringFunction = Interpretation::getId;
        InterpretationDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            InterpretationDBAdaptor.QueryParams param = InterpretationDBAdaptor.QueryParams.ID;
            if (UUIDUtils.isOpenCGAUUID(entry)) {
                param = InterpretationDBAdaptor.QueryParams.UUID;
                interpretationStringFunction = Interpretation::getUuid;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        QueryResult<Interpretation> interpretationQueryResult = interpretationDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        if (interpretationQueryResult.getNumResults() != uniqueList.size() && !silent) {
            throw CatalogException.notFound("interpretations",
                    getMissingFields(uniqueList, interpretationQueryResult.getResult(), interpretationStringFunction));
        }

        ArrayList<Interpretation> interpretationList = new ArrayList<>(interpretationQueryResult.getResult());
        Iterator<Interpretation> iterator = interpretationList.iterator();
        while (iterator.hasNext()) {
            Interpretation interpretation = iterator.next();
            // Check if the user has access to the corresponding clinical analysis
            try {
                catalogManager.getClinicalAnalysisManager().internalGet(studyUid,
                        interpretation.getClinicalAnalysisId(), ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, user);
            } catch (CatalogAuthorizationException e) {
                if (silent) {
                    // Remove interpretation. User will not have permissions
                    iterator.remove();
                } else {
                    throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the"
                            + " interpretations", e);
                }
            }
        }

        interpretationQueryResult.setResult(interpretationList);
        interpretationQueryResult.setNumResults(interpretationList.size());
        interpretationQueryResult.setNumTotalResults(interpretationList.size());

        return keepOriginalOrder(uniqueList, interpretationStringFunction, interpretationQueryResult, silent, false);
    }

    public QueryResult<Job> queue(String studyStr, String interpretationTool, String clinicalAnalysisId, List<String> panelIds,
                                  ObjectMap analysisOptions, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(), clinicalAnalysisId,
                ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, userId).first();

        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        // Queue job
        Map<String, String> params = new HashMap<>();
        params.put("includeLowCoverage", analysisOptions.getString("includeLowCoverage"));
        params.put("maxLowCoverage", analysisOptions.getString("maxLowCoverage"));
        params.put("includeNoTier", analysisOptions.getString("includeNoTier"));
        params.put("clinicalAnalysisId", clinicalAnalysisId);
        params.put("panelIds", StringUtils.join(panelIds, ","));

        ObjectMap attributes = new ObjectMap();
        attributes.putIfNotNull(Job.OPENCGA_STUDY, study.getFqn());

        return catalogManager.getJobManager().queue(studyStr, "Interpretation analysis", "opencga-analysis", "",
                "interpretation " + interpretationTool, Job.Type.ANALYSIS, params, Collections.emptyList(), Collections.emptyList(), null,
                attributes, token);
    }

    public QueryResult<Interpretation> create(String studyStr, Interpretation entry, QueryOptions options, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(entry.getClinicalAnalysisId())) {
            throw new IllegalArgumentException("Please call to create passing a clinical analysis id");
        }
        return create(studyStr, entry.getClinicalAnalysisId(), entry, options, sessionId);
    }

    public QueryResult<Interpretation> create(String studyStr, String clinicalAnalysisStr, Interpretation interpretation,
                                              QueryOptions options, String sessionId) throws CatalogException {
        // We check if the user can create interpretations in the clinical analysis
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(), clinicalAnalysisStr,
                ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, userId).first();

        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(),
                userId, ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(interpretation, "clinicalAnalysis");
        ParamUtils.checkAlias(interpretation.getId(), "id");

        interpretation.setClinicalAnalysisId(clinicalAnalysis.getId());

        interpretation.setCreationDate(TimeUtils.getTime());
        interpretation.setDescription(ParamUtils.defaultString(interpretation.getDescription(), ""));
        interpretation.setStatus(org.opencb.biodata.models.clinical.interpretation.Interpretation.Status.NOT_REVIEWED);
        interpretation.setAttributes(ParamUtils.defaultObject(interpretation.getAttributes(), Collections.emptyMap()));

        interpretation.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.INTERPRETATION));

        QueryResult<Interpretation> queryResult = interpretationDBAdaptor.insert(study.getUid(), interpretation, options);

        // Now, we add the interpretation to the clinical analysis
        ObjectMap parameters = new ObjectMap();
        parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), Collections.singletonList(queryResult.first()));
        QueryOptions queryOptions = new QueryOptions();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), ParamUtils.UpdateAction.ADD.name());
        queryOptions.put(Constants.ACTIONS, actionMap);
        clinicalDBAdaptor.update(clinicalAnalysis.getUid(), parameters, queryOptions);

        return queryResult;
    }

    @Override
    public QueryResult<Interpretation> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options,
                                              String token) throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);
        Interpretation interpretation = internalGet(study.getUid(), entryStr, QueryOptions.empty(), userId).first();

        // Check if user has permissions to write clinical analysis
        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(),
                interpretation.getClinicalAnalysisId(), ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, userId).first();
        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(),
                    (a) -> InterpretationDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        if (ListUtils.isNotEmpty(interpretation.getPrimaryFindings()) && (parameters.size() > 1
                || !parameters.containsKey(InterpretationDBAdaptor.UpdateParams.REPORTED_VARIANTS.key()))) {
            throw new CatalogException("Interpretation already has reported variants. Only array of reported variants can be updated.");
        }

        if (parameters.containsKey(InterpretationDBAdaptor.UpdateParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(InterpretationDBAdaptor.UpdateParams.ID.key()),
                    InterpretationDBAdaptor.UpdateParams.ID.key());
        }

        QueryResult<Interpretation> queryResult = interpretationDBAdaptor.update(interpretation.getUid(), parameters, options);
        auditManager.recordUpdate(AuditRecord.Resource.interpretation, interpretation.getUid(), userId, parameters,
                null, null);

        return queryResult;
    }

    @Override
    public QueryResult<Interpretation> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<Interpretation> queryResult = interpretationDBAdaptor.get(study.getUid(), query, options, userId);

        List<Interpretation> results = new ArrayList<>(queryResult.getResult().size());
        for (Interpretation interpretation : queryResult.getResult()) {
            if (StringUtils.isNotEmpty(interpretation.getClinicalAnalysisId())) {
                try {
                    catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(),
                            interpretation.getClinicalAnalysisId(), ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS,
                            userId);
                    results.add(interpretation);
                } catch (CatalogException e) {
                    logger.debug("Removing interpretation " + interpretation.getUuid() + " from results. User " + userId + " does not have "
                            + "proper permissions");
                }
            }
        }

        queryResult.setResult(results);
        queryResult.setNumTotalResults(results.size());
        queryResult.setNumResults(results.size());
        return queryResult;
    }

    @Override
    public DBIterator<Interpretation> iterator(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Interpretation> search(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        return get(studyStr, query, options, sessionId);
    }

    @Override
    public QueryResult<Interpretation> count(String studyStr, Query query, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        return null;
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        return null;
    }
}
