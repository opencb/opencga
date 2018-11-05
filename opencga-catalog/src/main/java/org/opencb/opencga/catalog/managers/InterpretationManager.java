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
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.ClinicalAnalysisAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

public class InterpretationManager extends ResourceManager<Interpretation> {

    protected static Logger logger = LoggerFactory.getLogger(InterpretationManager.class);

    public InterpretationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                                 DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                                 Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    @Override
    Interpretation smartResolutor(long studyUid, String entry, String user) throws CatalogException {
        Query query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(InterpretationDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(InterpretationDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                InterpretationDBAdaptor.QueryParams.UUID.key(), InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS.key(),
                InterpretationDBAdaptor.QueryParams.UID.key(), InterpretationDBAdaptor.QueryParams.STUDY_UID.key(),
                InterpretationDBAdaptor.QueryParams.ID.key(), InterpretationDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<Interpretation> interpretationQueryResult = interpretationDBAdaptor.get(query, options, user);
        if (interpretationQueryResult.getNumResults() == 0) {
            interpretationQueryResult = interpretationDBAdaptor.get(query, options);
            if (interpretationQueryResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the interpretation "
                        + entry);
            }
        } else if (interpretationQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one interpretation found based on " + entry);
        } else {
            Interpretation interpretation = interpretationQueryResult.first();

            // We perform this query to check permissions because interpretations doesn't have ACLs
            try {
                catalogManager.getClinicalAnalysisManager().smartResolutor(studyUid,
                        interpretation.getInterpretation().getClinicalAnalysisId(), user);
            } catch (CatalogException e) {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the interpretation "
                        + entry);
            }

            return interpretation;
        }
    }

    @Override
    public QueryResult<Interpretation> create(String studyStr, Interpretation entry, QueryOptions options, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(entry.getInterpretation().getClinicalAnalysisId())) {
            throw new IllegalArgumentException("Please call to create passing a clinical analysis");
        }
        return create(studyStr, entry.getInterpretation().getClinicalAnalysisId(), entry, options, sessionId);
    }

    public QueryResult<Interpretation> create(String studyStr, String clinicalAnalysisStr, Interpretation interpretation,
                                              QueryOptions options, String sessionId) throws CatalogException {
        // We check if the user can create interpretations in the clinical analysis
        MyResource<ClinicalAnalysis> resource = catalogManager.getClinicalAnalysisManager()
                .getUid(clinicalAnalysisStr, studyStr, sessionId);
        authorizationManager.checkClinicalAnalysisPermission(resource.getStudy().getUid(), resource.getResource().getUid(),
                resource.getUser(), ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(interpretation, "clinicalAnalysis");
        ParamUtils.checkAlias(interpretation.getInterpretation().getId(), "id");

        interpretation.getInterpretation().setCreationDate(TimeUtils.getTime());
        interpretation.getInterpretation().setDescription(
                ParamUtils.defaultString(interpretation.getInterpretation().getDescription(), ""));
        interpretation.getInterpretation().setStatus(Status.READY);
        interpretation.getInterpretation().setAttributes(
                ParamUtils.defaultObject(interpretation.getInterpretation().getAttributes(), Collections.emptyMap()));
        interpretation.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.INTERPRETATION));

        QueryResult<Interpretation> queryResult = interpretationDBAdaptor.insert(resource.getStudy().getUid(), interpretation, options);

        // Now, we add the interpretation to the clinical analysis
        ObjectMap parameters = new ObjectMap();
        parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), Arrays.asList(interpretation));
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), ParamUtils.UpdateAction.ADD.name());
        parameters.put(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(resource.getStudy().getFqn(), clinicalAnalysisStr, parameters, null, sessionId);

        return queryResult;
    }

    @Override
    public QueryResult<Interpretation> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options,
                                              String sessionId) throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResource<Interpretation> resource = getUid(entryStr, studyStr, sessionId);

        // Check if user has permissions to write clinical analysis
        MyResource<ClinicalAnalysis> clinicalAnalysisResource = catalogManager.getClinicalAnalysisManager()
                .getUid(resource.getResource().getInterpretation().getClinicalAnalysisId(), studyStr, sessionId);
        authorizationManager.checkClinicalAnalysisPermission(resource.getStudy().getUid(), clinicalAnalysisResource.getResource().getUid(),
                resource.getUser(), ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(),
                    (a) -> InterpretationDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        if (ListUtils.isNotEmpty(resource.getResource().getInterpretation().getReportedVariants()) && (parameters.size() > 1
                || !parameters.containsKey(InterpretationDBAdaptor.UpdateParams.REPORTED_VARIANTS.key()))) {
            throw new CatalogException("Interpretation already has reported variants. Only array of reported variants can be updated.");
        }

        if (parameters.containsKey(InterpretationDBAdaptor.UpdateParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(InterpretationDBAdaptor.UpdateParams.ID.key()),
                    InterpretationDBAdaptor.UpdateParams.ID.key());
        }

        QueryResult<Interpretation> queryResult = interpretationDBAdaptor.update(resource.getResource().getUid(), parameters, options);
        auditManager.recordUpdate(AuditRecord.Resource.interpretation, resource.getResource().getUid(), resource.getUser(), parameters,
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

        QueryResult<Interpretation> queryResult = interpretationDBAdaptor.get(query, options, userId);

        List<Interpretation> results = new ArrayList<>(queryResult.getResult().size());
        for (Interpretation interpretation : queryResult.getResult()) {
            if (StringUtils.isNotEmpty(interpretation.getInterpretation().getClinicalAnalysisId())) {
                try {
                    catalogManager.getClinicalAnalysisManager().smartResolutor(study.getUid(),
                            interpretation.getInterpretation().getClinicalAnalysisId(), userId);
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
