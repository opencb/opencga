package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.ISampleManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.CohortAcl;
import org.opencb.opencga.catalog.models.acls.SampleAcl;
import org.opencb.opencga.catalog.models.acls.StudyAcl;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleManager extends AbstractManager implements ISampleManager {


    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);

    @Deprecated
    public SampleManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                         CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public SampleManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                         CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);
    }

    @Override
    public Long getStudyId(long sampleId) throws CatalogException {
        return sampleDBAdaptor.getStudyIdBySampleId(sampleId);
    }

    @Override
    public Long getSampleId(String userId, String sampleStr) throws CatalogException {
        if (StringUtils.isNumeric(sampleStr)) {
            return Long.parseLong(sampleStr);
        }

        ObjectMap parsedSampleStr = parseFeatureId(userId, sampleStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String sampleName = parsedSampleStr.getString("featureName");

        Query query = new Query(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CatalogSampleDBAdaptor.QueryParams.NAME.key(), sampleName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.samples.id");
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one sample id found based on " + sampleName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Deprecated
    @Override
    public Long getSampleId(String id) throws CatalogException {
        if (StringUtils.isNumeric(id)) {
            return Long.parseLong(id);
        }

        Query query = new Query(CatalogSampleDBAdaptor.QueryParams.NAME.key(), id);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CatalogSampleDBAdaptor.QueryParams.ID.key());

        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, options);
        if (sampleQueryResult.getNumResults() == 1) {
            return sampleQueryResult.first().getId();
        } else {
            return -1L;
        }
    }

    @Override
    public QueryResult<AnnotationSet> annotate(long sampleId, String annotationSetId, long variableSetId, Map<String, Object> annotations,
                                               Map<String, Object> attributes, boolean checkAnnotationSet,
                                               String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkSamplePermission(sampleId, userId, SampleAcl.SamplePermissions.CREATE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        AnnotationSet annotationSet = new AnnotationSet(annotationSetId, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.getSample(sampleId,
                new QueryOptions("include", Collections.singletonList("projects.studies.samples.annotationSets")));

        List<AnnotationSet> annotationSets = sampleQueryResult.first().getAnnotationSets();
        if (checkAnnotationSet) {
            CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
        }

        QueryResult<AnnotationSet> queryResult = sampleDBAdaptor.annotateSample(sampleId, annotationSet, false);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId,
                new ObjectMap("annotationSets", queryResult.first()), "annotate", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotation(long sampleId, String annotationSetId, Map<String, Object> newAnnotations, String
            sessionId)
            throws CatalogException {

        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkSamplePermission(sampleId, userId, SampleAcl.SamplePermissions.UPDATE_ANNOTATIONS);

        // Get sample
        QueryOptions queryOptions = new QueryOptions("include", Collections.singletonList("projects.studies.samples.annotationSets"));
        Sample sample = sampleDBAdaptor.getSample(sampleId, queryOptions).first();

        List<AnnotationSet> annotationSets = sample.getAnnotationSets();

        // Get annotation set
        AnnotationSet annotationSet = null;
        for (AnnotationSet annotationSetAux : sample.getAnnotationSets()) {
            if (annotationSetAux.getId().equals(annotationSetId)) {
                annotationSet = annotationSetAux;
                sample.getAnnotationSets().remove(annotationSet);
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationSetId);
        }

        // Get variable set
        VariableSet variableSet = studyDBAdaptor.getVariableSet(annotationSet.getVariableSetId(), null).first();

        // Update and validate annotations
        CatalogAnnotationsValidator.mergeNewAnnotations(annotationSet, newAnnotations);
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);

        // Commit changes
        QueryResult<AnnotationSet> queryResult = sampleDBAdaptor.annotateSample(sampleId, annotationSet, true);

        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getId(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(long sampleId, String annotationId, String sessionId) throws CatalogException {

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, SampleAcl.SamplePermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> queryResult = sampleDBAdaptor.deleteAnnotation(sampleId, annotationId);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, new ObjectMap("annotationSets", queryResult.first()),
                "deleteAnnotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<Annotation> load(File file) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public QueryResult<Sample> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        return create(
                objectMap.getInt("studyId"),
                objectMap.getString("name"),
                objectMap.getString("source"),
                objectMap.getString("description"),
                objectMap.getMap("attributes"),
                options,
                sessionId
        );
    }

    @Override
    public QueryResult<Sample> create(long studyId, String name, String source, String description,
                                      Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        source = ParamUtils.defaultString(source, "");
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, Collections.<String, Object>emptyMap());

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.CREATE_SAMPLES);

        Sample sample = new Sample(-1, name, source, -1, description, Collections.emptyList(), Collections.emptyList(), attributes);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        QueryResult<Sample> queryResult = sampleDBAdaptor.createSample(studyId, sample, options);
//        auditManager.recordCreation(AuditRecord.Resource.sample, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Sample> read(Long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkSamplePermission(sampleId, userId, SampleAcl.SamplePermissions.VIEW);
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.getSample(sampleId, options);
        authorizationManager.filterSamples(userId, getStudyId(sampleId), sampleQueryResult.getResult());
        sampleQueryResult.setNumResults(sampleQueryResult.getResult().size());
        return sampleQueryResult;
    }

    @Override
    public QueryResult<Sample> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
            throw CatalogAuthorizationException.deny(userId, "view", "samples", studyId, null);
        }

        query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, options);
        authorizationManager.filterSamples(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
    }

    @Override
    public QueryResult<SampleAcl> getSampleAcls(String sampleStr, List<String> members, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long sampleId = getSampleId(userId, sampleStr);
        authorizationManager.checkSamplePermission(sampleId, userId, SampleAcl.SamplePermissions.SHARE);
        Long studyId = getStudyId(sampleId);

        // Split and obtain the set of members (users + groups), users and groups
        Set<String> memberSet = new HashSet<>();
        Set<String> userIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();
        for (String member: members) {
            memberSet.add(member);
            if (!member.startsWith("@")) {
                userIds.add(member);
            } else {
                groupIds.add(member);
            }
        }


        // Obtain the groups the user might belong to in order to be able to get the permissions properly
        // (the permissions might be given to the group instead of the user)
        // Map of group -> users
        Map<String, List<String>> groupUsers = new HashMap<>();

        if (userIds.size() > 0) {
            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
            // We add the groups where the users might belong to to the memberSet
            if (groups.getNumResults() > 0) {
                for (Group group : groups.getResult()) {
                    for (String tmpUserId : group.getUserIds()) {
                        if (userIds.contains(tmpUserId)) {
                            memberSet.add(group.getId());

                            if (!groupUsers.containsKey(group.getId())) {
                                groupUsers.put(group.getId(), new ArrayList<>());
                            }
                            groupUsers.get(group.getId()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<SampleAcl> sampleAclQueryResult = sampleDBAdaptor.getSampleAcl(sampleId, memberList);

        if (members.size() == 0) {
            return sampleAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one sampleAcl per member
        Map<String, SampleAcl> sampleAclHashMap = new HashMap<>();
        for (SampleAcl sampleAcl : sampleAclQueryResult.getResult()) {
            for (String tmpMember : sampleAcl.getUsers()) {
                if (memberList.contains(tmpMember)) {
                    if (tmpMember.startsWith("@")) {
                        // Check if the user was demanding the group directly or a user belonging to the group
                        if (groupIds.contains(tmpMember)) {
                            sampleAclHashMap.put(tmpMember,
                                    new SampleAcl(Collections.singletonList(tmpMember), sampleAcl.getPermissions()));
                        } else {
                            // Obtain the user(s) belonging to that group whose permissions wanted the userId
                            if (groupUsers.containsKey(tmpMember)) {
                                for (String tmpUserId : groupUsers.get(tmpMember)) {
                                    if (userIds.contains(tmpUserId)) {
                                        sampleAclHashMap.put(tmpUserId, new SampleAcl(Collections.singletonList(tmpUserId),
                                                sampleAcl.getPermissions()));
                                    }
                                }
                            }
                        }
                    } else {
                        // Add the user
                        sampleAclHashMap.put(tmpMember, new SampleAcl(Collections.singletonList(tmpMember), sampleAcl.getPermissions()));
                    }
                }
            }
        }

        // We recreate the output that is in sampleAclHashMap but in the same order the members were queried.
        List<SampleAcl> sampleAclList = new ArrayList<>(sampleAclHashMap.size());
        for (String member : members) {
            if (sampleAclHashMap.containsKey(member)) {
                sampleAclList.add(sampleAclHashMap.get(member));
            }
        }

        // Update queryResult info
        sampleAclQueryResult.setId(sampleStr);
        sampleAclQueryResult.setNumResults(sampleAclList.size());
        sampleAclQueryResult.setNumTotalResults(sampleAclList.size());
        sampleAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        sampleAclQueryResult.setResult(sampleAclList);

        return sampleAclQueryResult;

    }

    @Override
    public QueryResult<Sample> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(query, "query");
        QueryResult<Sample> result =
                readAll(query.getInt(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), -1), query, options, sessionId);
//        auditManager.recordRead(AuditRecord.Resource.sample, , userId, parameters, null, null);
        return result;
    }

    @Override
    public QueryResult<Sample> update(Long sampleId, ObjectMap parameters, QueryOptions options, String sessionId) throws
            CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, SampleAcl.SamplePermissions.UPDATE);

        options.putAll(parameters);
        QueryResult<Sample> queryResult = sampleDBAdaptor.modifySample(sampleId, options);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, parameters, null, null);
        return queryResult;

    }

    @Override
    public QueryResult<Sample> delete(Long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(sampleId, "sampleId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, SampleAcl.SamplePermissions.DELETE);

        QueryResult<Sample> queryResult = sampleDBAdaptor.delete(sampleId, new QueryOptions());
        auditManager.recordDeletion(AuditRecord.Resource.sample, sampleId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_SAMPLES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_SAMPLES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_SAMPLES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    /*
     * Cohort methods
     * ***************************
     */
    @Override
    public long getStudyIdByCohortId(long cohortId) throws CatalogException {
        return cohortDBAdaptor.getStudyIdByCohortId(cohortId);
    }

    @Override
    public Long getCohortId(String userId, String cohortStr) throws CatalogException {
        if (StringUtils.isNumeric(cohortStr)) {
            return Long.parseLong(cohortStr);
        }

        // Resolve the studyIds and filter the cohortName
        ObjectMap parsedSampleStr = parseFeatureId(userId, cohortStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String cohortName = parsedSampleStr.getString("featureName");

        Query query = new Query(CatalogCohortDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CatalogCohortDBAdaptor.QueryParams.NAME.key(), cohortName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.cohorts.id");
        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one cohort id found based on " + cohortName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Override
    public QueryResult<Cohort> readCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        //options = ParamUtils.defaultObject(options, QueryOptions::new);

        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAcl.CohortPermissions.VIEW);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.getCohort(cohortId, options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());

        return queryResult;
    }

    @Override
    public QueryResult<Cohort> readAllCohort(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
            throw CatalogAuthorizationException.deny(userId, "view", "cohorts", studyId, null);
        }

        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(new Query(query)
                .append(CatalogCohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId), options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> createCohort(long studyId, String name, Cohort.Type type, String description, List<Long> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(sampleIds, "Samples list");
        type = ParamUtils.defaultObject(type, Cohort.Type.COLLECTION);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (!sampleIds.isEmpty() && readAll(studyId, new Query(CatalogSampleDBAdaptor.QueryParams.ID.key(), sampleIds), null, sessionId)
                .getResult().size() != sampleIds.size()) {
            throw new CatalogException("Error: Some sampleId does not exist in the study " + studyId);
        }
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.CREATE_COHORTS);
        Cohort cohort = new Cohort(name, type, TimeUtils.getTime(), description, sampleIds, attributes);
        QueryResult<Cohort> queryResult = cohortDBAdaptor.createCohort(studyId, cohort, null);
//        auditManager.recordCreation(AuditRecord.Resource.cohort, queryResult.first().getId(), userId, queryResult.first(), null, new
//                ObjectMap());
        auditManager.recordAction(AuditRecord.Resource.cohort, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> updateCohort(long cohortId, ObjectMap params, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(params, "Update parameters");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAcl.CohortPermissions.UPDATE);

        Cohort cohort = readCohort(cohortId, new QueryOptions(QueryOptions.INCLUDE, "projects.studies.cohorts."
                + CatalogCohortDBAdaptor.QueryParams.STATUS_STATUS.key()), sessionId).first();
        if (params.containsKey(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key())
                || params.containsKey(CatalogCohortDBAdaptor.QueryParams.NAME.key())/* || params.containsKey("type")*/) {
            switch (cohort.getStatus().getStatus()) {
                case Cohort.CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to modify a cohort while it's in status \"" + Cohort.CohortStatus.CALCULATING
                            + "\"");
                case Cohort.CohortStatus.READY:
                    params.putIfAbsent("status.status", Cohort.CohortStatus.INVALID);
                    break;
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                default:
                    break;
            }
        }

        QueryResult<Cohort> queryResult = cohortDBAdaptor.modifyCohort(cohortId, params, options);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, params, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAcl.CohortPermissions.DELETE);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.deleteCohort(cohortId, options);
        auditManager.recordDeletion(AuditRecord.Resource.cohort, cohortId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> annotateCohort(String cohortStr, String annotationSetId, long variableSetId,
                                                     Map<String, Object> annotations, Map<String, Object> attributes,
                                                     boolean checkAnnotationSet, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, cohortStr);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAcl.CohortPermissions.CREATE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        AnnotationSet annotationSet = new AnnotationSet(annotationSetId, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.getCohort(cohortId,
                new QueryOptions("include", Collections.singletonList("projects.studies.cohorts.annotationSets")));

        List<AnnotationSet> annotationSets = cohortQueryResult.first().getAnnotationSets();
        if (checkAnnotationSet) {
            CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
        }

        QueryResult<AnnotationSet> queryResult = cohortDBAdaptor.annotateCohort(cohortId, annotationSet, false);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, new ObjectMap("annotationSets", queryResult.first()),
                "annotate", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> updateCohortAnnotation(String cohortStr, String annotationSetId, Map<String, Object> newAnnotations,
                                                             String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, cohortStr);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAcl.CohortPermissions.UPDATE_ANNOTATIONS);

        // Get sample
        QueryOptions queryOptions = new QueryOptions("include", Collections.singletonList("projects.studies.cohorts.annotationSets"));
        Cohort cohort = cohortDBAdaptor.getCohort(cohortId, queryOptions).first();

        List<AnnotationSet> annotationSets = cohort.getAnnotationSets();

        // Get annotation set
        AnnotationSet annotationSet = null;
        for (AnnotationSet annotationSetAux : cohort.getAnnotationSets()) {
            if (annotationSetAux.getId().equals(annotationSetId)) {
                annotationSet = annotationSetAux;
                cohort.getAnnotationSets().remove(annotationSet);
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationSetId);
        }

        // Get variable set
        VariableSet variableSet = studyDBAdaptor.getVariableSet(annotationSet.getVariableSetId(), null).first();

        // Update and validate annotations
        CatalogAnnotationsValidator.mergeNewAnnotations(annotationSet, newAnnotations);
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);

        // Commit changes
        QueryResult<AnnotationSet> queryResult = cohortDBAdaptor.annotateCohort(cohortId, annotationSet, true);

        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getId(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteCohortAnnotation(String cohortStr, String annotationId, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, cohortStr);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAcl.CohortPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> queryResult = cohortDBAdaptor.deleteAnnotation(cohortId, annotationId);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, new ObjectMap("annotationSets", queryResult.first()),
                "deleteAnnotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<CohortAcl> getCohortAcls(String cohortStr, List<String> members, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long cohortId = getCohortId(userId, cohortStr);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAcl.CohortPermissions.SHARE);
        Long studyId = getStudyIdByCohortId(cohortId);

        // Split and obtain the set of members (users + groups), users and groups
        Set<String> memberSet = new HashSet<>();
        Set<String> userIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();

        for (String member: members) {
            memberSet.add(member);
            if (!member.startsWith("@")) {
                userIds.add(member);
            } else {
                groupIds.add(member);
            }
        }


        // Obtain the groups the user might belong to in order to be able to get the permissions properly
        // (the permissions might be given to the group instead of the user)
        // Map of group -> users
        Map<String, List<String>> groupUsers = new HashMap<>();

        if (userIds.size() > 0) {
            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
            // We add the groups where the users might belong to to the memberSet
            if (groups.getNumResults() > 0) {
                for (Group group : groups.getResult()) {
                    for (String tmpUserId : group.getUserIds()) {
                        if (userIds.contains(tmpUserId)) {
                            memberSet.add(group.getId());

                            if (!groupUsers.containsKey(group.getId())) {
                                groupUsers.put(group.getId(), new ArrayList<>());
                            }
                            groupUsers.get(group.getId()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<CohortAcl> cohortAclQueryResult = cohortDBAdaptor.getCohortAcl(cohortId, memberList);

        if (members.size() == 0) {
            return cohortAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one sampleAcl per member
        Map<String, CohortAcl> cohortAclHashMap = new HashMap<>();
        for (CohortAcl cohortAcl : cohortAclQueryResult.getResult()) {
            for (String tmpMember : cohortAcl.getUsers()) {
                if (memberList.contains(tmpMember)) {
                    if (tmpMember.startsWith("@")) {
                        // Check if the user was demanding the group directly or a user belonging to the group
                        if (groupIds.contains(tmpMember)) {
                            cohortAclHashMap.put(tmpMember,
                                    new CohortAcl(Collections.singletonList(tmpMember), cohortAcl.getPermissions()));
                        } else {
                            // Obtain the user(s) belonging to that group whose permissions wanted the userId
                            if (groupUsers.containsKey(tmpMember)) {
                                for (String tmpUserId : groupUsers.get(tmpMember)) {
                                    if (userIds.contains(tmpUserId)) {
                                        cohortAclHashMap.put(tmpUserId, new CohortAcl(Collections.singletonList(tmpUserId),
                                                cohortAcl.getPermissions()));
                                    }
                                }
                            }
                        }
                    } else {
                        // Add the user
                        cohortAclHashMap.put(tmpMember, new CohortAcl(Collections.singletonList(tmpMember), cohortAcl.getPermissions()));
                    }
                }
            }
        }

        // We recreate the output that is in cohortAclHashMap but in the same order the members were queried.
        List<CohortAcl> cohortAclList = new ArrayList<>(cohortAclHashMap.size());
        for (String member : members) {
            if (cohortAclHashMap.containsKey(member)) {
                cohortAclList.add(cohortAclHashMap.get(member));
            }
        }

        // Update queryResult info
        cohortAclQueryResult.setId(cohortStr);
        cohortAclQueryResult.setNumResults(cohortAclList.size());
        cohortAclQueryResult.setNumTotalResults(cohortAclList.size());
        cohortAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        cohortAclQueryResult.setResult(cohortAclList);

        return cohortAclQueryResult;
    }

    @Override
    public QueryResult cohortGroupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_COHORTS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = cohortDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

}
