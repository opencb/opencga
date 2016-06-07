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
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.IndividualAcl;
import org.opencb.opencga.catalog.models.acls.StudyAcl;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualManager extends AbstractManager implements IIndividualManager {

    protected static Logger logger = LoggerFactory.getLogger(IndividualManager.class);

    @Deprecated
    public IndividualManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                             AuditManager auditManager,
                             CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                             Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public IndividualManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                             AuditManager auditManager,
                             CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                             CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);
    }

    @Deprecated
    @Override
    public QueryResult<Individual> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        return create(
                objectMap.getInt("studyId"),
                objectMap.getString("name"),
                objectMap.getString("family"),
                objectMap.getInt("fatherId"),
                objectMap.getInt("motherId"),
                objectMap.get("gender", Individual.Gender.class),
                options, sessionId);
    }

    @Override
    public QueryResult<Individual> create(long studyId, String name, String family, long fatherId, long motherId,
                                          Individual.Gender gender, QueryOptions options, String sessionId)
            throws CatalogException {

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        gender = ParamUtils.defaultObject(gender, Individual.Gender.UNKNOWN);
        ParamUtils.checkAlias(name, "name");
        family = ParamUtils.defaultObject(family, "");

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.CREATE_INDIVIDUALS);

        QueryResult<Individual> queryResult = individualDBAdaptor.createIndividual(studyId, new Individual(0, name, fatherId, motherId,
                family, gender, null, null, null, Collections.emptyList(), null), options);
//      auditManager.recordCreation(AuditRecord.Resource.individual, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Individual> read(Long individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(individualId, "individualId");
        ParamUtils.checkObj(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAcl.IndividualPermissions.VIEW);
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.getIndividual(individualId, options);
        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        authorizationManager.filterIndividuals(userId, studyId, individualQueryResult.getResult());
        individualQueryResult.setNumResults(individualQueryResult.getResult().size());
        return individualQueryResult;
    }

    @Override
    public QueryResult<Individual> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        return readAll(query.getInt("studyId"), query, options, sessionId);
    }

    @Override
    public QueryResult<Individual> readAll(long studyId, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
            throw CatalogAuthorizationException.deny(userId, "view", "individual", studyId, null);
        }
        query.append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, options);
        authorizationManager.filterIndividuals(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public QueryResult<IndividualAcl> getIndividualAcls(String individualStr, List<String> members, String sessionId)
            throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long individualId = getIndividualId(userId, individualStr);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAcl.IndividualPermissions.SHARE);
        Long studyId = getStudyId(individualId);

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
        QueryResult<IndividualAcl> individualAclQueryResult = individualDBAdaptor.getIndividualAcl(individualId, memberList);

        if (members.size() == 0) {
            return individualAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one individualAcl per member
        Map<String, IndividualAcl> individualAclHashMap = new HashMap<>();
        for (IndividualAcl individualAcl : individualAclQueryResult.getResult()) {
            for (String tmpMember : individualAcl.getUsers()) {
                if (memberList.contains(tmpMember)) {
                    if (tmpMember.startsWith("@")) {
                        // Check if the user was demanding the group directly or a user belonging to the group
                        if (groupIds.contains(tmpMember)) {
                            individualAclHashMap.put(tmpMember,
                                    new IndividualAcl(Collections.singletonList(tmpMember), individualAcl.getPermissions()));
                        } else {
                            // Obtain the user(s) belonging to that group whose permissions wanted the userId
                            if (groupUsers.containsKey(tmpMember)) {
                                for (String tmpUserId : groupUsers.get(tmpMember)) {
                                    if (userIds.contains(tmpUserId)) {
                                        individualAclHashMap.put(tmpUserId, new IndividualAcl(Collections.singletonList(tmpUserId),
                                                individualAcl.getPermissions()));
                                    }
                                }
                            }
                        }
                    } else {
                        // Add the user
                        individualAclHashMap.put(tmpMember, new IndividualAcl(Collections.singletonList(tmpMember),
                                individualAcl.getPermissions()));
                    }
                }
            }
        }

        // We recreate the output that is in fileAclHashMap but in the same order the members were queried.
        List<IndividualAcl> individualAclList = new ArrayList<>(individualAclHashMap.size());
        for (String member : members) {
            if (individualAclHashMap.containsKey(member)) {
                individualAclList.add(individualAclHashMap.get(member));
            }
        }

        // Update queryResult info
        individualAclQueryResult.setId(individualStr);
        individualAclQueryResult.setNumResults(individualAclList.size());
        individualAclQueryResult.setNumTotalResults(individualAclList.size());
        individualAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        individualAclQueryResult.setResult(individualAclList);

        return individualAclQueryResult;
    }

    @Override
    public Long getStudyId(long individualId) throws CatalogException {
        return individualDBAdaptor.getStudyIdByIndividualId(individualId);
    }

    @Override
    public Long getIndividualId(String userId, String individualStr) throws CatalogException {
        if (StringUtils.isNumeric(individualStr)) {
            return Long.parseLong(individualStr);
        }

        // Resolve the studyIds and filter the individualName
        ObjectMap parsedSampleStr = parseFeatureId(userId, individualStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String individualName = parsedSampleStr.getString("featureName");

        Query query = new Query(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CatalogIndividualDBAdaptor.QueryParams.NAME.key(), individualName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.individuals.id");
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one individual id found based on " + individualName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Deprecated
    @Override
    public Long getIndividualId(String id) throws CatalogDBException {
        if (StringUtils.isNumeric(id)) {
            return Long.parseLong(id);
        }

        Query query = new Query(CatalogIndividualDBAdaptor.QueryParams.NAME.key(), id);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CatalogIndividualDBAdaptor.QueryParams.ID.key());

        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options);
        if (individualQueryResult.getNumResults() == 1) {
            return individualQueryResult.first().getId();
        } else {
            return -1L;
        }
    }

    @Override
    public QueryResult<AnnotationSet> annotate(long individualId, String annotationSetId, long variableSetId, Map<String, Object>
            annotations, Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAcl.IndividualPermissions.CREATE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        AnnotationSet annotationSet =
                new AnnotationSet(annotationSetId, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.getIndividual(individualId,
                new QueryOptions("include", Collections.singletonList("projects.studies.individuals.annotationSets")));

        List<AnnotationSet> annotationSets = individualQueryResult.getResult().get(0).getAnnotationSets();
//        if (checkAnnotationSet) {
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
//        }

        QueryResult<AnnotationSet> queryResult = individualDBAdaptor.annotateIndividual(individualId, annotationSet, false);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, new ObjectMap("annotationSets", queryResult
                .first()), "annotate", null);
        return queryResult;
    }

    public QueryResult<AnnotationSet> updateAnnotation(long individualId, String annotationSetId, Map<String, Object> newAnnotations,
                                                       String sessionId) throws CatalogException {

        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAcl.IndividualPermissions.UPDATE_ANNOTATIONS);

        QueryOptions queryOptions = new QueryOptions("include", "projects.studies.individuals.annotationSets");

        // Get individual
        Individual individual = individualDBAdaptor.getIndividual(individualId, queryOptions).first();

        // Get annotation set
        AnnotationSet annotationSet = null;
        for (AnnotationSet annotationSetAux : individual.getAnnotationSets()) {
            if (annotationSetAux.getId().equals(annotationSetId)) {
                annotationSet = annotationSetAux;
                individual.getAnnotationSets().remove(annotationSet);
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
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, individual.getAnnotationSets());

        // Commit changes
        QueryResult<AnnotationSet> queryResult = individualDBAdaptor.annotateIndividual(individualId, annotationSet, true);

        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getId(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream().map(entry -> new Annotation(entry.getKey(), entry.getValue())).collect(Collectors
                        .toSet()),
                annotationSet.getDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);

        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(long individualId, String annotationId, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAcl.IndividualPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> queryResult = individualDBAdaptor.deleteAnnotation(individualId, annotationId);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId,
                new ObjectMap("annotationSets", queryResult.first()), "deleteAnnotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<Individual> update(Long individualId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        ParamUtils.defaultObject(parameters, QueryOptions::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAcl.IndividualPermissions.UPDATE);

        options.putAll(parameters); //FIXME: Use separated params and options, or merge
        QueryResult<Individual> queryResult = individualDBAdaptor.update(individualId, new ObjectMap(options));
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, parameters, null, null);
        return queryResult;

    }

    @Override
    public QueryResult<Individual> delete(Long individualId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAcl.IndividualPermissions.DELETE);

        QueryResult<Individual> queryResultBefore = individualDBAdaptor.deleteIndividual(individualId, options);
//        auditManager.recordCreation(AuditRecord.Resource.individual, individualId, userId, queryResultBefore.first(), null, null);
        return queryResultBefore;
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_INDIVIDUALS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = individualDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_INDIVIDUALS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = individualDBAdaptor.groupBy(query, field, options);
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
        authorizationManager.checkStudyPermission(studyId, userId, StudyAcl.StudyPermissions.VIEW_INDIVIDUALS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = individualDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }
}
