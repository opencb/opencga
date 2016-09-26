package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.AnnotationManager;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualManager extends AbstractManager implements IIndividualManager {

    protected static Logger logger = LoggerFactory.getLogger(IndividualManager.class);
    private IUserManager userManager;

    @Deprecated
    public IndividualManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                             DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                             Properties catalogProperties) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public IndividualManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                             DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                             CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                catalogConfiguration);
        this.userManager = catalogManager.getUserManager();
    }

    @Deprecated
    @Override
    public QueryResult<Individual> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        return create(
                objectMap.getInt(IndividualDBAdaptor.QueryParams.STUDY_ID.key()),
                objectMap.getString(IndividualDBAdaptor.QueryParams.NAME.key()),
                objectMap.getString(IndividualDBAdaptor.QueryParams.FAMILY.key()),
                objectMap.getInt(IndividualDBAdaptor.QueryParams.FATHER_ID.key()),
                objectMap.getInt(IndividualDBAdaptor.QueryParams.MOTHER_ID.key()),
                objectMap.get(IndividualDBAdaptor.QueryParams.SEX.key(), Individual.Sex.class),
                options, sessionId);
    }

    @Override
    public QueryResult<Individual> create(long studyId, String name, String family, long fatherId, long motherId,
                                          Individual.Sex sex, QueryOptions options, String sessionId)
            throws CatalogException {

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        sex = ParamUtils.defaultObject(sex, Individual.Sex.UNKNOWN);
        ParamUtils.checkAlias(name, "name");
        family = ParamUtils.defaultObject(family, "");

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_INDIVIDUALS);

        QueryResult<Individual> queryResult = individualDBAdaptor.insert(new Individual(0, name, fatherId, motherId,
                family, sex, null, null, null, Collections.emptyList(), null), studyId, options);
//      auditManager.recordCreation(AuditRecord.Resource.individual, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Individual> get(Long individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(individualId, "individualId");
        ParamUtils.checkObj(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.VIEW);
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId, options);
        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        authorizationManager.filterIndividuals(userId, studyId, individualQueryResult.getResult());
        individualQueryResult.setNumResults(individualQueryResult.getResult().size());
        return individualQueryResult;
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        return get(query.getInt("studyId"), query, options, sessionId);
    }

    @Override
    public QueryResult<Individual> get(long studyId, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
            throw CatalogAuthorizationException.deny(userId, "view", "individual", studyId, null);
        }
        query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, options);
        authorizationManager.filterIndividuals(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public List<QueryResult<Individual>> restore(String individualIdStr, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(individualIdStr, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        List<Long> individualIds = getIds(userId, individualIdStr);

        List<QueryResult<Individual>> queryResultList = new ArrayList<>(individualIds.size());
        for (Long individualId : individualIds) {
            QueryResult<Individual> queryResult = null;
            try {
                authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.DELETE);
                queryResult = individualDBAdaptor.restore(individualId, options);
                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.restore, AuditRecord.Magnitude.medium,
                        individualId, userId, Status.DELETED, Status.READY, "Individual restore", new ObjectMap());
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.restore, AuditRecord.Magnitude.high,
                        individualId, userId, null, null, e.getMessage(), null);
                queryResult = new QueryResult<>("Restore individual " + individualId);
                queryResult.setErrorMsg(e.getMessage());
            } catch (CatalogException e) {
                e.printStackTrace();
                queryResult = new QueryResult<>("Restore individual " + individualId);
                queryResult.setErrorMsg(e.getMessage());
            } finally {
                queryResultList.add(queryResult);
            }
        }

        return queryResultList;
    }

    @Override
    public List<QueryResult<Individual>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
        List<Long> individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
        String individualStr = StringUtils.join(individualIds, ",");
        return restore(individualStr, options, sessionId);
    }

    @Override
    public QueryResult<IndividualAclEntry> getAcls(String individualStr, List<String> members, String sessionId)
            throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long individualId = getId(userId, individualStr);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);
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
                            memberSet.add(group.getName());

                            if (!groupUsers.containsKey(group.getName())) {
                                groupUsers.put(group.getName(), new ArrayList<>());
                            }
                            groupUsers.get(group.getName()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<IndividualAclEntry> individualAclQueryResult = individualDBAdaptor.getAcl(individualId, memberList);

        if (members.size() == 0) {
            return individualAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one individualAcl per member
        Map<String, IndividualAclEntry> individualAclHashMap = new HashMap<>();
        for (IndividualAclEntry individualAcl : individualAclQueryResult.getResult()) {
            if (memberList.contains(individualAcl.getMember())) {
                if (individualAcl.getMember().startsWith("@")) {
                    // Check if the user was demanding the group directly or a user belonging to the group
                    if (groupIds.contains(individualAcl.getMember())) {
                        individualAclHashMap.put(individualAcl.getMember(),
                                new IndividualAclEntry(individualAcl.getMember(), individualAcl.getPermissions()));
                    } else {
                        // Obtain the user(s) belonging to that group whose permissions wanted the userId
                        if (groupUsers.containsKey(individualAcl.getMember())) {
                            for (String tmpUserId : groupUsers.get(individualAcl.getMember())) {
                                if (userIds.contains(tmpUserId)) {
                                    individualAclHashMap.put(tmpUserId, new IndividualAclEntry(tmpUserId, individualAcl.getPermissions()));
                                }
                            }
                        }
                    }
                } else {
                    // Add the user
                    individualAclHashMap.put(individualAcl.getMember(), new IndividualAclEntry(individualAcl.getMember(),
                            individualAcl.getPermissions()));
                }
            }

        }

        // We recreate the output that is in fileAclHashMap but in the same order the members were queried.
        List<IndividualAclEntry> individualAclList = new ArrayList<>(individualAclHashMap.size());
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
    public Long getId(String userId, String individualStr) throws CatalogException {
        if (StringUtils.isNumeric(individualStr)) {
            return Long.parseLong(individualStr);
        }

        // Resolve the studyIds and filter the individualName
        ObjectMap parsedSampleStr = parseFeatureId(userId, individualStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String individualName = parsedSampleStr.getString("featureName");

        Query query = new Query(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(IndividualDBAdaptor.QueryParams.NAME.key(), individualName);
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
    public Long getId(String id) throws CatalogDBException {
        if (StringUtils.isNumeric(id)) {
            return Long.parseLong(id);
        }

        Query query = new Query(IndividualDBAdaptor.QueryParams.NAME.key(), id);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());

        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options);
        if (individualQueryResult.getNumResults() == 1) {
            return individualQueryResult.first().getId();
        } else {
            return -1L;
        }
    }

    @Override
    @Deprecated
    public QueryResult<AnnotationSet> annotate(long individualId, String annotationSetName, long variableSetId, Map<String, Object>
            annotations, Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.CREATE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        AnnotationSet annotationSet =
                new AnnotationSet(annotationSetName, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId,
                new QueryOptions("include", Collections.singletonList("projects.studies.individuals.annotationSets")));

        List<AnnotationSet> annotationSets = individualQueryResult.getResult().get(0).getAnnotationSets();
//        if (checkAnnotationSet) {
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
//        }

        QueryResult<AnnotationSet> queryResult = individualDBAdaptor.annotate(individualId, annotationSet, false);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, new ObjectMap("annotationSets", queryResult
                .first()), "annotate", null);
        return queryResult;
    }

    @Deprecated
    public QueryResult<AnnotationSet> updateAnnotation(long individualId, String annotationSetName, Map<String, Object> newAnnotations,
                                                       String sessionId) throws CatalogException {

        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.UPDATE_ANNOTATIONS);

        QueryOptions queryOptions = new QueryOptions("include", "projects.studies.individuals.annotationSets");

        // Get individual
        Individual individual = individualDBAdaptor.get(individualId, queryOptions).first();

        // Get annotation set
        AnnotationSet annotationSet = null;
        for (AnnotationSet annotationSetAux : individual.getAnnotationSets()) {
            if (annotationSetAux.getName().equals(annotationSetName)) {
                annotationSet = annotationSetAux;
                individual.getAnnotationSets().remove(annotationSet);
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationSetName);
        }

        // Get variable set
        VariableSet variableSet = studyDBAdaptor.getVariableSet(annotationSet.getVariableSetId(), null).first();

        // Update and validate annotations
        CatalogAnnotationsValidator.mergeNewAnnotations(annotationSet, newAnnotations);
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, individual.getAnnotationSets());

        // Commit changes
        QueryResult<AnnotationSet> queryResult = individualDBAdaptor.annotate(individualId, annotationSet, true);

        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream().map(entry -> new Annotation(entry.getKey(), entry.getValue())).collect(Collectors
                        .toSet()),
                annotationSet.getCreationDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);

        return queryResult;
    }

    @Override
    @Deprecated
    public QueryResult<AnnotationSet> deleteAnnotation(long individualId, String annotationId, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.DELETE_ANNOTATIONS);

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
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.UPDATE);

        options.putAll(parameters); //FIXME: Use separated params and options, or merge
        QueryResult<Individual> queryResult = individualDBAdaptor.update(individualId, new ObjectMap(options));
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, parameters, null, null);
        return queryResult;

    }

    @Override
    public List<QueryResult<Individual>> delete(String individualIdStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(individualIdStr, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        List<Long> individualIds = getIds(userId, individualIdStr);

        List<QueryResult<Individual>> queryResultList = new ArrayList<>(individualIds.size());
        for (Long individualId : individualIds) {
            QueryResult<Individual> queryResult = null;
            try {
                authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.DELETE);
                queryResult = individualDBAdaptor.delete(individualId, options);
                auditManager.recordDeletion(AuditRecord.Resource.individual, individualId, userId, queryResult.first(), null, null);
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.delete, AuditRecord.Magnitude.high,
                        individualId, userId, null, null, e.getMessage(), null);
                queryResult = new QueryResult<>("Delete individual " + individualId);
                queryResult.setErrorMsg(e.getMessage());
            } catch (CatalogException e) {
                e.printStackTrace();
                queryResult = new QueryResult<>("Delete individual " + individualId);
                queryResult.setErrorMsg(e.getMessage());
            } finally {
                queryResultList.add(queryResult);
            }
        }

        return queryResultList;
    }

    @Override
    public List<QueryResult<Individual>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
        List<Long> individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
        String individualStr = StringUtils.join(individualIds, ",");
        return delete(individualStr, options, sessionId);
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);

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
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);

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
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);

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

    @Override
    public QueryResult<AnnotationSet> createAnnotationSet(String id, long variableSetId, String annotationSetName,
                                                          Map<String, Object> annotations, Map<String, Object> attributes,
                                                          String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long individualId = getId(userId, id);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.CREATE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        QueryResult<AnnotationSet> annotationSet = AnnotationManager.createAnnotationSet(individualId, variableSet, annotationSetName,
                annotations, attributes, individualDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId,
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> getAllAnnotationSets(String id, String sessionId) throws CatalogException {
        long individualId = commonGetAllInvidualSets(id, sessionId);
        return individualDBAdaptor.getAnnotationSet(individualId, null);
    }

    @Override
    public QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, String sessionId) throws CatalogException {
        long individualId = commonGetAllInvidualSets(id, sessionId);
        return individualDBAdaptor.getAnnotationSetAsMap(individualId, null);
    }

    private long commonGetAllInvidualSets(String id, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(id, "id");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long individualId = getId(userId, id);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);
        return individualId;
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException {
        long individualId = commonGetAnnotationSet(id, annotationSetName, sessionId);
        return individualDBAdaptor.getAnnotationSet(individualId, annotationSetName);
    }

    @Override
    public QueryResult<ObjectMap> getAnnotationSetAsMap(String id, String annotationSetName, String sessionId) throws CatalogException {
        long individualId = commonGetAnnotationSet(id, annotationSetName, sessionId);
        return individualDBAdaptor.getAnnotationSetAsMap(individualId, annotationSetName);
    }

    private long commonGetAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkAlias(annotationSetName, "annotationSetName");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long individualId = getId(userId, id);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);
        return individualId;
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, String annotationSetName, Map<String, Object> newAnnotations,
                                                          String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long individualId = getId(userId, id);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.UPDATE_ANNOTATIONS);

        // Update the annotation
        QueryResult<AnnotationSet> queryResult =
                AnnotationManager.updateAnnotationSet(individualId, annotationSetName, newAnnotations, individualDBAdaptor, studyDBAdaptor);

        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("There was an error with the update");
        }

        AnnotationSet annotationSet = queryResult.first();

        // Audit the changes
        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getCreationDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);

        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long individualId = getId(userId, id);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> annotationSet = individualDBAdaptor.getAnnotationSet(individualId, annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }

        individualDBAdaptor.deleteAnnotationSet(individualId, annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.individual, individualId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }

    @Override
    public QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, long variableSetId, @Nullable String annotation, String sessionId)
            throws CatalogException {
        QueryResult<Individual> individualQueryResult = commonSearchAnnotationSet(id, variableSetId, annotation, sessionId);
        List<ObjectMap> annotationSets;

        if (individualQueryResult == null || individualQueryResult.getNumResults() == 0) {
            annotationSets = Collections.emptyList();
        } else {
            annotationSets = individualQueryResult.first().getAnnotationSetAsMap();
        }

        return new QueryResult<>("Search annotation sets", individualQueryResult.getDbTime(), annotationSets.size(), annotationSets.size(),
                individualQueryResult.getWarningMsg(), individualQueryResult.getErrorMsg(), annotationSets);
    }

    @Override
    public QueryResult<AnnotationSet> searchAnnotationSet(String id, long variableSetId, @Nullable String annotation,
                                                          String sessionId) throws CatalogException {
        QueryResult<Individual> individualQueryResult = commonSearchAnnotationSet(id, variableSetId, annotation, sessionId);
        List<AnnotationSet> annotationSets;

        if (individualQueryResult == null || individualQueryResult.getNumResults() == 0) {
            annotationSets = Collections.emptyList();
        } else {
            annotationSets = individualQueryResult.first().getAnnotationSets();
        }

        return new QueryResult<>("Search annotation sets", individualQueryResult.getDbTime(), annotationSets.size(), annotationSets.size(),
                individualQueryResult.getWarningMsg(), individualQueryResult.getErrorMsg(), annotationSets);
    }

    private QueryResult<Individual> commonSearchAnnotationSet(String id, long variableSetId, @Nullable String annotation, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long individualId = getId(userId, id);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);

        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), id);

        if (variableSetId > 0) {
            query.append(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        }
        if (annotation != null) {
            query.append(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), annotation);
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key());
        return individualDBAdaptor.get(query, queryOptions);
    }

}
