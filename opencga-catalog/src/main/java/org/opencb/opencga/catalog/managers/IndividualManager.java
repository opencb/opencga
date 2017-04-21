/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.AnnotationManager;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

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
                             Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
        this.userManager = catalogManager.getUserManager();
    }

    @Override
    public QueryResult<Individual> create(long studyId, String name, String family, long fatherId, long motherId, Individual.Sex sex,
                                          String ethnicity, String speciesCommonName, String speciesScientificName,
                                          String speciesTaxonomyCode, String populationName, String populationSubpopulation,
                                          String populationDescription, Individual.KaryotypicSex karyotypicSex,
                                          Individual.LifeStatus lifeStatus, Individual.AffectationStatus affectationStatus,
                                          QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        sex = ParamUtils.defaultObject(sex, Individual.Sex.UNKNOWN);
        logger.info(Long.toString(configuration.getCatalog().getOffset()));
        ParamUtils.checkAlias(name, "name", configuration.getCatalog().getOffset());
        family = ParamUtils.defaultObject(family, "");
        ethnicity = ParamUtils.defaultObject(ethnicity, "");
        speciesCommonName = ParamUtils.defaultObject(speciesCommonName, "");
        speciesScientificName = ParamUtils.defaultObject(speciesScientificName, "");
        speciesTaxonomyCode = ParamUtils.defaultObject(speciesTaxonomyCode, "");
        populationName = ParamUtils.defaultObject(populationName, "");
        populationSubpopulation = ParamUtils.defaultObject(populationSubpopulation, "");
        populationDescription = ParamUtils.defaultObject(populationDescription, "");
        karyotypicSex = ParamUtils.defaultObject(karyotypicSex, Individual.KaryotypicSex.UNKNOWN);
        lifeStatus = ParamUtils.defaultObject(lifeStatus, Individual.LifeStatus.UNKNOWN);
        affectationStatus = ParamUtils.defaultObject(affectationStatus, Individual.AffectationStatus.UNKNOWN);

        String userId = userManager.getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);

        Individual individual = new Individual(0, name, fatherId, motherId,
                family, sex, karyotypicSex, ethnicity, new Individual.Species(speciesCommonName, speciesScientificName,
                speciesTaxonomyCode), new Individual.Population(populationName, populationSubpopulation, populationDescription), lifeStatus,
                affectationStatus);

        QueryResult<Individual> queryResult = individualDBAdaptor.insert(individual, studyId, options);
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

        String userId = userManager.getId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.VIEW);
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId, options);
        long studyId = individualDBAdaptor.getStudyId(individualId);
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

        String userId = userManager.getId(sessionId);
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
    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        throw new NotImplementedException("Project: Operation not yet supported");
    }
//
//    @Override
//    public QueryResult<IndividualAclEntry> getAcls(String individualStr, List<String> members, String sessionId)
//            throws CatalogException {
//        long startTime = System.currentTimeMillis();
//        String userId = userManager.getId(sessionId);
//        Long individualId = getId(userId, individualStr);
//        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);
//        Long studyId = getStudyId(individualId);
//
//        // Split and obtain the set of members (users + groups), users and groups
//        Set<String> memberSet = new HashSet<>();
//        Set<String> userIds = new HashSet<>();
//        Set<String> groupIds = new HashSet<>();
//        for (String member: members) {
//            memberSet.add(member);
//            if (!member.startsWith("@")) {
//                userIds.add(member);
//            } else {
//                groupIds.add(member);
//            }
//        }
//
//
//        // Obtain the groups the user might belong to in order to be able to get the permissions properly
//        // (the permissions might be given to the group instead of the user)
//        // Map of group -> users
//        Map<String, List<String>> groupUsers = new HashMap<>();
//
//        if (userIds.size() > 0) {
//            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
//            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
//            // We add the groups where the users might belong to to the memberSet
//            if (groups.getNumResults() > 0) {
//                for (Group group : groups.getResult()) {
//                    for (String tmpUserId : group.getUserIds()) {
//                        if (userIds.contains(tmpUserId)) {
//                            memberSet.add(group.getName());
//
//                            if (!groupUsers.containsKey(group.getName())) {
//                                groupUsers.put(group.getName(), new ArrayList<>());
//                            }
//                            groupUsers.get(group.getName()).add(tmpUserId);
//                        }
//                    }
//                }
//            }
//        }
//        List<String> memberList = memberSet.stream().collect(Collectors.toList());
//        QueryResult<IndividualAclEntry> individualAclQueryResult = individualDBAdaptor.getAcl(individualId, memberList);
//
//        if (members.size() == 0) {
//            return individualAclQueryResult;
//        }
//
//        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the
// user
//        // instead of the group.
//        // We loop over the results and recreate one individualAcl per member
//        Map<String, IndividualAclEntry> individualAclHashMap = new HashMap<>();
//        for (IndividualAclEntry individualAcl : individualAclQueryResult.getResult()) {
//            if (memberList.contains(individualAcl.getMember())) {
//                if (individualAcl.getMember().startsWith("@")) {
//                    // Check if the user was demanding the group directly or a user belonging to the group
//                    if (groupIds.contains(individualAcl.getMember())) {
//                        individualAclHashMap.put(individualAcl.getMember(),
//                                new IndividualAclEntry(individualAcl.getMember(), individualAcl.getPermissions()));
//                    } else {
//                        // Obtain the user(s) belonging to that group whose permissions wanted the userId
//                        if (groupUsers.containsKey(individualAcl.getMember())) {
//                            for (String tmpUserId : groupUsers.get(individualAcl.getMember())) {
//                                if (userIds.contains(tmpUserId)) {
//                                    individualAclHashMap.put(tmpUserId, new IndividualAclEntry(tmpUserId,
// individualAcl.getPermissions()));
//                                }
//                            }
//                        }
//                    }
//                } else {
//                    // Add the user
//                    individualAclHashMap.put(individualAcl.getMember(), new IndividualAclEntry(individualAcl.getMember(),
//                            individualAcl.getPermissions()));
//                }
//            }
//
//        }
//
//        // We recreate the output that is in fileAclHashMap but in the same order the members were queried.
//        List<IndividualAclEntry> individualAclList = new ArrayList<>(individualAclHashMap.size());
//        for (String member : members) {
//            if (individualAclHashMap.containsKey(member)) {
//                individualAclList.add(individualAclHashMap.get(member));
//            }
//        }
//
//        // Update queryResult info
//        individualAclQueryResult.setId(individualStr);
//        individualAclQueryResult.setNumResults(individualAclList.size());
//        individualAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
//        individualAclQueryResult.setResult(individualAclList);
//
//        return individualAclQueryResult;
//    }

    @Override
    public Long getStudyId(long individualId) throws CatalogException {
        return individualDBAdaptor.getStudyId(individualId);
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
    public MyResourceId getId(String individualStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(individualStr)) {
            throw new CatalogException("Missing individual parameter");
        }

        String userId;
        long studyId;
        long individualId;

        if (StringUtils.isNumeric(individualStr) && Long.parseLong(individualStr) > configuration.getCatalog().getOffset()) {
            individualId = Long.parseLong(individualStr);
            individualDBAdaptor.exists(individualId);
            studyId = individualDBAdaptor.getStudyId(individualId);
            userId = userManager.getId(sessionId);
        } else {
            if (individualStr.contains(",")) {
                throw new CatalogException("More than one individual found");
            }

            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(IndividualDBAdaptor.QueryParams.NAME.key(), individualStr);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
            QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
            if (individualQueryResult.getNumResults() == 1) {
                individualId = individualQueryResult.first().getId();
            } else {
                if (individualQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Individual " + individualStr + " not found in study " + studyStr);
                } else {
                    throw new CatalogException("More than one individual found under " + individualStr + " in study " + studyStr);
                }
            }
        }

        return new MyResourceId(userId, studyId, individualId);
    }

    @Override
    public MyResourceIds getIds(String individualStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(individualStr)) {
            throw new CatalogException("Missing individual parameter");
        }

        String userId;
        long studyId;
        List<Long> individualIds;

        if (StringUtils.isNumeric(individualStr) && Long.parseLong(individualStr) > configuration.getCatalog().getOffset()) {
            individualIds = Arrays.asList(Long.parseLong(individualStr));
            individualDBAdaptor.exists(individualIds.get(0));
            studyId = individualDBAdaptor.getStudyId(individualIds.get(0));
            userId = userManager.getId(sessionId);
        } else {
            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            List<String> individualSplit = Arrays.asList(individualStr.split(","));
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(IndividualDBAdaptor.QueryParams.NAME.key(), individualSplit);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
            QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
            if (individualQueryResult.getNumResults() == individualSplit.size()) {
                individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
            } else {
                throw new CatalogException("Found only " + individualQueryResult.getNumResults() + " out of the " + individualSplit.size()
                        + " individuals looked for in study " + studyStr);
            }
        }

        return new MyResourceIds(userId, studyId, individualIds);
    }

    @Override
    public QueryResult<Individual> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        List<Long> studyIds = catalogManager.getStudyManager().getIds(userId, studyStr);

        // Check any permission in studies
        for (Long studyId : studyIds) {
            authorizationManager.memberHasPermissionsInStudy(studyId, userId);
        }

        QueryResult<Individual> queryResult = null;
        for (Long studyId : studyIds) {
            query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
            QueryResult<Individual> queryResultAux = individualDBAdaptor.get(query, options);
            authorizationManager.filterIndividuals(userId, studyId, queryResultAux.getResult());

            if (queryResult == null) {
                queryResult = queryResultAux;
            } else {
                queryResult.getResult().addAll(queryResultAux.getResult());
                queryResult.setNumTotalResults(queryResult.getNumTotalResults() + queryResultAux.getNumTotalResults());
                queryResult.setDbTime(queryResult.getDbTime() + queryResultAux.getDbTime());
            }
        }
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
    }

    @Override
    @Deprecated
    public QueryResult<AnnotationSet> annotate(long individualId, String annotationSetName, long variableSetId, Map<String, Object>
            annotations, Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userManager.getId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);

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

        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userManager.getId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);

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
    public QueryResult<Individual> update(Long individualId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.defaultObject(parameters, QueryOptions::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.UPDATE);

        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            IndividualDBAdaptor.QueryParams queryParam = IndividualDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case NAME:
                    ParamUtils.checkAlias(parameters.getString(queryParam.key()), "name", configuration.getCatalog().getOffset());

                    String myName = parameters.getString(IndividualDBAdaptor.QueryParams.NAME.key());
                    long studyId = individualDBAdaptor.getStudyId(individualId);
                    Query query = new Query()
                            .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                            .append(IndividualDBAdaptor.QueryParams.NAME.key(), myName);
                    if (individualDBAdaptor.count(query).first() > 0) {
                        throw new CatalogException("Individual name " + myName + " already in use");
                    }
                    break;
                case FATHER_ID:
                case MOTHER_ID:
                case FAMILY:
                case SEX:
                case ETHNICITY:
                case SPECIES_COMMON_NAME:
                case SPECIES_SCIENTIFIC_NAME:
                case SPECIES_TAXONOMY_CODE:
                case POPULATION_DESCRIPTION:
                case POPULATION_NAME:
                case POPULATION_SUBPOPULATION:
                case KARYOTYPIC_SEX:
                case LIFE_STATUS:
                case AFFECTATION_STATUS:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        options.putAll(parameters); //FIXME: Use separated params and options, or merge
        QueryResult<Individual> queryResult = individualDBAdaptor.update(individualId, new ObjectMap(options));
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, parameters, null, null);
        return queryResult;

    }

    @Override
    public List<QueryResult<Individual>> delete(String individualIdStr, @Nullable String studyStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(individualIdStr, "id");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceIds resourceId = getIds(individualIdStr, studyStr, sessionId);
        List<Long> individualIds = resourceId.getResourceIds();
        String userId = resourceId.getUser();

        List<QueryResult<Individual>> queryResultList = new ArrayList<>(individualIds.size());
        for (Long individualId : individualIds) {
            QueryResult<Individual> queryResult = null;
            try {
                authorizationManager.checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.DELETE);

                // We can delete an individual if their samples can be deleted
                // We obtain the samples associated to the individual and check if those can be deleted
                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), resourceId.getStudyId())
                        .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());
                QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);

                if (sampleQueryResult.getNumResults() > 0) {
                    List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
                    MyResourceIds sampleResource = new MyResourceIds(resourceId.getUser(), resourceId.getStudyId(), sampleIds);
                    // FIXME:
                    // We are first checking and deleting later because that delete method does not check if all the samples can be deleted
                    // directly. Instead, it makes a loop and checks one by one. Changes should be done there.
                    catalogManager.getSampleManager().checkCanDeleteSamples(sampleResource);
                    catalogManager.getSampleManager().delete(StringUtils.join(sampleIds, ","),
                            Long.toString(resourceId.getStudyId()), QueryOptions.empty(), sessionId);
                }

                // Get the individual info before the update
                QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId, QueryOptions.empty());

                String newIndividualName = individualQueryResult.first().getName() + ".DELETED_" + TimeUtils.getTime();
                ObjectMap updateParams = new ObjectMap()
                        .append(IndividualDBAdaptor.QueryParams.NAME.key(), newIndividualName)
                        .append(IndividualDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);
                queryResult = individualDBAdaptor.update(individualId, updateParams);

                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.delete, AuditRecord.Magnitude.high,
                        individualId, resourceId.getUser(), individualQueryResult.first(), queryResult.first(), "", null);
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
        return delete(individualStr, null, options, sessionId);
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getId(sessionId);
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
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);

        // Add study id to the query
        query.put(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

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
    public List<QueryResult<IndividualAclEntry>> updateAcl(String individual, String studyStr, String memberIds,
                                                           Individual.IndividualAclParams aclParams, String sessionId)
            throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(individual) ? 1 : 0;
        count += StringUtils.isNotEmpty(aclParams.getSample()) ? 1 : 0;

        if (count > 1) {
            throw new CatalogException("Update ACL: Only one of these parameters are allowed: individual or sample per query.");
        } else if (count == 0) {
            throw new CatalogException("Update ACL: At least one of these parameters should be provided: individual or sample");
        }

        if (aclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
            permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);
        }

        if (StringUtils.isNotEmpty(aclParams.getSample())) {
            // Obtain the sample ids
            MyResourceIds ids = catalogManager.getSampleManager().getIds(aclParams.getSample(), studyStr, sessionId);

            Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
            QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(ids.getStudyId(), query, options, sessionId);

            Set<Long> individualSet = sampleQueryResult.getResult().stream().map(sample -> sample.getIndividual().getId())
                    .collect(Collectors.toSet());
            individual = StringUtils.join(individualSet, ",");

            studyStr = Long.toString(ids.getStudyId());
        }

        // Obtain the resource ids
        MyResourceIds resourceIds = getIds(individual, studyStr, sessionId);

        // Check the user has the permissions needed to change permissions over those individuals
        for (Long individualId : resourceIds.getResourceIds()) {
            authorizationManager.checkIndividualPermission(individualId, resourceIds.getUser(),
                    IndividualAclEntry.IndividualPermissions.SHARE);
        }

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        CatalogMemberValidator.checkMembers(catalogDBAdaptorFactory, resourceIds.getStudyId(), members);
        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        String collectionName = MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION;

        List<QueryResult<IndividualAclEntry>> queryResults;
        switch (aclParams.getAction()) {
            case SET:
                queryResults = authorizationManager.setAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(), AclParams.Action.SET,
                            null, null, null);
                    catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds,
                            sampleAclParams, sessionId);
                }
                break;
            case ADD:
                queryResults = authorizationManager.addAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(), AclParams.Action.ADD,
                            null, null, null);
                    catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds,
                            sampleAclParams, sessionId);
                }
                break;
            case REMOVE:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(), AclParams.Action.REMOVE,
                            null, null, null);
                    catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds,
                            sampleAclParams, sessionId);
                }
                break;
            case RESET:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(), AclParams.Action.RESET,
                            null, null, null);
                    catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds,
                            sampleAclParams, sessionId);
                }
                break;
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }

        return queryResults;
    }

    private List<Long> getSamplesFromIndividuals(MyResourceIds resourceIds) throws CatalogDBException {
        // Look for all the samples belonging to the individual
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), resourceIds.getResourceIds());

        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()));

        return sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
    }

    private long commonGetAllInvidualSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);
        return resource.getResourceId();
    }

    private long commonGetAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(annotationSetName, "annotationSetName", configuration.getCatalog().getOffset());
        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);
        return resource.getResourceId();
    }

    @Override
    public QueryResult<AnnotationSet> createAnnotationSet(String id, @Nullable String studyStr, long variableSetId,
                                                          String annotationSetName, Map<String, Object> annotations,
                                                          Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        MyResourceId resource = catalogManager.getIndividualManager().getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        QueryResult<AnnotationSet> annotationSet = AnnotationManager.createAnnotationSet(resource.getResourceId(), variableSet,
                annotationSetName, annotations, attributes, individualDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.individual, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> getAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        long individualId = commonGetAllInvidualSets(id, studyStr, sessionId);
        return individualDBAdaptor.getAnnotationSet(individualId, null);
    }

    @Override
    public QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        long individualId = commonGetAllInvidualSets(id, studyStr, sessionId);
        return individualDBAdaptor.getAnnotationSetAsMap(individualId, null);
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        long individualId = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return individualDBAdaptor.getAnnotationSet(individualId, annotationSetName);
    }

    @Override
    public QueryResult<ObjectMap> getAnnotationSetAsMap(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        long individualId = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return individualDBAdaptor.getAnnotationSetAsMap(individualId, annotationSetName);
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, Map<String,
                Object> newAnnotations, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);

        // Update the annotation
        QueryResult<AnnotationSet> queryResult =
                AnnotationManager.updateAnnotationSet(resource.getResourceId(), annotationSetName, newAnnotations, individualDBAdaptor,
                        studyDBAdaptor);

        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("There was an error with the update");
        }

        AnnotationSet annotationSet = queryResult.first();

        // Audit the changes
        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getCreationDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.individual, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSetUpdate)), "update annotation", null);

        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");

        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> annotationSet = individualDBAdaptor.getAnnotationSet(resource.getResourceId(), annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }

        individualDBAdaptor.deleteAnnotationSet(resource.getResourceId(), annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.individual, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }

    @Override
    public QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, @Nullable String studyStr, long variableSetId,
                                                           @Nullable String annotation, String sessionId) throws CatalogException {
        QueryResult<Individual> individualQueryResult = commonSearchAnnotationSet(id, studyStr, variableSetId, annotation, sessionId);
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
    public QueryResult<AnnotationSet> searchAnnotationSet(String id, @Nullable String studyStr, long variableSetId,
                                                          @Nullable String annotation, String sessionId) throws CatalogException {
        QueryResult<Individual> individualQueryResult = commonSearchAnnotationSet(id, studyStr, variableSetId, annotation, sessionId);
        List<AnnotationSet> annotationSets;

        if (individualQueryResult == null || individualQueryResult.getNumResults() == 0) {
            annotationSets = Collections.emptyList();
        } else {
            annotationSets = individualQueryResult.first().getAnnotationSets();
        }

        return new QueryResult<>("Search annotation sets", individualQueryResult.getDbTime(), annotationSets.size(), annotationSets.size(),
                individualQueryResult.getWarningMsg(), individualQueryResult.getErrorMsg(), annotationSets);
    }

    private QueryResult<Individual> commonSearchAnnotationSet(String id, @Nullable String studyStr, long variableSetId,
                                                              @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");

        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);

        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), id);

        if (variableSetId > 0) {
            query.append(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        }
        if (annotation != null) {
            query.append(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), annotation);
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key());
        logger.debug("Query: {}, \t QueryOptions: {}", query.safeToString(), queryOptions.safeToString());

        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);

        // Filter out annotation sets only from the variable set id specified
        for (Individual individual : individualQueryResult.getResult()) {
            List<AnnotationSet> finalAnnotationSets = new ArrayList<>();
            for (AnnotationSet annotationSet : individual.getAnnotationSets()) {
                if (annotationSet.getVariableSetId() == variableSetId) {
                    finalAnnotationSets.add(annotationSet);
                }
            }

            if (finalAnnotationSets.size() > 0) {
                individual.setAnnotationSets(finalAnnotationSets);
            }
        }

        return individualQueryResult;
    }

}
