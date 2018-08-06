/*
 * Copyright 2015-2017 OpenCB
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FamilyAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * Created by pfurio on 02/05/17.
 */
public class FamilyManager extends AnnotationSetManager<Family> {

    protected static Logger logger = LoggerFactory.getLogger(FamilyManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    FamilyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Family smartResolutor(long studyUid, String entry, String user) throws CatalogException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(FamilyDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(FamilyDBAdaptor.QueryParams.ID.key(), entry);
        }
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                FamilyDBAdaptor.QueryParams.UUID.key(), FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
                FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.RELEASE.key(), FamilyDBAdaptor.QueryParams.VERSION.key(),
                FamilyDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, options, user);
        if (familyQueryResult.getNumResults() == 0) {
            familyQueryResult = familyDBAdaptor.get(query, options);
            if (familyQueryResult.getNumResults() == 0) {
                throw new CatalogException("Family " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the family " + entry);
            }
        } else if (familyQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one family found based on " + entry);
        } else {
            return familyQueryResult.first();
        }
    }

    private long getFamilyId(boolean silent, String familyStrAux) throws CatalogException {
        long familyId = Long.parseLong(familyStrAux);
        try {
            familyDBAdaptor.checkId(familyId);
        } catch (CatalogException e) {
            if (silent) {
                return -1L;
            } else {
                throw e;
            }
        }
        return familyId;
    }

    @Override
    public DBIterator<Family> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    public QueryResult<Family> create(String studyStr, Family family, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_FAMILIES);

        ParamUtils.checkObj(family, "family");
        ParamUtils.checkAlias(family.getId(), "id");
        family.setName(ParamUtils.defaultObject(family.getName(), family.getId()));
        family.setMembers(ParamUtils.defaultObject(family.getMembers(), Collections.emptyList()));
        family.setPhenotypes(ParamUtils.defaultObject(family.getPhenotypes(), Collections.emptyList()));
        family.setCreationDate(TimeUtils.getTime());
        family.setDescription(ParamUtils.defaultString(family.getDescription(), ""));
        family.setStatus(new Family.FamilyStatus());
        family.setAnnotationSets(ParamUtils.defaultObject(family.getAnnotationSets(), Collections.emptyList()));
        family.setRelease(catalogManager.getStudyManager().getCurrentRelease(study, userId));
        family.setVersion(1);
        family.setAttributes(ParamUtils.defaultObject(family.getAttributes(), Collections.emptyMap()));

        List<VariableSet> variableSetList = validateNewAnnotationSetsAndExtractVariableSets(study.getUid(), family.getAnnotationSets());

        autoCompleteFamilyMembers(family, study, sessionId);
        validateFamily(family);
        validateMultiples(family);
        validatePhenotypes(family);
        createMissingMembers(family, study, sessionId);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        family.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FAMILY));
        QueryResult<Family> queryResult = familyDBAdaptor.insert(study.getUid(), family, variableSetList, options);
        auditManager.recordCreation(AuditRecord.Resource.family, queryResult.first().getId(), userId, queryResult.first(), null, null);

        return queryResult;
    }

    @Override
    public QueryResult<Family> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(study.getUid(), query);
        fixQueryOptionAnnotation(options);

        query.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, options, userId);

        if (familyQueryResult.getNumResults() == 0 && query.containsKey(FamilyDBAdaptor.QueryParams.UID.key())) {
            List<Long> idList = query.getAsLongList(FamilyDBAdaptor.QueryParams.UID.key());
            for (Long myId : idList) {
                authorizationManager.checkFamilyPermission(study.getUid(), myId, userId, FamilyAclEntry.FamilyPermissions.VIEW);
            }
        }

        return familyQueryResult;
    }

    public QueryResult<Family> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        Query finalQuery = new Query(query);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(study.getUid(), finalQuery);
        fixQueryOptionAnnotation(options);

        fixQueryObject(study, finalQuery, sessionId);

        finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<Family> queryResult = familyDBAdaptor.get(finalQuery, options, userId);
//        addMemberInformation(queryResult, study.getUid(), sessionId);

        return queryResult;
    }

    private void fixQueryObject(Study study, Query query, String sessionId) throws CatalogException {

        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MEMBERS.key()))
                && StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()))) {
            throw new CatalogException("Cannot look for samples and members at the same time");
        }

        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        // We change the MEMBERS parameters for MEMBER_UID which is what the DBAdaptor understands
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MEMBERS.key()))) {
            try {
                MyResources<Individual> resource = catalogManager.getIndividualManager().getUids(
                        query.getAsStringList(FamilyDBAdaptor.QueryParams.MEMBERS.key()), study.getFqn(), sessionId);
                query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), resource.getResourceList().stream().map(Individual::getUid)
                        .collect(Collectors.toList()));
            } catch (CatalogException e) {
                // Add -1 to query so no results are obtained
                query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), -1);
            }

            query.remove(FamilyDBAdaptor.QueryParams.MEMBERS.key());
        }

        // We look for the individuals containing those samples
        if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()))) {
            Query newQuery = new Query()
                    .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(), query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()));
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.UID.key());
            QueryResult<Individual> individualResult = catalogManager.getIndividualManager().get(study.getFqn(), newQuery, options,
                    sessionId);

            query.remove(IndividualDBAdaptor.QueryParams.SAMPLES.key());
            if (individualResult.getNumResults() == 0) {
                // Add -1 to query so no results are obtained
                query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), -1);
            } else {
                // Look for the individuals containing those samples
                query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(),
                        individualResult.getResult().stream().map(Individual::getUid).collect(Collectors.toList()));
            }
        }
    }

    public QueryResult<Family> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        Query finalQuery = new Query(query);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(study.getUid(), finalQuery);
        fixQueryObject(study, finalQuery, sessionId);

        finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = familyDBAdaptor.count(finalQuery, userId, StudyAclEntry.StudyPermissions.VIEW_FAMILIES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        WriteResult writeResult = new WriteResult("delete", -1, -1, -1, null, null, null);

        String userId;
        Study study;

        StopWatch watch = StopWatch.createStarted();

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the families to be deleted
        DBIterator<Family> iterator;
        try {
            userId = catalogManager.getUserManager().getUserId(sessionId);
            study = studyManager.resolveId(studyStr, userId);

            // Fix query if it contains any annotation
            fixQueryAnnotationSearch(study.getUid(), finalQuery);
            fixQueryObject(study, finalQuery, sessionId);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = familyDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error("Delete family: {}", e.getMessage(), e);
            writeResult.setError(new Error(-1, null, e.getMessage()));
            writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
            return writeResult;
        }

        long numMatches = 0;
        long numModified = 0;
        List<WriteResult.Fail> failedList = new ArrayList<>();

        String suffixName = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

        while (iterator.hasNext()) {
            Family family = iterator.next();
            numMatches += 1;

            try {
                if (checkPermissions) {
                    authorizationManager.checkFamilyPermission(study.getUid(), family.getUid(), userId,
                            FamilyAclEntry.FamilyPermissions.DELETE);
                }

                // Check if the family can be deleted
                // TODO: Check if the family is used in a clinical analysis. At this point, it can be deleted no matter what.

                // Delete the family
                Query updateQuery = new Query()
                        .append(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid())
                        .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(Constants.ALL_VERSIONS, true);
                ObjectMap updateParams = new ObjectMap()
                        .append(FamilyDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED)
                        .append(FamilyDBAdaptor.QueryParams.ID.key(), family.getName() + suffixName);
                QueryResult<Long> update = familyDBAdaptor.update(updateQuery, updateParams, QueryOptions.empty());
                if (update.first() > 0) {
                    numModified += 1;
                    auditManager.recordDeletion(AuditRecord.Resource.family, family.getUid(), userId, null, updateParams, null, null);
                } else {
                    failedList.add(new WriteResult.Fail(family.getId(), "Unknown reason"));
                }
            } catch (Exception e) {
                failedList.add(new WriteResult.Fail(family.getId(), e.getMessage()));
                logger.debug("Cannot delete family {}: {}", family.getId(), e.getMessage(), e);
            }
        }

        writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
        writeResult.setNumMatches(numMatches);
        writeResult.setNumModified(numModified);
        writeResult.setFailed(failedList);

        if (!failedList.isEmpty()) {
            writeResult.setWarning(Collections.singletonList(new Error(-1, null, "There are families that could not be deleted")));
        }

        return writeResult;
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId) throws
            CatalogException {
        return null;
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        Query finalQuery = new Query(query);
        fixQueryObject(study, finalQuery, sessionId);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(study.getUid(), userId, query, true);
        fixQueryOptionAnnotation(options);

        // Add study id to the query
        finalQuery.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult queryResult = familyDBAdaptor.groupBy(finalQuery, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult<Family> updateAnnotationSet(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                   ParamUtils.UpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, familyStr, params, options, token);
    }

    public QueryResult<Family> addAnnotationSet(String studyStr, String familyStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return addAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSet), options, token);
    }

    public QueryResult<Family> addAnnotationSets(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.UpdateAction.ADD, options, token);
    }

    public QueryResult<Family> setAnnotationSet(String studyStr, String familyStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return setAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSet), options, token);
    }

    public QueryResult<Family> setAnnotationSets(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.UpdateAction.SET, options, token);
    }

    public QueryResult<Family> removeAnnotationSet(String studyStr, String familyStr, String annotationSetId, QueryOptions options,
                                                   String token) throws CatalogException {
        return removeAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSetId), options, token);
    }

    public QueryResult<Family> removeAnnotationSets(String studyStr, String familyStr, List<String> annotationSetIdList,
                                                    QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.UpdateAction.REMOVE, options, token);
    }

    public QueryResult<Family> updateAnnotations(String studyStr, String familyStr, String annotationSetId,
                                                     Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                     QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            return new QueryResult<>(familyStr, -1, -1, -1, "Nothing to do: The map of annotations is empty", "", Collections.emptyList());
        }
        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATIONS, new AnnotationSet(annotationSetId, "", annotations));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, familyStr, params, options, token);
    }

    public QueryResult<Family> removeAnnotations(String studyStr, String familyStr, String annotationSetId,
                                                 List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, familyStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public QueryResult<Family> resetAnnotations(String studyStr, String familyStr, String annotationSetId, List<String> annotations,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, familyStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    @Override
    public QueryResult<Family> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Missing parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResource resource = getUid(entryStr, studyStr, sessionId);
        long familyId = resource.getResource().getUid();

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (parameters.containsKey(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            authorizationManager.checkFamilyPermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                    FamilyAclEntry.FamilyPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkFamilyPermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                    FamilyAclEntry.FamilyPermissions.UPDATE);
        }

        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.UID.key(), familyId)
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), resource.getStudy().getUid());
        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, new QueryOptions());
        if (familyQueryResult.getNumResults() == 0) {
            throw new CatalogException("Family " + familyId + " not found");
        }

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> FamilyDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        // In case the user is updating members or phenotype list, we will create the family variable. If it is != null, it will mean that
        // all or some of those parameters have been passed to be updated, and we will need to call the private validator to check if the
        // fields are valid.
        Family family = null;

        if (parameters.containsKey(FamilyDBAdaptor.QueryParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(FamilyDBAdaptor.QueryParams.ID.key()), FamilyDBAdaptor.QueryParams.ID.key());
        }
        if (parameters.containsKey(FamilyDBAdaptor.QueryParams.PHENOTYPES.key())
                || parameters.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
            // We parse the parameters to a family object
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

                family = objectMapper.readValue(objectMapper.writeValueAsString(parameters), Family.class);
            } catch (IOException e) {
                logger.error("{}", e.getMessage(), e);
                throw new CatalogException(e);
            }
        }

        if (family != null) {
            // MEMBERS or PHENOTYPES have been passed. We will complete the family object with the stored parameters that are not expected
            // to be updated
            if (family.getMembers() == null || family.getMembers().isEmpty()) {
                family.setMembers(familyQueryResult.first().getMembers());
            } else {
                // We will need to complete the individual information provided
                autoCompleteFamilyMembers(family, resource.getStudy(), sessionId);
            }
            if (family.getPhenotypes() == null || family.getMembers().isEmpty()) {
                family.setPhenotypes(familyQueryResult.first().getPhenotypes());
            }

            validateFamily(family);
            validateMultiples(family);
            validatePhenotypes(family);

            ObjectMap tmpParams;
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                tmpParams = new ObjectMap(objectMapper.writeValueAsString(family));
            } catch (JsonProcessingException e) {
                logger.error("{}", e.getMessage(), e);
                throw new CatalogException(e);
            }

            if (parameters.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
                parameters.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));
            }
            if (parameters.containsKey(FamilyDBAdaptor.QueryParams.PHENOTYPES.key())) {
                parameters.put(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.PHENOTYPES.key()));
            }
        }

        List<VariableSet> variableSetList = checkUpdateAnnotationsAndExtractVariableSets(resource, parameters, options, familyDBAdaptor);

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(resource.getStudy(), resource.getUser()));
        }

        QueryResult<Family> queryResult = familyDBAdaptor.update(familyId, parameters, variableSetList, options);
        auditManager.recordUpdate(AuditRecord.Resource.family, familyId, resource.getUser(), parameters, null, null);

        return queryResult;
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<FamilyAclEntry>> getAcls(String studyStr, List<String> familyList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        List<QueryResult<FamilyAclEntry>> familyAclList = new ArrayList<>(familyList.size());
        for (String family : familyList) {
            try {
                MyResource<Family> resource = getUid(family, studyStr, sessionId);

                QueryResult<FamilyAclEntry> allFamilyAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allFamilyAcls = authorizationManager.getFamilyAcl(resource.getStudy().getUid(), resource.getResource().getUid(),
                            resource.getUser(),
                            member);
                } else {
                    allFamilyAcls = authorizationManager.getAllFamilyAcls(resource.getStudy().getUid(), resource.getResource().getUid(),
                            resource.getUser());
                }
                allFamilyAcls.setId(family);
                familyAclList.add(allFamilyAcls);
            } catch (CatalogException e) {
                if (silent) {
                    familyAclList.add(new QueryResult<>(family, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return familyAclList;
    }

    public List<QueryResult<FamilyAclEntry>> updateAcl(String studyStr, List<String> familyList, String memberIds,
                                                       AclParams familyAclParams, String sessionId) throws CatalogException {
        if (familyList == null || familyList.isEmpty()) {
            throw new CatalogException("Update ACL: Missing family parameter");
        }

        if (familyAclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(familyAclParams.getPermissions())) {
            permissions = Arrays.asList(familyAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, FamilyAclEntry.FamilyPermissions::valueOf);
        }

        MyResources<Family> resource = getUids(familyList, studyStr, sessionId);
        authorizationManager.checkCanAssignOrSeePermissions(resource.getStudy().getUid(), resource.getUser());

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resource.getStudy().getUid(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        switch (familyAclParams.getAction()) {
            case SET:
                // Todo: Remove this in 1.4
                List<String> allFamilyPermissions = EnumSet.allOf(FamilyAclEntry.FamilyPermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                return authorizationManager.setAcls(resource.getStudy().getUid(), resource.getResourceList().stream().map(Family::getUid)
                                .collect(Collectors.toList()), members, permissions,
                        allFamilyPermissions, Entity.FAMILY);
            case ADD:
                return authorizationManager.addAcls(resource.getStudy().getUid(), resource.getResourceList().stream().map(Family::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.FAMILY);
            case REMOVE:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(Family::getUid).collect(Collectors.toList()),
                        members, permissions, Entity.FAMILY);
            case RESET:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(Family::getUid).collect(Collectors.toList()),
                        members, null, Entity.FAMILY);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

    public DBIterator<Family> indexSolr(Query query) throws CatalogException {
        return familyDBAdaptor.iterator(query, null, null);
    }


    public FacetedQueryResult facet(Query query, QueryOptions queryOptions, String sessionId) throws IOException, CatalogDBException {

        CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager);
        String collection = catalogManager.getConfiguration().getDatabasePrefix() + "_"
                + CatalogSolrManager.FAMILY_SOLR_COLLECTION;

        return catalogSolrManager.facetedQuery(collection, query, queryOptions);
    }


    /**
     * Looks for all the members in the database. If they exist, the data will be overriden. It also fetches the parents individuals if they
     * haven't been provided.
     *
     * @param family    family object.
     * @param study     study.
     * @param sessionId session id.
     * @throws CatalogException if there is any kind of error.
     */
    private void autoCompleteFamilyMembers(Family family, Study study, String sessionId) throws CatalogException {
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            return;
        }

        Map<String, Individual> memberMap = new HashMap<>();
        Set<String> individualIds = new HashSet<>();
        for (Individual individual : family.getMembers()) {
            memberMap.put(individual.getId(), individual);
            individualIds.add(individual.getId());

            if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getId())) {
                individualIds.add(individual.getFather().getId());
            }
            if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getId())) {
                individualIds.add(individual.getMother().getId());
            }
        }

        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualIds);
        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(study.getFqn(), query,
                new QueryOptions(), sessionId);
        for (Individual individual : individualQueryResult.getResult()) {
            // We override the individuals from the map
            memberMap.put(individual.getId(), individual);
        }

        family.setMembers(memberMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList()));
    }

    private void validateFamily(Family family) throws CatalogException {
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            return;
        }

        Map<String, Individual> membersMap = new HashMap<>();       // individualName|individualId: Individual
        Map<String, List<Individual>> parentsMap = new HashMap<>(); // motherName||F---fatherName||M: List<children>
        Set<Individual> noParentsSet = new HashSet<>();             // Set with individuals without parents

        // 1. Fill in the objects initialised above
        for (Individual individual : family.getMembers()) {
            membersMap.put(individual.getId(), individual);
            if (individual.getUid() > 0) {
                membersMap.put(String.valueOf(individual.getUid()), individual);
            }

            String parentsKey = null;
            if (individual.getMother() != null) {
                if (individual.getMother().getUid() > 0) {
                    individual.getMother().setId(String.valueOf(individual.getMother().getUid()));
                }
                if (!StringUtils.isEmpty(individual.getMother().getId())) {
                    parentsKey = individual.getMother().getId() + "||F";
                }
            }
            if (individual.getFather() != null) {
                if (parentsKey != null) {
                    parentsKey += "---";
                }
                if (individual.getFather().getUid() > 0) {
                    individual.getFather().setId(String.valueOf(individual.getFather().getUid()));
                }
                if (!StringUtils.isEmpty(individual.getFather().getId())) {
                    if (parentsKey != null) {
                        parentsKey += individual.getFather().getId() + "||M";
                    } else {
                        parentsKey = individual.getFather().getId() + "||M";
                    }
                }
            }
            if (parentsKey == null) {
                noParentsSet.add(individual);
            } else {
                if (!parentsMap.containsKey(parentsKey)) {
                    parentsMap.put(parentsKey, new ArrayList<>());
                }
                parentsMap.get(parentsKey).add(individual);
            }
        }

        // 2. Loop over the parentsMap object. We will be emptying the noParentsSet as soon as we find a parent in the set. Once,
        // everything finishes, that set should be empty. Otherwise, it will mean that parent is not in use
        // On the other hand, all the parents should exist in the membersMap, otherwise it will mean that is missing in the family
        for (Map.Entry<String, List<Individual>> parentListEntry : parentsMap.entrySet()) {
            String[] split = parentListEntry.getKey().split("---");
            for (String parentName : split) {
                String[] splitNameSex = parentName.split("\\|\\|");
                String name = splitNameSex[0];
                Individual.Sex sex = splitNameSex[1].equals("F") ? Individual.Sex.FEMALE : Individual.Sex.MALE;

                if (!membersMap.containsKey(name)) {
                    throw new CatalogException("The parent " + name + " is not present in the members list");
                } else {
                    // Check if the sex is correct
                    Individual.Sex sex1 = membersMap.get(name).getSex();
                    if (sex1 != null && sex1 != sex && sex1 != Individual.Sex.UNKNOWN) {
                        throw new CatalogException("Sex of parent " + name + " is incorrect or the relationship is incorrect. In "
                                + "principle, it should be " + sex);
                    }
                    membersMap.get(name).setSex(sex);

                    // We attempt to remove the individual from the noParentsSet
                    noParentsSet.remove(membersMap.get(name));
                }
            }
        }

        // FIXME Pedro: this is a quick fix to allow create families without the parents, this needs to be reviewed.
        if (noParentsSet.size() > 0) {
//            throw new CatalogException("Some members that are not related to any other have been found: "
//                    + noParentsSet.stream().map(Individual::getName).collect(Collectors.joining(", ")));
            logger.warn("Some members that are not related to any other have been found: {}",
                    noParentsSet.stream().map(Individual::getName).collect(Collectors.joining(", ")));
        }
    }

    private void validateMultiples(Family family) throws CatalogException {
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            return;
        }

        Map<String, List<String>> multiples = new HashMap<>();
        // Look for all the multiples
        for (Individual individual : family.getMembers()) {
            if (individual.getMultiples() != null && individual.getMultiples().getSiblings() != null
                    && !individual.getMultiples().getSiblings().isEmpty()) {
                multiples.put(individual.getId(), individual.getMultiples().getSiblings());
            }
        }

        if (multiples.size() > 0) {
            // Check if they are all cross-referenced
            for (Map.Entry<String, List<String>> entry : multiples.entrySet()) {
                for (String sibling : entry.getValue()) {
                    if (!multiples.containsKey(sibling)) {
                        throw new CatalogException("Missing sibling " + sibling + " of member " + entry.getKey());
                    }
                    if (!multiples.get(sibling).contains(entry.getKey())) {
                        throw new CatalogException("Incomplete sibling information. Sibling " + sibling + " does not contain "
                                + entry.getKey() + " as its sibling");
                    }
                }
            }
        }
    }

    private void validatePhenotypes(Family family) throws CatalogException {
        if (family.getPhenotypes() == null || family.getPhenotypes().isEmpty()) {
            return;
        }

        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            throw new CatalogException("Missing family members");
        }

        Set<String> memberPhenotypes = new HashSet<>();
        for (Individual individual : family.getMembers()) {
            if (individual.getPhenotypes() != null && !individual.getPhenotypes().isEmpty()) {
                memberPhenotypes.addAll(individual.getPhenotypes().stream().map(OntologyTerm::getId).collect(Collectors.toSet()));
            }
        }
        Set<String> familyPhenotypes = family.getPhenotypes().stream().map(OntologyTerm::getId).collect(Collectors.toSet());
        if (!familyPhenotypes.containsAll(memberPhenotypes)) {
            throw new CatalogException("Some of the phenotypes are not present in any member of the family");
        }
    }

    private void createMissingMembers(Family family, Study study, String sessionId) throws CatalogException {
        if (family.getMembers() == null) {
            return;
        }

        // First, we will need to fix all the relationships. This means, that all children will be pointing to the latest parent individual
        // information available before it is created ! On the other hand, individuals will be created from the top to the bottom of the
        // family. Otherwise, references to parents might be lost.

        // We will assume that before calling to this method, the autoCompleteFamilyMembers method would have been called.
        // In that case, only individuals with ids <= 0 will have to be created

        // We initialize the individual map containing all the individuals
        Map<String, Individual> individualMap = new HashMap<>();
        List<Individual> individualsToCreate = new ArrayList<>();
        for (Individual individual : family.getMembers()) {
            individualMap.put(individual.getId(), individual);
            if (individual.getUid() <= 0) {
                individualsToCreate.add(individual);
            }
        }

        // We link father and mother to individual objects
        for (Map.Entry<String, Individual> entry : individualMap.entrySet()) {
            if (entry.getValue().getFather() != null && StringUtils.isNotEmpty(entry.getValue().getFather().getId())) {
                entry.getValue().setFather(individualMap.get(entry.getValue().getFather().getId()));
            }
            if (entry.getValue().getMother() != null && StringUtils.isNotEmpty(entry.getValue().getMother().getId())) {
                entry.getValue().setMother(individualMap.get(entry.getValue().getMother().getId()));
            }
        }

        // We start creating missing individuals
        for (Individual individual : individualsToCreate) {
            createMissingIndividual(individual, individualMap, study, sessionId);
        }
    }

    private void createMissingIndividual(Individual individual, Map<String, Individual> individualMap, Study study, String sessionId)
            throws CatalogException {
        if (individual == null || individual.getUid() > 0) {
            return;
        }
        if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getId())) {
            createMissingIndividual(individual.getFather(), individualMap, study, sessionId);
            individual.setFather(individualMap.get(individual.getFather().getId()));
        }
        if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getId())) {
            createMissingIndividual(individual.getMother(), individualMap, study, sessionId);
            individual.setMother(individualMap.get(individual.getMother().getId()));
        }
        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().create(study.getFqn(), individual,
                QueryOptions.empty(), sessionId);
        if (individualQueryResult.getNumResults() == 0) {
            throw new CatalogException("Unexpected error when trying to create individual " + individual.getId());
        }
        individualMap.put(individual.getId(), individualQueryResult.first());
    }

}
