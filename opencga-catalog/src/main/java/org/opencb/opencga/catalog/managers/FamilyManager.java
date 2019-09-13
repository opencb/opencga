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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetQueryResult;
import org.opencb.opencga.catalog.models.update.FamilyUpdateParams;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.Penetrance;
import static org.opencb.opencga.catalog.audit.AuditRecord.ERROR;
import static org.opencb.opencga.catalog.audit.AuditRecord.SUCCESS;
import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by pfurio on 02/05/17.
 */
public class FamilyManager extends AnnotationSetManager<Family> {

    protected static Logger logger = LoggerFactory.getLogger(FamilyManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    private final String defaultFacet = "creationYear>>creationMonth;status;phenotypes;expectedSize;numMembers[0..20]:2";

    public static final QueryOptions INCLUDE_FAMILY_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.UUID.key(),
            FamilyDBAdaptor.QueryParams.VERSION.key()));

    FamilyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    QueryResult<Family> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            queryCopy.put(FamilyDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(FamilyDBAdaptor.QueryParams.ID.key(), entry);
        }
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//               FamilyDBAdaptor.QueryParams.UUID.key(), FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
//               FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.RELEASE.key(), FamilyDBAdaptor.QueryParams.VERSION.key(),
//                FamilyDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(queryCopy, options, user);
        if (familyQueryResult.getNumResults() == 0) {
            familyQueryResult = familyDBAdaptor.get(queryCopy, options);
            if (familyQueryResult.getNumResults() == 0) {
                throw new CatalogException("Family " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the family " + entry);
            }
        } else if (familyQueryResult.getNumResults() > 1 && !queryCopy.getBoolean(Constants.ALL_VERSIONS)) {
            throw new CatalogException("More than one family found based on " + entry);
        } else {
            return familyQueryResult;
        }
    }

    @Override
    InternalGetQueryResult<Family> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                               String user, boolean silent) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing family entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<Family, String> familyStringFunction = Family::getId;
        FamilyDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            FamilyDBAdaptor.QueryParams param = FamilyDBAdaptor.QueryParams.ID;
            if (UUIDUtils.isOpenCGAUUID(entry)) {
                param = FamilyDBAdaptor.QueryParams.UUID;
                familyStringFunction = Family::getUuid;
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

        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(queryCopy, queryOptions, user);

        if (silent || familyQueryResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, familyStringFunction, familyQueryResult, silent,
                    queryCopy.getBoolean(Constants.ALL_VERSIONS));
        }
        // Query without adding the user check
        QueryResult<Family> resultsNoCheck = familyDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == familyQueryResult.getNumResults()) {
            throw CatalogException.notFound("families",
                    getMissingFields(uniqueList, familyQueryResult.getResult(), familyStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the families.");
        }
    }

    private QueryResult<Family> getFamily(long studyUid, String familyUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FamilyDBAdaptor.QueryParams.UUID.key(), familyUuid);
        return familyDBAdaptor.get(query, options);
    }

    @Override
    public DBIterator<Family> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Family> create(String studyStr, Family family, QueryOptions options, String token) throws CatalogException {
        return create(studyStr, family, null, options, token);
    }

    public QueryResult<Family> create(String studyStr, Family family, List<String> members, QueryOptions options, String token)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("family", family)
                .append("members", members)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_FAMILIES);

            ParamUtils.checkObj(family, "family");
            ParamUtils.checkAlias(family.getId(), "id");
            family.setName(ParamUtils.defaultObject(family.getName(), family.getId()));
            family.setMembers(ParamUtils.defaultObject(family.getMembers(), Collections.emptyList()));
            family.setPhenotypes(ParamUtils.defaultObject(family.getPhenotypes(), Collections.emptyList()));
            family.setDisorders(ParamUtils.defaultObject(family.getDisorders(), Collections.emptyList()));
            family.setCreationDate(TimeUtils.getTime());
            family.setDescription(ParamUtils.defaultString(family.getDescription(), ""));
            family.setStatus(new Family.FamilyStatus());
            family.setAnnotationSets(ParamUtils.defaultObject(family.getAnnotationSets(), Collections.emptyList()));
            family.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
            family.setVersion(1);
            family.setAttributes(ParamUtils.defaultObject(family.getAttributes(), Collections.emptyMap()));

            // Check the id is not in use
            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FamilyDBAdaptor.QueryParams.ID.key(), family.getId());
            if (familyDBAdaptor.count(query).first() > 0) {
                throw new CatalogException("Family '" + family.getId() + "' already exists.");
            }

            validateNewAnnotationSets(study.getVariableSets(), family.getAnnotationSets());

            autoCompleteFamilyMembers(study, family, members, userId);
            validateFamily(family);
            validateMultiples(family);
            validatePhenotypes(family);
            validateDisorders(family);

            options = ParamUtils.defaultObject(options, QueryOptions::new);
            family.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FAMILY));

            familyDBAdaptor.insert(study.getUid(), family, study.getVariableSets(), options);
            QueryResult<Family> queryResult = getFamily(study.getUid(), family.getUuid(), options);

            auditManager.auditCreate(userId, family.getId(), family.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.FAMILY, SUCCESS);

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, family.getId(), "", study.getId(), study.getUuid(), auditParams, AuditRecord.Entity.FAMILY,
                    ERROR);

            throw e;
        }
    }

    @Override
    public QueryResult<Family> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        AnnotationUtils.fixQueryOptionAnnotation(options);

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
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        Query finalQuery = new Query(query);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
        AnnotationUtils.fixQueryOptionAnnotation(options);

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
            String userId = userManager.getUserId(sessionId);

            List<Individual> memberList = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    query.getAsStringList(FamilyDBAdaptor.QueryParams.MEMBERS.key()), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId,
                    true).getResult();
            if (ListUtils.isNotEmpty(memberList)) {
                query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), memberList.stream().map(Individual::getUid)
                        .collect(Collectors.toList()));
            } else {
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
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        Query finalQuery = new Query(query);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
        fixQueryObject(study, finalQuery, sessionId);

        finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = familyDBAdaptor.count(finalQuery, userId, StudyAclEntry.StudyPermissions.VIEW_FAMILIES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        WriteResult writeResult = new WriteResult();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        Query auditQuery = new Query(query);
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("params", params)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the families to be deleted
        DBIterator<Family> iterator;
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, token);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = familyDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(userId, operationUuid, "", "", study.getId(), study.getUuid(), auditQuery, auditParams,
                    AuditRecord.Entity.FAMILY, ERROR);
            throw e;
        }

        while (iterator.hasNext()) {
            Family family = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkFamilyPermission(study.getUid(), family.getUid(), userId,
                            FamilyAclEntry.FamilyPermissions.DELETE);
                }

                // TODO: Check if the family is used in a clinical analysis. At this point, it can be deleted no matter what.

                // Delete the family
                writeResult.concat(familyDBAdaptor.delete(family.getUid()));

                auditManager.auditDelete(userId, operationUuid, family.getId(), family.getUuid(), study.getId(), study.getUuid(),
                        auditQuery, auditParams, AuditRecord.Entity.FAMILY, SUCCESS);
            } catch (Exception e) {
                writeResult.getFailed().add(new WriteResult.Fail(family.getId(), e.getMessage()));
                logger.debug("Cannot delete family {}: {}", family.getId(), e.getMessage(), e);

                auditManager.auditDelete(userId, operationUuid, family.getId(), family.getUuid(), study.getId(), study.getUuid(),
                        auditQuery, auditParams, AuditRecord.Entity.FAMILY, ERROR);
            }
        }

        if (!writeResult.getFailed().isEmpty()) {
            writeResult.setWarning(Collections.singletonList("Some families could not be deleted"));
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
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        // Add study id to the query
        finalQuery.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult queryResult = familyDBAdaptor.groupBy(finalQuery, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult<Family> updateAnnotationSet(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                   ParamUtils.UpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        FamilyUpdateParams updateParams = new FamilyUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, familyStr, updateParams, options, token);
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
        FamilyUpdateParams updateParams = new FamilyUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, "", annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, familyStr, updateParams, options, token);
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

    /**
     * Update an Family from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param familyId   Family id in string format. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A QueryResult with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public QueryResult<Family> update(String studyStr, String familyId, FamilyUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("familyId", familyId)
                .append("updateParams", updateParams != null ? updateParams.getUpdateMap() : null)
                .append("options", options)
                .append("token", token);

        Family storedFamily;
        try {
            storedFamily = internalGet(study.getUid(), familyId, QueryOptions.empty(), userId).first();
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, familyId, "", study.getId(), study.getUuid(), auditParams, AuditRecord.Entity.FAMILY,
                    ERROR);
            throw e;
        }

        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);

            ObjectMap parameters = new ObjectMap();
            if (updateParams != null) {
                parameters = updateParams.getUpdateMap();
            }

            if (parameters.isEmpty() && !options.getBoolean(Constants.INCREMENT_VERSION, false)) {
                ParamUtils.checkUpdateParametersMap(parameters);
            }

            if (parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
                Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
                if (!actionMap.containsKey(AnnotationSetManager.ANNOTATION_SETS)
                        && !actionMap.containsKey(AnnotationSetManager.ANNOTATIONS)) {
                    logger.warn("Assuming the user wants to add the list of annotation sets provided");
                    actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.UpdateAction.ADD);
                    options.put(Constants.ACTIONS, actionMap);
                }
            }

            // Check permissions...
            // Only check write annotation permissions if the user wants to update the annotation sets
            if (updateParams != null && updateParams.getAnnotationSets() != null) {
                authorizationManager.checkFamilyPermission(study.getUid(), storedFamily.getUid(), userId,
                        FamilyAclEntry.FamilyPermissions.WRITE_ANNOTATIONS);
            }
            // Only check update permissions if the user wants to update anything apart from the annotation sets
            if ((parameters.size() == 1 && !parameters.containsKey(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                    || parameters.size() > 1) {
                authorizationManager.checkFamilyPermission(study.getUid(), storedFamily.getUid(), userId,
                        FamilyAclEntry.FamilyPermissions.UPDATE);
            }

            if (updateParams != null && StringUtils.isNotEmpty(updateParams.getId())) {
                ParamUtils.checkAlias(updateParams.getId(), FamilyDBAdaptor.QueryParams.ID.key());
            }

            if (updateParams != null && (ListUtils.isNotEmpty(updateParams.getPhenotypes())
                    || ListUtils.isNotEmpty(updateParams.getMembers()) || ListUtils.isNotEmpty(updateParams.getDisorders()))) {
                Family family = new Family();
                if (ListUtils.isNotEmpty(updateParams.getMembers())) {
                    // We obtain the members from catalog
                    autoCompleteFamilyMembers(study, family, updateParams.getMembers(), userId);
                } else {
                    // We use the list of members from the stored family
                    family.setMembers(storedFamily.getMembers());
                }

                if (ListUtils.isEmpty(updateParams.getPhenotypes())) {
                    family.setPhenotypes(storedFamily.getPhenotypes());
                } else {
                    family.setPhenotypes(updateParams.getPhenotypes());
                }
                if (ListUtils.isEmpty(updateParams.getDisorders())) {
                    family.setDisorders(storedFamily.getDisorders());
                } else {
                    family.setDisorders(updateParams.getDisorders());
                }

                validateFamily(family);
                validateMultiples(family);
                validatePhenotypes(family);
                validateDisorders(family);

                ObjectMap tmpParams;
                try {
                    ObjectMapper objectMapper = getDefaultObjectMapper();
                    tmpParams = new ObjectMap(objectMapper.writeValueAsString(family));
                } catch (JsonProcessingException e) {
                    logger.error("{}", e.getMessage(), e);
                    throw new CatalogException(e);
                }

                if (parameters.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
                    parameters.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));
                }
                if (parameters.containsKey(FamilyDBAdaptor.QueryParams.PHENOTYPES.key())) {
                    parameters.put(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(),
                            tmpParams.get(FamilyDBAdaptor.QueryParams.PHENOTYPES.key()));
                }
                if (parameters.containsKey(FamilyDBAdaptor.QueryParams.DISORDERS.key())) {
                    parameters.put(FamilyDBAdaptor.QueryParams.DISORDERS.key(), tmpParams.get(FamilyDBAdaptor.QueryParams.DISORDERS.key()));
                }
            }

            checkUpdateAnnotations(study, storedFamily, parameters, options, VariableSet.AnnotableDataModels.FAMILY, familyDBAdaptor,
                    userId);

            if (options.getBoolean(Constants.INCREMENT_VERSION)) {
                // We do need to get the current release to properly create a new version
                options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study));
            }

            WriteResult result = familyDBAdaptor.update(storedFamily.getUid(), parameters, study.getVariableSets(), options);

            auditManager.auditUpdate(userId, storedFamily.getId(), storedFamily.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.FAMILY, SUCCESS);

            QueryResult<Family> queryResult = familyDBAdaptor.get(storedFamily.getUid(),
                    new QueryOptions(QueryOptions.INCLUDE, parameters.keySet()));
            queryResult.setDbTime(result.getDbTime() + queryResult.getDbTime());

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, storedFamily.getId(), storedFamily.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.FAMILY, ERROR);
            throw e;
        }
    }

    public Map<String, List<String>> calculateFamilyGenotypes(String studyStr, String clinicalAnalysisId, String familyId,
                                                              ClinicalProperty.ModeOfInheritance moi, String disorderId,
                                                              Penetrance penetrance, String token) throws CatalogException {
        Pedigree pedigree;
        Disorder disorder = null;

        if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
            QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyStr,
                    clinicalAnalysisId, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                            ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
                            ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key())), token);
            if (clinicalAnalysisQueryResult.getNumResults() == 0) {
                throw new CatalogException("Clinical analysis " + clinicalAnalysisId + " not found");
            }

            disorder = clinicalAnalysisQueryResult.first().getDisorder();
            pedigree = getPedigreeFromFamily(clinicalAnalysisQueryResult.first().getFamily(),
                    clinicalAnalysisQueryResult.first().getProband().getId());

        } else if (StringUtils.isNotEmpty(familyId) && StringUtils.isNotEmpty(disorderId)) {
            QueryResult<Family> familyQueryResult = get(studyStr, familyId, QueryOptions.empty(), token);

            if (familyQueryResult.getNumResults() == 0) {
                throw new CatalogException("Family " + familyId + " not found");
            }

            for (Disorder tmpDisorder : familyQueryResult.first().getDisorders()) {
                if (tmpDisorder.getId().equals(disorderId)) {
                    disorder = tmpDisorder;
                    break;
                }
            }
            if (disorder == null) {
                throw new CatalogException("Disorder " + disorderId + " not found in any member of the family");
            }

            pedigree = getPedigreeFromFamily(familyQueryResult.first(), null);
        } else {
            throw new CatalogException("Missing 'clinicalAnalysis' or ('family' and 'disorderId') parameters");
        }

        switch (moi) {
            case MONOALLELIC:
                return ModeOfInheritance.dominant(pedigree, disorder, penetrance);
            case BIALLELIC:
                return ModeOfInheritance.recessive(pedigree, disorder, penetrance);
            case XLINKED_BIALLELIC:
                return ModeOfInheritance.xLinked(pedigree, disorder, false, penetrance);
            case XLINKED_MONOALLELIC:
                return ModeOfInheritance.xLinked(pedigree, disorder, true, penetrance);
            case YLINKED:
                return ModeOfInheritance.yLinked(pedigree, disorder, penetrance);
            case MITOCHONDRIAL:
                return ModeOfInheritance.mitochondrial(pedigree, disorder, penetrance);
            case DE_NOVO:
                return ModeOfInheritance.deNovo(pedigree);
            case COMPOUND_HETEROZYGOUS:
                return ModeOfInheritance.compoundHeterozygous(pedigree);
            default:
                throw new CatalogException("Unsupported or unknown mode of inheritance " + moi);
        }
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<FamilyAclEntry>> getAcls(String studyStr, List<String> familyList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        List<QueryResult<FamilyAclEntry>> familyAclList = new ArrayList<>(familyList.size());
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        InternalGetQueryResult<Family> familyQueryResult = internalGet(study.getUid(), familyList, INCLUDE_FAMILY_IDS, user, silent);

        Map<String, InternalGetQueryResult.Missing> missingMap = new HashMap<>();
        if (familyQueryResult.getMissing() != null) {
            missingMap = familyQueryResult.getMissing().stream()
                    .collect(Collectors.toMap(InternalGetQueryResult.Missing::getId, Function.identity()));
        }
        int counter = 0;
        for (String familyId : familyList) {
            if (!missingMap.containsKey(familyId)) {
                try {
                    QueryResult<FamilyAclEntry> allFamilyAcls;
                    if (StringUtils.isNotEmpty(member)) {
                        allFamilyAcls = authorizationManager.getFamilyAcl(study.getUid(),
                                familyQueryResult.getResult().get(counter).getUid(), user, member);
                    } else {
                        allFamilyAcls = authorizationManager.getAllFamilyAcls(study.getUid(),
                                familyQueryResult.getResult().get(counter).getUid(), user);
                    }
                    allFamilyAcls.setId(familyId);
                    familyAclList.add(allFamilyAcls);
                } catch (CatalogException e) {
                    if (!silent) {
                        throw e;
                    } else {
                        familyAclList.add(new QueryResult<>(familyId, familyQueryResult.getDbTime(), 0, 0, "",
                                missingMap.get(familyId).getErrorMsg(), Collections.emptyList()));
                    }
                }
                counter += 1;
            } else {
                familyAclList.add(new QueryResult<>(familyId, familyQueryResult.getDbTime(), 0, 0, "",
                        missingMap.get(familyId).getErrorMsg(), Collections.emptyList()));
            }
        }
        return familyAclList;
    }

    public List<QueryResult<FamilyAclEntry>> updateAcl(String studyStr, List<String> familyStringList, String memberIds,
                                                       AclParams familyAclParams, String sessionId) throws CatalogException {
        if (familyStringList == null || familyStringList.isEmpty()) {
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

        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);
        List<Family> familyList = internalGet(study.getUid(), familyStringList, INCLUDE_FAMILY_IDS, user, false).getResult();

        authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(study.getUid(), members);

        switch (familyAclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(study.getUid(), familyList.stream().map(Family::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.FAMILY);
            case ADD:
                return authorizationManager.addAcls(study.getUid(), familyList.stream().map(Family::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.FAMILY);
            case REMOVE:
                return authorizationManager.removeAcls(familyList.stream().map(Family::getUid).collect(Collectors.toList()),
                        members, permissions, Entity.FAMILY);
            case RESET:
                return authorizationManager.removeAcls(familyList.stream().map(Family::getUid).collect(Collectors.toList()),
                        members, null, Entity.FAMILY);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

    public FacetQueryResult facet(String studyStr, Query query, QueryOptions queryOptions, boolean defaultStats, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(queryOptions, QueryOptions::new);

        if (defaultStats || StringUtils.isEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String facet = queryOptions.getString(QueryOptions.FACET);
            queryOptions.put(QueryOptions.FACET, StringUtils.isNotEmpty(facet) ? defaultFacet + ";" + facet : defaultFacet);
        }

        CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager);

        String userId = userManager.getUserId(sessionId);
        // We need to add variableSets and groups to avoid additional queries as it will be used in the catalogSolrManager
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key())));

        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

        return catalogSolrManager.facetedQuery(study, CatalogSolrManager.FAMILY_SOLR_COLLECTION, query, queryOptions, userId);
    }

    public static Pedigree getPedigreeFromFamily(Family family, String probandId) {
        List<Individual> members = family.getMembers();
        Map<String, Member> individualMap = new HashMap<>();

        // Parse all the individuals
        for (Individual member : members) {
            Member individual = new Member(
                    member.getId(), member.getName(), null, null, member.getMultiples(),
                    Member.Sex.getEnum(member.getSex().toString()), member.getLifeStatus(),
                    Member.AffectionStatus.getEnum(member.getAffectationStatus().toString()),
                    member.getPhenotypes(), member.getDisorders(), member.getAttributes());
            individualMap.put(individual.getId(), individual);
        }

        // Fill parent information
        for (Individual member : members) {
            if (member.getFather() != null && StringUtils.isNotEmpty(member.getFather().getId())) {
                individualMap.get(member.getId()).setFather(individualMap.get(member.getFather().getId()));
            }
            if (member.getMother() != null && StringUtils.isNotEmpty(member.getMother().getId())) {
                individualMap.get(member.getId()).setMother(individualMap.get(member.getMother().getId()));
            }
        }

        Member proband = null;
        if (StringUtils.isNotEmpty(probandId)) {
            proband = individualMap.get(probandId);
        }

        List<Member> individuals = new ArrayList<>(individualMap.values());
        return new Pedigree(family.getId(), individuals, proband, family.getPhenotypes(), family.getDisorders(), family.getAttributes());
    }

    /**
     * Validate the list of members provided in the members list already exists (and retrieves their information).
     * It also makes sure the members provided inside the family object are valid and can be successfully created.
     * It merges all those members inside the family object afterwards.
     *
     * @param study     study.
     * @param family    family object.
     * @param members   Already existing members.
     * @param userId    user id.
     * @throws CatalogException if there is any kind of error.
     */
    private void autoCompleteFamilyMembers(Study study, Family family, List<String> members, String userId) throws CatalogException {
        List<Individual> memberList = new ArrayList<>();

        if (family.getMembers() != null && !family.getMembers().isEmpty()) {
            // Check the user can create new individuals
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);

            // Validate the individuals can be created and are valid
            for (Individual individual : family.getMembers()) {
                catalogManager.getIndividualManager().validateNewIndividual(study, individual, null, userId, false);
                memberList.add(individual);
            }
        }

        if (members != null && !members.isEmpty()) {
            // We remove any possible duplicate
            ArrayList<String> deduplicatedMemberIds = new ArrayList<>(new HashSet<>(members));

            InternalGetQueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    deduplicatedMemberIds, IndividualManager.INCLUDE_INDIVIDUAL_DISORDERS_PHENOTYPES, userId, false);

            memberList.addAll(individualQueryResult.getResult());
        }

        family.setMembers(memberList);
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
                IndividualProperty.Sex sex = splitNameSex[1].equals("F") ? IndividualProperty.Sex.FEMALE : IndividualProperty.Sex.MALE;

                if (!membersMap.containsKey(name)) {
                    throw new CatalogException("The parent " + name + " is not present in the members list");
                } else {
                    // Check if the sex is correct
                    IndividualProperty.Sex sex1 = membersMap.get(name).getSex();
                    if (sex1 != null && sex1 != sex && sex1 != IndividualProperty.Sex.UNKNOWN) {
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
                    noParentsSet.stream().map(Individual::getId).collect(Collectors.joining(", ")));
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
            if (ListUtils.isNotEmpty(family.getMembers())) {
                Map<String, Phenotype> phenotypeMap = new HashMap<>();

                for (Individual member : family.getMembers()) {
                    if (ListUtils.isNotEmpty(member.getPhenotypes())) {
                        for (Phenotype phenotype : member.getPhenotypes()) {
                            phenotypeMap.put(phenotype.getId(), phenotype);
                        }
                    }
                }

                // Set the new phenotype list
                List<Phenotype> phenotypeList = new ArrayList<>(phenotypeMap.values());
                family.setPhenotypes(phenotypeList);
            }
        } else {
            // We need to validate the phenotypes are actually correct
            if (family.getMembers() == null || family.getMembers().isEmpty()) {
                throw new CatalogException("Missing family members");
            }

            // Validate all the phenotypes are contained in at least one individual
            Set<String> memberPhenotypes = new HashSet<>();
            for (Individual individual : family.getMembers()) {
                if (individual.getPhenotypes() != null && !individual.getPhenotypes().isEmpty()) {
                    memberPhenotypes.addAll(individual.getPhenotypes().stream().map(Phenotype::getId).collect(Collectors.toSet()));
                }
            }
            Set<String> familyPhenotypes = family.getPhenotypes().stream().map(Phenotype::getId).collect(Collectors.toSet());
            if (!familyPhenotypes.containsAll(memberPhenotypes)) {
                throw new CatalogException("Some of the phenotypes are not present in any member of the family");
            }
        }
    }

    private void validateDisorders(Family family) throws CatalogException {
        if (ListUtils.isEmpty(family.getDisorders())) {
            if (ListUtils.isNotEmpty(family.getMembers())) {
                // Obtain the union of all disorders
                Map<String, Disorder> disorderMap = new HashMap<>();
                Map<String, Map<String, Phenotype>> disorderPhenotypeMap = new HashMap<>();

                for (Individual member : family.getMembers()) {
                    if (ListUtils.isNotEmpty(member.getDisorders())) {
                        for (Disorder disorder : member.getDisorders()) {
                            disorderMap.put(disorder.getId(), disorder);

                            if (ListUtils.isNotEmpty(disorder.getEvidences())) {
                                if (!disorderPhenotypeMap.containsKey(disorder.getId())) {
                                    disorderPhenotypeMap.put(disorder.getId(), new HashMap<>());
                                }

                                for (Phenotype evidence : disorder.getEvidences()) {
                                    disorderPhenotypeMap.get(disorder.getId()).put(evidence.getId(), evidence);
                                }
                            }
                        }
                    }
                }

                // Set the new disorder list
                List<Disorder> disorderList = new ArrayList<>(disorderMap.size());
                for (Disorder disorder : disorderMap.values()) {
                    List<Phenotype> phenotypeList = null;
                    if (disorderPhenotypeMap.get(disorder.getId()) != null) {
                        phenotypeList = new ArrayList<>(disorderPhenotypeMap.get(disorder.getId()).values());
                    }
                    disorder.setEvidences(phenotypeList);
                    disorderList.add(disorder);
                }

                family.setDisorders(disorderList);
            }
        } else {
            // We need to validate the disorders are actually correct
            if (family.getMembers() == null || family.getMembers().isEmpty()) {
                throw new CatalogException("Missing family members");
            }

            // Validate all the disorders are contained in at least one individual
            Set<String> memberDisorders = new HashSet<>();
            for (Individual individual : family.getMembers()) {
                if (ListUtils.isNotEmpty(individual.getDisorders())) {
                    memberDisorders.addAll(individual.getDisorders().stream().map(Disorder::getId).collect(Collectors.toSet()));
                }
            }
            Set<String> familyDisorders = family.getDisorders().stream().map(Disorder::getId).collect(Collectors.toSet());
            if (!familyDisorders.containsAll(memberDisorders)) {
                throw new CatalogException("Some of the disorders are not present in any member of the family");
            }
        }
    }

}
