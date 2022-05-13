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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.*;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
            FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final QueryOptions INCLUDE_FAMILY_MEMBERS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.UUID.key(),
            FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
            FamilyDBAdaptor.QueryParams.MEMBERS.key()));

    FamilyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.FAMILY;
    }

    @Override
    InternalGetDataResult<Family> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                              String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing family entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(FamilyDBAdaptor.QueryParams.VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one family allowed when requesting multiple versions");
        }

        FamilyDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Family> familyDataResult = familyDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        Function<Family, String> familyStringFunction = Family::getId;
        if (idQueryParam.equals(FamilyDBAdaptor.QueryParams.UUID)) {
            familyStringFunction = Family::getUuid;
        }

        if (ignoreException || familyDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, familyStringFunction, familyDataResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Family> resultsNoCheck = familyDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == familyDataResult.getNumResults()) {
            throw CatalogException.notFound("families",
                    getMissingFields(uniqueList, familyDataResult.getResults(), familyStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the families.");
        }
    }

    FamilyDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        FamilyDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            FamilyDBAdaptor.QueryParams param = FamilyDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = FamilyDBAdaptor.QueryParams.UUID;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        return idQueryParam;
    }

    private OpenCGAResult<Family> getFamily(long studyUid, String familyUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FamilyDBAdaptor.QueryParams.UUID.key(), familyUuid);
        return familyDBAdaptor.get(query, options);
    }

    @Override
    public DBIterator<Family> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        ParamUtils.checkObj(token, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        Query finalQuery = new Query(query);
        fixQueryObject(study, finalQuery, token);
        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return familyDBAdaptor.iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<Family> create(String studyStr, Family family, QueryOptions options, String token) throws CatalogException {
        return create(studyStr, family, null, options, token);
    }

    public OpenCGAResult<Family> create(String studyStr, Family family, List<String> members, QueryOptions options, String token)
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
            ParamUtils.checkIdentifier(family.getId(), "id");
            family.setName(ParamUtils.defaultObject(family.getName(), family.getId()));
            family.setMembers(ParamUtils.defaultObject(family.getMembers(), Collections.emptyList()));
            family.setPhenotypes(ParamUtils.defaultObject(family.getPhenotypes(), Collections.emptyList()));
            family.setDisorders(ParamUtils.defaultObject(family.getDisorders(), Collections.emptyList()));
            family.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(family.getCreationDate(),
                    FamilyDBAdaptor.QueryParams.CREATION_DATE.key()));
            family.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(family.getModificationDate(),
                    FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
            family.setDescription(ParamUtils.defaultString(family.getDescription(), ""));
            family.setInternal(FamilyInternal.init());
            family.setAnnotationSets(ParamUtils.defaultObject(family.getAnnotationSets(), Collections.emptyList()));
            family.setStatus(ParamUtils.defaultObject(family.getStatus(), Status::new));
            family.setQualityControl(ParamUtils.defaultObject(family.getQualityControl(), FamilyQualityControl::new));
            family.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
            family.setVersion(1);
            family.setAttributes(ParamUtils.defaultObject(family.getAttributes(), Collections.emptyMap()));

            // Check the id is not in use
            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FamilyDBAdaptor.QueryParams.ID.key(), family.getId());
            if (familyDBAdaptor.count(query).getNumMatches() > 0) {
                throw new CatalogException("Family '" + family.getId() + "' already exists.");
            }

            validateNewAnnotationSets(study.getVariableSets(), family.getAnnotationSets());

            List<Individual> existingMembers = autoCompleteFamilyMembers(study, family, members, userId);
            validateFamily(family, existingMembers);
            validatePhenotypes(family, existingMembers);
            validateDisorders(family, existingMembers);

            options = ParamUtils.defaultObject(options, QueryOptions::new);
            family.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FAMILY));

            OpenCGAResult<Family> insert = familyDBAdaptor.insert(study.getUid(), family, existingMembers, study.getVariableSets(),
                    options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated family
                OpenCGAResult<Family> queryResult = getFamily(study.getUid(), family.getUuid(), options);
                insert.setResults(queryResult.getResults());
            }

            auditManager.auditCreate(userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.FAMILY, family.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Family> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        Query finalQuery = new Query(query);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            fixQueryObject(study, finalQuery, token);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            AnnotationUtils.fixQueryOptionAnnotation(options);

            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Family> queryResult = familyDBAdaptor.get(study.getUid(), finalQuery, options, userId);

            auditManager.auditSearch(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", new Query(query))
                .append("query", new Query(query))
                .append("token", token);
        try {
            FamilyDBAdaptor.QueryParams param = FamilyDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(study, query, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);

            query.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = familyDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void fixQueryObject(Study study, Query query, String sessionId) throws CatalogException {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.FAMILY_INTERNAL_STATUS_PARAM, FamilyDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());
        changeQueryId(query, ParamConstants.FAMILY_STATUS_PARAM, FamilyDBAdaptor.QueryParams.STATUS_ID.key());

        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MEMBERS.key()))
                && StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()))) {
            throw new CatalogException("Cannot look for samples and members at the same time");
        }

        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        // We change the MEMBERS parameters for MEMBER_UID which is what the DBAdaptor understands
        if (query.containsKey(FamilyDBAdaptor.QueryParams.MEMBERS.key())) {
            String userId = userManager.getUserId(sessionId);

            List<Individual> memberList = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    query.getAsStringList(FamilyDBAdaptor.QueryParams.MEMBERS.key()), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId,
                    true).getResults();
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
        if (query.containsKey(IndividualDBAdaptor.QueryParams.SAMPLES.key())) {
            if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()))) {
                Query newQuery = new Query()
                        .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                                query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()));
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.UID.key());
                OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager()
                        .search(study.getFqn(), newQuery, options, sessionId);

                if (individualResult.getNumResults() == 0) {
                    // Add -1 to query so no results are obtained
                    query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), -1);
                } else {
                    // Look for the individuals containing those samples
                    query.put(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(),
                            individualResult.getResults().stream().map(Individual::getUid).collect(Collectors.toList()));
                }
            }

            query.remove(IndividualDBAdaptor.QueryParams.SAMPLES.key());
        }
    }

    public OpenCGAResult<Family> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        Query finalQuery = new Query(query);

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, token);

            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = familyDBAdaptor.count(finalQuery, userId);

            auditManager.auditCount(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> familyIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, familyIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> familyIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("familyIds", familyIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.FAMILY, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        OpenCGAResult result = OpenCGAResult.empty();
        for (String id : familyIds) {
            String familyId = id;
            String familyUuid = "";

            try {
                OpenCGAResult<Family> internalResult = internalGet(study.getUid(), id, INCLUDE_FAMILY_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Family '" + id + "' not found");
                }

                Family family = internalResult.first();
                // We set the proper values for the audit
                familyId = family.getId();
                familyUuid = family.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkFamilyPermission(study.getUid(), family.getUid(), userId,
                            FamilyAclEntry.FamilyPermissions.DELETE);
                }

                // Check family can be deleted
                checkCanBeDeleted(study, family);

                // Delete the family
                result.append(familyDBAdaptor.delete(family));

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, familyId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot delete family {}: {}", familyId, e.getMessage(), e);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FAMILY, familyId, familyUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    private void checkCanBeDeleted(Study study, Family family) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(), family.getUid());
        OpenCGAResult<ClinicalAnalysis> result = clinicalDBAdaptor.get(query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS);
        if (result.getNumResults() > 0) {
            String clinicalIds = result.getResults().stream().map(ClinicalAnalysis::getId).collect(Collectors.joining(", "));
            throw new CatalogException("Family {" + family.getId() + "} in use in Clinical Analyses: {" + clinicalIds + "}");
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        OpenCGAResult result = OpenCGAResult.empty();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
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

            iterator = familyDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_FAMILY_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.FAMILY, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            Family family = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkFamilyPermission(study.getUid(), family.getUid(), userId,
                            FamilyAclEntry.FamilyPermissions.DELETE);
                }

                // TODO: Check if the family is used in a clinical analysis. At this point, it can be deleted no matter what.

                // Delete the family
                result.append(familyDBAdaptor.delete(family));

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete family " + family.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, family.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg, e);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId) throws
            CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
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

        OpenCGAResult queryResult = familyDBAdaptor.groupBy(finalQuery, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public OpenCGAResult<Family> updateAnnotationSet(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                     ParamUtils.BasicUpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        FamilyUpdateParams updateParams = new FamilyUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, familyStr, updateParams, options, token);
    }

    public OpenCGAResult<Family> addAnnotationSet(String studyStr, String familyStr, AnnotationSet annotationSet, QueryOptions options,
                                                  String token) throws CatalogException {
        return addAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Family> addAnnotationSets(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.BasicUpdateAction.ADD, options, token);
    }

    public OpenCGAResult<Family> setAnnotationSet(String studyStr, String familyStr, AnnotationSet annotationSet, QueryOptions options,
                                                  String token) throws CatalogException {
        return setAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Family> setAnnotationSets(String studyStr, String familyStr, List<AnnotationSet> annotationSetList,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.BasicUpdateAction.SET, options, token);
    }

    public OpenCGAResult<Family> removeAnnotationSet(String studyStr, String familyStr, String annotationSetId, QueryOptions options,
                                                     String token) throws CatalogException {
        return removeAnnotationSets(studyStr, familyStr, Collections.singletonList(annotationSetId), options, token);
    }

    public OpenCGAResult<Family> removeAnnotationSets(String studyStr, String familyStr, List<String> annotationSetIdList,
                                                      QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, familyStr, annotationSetList, ParamUtils.BasicUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Family> updateAnnotations(String studyStr, String familyStr, String annotationSetId,
                                                   Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                   QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            throw new CatalogException("Missing array of annotations.");
        }
        FamilyUpdateParams updateParams = new FamilyUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, "", annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, familyStr, updateParams, options, token);
    }

    public OpenCGAResult<Family> removeAnnotations(String studyStr, String familyStr, String annotationSetId,
                                                   List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, familyStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Family> resetAnnotations(String studyStr, String familyStr, String annotationSetId, List<String> annotations,
                                                  QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, familyStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    public OpenCGAResult<Family> update(String studyStr, Query query, FamilyUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Family> update(String studyStr, Query query, FamilyUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse FamilyUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        DBIterator<Family> iterator;
        try {
            fixQueryObject(study, finalQuery, token);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);

            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = familyDBAdaptor.iterator(study.getUid(), finalQuery, QueryOptions.empty(), userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.FAMILY, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Family> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Family family = iterator.next();
            try {
                OpenCGAResult<Family> queryResult = update(study, family, updateParams, options, userId);
                result.append(queryResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, family.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot update family {}: {}", family.getId(), e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Family> update(String studyStr, String familyId, FamilyUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse FamilyUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("familyId", familyId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Family> result = OpenCGAResult.empty();
        String familyUuid = "";

        try {
            OpenCGAResult<Family> internalResult = internalGet(study.getUid(), familyId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Family '" + familyId + "' not found");
            }
            Family family = internalResult.first();

            // We set the proper values for the audit
            familyId = family.getId();
            familyUuid = family.getUuid();

            OpenCGAResult<Family> updateResult = update(study, family, updateParams, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(operationId, userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, familyId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Cannot update family {}: {}", familyId, e.getMessage());
            auditManager.auditUpdate(operationId, userId, Enums.Resource.FAMILY, familyId, familyUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    /**
     * Update families from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param familyIds    List of family ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Family> update(String studyStr, List<String> familyIds, FamilyUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, familyIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Family> update(String studyStr, List<String> familyIds, FamilyUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse FamilyUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("familyIds", familyIds)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Family> result = OpenCGAResult.empty();
        for (String id : familyIds) {
            String familyId = id;
            String familyUuid = "";

            try {
                OpenCGAResult<Family> internalResult = internalGet(study.getUid(), familyId, QueryOptions.empty(), userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Family '" + id + "' not found");
                }
                Family family = internalResult.first();

                // We set the proper values for the audit
                familyId = family.getId();
                familyUuid = family.getUuid();

                OpenCGAResult<Family> updateResult = update(study, family, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.FAMILY, family.getId(), family.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot update family {}: {}", familyId, e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.FAMILY, familyId, familyUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult<Family> update(Study study, Family family, FamilyUpdateParams updateParams, QueryOptions options, String userId)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse FamilyUpdateParams object: " + e.getMessage(), e);
            }
        }

        if (StringUtils.isNotEmpty(parameters.getString(FamilyDBAdaptor.QueryParams.CREATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(FamilyDBAdaptor.QueryParams.CREATION_DATE.key()),
                    FamilyDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (StringUtils.isNotEmpty(parameters.getString(FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key()),
                    FamilyDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

        // If there is nothing to update, we fail
        if (parameters.isEmpty() && !options.getBoolean(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, false)) {
            ParamUtils.checkUpdateParametersMap(parameters);
        }

        if (parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
            if (!actionMap.containsKey(AnnotationSetManager.ANNOTATION_SETS)
                    && !actionMap.containsKey(AnnotationSetManager.ANNOTATIONS)) {
                logger.warn("Assuming the user wants to add the list of annotation sets provided");
                actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.BasicUpdateAction.ADD);
                options.put(Constants.ACTIONS, actionMap);
            }
        }

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (updateParams != null && updateParams.getAnnotationSets() != null) {
            authorizationManager.checkFamilyPermission(study.getUid(), family.getUid(), userId,
                    FamilyAclEntry.FamilyPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkFamilyPermission(study.getUid(), family.getUid(), userId,
                    FamilyAclEntry.FamilyPermissions.WRITE);
        }

        if (updateParams != null && StringUtils.isNotEmpty(updateParams.getId())) {
            ParamUtils.checkIdentifier(updateParams.getId(), FamilyDBAdaptor.QueryParams.ID.key());
        }

        boolean updateRoles = options.getBoolean(ParamConstants.FAMILY_UPDATE_ROLES_PARAM);
        if (updateRoles || (updateParams != null && CollectionUtils.isNotEmpty(updateParams.getMembers()))) {
            Family tmpFamily = new Family();
            if (updateParams != null && CollectionUtils.isNotEmpty(updateParams.getMembers())) {
                // We obtain the members from catalog
                List<String> memberIds = new ArrayList<>(updateParams.getMembers().size());
                for (IndividualReferenceParam member : updateParams.getMembers()) {
                    if (StringUtils.isNotEmpty(member.getId())) {
                        memberIds.add(member.getId());
                    } else if (StringUtils.isNotEmpty(member.getUuid())) {
                        memberIds.add(member.getUuid());
                    } else {
                        throw new CatalogException("Found members without any id.");
                    }
                }
                List<Individual> updatedMembers = autoCompleteFamilyMembers(study, tmpFamily, memberIds, userId);
                tmpFamily.setMembers(updatedMembers);
            } else {
                // We use the list of members from the stored family
                tmpFamily.setMembers(family.getMembers());
            }

            validateFamily(tmpFamily, null);
            validatePhenotypes(tmpFamily, null);
            validateDisorders(tmpFamily, null);

            ObjectMap tmpParams;
            try {
                ObjectMapper objectMapper = getDefaultObjectMapper();
                tmpParams = new ObjectMap(objectMapper.writeValueAsString(tmpFamily));
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

        checkUpdateAnnotations(study, family, parameters, options, VariableSet.AnnotableDataModels.FAMILY, familyDBAdaptor,
                userId);

        OpenCGAResult<Family> update = familyDBAdaptor.update(family.getUid(), parameters, study.getVariableSets(), options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated family
            OpenCGAResult<Family> result = familyDBAdaptor.get(study.getUid(),
                    new Query(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid()), options, userId);
            update.setResults(result.getResults());
        }
        return update;
    }

    public Map<String, List<String>> calculateFamilyGenotypes(String studyStr, String clinicalAnalysisId, String familyId,
                                                              ClinicalProperty.ModeOfInheritance moi, String disorderId,
                                                              Penetrance penetrance, String token) throws CatalogException {
        Pedigree pedigree;
        Disorder disorder = null;

        if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
            OpenCGAResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().get(studyStr,
                    clinicalAnalysisId, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                            ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
                            ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key())), token);
            if (clinicalAnalysisDataResult.getNumResults() == 0) {
                throw new CatalogException("Clinical analysis " + clinicalAnalysisId + " not found");
            }

            disorder = clinicalAnalysisDataResult.first().getDisorder();
            pedigree = getPedigreeFromFamily(clinicalAnalysisDataResult.first().getFamily(),
                    clinicalAnalysisDataResult.first().getProband().getId());

        } else if (StringUtils.isNotEmpty(familyId) && StringUtils.isNotEmpty(disorderId)) {
            OpenCGAResult<Family> familyDataResult = get(studyStr, familyId, QueryOptions.empty(), token);

            if (familyDataResult.getNumResults() == 0) {
                throw new CatalogException("Family " + familyId + " not found");
            }

            for (Disorder tmpDisorder : familyDataResult.first().getDisorders()) {
                if (tmpDisorder.getId().equals(disorderId)) {
                    disorder = tmpDisorder;
                    break;
                }
            }
            if (disorder == null) {
                throw new CatalogException("Disorder " + disorderId + " not found in any member of the family");
            }

            pedigree = getPedigreeFromFamily(familyDataResult.first(), null);
        } else {
            throw new CatalogException("Missing 'clinicalAnalysis' or ('family' and 'disorderId') parameters");
        }

        switch (moi) {
            case AUTOSOMAL_DOMINANT:
                return ModeOfInheritance.dominant(pedigree, disorder, penetrance);
            case AUTOSOMAL_RECESSIVE:
                return ModeOfInheritance.recessive(pedigree, disorder, penetrance);
            case X_LINKED_RECESSIVE:
                return ModeOfInheritance.xLinked(pedigree, disorder, false, penetrance);
            case X_LINKED_DOMINANT:
                return ModeOfInheritance.xLinked(pedigree, disorder, true, penetrance);
            case Y_LINKED:
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
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> familyList, String member, boolean ignoreException,
                                                            String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("familyList", familyList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);

        try {
            OpenCGAResult<Map<String, List<String>>> familyAclList = OpenCGAResult.empty();
            InternalGetDataResult<Family> familyDataResult = internalGet(study.getUid(), familyList, INCLUDE_FAMILY_IDS, user,
                    ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (familyDataResult.getMissing() != null) {
                missingMap = familyDataResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String familyId : familyList) {
                if (!missingMap.containsKey(familyId)) {
                    Family family = familyDataResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> allFamilyAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allFamilyAcls = authorizationManager.getFamilyAcl(study.getUid(), family.getUid(), user, member);
                        } else {
                            allFamilyAcls = authorizationManager.getAllFamilyAcls(study.getUid(), family.getUid(), user);
                        }
                        familyAclList.append(allFamilyAcls);

                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FAMILY, family.getId(),
                                family.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FAMILY, family.getId(),
                                family.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());

                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, familyId, missingMap.get(familyId).getErrorMsg());
                            familyAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, familyId, missingMap.get(familyId).getErrorMsg());
                    familyAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FAMILY, familyId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(familyId).getErrorMsg())), new ObjectMap());
                }
            }
            return familyAclList;
        } catch (CatalogException e) {
            for (String familyId : familyList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FAMILY, familyId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, FamilyAclParams aclParams, String memberList,
                                                              ParamUtils.AclAction action, String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("aclParams", aclParams)
                .append("memberList", memberList)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        List<Family> familyList = null;
        try {
            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

            int count = 0;
            count += aclParams.getFamily() != null && !aclParams.getFamily().isEmpty() ? 1 : 0;
            count += aclParams.getIndividual() != null && !aclParams.getIndividual().isEmpty() ? 1 : 0;
            count += aclParams.getSample() != null && !aclParams.getSample().isEmpty() ? 1 : 0;

            if (count > 1) {
                throw new CatalogException("Update ACL: Only one of these parameters are allowed: family, individual or sample per query.");
            } else if (count == 0) {
                throw new CatalogException("Update ACL: At least one of these parameters should be provided: family, individual or sample");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, FamilyAclEntry.FamilyPermissions::valueOf);
            }

            String individualStr = aclParams.getIndividual();
            if (StringUtils.isNotEmpty(aclParams.getSample())) {
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyId, aclParams.getSample(),
                        new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()), token);
                individualStr = sampleResult.getResults().stream().map(Sample::getIndividualId).collect(Collectors.joining(","));
            }

            if (StringUtils.isNotEmpty(individualStr)) {
                familyList = catalogManager.getFamilyManager().search(studyId,
                                new Query(FamilyDBAdaptor.QueryParams.MEMBERS.key(), individualStr), FamilyManager.INCLUDE_FAMILY_MEMBERS,
                                token)
                        .getResults();
            } else if (StringUtils.isNotEmpty(aclParams.getFamily())) {
                familyList = catalogManager.getFamilyManager().get(studyId, Arrays.asList(aclParams.getFamily().split(",")),
                        FamilyManager.INCLUDE_FAMILY_MEMBERS, token).getResults();
            }

            if (familyList == null || familyList.isEmpty()) {
                throw new CatalogException("No families found to set permissions");
            }

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(study.getUid(), members);

            List<AuthorizationManager.CatalogAclParams> aclParamsList = new LinkedList<>();
            List<Long> familyUids = familyList.stream().map(Family::getUid).collect(Collectors.toList());
            aclParamsList.add(new AuthorizationManager.CatalogAclParams(familyUids, permissions, Enums.Resource.FAMILY));

            if (aclParams.getPropagate() == FamilyAclParams.Propagate.YES
                    || aclParams.getPropagate() == FamilyAclParams.Propagate.YES_AND_VARIANT_VIEW) {
                // Obtain the list of members and samples
                Set<Long> memberUids = new HashSet<>();
                Set<Long> sampleUids = new HashSet<>();
                for (Family family : familyList) {
                    if (family.getMembers() != null) {
                        for (Individual member : family.getMembers()) {
                            memberUids.add(member.getUid());
                            if (member.getSamples() != null) {
                                for (Sample sample : member.getSamples()) {
                                    sampleUids.add(sample.getUid());
                                }
                            }
                        }
                    }
                }
                if (!memberUids.isEmpty()) {
                    // Add permissions to those individuals
                    aclParamsList.add(new AuthorizationManager.CatalogAclParams(new ArrayList<>(memberUids), permissions,
                            Enums.Resource.INDIVIDUAL));
                }

                if (!sampleUids.isEmpty()) {
                    // Add permissions to those samples
                    List<String> samplePermissions = new ArrayList<>(permissions);
                    if (aclParams.getPropagate() == FamilyAclParams.Propagate.YES_AND_VARIANT_VIEW) {
                        samplePermissions.add(SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name());
                    }
                    aclParamsList.add(new AuthorizationManager.CatalogAclParams(new ArrayList<>(sampleUids), samplePermissions,
                            Enums.Resource.SAMPLE));
                }
            }

            OpenCGAResult<Map<String, List<String>>> aclResults;
            switch (action) {
                case SET:
                    aclResults = authorizationManager.setAcls(study.getUid(), members, aclParamsList);
                    break;
                case ADD:
                    aclResults = authorizationManager.addAcls(study.getUid(), members, aclParamsList);
                    break;
                case REMOVE:
                    aclResults = authorizationManager.removeAcls(members, aclParamsList);
                    break;
                case RESET:
                    for (AuthorizationManager.CatalogAclParams auxAclParams : aclParamsList) {
                        auxAclParams.setPermissions(null);
                    }
                    aclResults = authorizationManager.removeAcls(members, aclParamsList);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (Family family : familyList) {
                auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.FAMILY, family.getId(),
                        family.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return aclResults;
        } catch (CatalogException e) {
            if (familyList != null) {
                for (Family family : familyList) {
                    auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.FAMILY, family.getId(), "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

    public DataResult<FacetField> facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
            throws CatalogException {
        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        // We need to add variableSets and groups to avoid additional queries as it will be used in the catalogSolrManager
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key())));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("defaultStats", defaultStats)
                .append("token", token);

        try {
            if (defaultStats || StringUtils.isEmpty(options.getString(QueryOptions.FACET))) {
                String facet = options.getString(QueryOptions.FACET);
                options.put(QueryOptions.FACET, StringUtils.isNotEmpty(facet) ? defaultFacet + ";" + facet : defaultFacet);
            }

            AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

            try (CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager)) {
                DataResult<FacetField> result = catalogSolrManager.facetedQuery(study, CatalogSolrManager.FAMILY_SOLR_COLLECTION, query,
                        options, userId);
                auditManager.auditFacet(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                return result;
            }
        } catch (CatalogException e) {
            auditManager.auditFacet(userId, Enums.Resource.FAMILY, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage())));
            throw e;
        }
    }

    public static Pedigree getPedigreeFromFamily(Family family, String probandId) {
        List<Individual> members = family.getMembers();
        Map<String, Member> individualMap = new HashMap<>();

        // Parse all the individuals
        for (Individual member : members) {
            Member individual = new Member(member.getId(), member.getName(), null, null, null, member.getSex(), member.getLifeStatus(),
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
     * Returns the list of already existing members that will be associated to the family.
     *
     * @param study   study.
     * @param family  family object.
     * @param members Already existing members.
     * @param userId  user id.
     * @return list of already existing members that will be associated to the family.
     * @throws CatalogException if there is any kind of error.
     */
    private List<Individual> autoCompleteFamilyMembers(Study study, Family family, List<String> members, String userId)
            throws CatalogException {
        if (family.getMembers() != null && !family.getMembers().isEmpty()) {
            List<Individual> memberList = new ArrayList<>();
            // Check the user can create new individuals
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);

            // Validate the individuals can be created and are valid
            for (Individual individual : family.getMembers()) {
                catalogManager.getIndividualManager().validateNewIndividual(study, individual, null, userId, false);
                memberList.add(individual);
            }

            family.setMembers(memberList);
        }

        if (members != null && !members.isEmpty()) {
            // We remove any possible duplicate
            ArrayList<String> deduplicatedMemberIds = new ArrayList<>(new HashSet<>(members));

            InternalGetDataResult<Individual> individualDataResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    deduplicatedMemberIds, IndividualManager.INCLUDE_INDIVIDUAL_DISORDERS_PHENOTYPES, userId, false);

            return individualDataResult.getResults();
        }

        return Collections.emptyList();
    }

    private void validateFamily(Family family, List<Individual> existingMembers) throws CatalogException {
        List<Individual> members = new LinkedList<>();
        if (family.getMembers() != null) {
            members.addAll(family.getMembers());
        }
        if (existingMembers != null) {
            members.addAll(existingMembers);
        }
        if (members.isEmpty()) {
            return;
        }

        Map<String, Individual> membersMap = new HashMap<>();       // individualName|individualId: Individual
        Map<String, List<Individual>> parentsMap = new HashMap<>(); // motherName||F---fatherName||M: List<children>
        Set<String> noParentsSet = new HashSet<>();                 // Set with individuals without parents

        // 1. Fill in the objects initialised above
        for (Individual individual : members) {
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
                noParentsSet.add(individual.getId());
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
                SexOntologyTermAnnotation sexTerm = splitNameSex[1].equals("F")
                        ? SexOntologyTermAnnotation.initFemale()
                        : SexOntologyTermAnnotation.initMale();
                IndividualProperty.Sex sex = sexTerm.getSex();

                if (!membersMap.containsKey(name)) {
                    throw new CatalogException("The parent " + name + " is not present in the members list");
                } else {
                    // Check if the sex is correct
                    IndividualProperty.Sex sex1 = membersMap.get(name).getSex() != null
                            ? membersMap.get(name).getSex().getSex()
                            : IndividualProperty.Sex.UNKNOWN;
                    if (sex1 != null && sex1 != sex && sex1 != IndividualProperty.Sex.UNKNOWN) {
                        throw new CatalogException("Sex of parent " + name + " is incorrect or the relationship is incorrect. In "
                                + "principle, it should be " + sexTerm);
                    }
                    membersMap.get(name).setSex(sexTerm);

                    // We attempt to remove the individual from the noParentsSet
                    noParentsSet.remove(membersMap.get(name).getId());
                }
            }
        }

        // FIXME Pedro: this is a quick fix to allow create families without the parents, this needs to be reviewed.
        if (noParentsSet.size() > 0) {
//            throw new CatalogException("Some members that are not related to any other have been found: "
//                    + noParentsSet.stream().map(Individual::getName).collect(Collectors.joining(", ")));
            logger.warn("Some members that are not related to any other have been found: {}", StringUtils.join(noParentsSet, ", "));
        }
    }

    private void validatePhenotypes(Family family, List<Individual> existingMembers) throws CatalogException {
        List<Individual> members = new LinkedList<>();
        if (family.getMembers() != null) {
            members.addAll(family.getMembers());
        }
        if (existingMembers != null) {
            members.addAll(existingMembers);
        }
        if (family.getPhenotypes() == null || family.getPhenotypes().isEmpty()) {
            if (CollectionUtils.isNotEmpty(members)) {
                Map<String, Phenotype> phenotypeMap = new HashMap<>();

                for (Individual member : members) {
                    if (CollectionUtils.isNotEmpty(member.getPhenotypes())) {
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
            if (members.isEmpty()) {
                throw new CatalogException("Missing family members");
            }

            // Validate all the phenotypes are contained in at least one individual
            Set<String> memberPhenotypes = new HashSet<>();
            for (Individual individual : members) {
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

    private void validateDisorders(Family family, List<Individual> existingMembers) throws CatalogException {
        List<Individual> members = new LinkedList<>();
        if (family.getMembers() != null) {
            members.addAll(family.getMembers());
        }
        if (existingMembers != null) {
            members.addAll(existingMembers);
        }
        if (CollectionUtils.isEmpty(family.getDisorders())) {
            if (CollectionUtils.isNotEmpty(members)) {
                // Obtain the union of all disorders
                Map<String, Disorder> disorderMap = new HashMap<>();
                Map<String, Map<String, Phenotype>> disorderPhenotypeMap = new HashMap<>();

                for (Individual member : members) {
                    if (CollectionUtils.isNotEmpty(member.getDisorders())) {
                        for (Disorder disorder : member.getDisorders()) {
                            disorderMap.put(disorder.getId(), disorder);

                            if (CollectionUtils.isNotEmpty(disorder.getEvidences())) {
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
            if (members.isEmpty()) {
                throw new CatalogException("Missing family members");
            }

            // Validate all the disorders are contained in at least one individual
            Set<String> memberDisorders = new HashSet<>();
            for (Individual individual : members) {
                if (CollectionUtils.isNotEmpty(individual.getDisorders())) {
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
