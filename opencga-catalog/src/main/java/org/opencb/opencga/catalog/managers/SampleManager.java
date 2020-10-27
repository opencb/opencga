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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
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
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileIndex;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleManager extends AnnotationSetManager<Sample> {

    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    private final String defaultFacet = "creationYear>>creationMonth;status;phenotypes;somatic";

    public static final QueryOptions INCLUDE_SAMPLE_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.UUID.key(),
            SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.STUDY_UID.key()));

    SampleManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.SAMPLE;
    }

//    @Override
//    OpenCGAResult<Sample> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
//            throws CatalogException {
//        ParamUtils.checkIsSingleID(entry);
//        Query queryCopy = query == null ? new Query() : new Query(query);
//        queryCopy.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
//
//        if (UuidUtils.isOpenCgaUuid(entry)) {
//            queryCopy.put(SampleDBAdaptor.QueryParams.UUID.key(), entry);
//        } else {
//            queryCopy.put(SampleDBAdaptor.QueryParams.ID.key(), entry);
//        }
//
//        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
////        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
////               SampleDBAdaptor.QueryParams.UUID.key(), SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.STUDY_UID.key(),
////               SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.RELEASE.key(), SampleDBAdaptor.QueryParams.VERSION.key(),
////                SampleDBAdaptor.QueryParams.STATUS.key()));
//        OpenCGAResult<Sample> sampleDataResult = sampleDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
//        if (sampleDataResult.getNumResults() == 0) {
//            sampleDataResult = sampleDBAdaptor.get(queryCopy, queryOptions);
//            if (sampleDataResult.getNumResults() == 0) {
//                throw new CatalogException("Sample " + entry + " not found");
//            } else {
//                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the sample " + entry);
//            }
//        } else if (sampleDataResult.getNumResults() > 1 && !queryCopy.getBoolean(Constants.ALL_VERSIONS)) {
//            throw new CatalogException("More than one sample found based on " + entry);
//        } else {
//            return sampleDataResult;
//        }
//    }

    @Override
    InternalGetDataResult<Sample> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                              String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing sample entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(SampleDBAdaptor.QueryParams.VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one sample allowed when requesting multiple versions");
        }

        Function<Sample, String> sampleStringFunction = Sample::getId;
        SampleDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            SampleDBAdaptor.QueryParams param = SampleDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = SampleDBAdaptor.QueryParams.UUID;
                sampleStringFunction = Sample::getUuid;
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

        OpenCGAResult<Sample> sampleDataResult = sampleDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        if (ignoreException || sampleDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, sampleStringFunction, sampleDataResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Sample> resultsNoCheck = sampleDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == sampleDataResult.getNumResults()) {
            throw CatalogException.notFound("samples", getMissingFields(uniqueList, sampleDataResult.getResults(), sampleStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the samples.");
        }
    }

    private OpenCGAResult<Sample> getSample(long studyUid, String sampleUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.UUID.key(), sampleUuid);
        return sampleDBAdaptor.get(query, options);
    }

    void validateNewSample(Study study, Sample sample, String userId) throws CatalogException {
        ParamUtils.checkAlias(sample.getId(), "id");

        // Check the id is not in use
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(SampleDBAdaptor.QueryParams.ID.key(), sample.getId());
        if (sampleDBAdaptor.count(query).getNumMatches() > 0) {
            throw new CatalogException("Sample '" + sample.getId() + "' already exists.");
        }

        if (StringUtils.isNotEmpty(sample.getIndividualId())) {
            // Check individual exists
            OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    sample.getIndividualId(), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
            if (individualDataResult.getNumResults() == 0) {
                throw new CatalogException("Individual '" + sample.getIndividualId() + "' not found.");
            }

            // Just in case the user provided a uuid or other kind of individual identifier, we set again the id value
            sample.setIndividualId(individualDataResult.first().getId());
        }

        sample.setProcessing(ParamUtils.defaultObject(sample.getProcessing(), SampleProcessing::new));
        sample.setCollection(ParamUtils.defaultObject(sample.getCollection(), SampleCollection::new));
        sample.setQualityControl(ParamUtils.defaultObject(sample.getQualityControl(), SampleQualityControl::new));
        sample.setCreationDate(ParamUtils.defaultString(sample.getCreationDate(), TimeUtils.getTime()));
        sample.setModificationDate(TimeUtils.getTime());
        sample.setDescription(ParamUtils.defaultString(sample.getDescription(), ""));
        sample.setPhenotypes(ParamUtils.defaultObject(sample.getPhenotypes(), Collections.emptyList()));

        sample.setIndividualId(ParamUtils.defaultObject(sample.getIndividualId(), ""));
        sample.setFileIds(ParamUtils.defaultObject(sample.getFileIds(), Collections.emptyList()));

        sample.setStatus(ParamUtils.defaultObject(sample.getStatus(), CustomStatus::new));
        sample.setInternal(ParamUtils.defaultObject(sample.getInternal(), SampleInternal::new));
        sample.getInternal().setStatus(new Status());
        sample.setAttributes(ParamUtils.defaultObject(sample.getAttributes(), Collections.emptyMap()));

        sample.setAnnotationSets(ParamUtils.defaultObject(sample.getAnnotationSets(), Collections.emptyList()));

        sample.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE));
        sample.setRelease(studyManager.getCurrentRelease(study));
        sample.setVersion(1);

        validateNewAnnotationSets(study.getVariableSets(), sample.getAnnotationSets());
    }

    @Override
    public OpenCGAResult<Sample> create(String studyStr, Sample sample, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("sample", sample)
                .append("options", options)
                .append("token", token);
        try {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_SAMPLES);

            validateNewSample(study, sample, userId);

            // We create the sample
            sampleDBAdaptor.insert(study.getUid(), sample, study.getVariableSets(), options);
            OpenCGAResult<Sample> queryResult = getSample(study.getUid(), sample.getUuid(), options);
            auditManager.auditCreate(userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.SAMPLE, sample.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public DBIterator<Sample> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        Query finalQuery = new Query(query);
        fixQueryObject(study, finalQuery, userId);
        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return sampleDBAdaptor.iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<Sample> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            fixQueryObject(study, query, userId);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);
            AnnotationUtils.fixQueryOptionAnnotation(options);

            query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Sample> queryResult = sampleDBAdaptor.get(study.getUid(), query, options, userId);

            auditManager.auditSearch(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
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
            SampleDBAdaptor.QueryParams param = SampleDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(study, query, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);

            query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = sampleDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        super.fixQueryObject(query);
//        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
//        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
//            OpenCGAResult<Individual> queryResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
//                    query.getAsStringList(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId,
//                    false);
//            query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), queryResult.getResults().stream().map(Individual::getUid)
//                    .collect(Collectors.toList()));
//            query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
//        }
    }

    @Override
    public OpenCGAResult<Sample> count(String studyId, Query query, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        query = ParamUtils.defaultObject(query, Query::new);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);
            fixQueryObject(study, query, userId);

            query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = sampleDBAdaptor.count(query, userId);

            auditManager.auditCount(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> sampleIds, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, sampleIds, params, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> sampleIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        if (sampleIds == null || ListUtils.isEmpty(sampleIds)) {
            throw new CatalogException("Missing list of sample ids");
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("sampleIds", sampleIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationId, userId, Enums.Resource.SAMPLE, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult result = OpenCGAResult.empty();
        for (String id : sampleIds) {
            String sampleId = id;
            String sampleUuid = "";
            try {
                OpenCGAResult<Sample> internalResult = internalGet(study.getUid(), id, INCLUDE_SAMPLE_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Sample '" + id + "' not found");
                }
                Sample sample = internalResult.first();

                // We set the proper values for the audit
                sampleId = sample.getId();
                sampleUuid = sample.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                            SampleAclEntry.SamplePermissions.DELETE);
                }

                // Check if the sample can be deleted
                checkSampleCanBeDeleted(study.getUid(), sample, params.getBoolean(Constants.FORCE, false));

                result.append(sampleDBAdaptor.delete(sample));

                auditManager.auditDelete(operationId, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete sample " + sampleId + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, sampleId, e.getMessage());
                result.getEvents().add(event);

                logger.error(errorMsg);
                auditManager.auditDelete(operationId, userId, Enums.Resource.SAMPLE, sampleId, sampleUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, query, params, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        params = ParamUtils.defaultObject(params, ObjectMap::new);

        OpenCGAResult result = OpenCGAResult.empty();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the samples to be deleted
        DBIterator<Sample> iterator;
        try {
            // TODO: Propagation of delete to orphan files and cohorts need to be implemented in the dbAdaptor layer
//            if (StringUtils.isNotEmpty(params.getString(Constants.EMPTY_FILES_ACTION))) {
//                // Validate the action
//                String filesAction = params.getString(Constants.EMPTY_FILES_ACTION);
//                params.put(Constants.EMPTY_FILES_ACTION, filesAction.toUpperCase());
//                if (!"NONE".equals(filesAction) && !"TRASH".equals(filesAction) && !"DELETE".equals(filesAction)) {
//                    throw new CatalogException("Unrecognised " + Constants.EMPTY_FILES_ACTION + " value. Accepted actions are NONE,TRASH,"
//                            + " DELETE");
//                }
//            } else {
//                params.put(Constants.EMPTY_FILES_ACTION, "NONE");
//            }

            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = sampleDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_SAMPLE_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.SAMPLE, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            Sample sample = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                            SampleAclEntry.SamplePermissions.DELETE);
                }

                // Check if the sample can be deleted
                checkSampleCanBeDeleted(study.getUid(), sample, params.getBoolean(Constants.FORCE, false));

                result.append(sampleDBAdaptor.delete(sample));

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete sample " + sample.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, sample.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error(errorMsg);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Sample> updateAnnotationSet(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                  ParamUtils.UpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        SampleUpdateParams sampleUpdateParams = new SampleUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, sampleStr, sampleUpdateParams, options, token);
    }

    public OpenCGAResult<Sample> addAnnotationSet(String studyStr, String sampleStr, AnnotationSet annotationSet, QueryOptions options,
                                               String token) throws CatalogException {
        return addAnnotationSets(studyStr, sampleStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Sample> addAnnotationSets(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.UpdateAction.ADD, options, token);
    }

    public OpenCGAResult<Sample> setAnnotationSet(String studyStr, String sampleStr, AnnotationSet annotationSet, QueryOptions options,
                                               String token) throws CatalogException {
        return setAnnotationSets(studyStr, sampleStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Sample> setAnnotationSets(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.UpdateAction.SET, options, token);
    }

    public OpenCGAResult<Sample> removeAnnotationSet(String studyStr, String sampleStr, String annotationSetId, QueryOptions options,
                                                  String token) throws CatalogException {
        return removeAnnotationSets(studyStr, sampleStr, Collections.singletonList(annotationSetId), options, token);
    }

    public OpenCGAResult<Sample> removeAnnotationSets(String studyStr, String sampleStr, List<String> annotationSetIdList,
                                                   QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.UpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Sample> updateAnnotations(String studyStr, String sampleStr, String annotationSetId,
                                                   Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                   QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            throw new CatalogException("Missing array of annotations.");
        }
        SampleUpdateParams sampleUpdateParams = new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, null, annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, sampleStr, sampleUpdateParams, options, token);
    }

    public OpenCGAResult<Sample> removeAnnotations(String studyStr, String sampleStr, String annotationSetId, List<String> annotations,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, sampleStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Sample> resetAnnotations(String studyStr, String sampleStr, String annotationSetId, List<String> annotations,
                                               QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, sampleStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    private void checkSampleCanBeDeleted(long studyId, Sample sample, boolean force) throws CatalogException {
        // Look for files related with the sample
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sample.getId())
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        DBIterator<File> fileIterator = fileDBAdaptor.iterator(query, QueryOptions.empty());
        List<String> errorFiles = new ArrayList<>();
        while (fileIterator.hasNext()) {
            File file = fileIterator.next();
            if (force) {
                // Check index status
                if (file.getInternal().getIndex() != null && file.getInternal().getIndex().getStatus() != null
                        && !FileIndex.IndexStatus.NONE.equals(file.getInternal().getIndex().getStatus().getName())) {
                    errorFiles.add(file.getPath() + "(" + file.getUid() + ")");
                }
            } else {
                errorFiles.add(file.getPath() + "(" + file.getUid() + ")");
            }
        }
        if (!errorFiles.isEmpty()) {
            if (force) {
                throw new CatalogException("Associated files are used in storage: " + StringUtils.join(errorFiles, ", "));
            } else {
                throw new CatalogException("Sample associated to the files: " + StringUtils.join(errorFiles, ", "));
            }
        }

        // Look for cohorts containing the sample
        query = new Query()
                .append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        DBIterator<Cohort> cohortIterator = cohortDBAdaptor.iterator(query, QueryOptions.empty());
        List<String> errorCohorts = new ArrayList<>();
        boolean associatedToDefaultCohort = false;
        while (cohortIterator.hasNext()) {
            Cohort cohort = cohortIterator.next();
            if (force) {
                // Check it is not the default cohort
                if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
                    associatedToDefaultCohort = true;
                }

                // Check the status of the cohort
                if (cohort.getInternal().getStatus() != null
                        && CohortStatus.CALCULATING.equals(cohort.getInternal().getStatus().getName())) {
                    errorCohorts.add(cohort.getId() + "(" + cohort.getUid() + ")");
                }
            } else {
                errorCohorts.add(cohort.getId() + "(" + cohort.getUid() + ")");
            }
        }
        if (associatedToDefaultCohort) {
            throw new CatalogException("Sample in cohort " + StudyEntry.DEFAULT_COHORT);
        }
        if (!errorCohorts.isEmpty()) {
            if (force) {
                throw new CatalogException("Sample present in cohorts in the process of calculating the stats: "
                        + StringUtils.join(errorCohorts, ", "));
            } else {
                throw new CatalogException("Sample present in cohorts: " + StringUtils.join(errorCohorts, ", "));
            }
        }

        // Look for individuals containing the sample
        if (!force) {
            query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Individual> individualDataResult = individualDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(IndividualDBAdaptor.QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.ID.key())));
            if (individualDataResult.getNumResults() > 0) {
                throw new CatalogException("Sample from individual " + individualDataResult.first().getName() + "("
                        + individualDataResult.first().getUid() + ")");
            }
        }
    }

    public OpenCGAResult<Sample> update(String studyStr, Query query, SampleUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Sample> update(String studyStr, Query query, SampleUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse SampleUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        DBIterator<Sample> iterator;
        try {
            fixQueryObject(study, finalQuery, userId);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = sampleDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_SAMPLE_IDS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.SAMPLE, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Sample> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Sample sample = iterator.next();
            try {
                OpenCGAResult updateResult = update(study, sample, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, sample.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update sample {}: {}", sample.getId(), e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Sample> update(String studyStr, String sampleId, SampleUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse SampleUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("sampleId", sampleId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Sample> result = OpenCGAResult.empty();
        String sampleUuid = "";
        try {
            OpenCGAResult<Sample> internalResult = internalGet(study.getUid(), sampleId, INCLUDE_SAMPLE_IDS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Sample '" + sampleId + "' not found");
            }
            Sample sample = internalResult.first();

            // We set the proper values for the audit
            sampleId = sample.getId();
            sampleUuid = sample.getUuid();

            OpenCGAResult updateResult = update(study, sample, updateParams, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(operationId, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, sampleId, e.getMessage());
            result.getEvents().add(event);

            logger.error("Could not update sample {}: {}", sampleId, e.getMessage(), e);
            auditManager.auditUpdate(operationId, userId, Enums.Resource.SAMPLE, sampleId, sampleUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    /**
     * Update Sample from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sampleIds  List of Sample ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A OpenCGAResult with the objects updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Sample> update(String studyStr, List<String> sampleIds, SampleUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, sampleIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Sample> update(String studyStr, List<String> sampleIds, SampleUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse SampleUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("sampleIds", sampleIds)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Sample> result = OpenCGAResult.empty();
        for (String id : sampleIds) {
            String sampleId = id;
            String sampleUuid = "";

            try {
                OpenCGAResult<Sample> internalResult = internalGet(study.getUid(), id, INCLUDE_SAMPLE_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Sample '" + id + "' not found");
                }
                Sample sample = internalResult.first();

                // We set the proper values for the audit
                sampleId = sample.getId();
                sampleUuid = sample.getUuid();

                OpenCGAResult updateResult = update(study, sample, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, sampleId, e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update sample {}: {}", sampleId, e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.SAMPLE, sampleId, sampleUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult update(Study study, Sample sample, SampleUpdateParams updateParams, QueryOptions options, String userId)
            throws CatalogException {
        ObjectMap parameters = new ObjectMap();

        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse SampleUpdateParams object: " + e.getMessage(), e);
            }
        }

        options = ParamUtils.defaultObject(options, QueryOptions::new);

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
            authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                    SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                    SampleAclEntry.SamplePermissions.UPDATE);
        }

        if (updateParams != null && StringUtils.isNotEmpty(updateParams.getId())) {
            ParamUtils.checkAlias(updateParams.getId(), SampleDBAdaptor.QueryParams.ID.key());
        }

        if (updateParams != null && StringUtils.isNotEmpty(updateParams.getIndividualId())) {
            // Check individual id exists
            OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    updateParams.getIndividualId(), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
            if (individualDataResult.getNumResults() == 0) {
                throw new CatalogException("Individual '" + updateParams.getIndividualId() + "' not found.");
            }

            // Overwrite individual id parameter just in case the user used a uuid or other individual identifier
            parameters.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualDataResult.first().getId());
        }

        checkUpdateAnnotations(study, sample, parameters, options, VariableSet.AnnotableDataModels.SAMPLE, sampleDBAdaptor, userId);

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study));
        }

        return sampleDBAdaptor.update(sample.getUid(), parameters, study.getVariableSets(), options);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_SAMPLES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
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
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        // Add study id to the query
        query.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = sampleDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> sampleList, String member, boolean ignoreException,
                                                         String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("sampleList", sampleList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);

        try {
            OpenCGAResult<Map<String, List<String>>> sampleAclList = OpenCGAResult.empty();

            InternalGetDataResult<Sample> queryResult = internalGet(study.getUid(), sampleList, INCLUDE_SAMPLE_IDS, user, ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String sampleId : sampleList) {
                if (!missingMap.containsKey(sampleId)) {
                    Sample sample = queryResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> sampleAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            sampleAcls = authorizationManager.getSampleAcl(study.getUid(), sample.getUid(), user, member);
                        } else {
                            sampleAcls = authorizationManager.getAllSampleAcls(study.getUid(), sample.getUid(), user);
                        }
                        sampleAclList.append(sampleAcls);

                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.SAMPLE, sample.getId(),
                                sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.SAMPLE, sample.getId(),
                                sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());

                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, sampleId, missingMap.get(sampleId).getErrorMsg());
                            sampleAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, sampleId, missingMap.get(sampleId).getErrorMsg());
                    sampleAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.SAMPLE, sampleId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(sampleId).getErrorMsg())), new ObjectMap());
                }
            }

            return sampleAclList;
        } catch (CatalogException e) {
            for (String sampleId : sampleList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.SAMPLE, sampleId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, List<String> sampleStringList, String memberList,
                                                              SampleAclParams sampleAclParams, ParamUtils.AclAction action,
                                                              boolean propagate, String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("sampleStringList", sampleStringList)
                .append("memberList", memberList)
                .append("sampleAclParams", sampleAclParams)
                .append("action", action)
                .append("propagate", propagate)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        List<String> members;
        List<Sample> sampleList;
        List<String> permissions = Collections.emptyList();
        try {
            int count = 0;
            count += sampleStringList != null && !sampleStringList.isEmpty() ? 1 : 0;
            count += StringUtils.isNotEmpty(sampleAclParams.getIndividual()) ? 1 : 0;
            count += StringUtils.isNotEmpty(sampleAclParams.getCohort()) ? 1 : 0;
            count += StringUtils.isNotEmpty(sampleAclParams.getFile()) ? 1 : 0;

            if (count > 1) {
                throw new CatalogException("Update ACL: Only one of these parameters are allowed: sample, individual, file or cohort per "
                        + "query.");
            } else if (count == 0) {
                throw new CatalogException("Update ACL: At least one of these parameters should be provided: sample, individual, file or "
                        + "cohort");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            if (StringUtils.isNotEmpty(sampleAclParams.getPermissions())) {
                permissions = Arrays.asList(sampleAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, SampleAclEntry.SamplePermissions::valueOf);
            }

            if (StringUtils.isNotEmpty(sampleAclParams.getIndividual())) {
                Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), sampleAclParams.getIndividual());
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key());
                OpenCGAResult<Individual> indDataResult = catalogManager.getIndividualManager().search(studyId, query, options, token);

                Set<String> sampleSet = new HashSet<>();
                for (Individual individual : indDataResult.getResults()) {
                    sampleSet.addAll(individual.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()));
                }
                sampleStringList = new ArrayList<>();
                sampleStringList.addAll(sampleSet);
            }

            if (StringUtils.isNotEmpty(sampleAclParams.getFile())) {
//            // Obtain the samples of the files
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.SAMPLE_IDS.key());
                OpenCGAResult<File> fileDataResult = catalogManager.getFileManager().internalGet(study.getUid(),
                        Arrays.asList(StringUtils.split(sampleAclParams.getFile(), ",")), options, user, false);

                Set<String> sampleSet = new HashSet<>();
                for (File file : fileDataResult.getResults()) {
                    sampleSet.addAll(file.getSampleIds());
                }
                sampleStringList = new ArrayList<>();
                sampleStringList.addAll(sampleSet);
            }

            if (StringUtils.isNotEmpty(sampleAclParams.getCohort())) {
                Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), sampleAclParams.getCohort());
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());
                OpenCGAResult<Cohort> cohortDataResult = catalogManager.getCohortManager().search(studyId, query, options, token);

                Set<String> sampleSet = new HashSet<>();
                for (Cohort cohort : cohortDataResult.getResults()) {
                    sampleSet.addAll(cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
                }
                sampleStringList = new ArrayList<>();
                sampleStringList.addAll(sampleSet);
            }

            sampleList = internalGet(study.getUid(), sampleStringList, INCLUDE_SAMPLE_IDS, user, false).getResults();
            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

            // Validate that the members are actually valid members
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            checkMembers(study.getUid(), members);
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        } catch (CatalogException e) {
            if (sampleStringList != null) {
                for (String sampleId : sampleStringList) {
                    auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.SAMPLE, sampleId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        }

        OpenCGAResult<Map<String, List<String>>> aclResultList = OpenCGAResult.empty();
        int numProcessed = 0;
        do {
            List<Sample> batchSampleList = new ArrayList<>();
            while (numProcessed < Math.min(numProcessed + BATCH_OPERATION_SIZE, sampleList.size())) {
                batchSampleList.add(sampleList.get(numProcessed));
                numProcessed += 1;
            }

            List<Long> sampleUids = batchSampleList.stream().map(Sample::getUid).collect(Collectors.toList());
            List<AuthorizationManager.CatalogAclParams> aclParamsList = new LinkedList<>();
            AuthorizationManager.CatalogAclParams.addToList(sampleUids, permissions, Enums.Resource.SAMPLE, aclParamsList);

            try {
                if (propagate) {
                    // Obtain the whole list of implicity permissions
                    Set<String> allPermissions = new HashSet<>(permissions);
                    if (action == ParamUtils.AclAction.ADD || action == ParamUtils.AclAction.SET) {
                        // We also fetch the implicit permissions just in case
                        allPermissions.addAll(permissions
                                .stream()
                                .map(SampleAclEntry.SamplePermissions::valueOf)
                                .map(SampleAclEntry.SamplePermissions::getImplicitPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                    }

                    // Only propagate VIEW, WRITE and DELETE permissions
                    List<String> propagatedPermissions = new LinkedList<>();
                    for (String permission : allPermissions) {
                        if (SampleAclEntry.SamplePermissions.VIEW.name().equals(permission)
                                || SampleAclEntry.SamplePermissions.UPDATE.name().equals(permission)
                                || SampleAclEntry.SamplePermissions.DELETE.name().equals(permission)
                                || SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name().equals(permission)
                                || SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS.name().equals(permission)
                                || SampleAclEntry.SamplePermissions.DELETE_ANNOTATIONS.name().equals(permission)) {
                            propagatedPermissions.add(permission);
                        }
                    }

                    List<Long> individualUids = getIndividualsUidsFromSampleUids(study.getUid(), sampleUids);
                    AuthorizationManager.CatalogAclParams.addToList(individualUids, propagatedPermissions, Enums.Resource.INDIVIDUAL,
                            aclParamsList);
                }

                OpenCGAResult<Map<String, List<String>>> queryResults;
                switch (action) {
                    case SET:
                        queryResults = authorizationManager.setAcls(study.getUid(), members, aclParamsList);
                        break;
                    case ADD:
                        queryResults = authorizationManager.addAcls(study.getUid(), members, aclParamsList);
                        break;
                    case REMOVE:
                        queryResults = authorizationManager.removeAcls(members, aclParamsList);
                        break;
                    case RESET:
                        for (AuthorizationManager.CatalogAclParams aclParam : aclParamsList) {
                            aclParam.setPermissions(null);
                        }
                        queryResults = authorizationManager.removeAcls(members, aclParamsList);
                        break;
                    default:
                        throw new CatalogException("Unexpected error occurred. No valid action found.");
                }
                aclResultList.append(queryResults);

                for (Sample sample : batchSampleList) {
                    auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.SAMPLE, sample.getId(),
                            sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                }
            } catch (CatalogException e) {
                // Process current batch
                for (Sample sample : batchSampleList) {
                    auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.SAMPLE, sample.getId(),
                            sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }

                // Process remaining unprocessed batches
                while (numProcessed < sampleList.size()) {
                    Sample sample = sampleList.get(numProcessed);
                    auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.SAMPLE, sample.getId(),
                            sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }

                throw e;
            }

        } while (numProcessed < sampleList.size());

        return aclResultList;
    }

    private List<Long> getIndividualsUidsFromSampleUids(long studyUid, List<Long> sampleUids) throws CatalogException {
        // Look for all the individuals owning the samples
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleUids);

        OpenCGAResult<Individual> individualDataResult = individualDBAdaptor.get(query, IndividualManager.INCLUDE_INDIVIDUAL_IDS);

        return individualDataResult.getResults().stream().map(Individual::getUid).collect(Collectors.toList());
    }

    public DataResult<FacetField> facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
            throws CatalogException, IOException {
        String userId = userManager.getUserId(token);
        // We need to add variableSets and groups to avoid additional queries as it will be used in the catalogSolrManager
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key())));

        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

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
                DataResult<FacetField> result = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION, query,
                        options, userId);

                auditManager.auditFacet(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                return result;
            }
        } catch (CatalogException e) {
            auditManager.auditFacet(userId, Enums.Resource.SAMPLE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage())));
            throw e;
        }
    }

}
