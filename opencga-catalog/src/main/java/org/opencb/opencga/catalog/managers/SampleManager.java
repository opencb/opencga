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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
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
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.ExternalSource;
import org.opencb.opencga.core.models.common.RgaIndex;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternal;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
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
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleManager extends AnnotationSetManager<Sample> {

    public static final QueryOptions INCLUDE_SAMPLE_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.UUID.key(),
            SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.STUDY_UID.key()));
    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);
    private final String defaultFacet = "creationYear>>creationMonth;status;phenotypes;somatic";
    private UserManager userManager;
    private StudyManager studyManager;

    SampleManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        userManager = catalogManager.getUserManager();
        studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.SAMPLE;
    }

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

        SampleDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Sample> sampleDataResult = sampleDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        Function<Sample, String> sampleStringFunction = Sample::getId;
        if (idQueryParam.equals(SampleDBAdaptor.QueryParams.UUID)) {
            sampleStringFunction = Sample::getUuid;
        }

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

    SampleDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        SampleDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            SampleDBAdaptor.QueryParams param = SampleDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = SampleDBAdaptor.QueryParams.UUID;
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

    private OpenCGAResult<Sample> getSample(long studyUid, String sampleUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.UUID.key(), sampleUuid);
        return sampleDBAdaptor.get(query, options);
    }

    void validateNewSample(Study study, Sample sample, String userId) throws CatalogException {
        ParamUtils.checkIdentifier(sample.getId(), "id");

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

        if (CollectionUtils.isNotEmpty(sample.getCohortIds())) {
            throw new CatalogException("'cohortIds' list is not empty");
        } else {
            sample.setCohortIds(Collections.emptyList());
        }

        sample.setSource(ParamUtils.defaultObject(sample.getSource(), ExternalSource::init));
        sample.setProcessing(ParamUtils.defaultObject(sample.getProcessing(), SampleProcessing::init));
        sample.setCollection(ParamUtils.defaultObject(sample.getCollection(), SampleCollection::init));
        sample.setQualityControl(ParamUtils.defaultObject(sample.getQualityControl(), SampleQualityControl::new));
        sample.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(sample.getCreationDate(),
                SampleDBAdaptor.QueryParams.CREATION_DATE.key()));
        sample.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(sample.getModificationDate(),
                SampleDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        sample.setDescription(ParamUtils.defaultString(sample.getDescription(), ""));
        sample.setPhenotypes(ParamUtils.defaultObject(sample.getPhenotypes(), Collections.emptyList()));

        sample.setIndividualId(ParamUtils.defaultObject(sample.getIndividualId(), ""));
        sample.setFileIds(ParamUtils.defaultObject(sample.getFileIds(), Collections.emptyList()));

        sample.setStatus(ParamUtils.defaultObject(sample.getStatus(), Status::new));
        sample.setInternal(SampleInternal.init());
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
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.WRITE_SAMPLES);

            validateNewSample(study, sample, userId);

            // We create the sample
            OpenCGAResult<Sample> insert = sampleDBAdaptor.insert(study.getUid(), sample, study.getVariableSets(), options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created sample
                OpenCGAResult<Sample> result = getSample(study.getUid(), sample.getUuid(), options);
                insert.setResults(result.getResults());
            }
            auditManager.auditCreate(userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
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

    void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        changeQueryId(query, ParamConstants.SAMPLE_RGA_STATUS_PARAM, SampleDBAdaptor.QueryParams.INTERNAL_RGA_STATUS.key());
        changeQueryId(query, ParamConstants.SAMPLE_PROCESSING_PREPARATION_METHOD_PARAM,
                SampleDBAdaptor.QueryParams.PROCESSING_PREPARATION_METHOD.key());
        changeQueryId(query, ParamConstants.SAMPLE_PROCESSING_EXTRACTION_METHOD_PARAM,
                SampleDBAdaptor.QueryParams.PROCESSING_EXTRACTION_METHOD.key());
        changeQueryId(query, ParamConstants.SAMPLE_PROCESSING_LAB_SAMPLE_ID_PARAM,
                SampleDBAdaptor.QueryParams.PROCESSING_LAB_SAMPLE_ID.key());
        changeQueryId(query, ParamConstants.SAMPLE_COLLECTION_METHOD_PARAM, SampleDBAdaptor.QueryParams.COLLECTION_METHOD.key());
        changeQueryId(query, ParamConstants.SAMPLE_PROCESSING_PRODUCT_PARAM, SampleDBAdaptor.QueryParams.PROCESSING_PRODUCT_ID.key());
        changeQueryId(query, ParamConstants.SAMPLE_COLLECTION_FROM_PARAM, SampleDBAdaptor.QueryParams.COLLECTION_FROM_ID.key());
        changeQueryId(query, ParamConstants.SAMPLE_COLLECTION_TYPE_PARAM, SampleDBAdaptor.QueryParams.COLLECTION_TYPE.key());

        fixQualityControlQuery(query);
        super.fixQueryObject(query);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);

        // The files introduced could be either ids, paths or uuids. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.FILE_IDS.key()))) {
            List<String> fileIds = query.getAsStringList(SampleDBAdaptor.QueryParams.FILE_IDS.key());
            boolean queryFileManager = false;
            for (String fileId : fileIds) {
                if (!fileId.contains(":")) {
                    // In sample documents, we only store fileIds. If it does not contain ":", we will assume it is not a fileId, so we will
                    // query FileManager
                    queryFileManager = true;
                    break;
                }
            }

            if (queryFileManager) {
                // Obtain the corresponding fileIds
                InternalGetDataResult<File> result = catalogManager.getFileManager().internalGet(study.getUid(),
                        query.getAsStringList(SampleDBAdaptor.QueryParams.FILE_IDS.key()),
                        FileManager.INCLUDE_FILE_IDS, userId, true);
                if (result.getMissing() == null || result.getMissing().isEmpty()) {
                    // We have obtained all the results, so we add them to the query object
                    query.put(SampleDBAdaptor.QueryParams.FILE_IDS.key(), result.getResults().stream().map(File::getId)
                            .collect(Collectors.toList()));
                } else {
                    // We must not fail because of the additional file query, but this query should not get any results
                    logger.warn("Missing files: {}\nChanged query to ensure no results are returned", result.getMissing());
                    query.put(SampleDBAdaptor.QueryParams.UID.key(), -1);
                    query.remove(SampleDBAdaptor.QueryParams.FILE_IDS.key());
                }
            }
        }

        // The individuals introduced could be either ids or uuids. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()))) {
            List<String> individualIds = query.getAsStringList(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
            // In sample documents we store individual ids, so we will only do an additional query if user is passing an individual uuid
            boolean queryIndividualManager = false;
            for (String individualId : individualIds) {
                if (UuidUtils.isOpenCgaUuid(individualId)) {
                    queryIndividualManager = true;
                }
            }

            if (queryIndividualManager) {
                InternalGetDataResult<Individual> result = catalogManager.getIndividualManager().internalGet(study.getUid(),
                        query.getAsStringList(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()),
                        IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId, true);
                if (result.getMissing() == null || result.getMissing().isEmpty()) {
                    // We have obtained all the results, so we add them to the query object
                    query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), result.getResults().stream().map(Individual::getId)
                            .collect(Collectors.toList()));
                } else {
                    // We must not fail because of the additional individual query, but this query should not get any results
                    logger.warn("Missing individuals: {}\nChanged query to ensure no results are returned", result.getMissing());
                    query.put(SampleDBAdaptor.QueryParams.UID.key(), -1);
                    query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
                }
            }
        }
    }

    @Override
    public OpenCGAResult<Sample> count(String studyId, Query query, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        query = new Query(ParamUtils.defaultObject(query, Query::new));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("token", token);
        try {
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
    public OpenCGAResult delete(String studyStr, List<String> sampleIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, sampleIds, options, false, token);
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
                            SamplePermissions.DELETE);
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
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(operationId, userId, Enums.Resource.SAMPLE, sampleId, sampleUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
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
                            SamplePermissions.DELETE);
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
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    // TODO: This method should be private. This should only be accessible internally.
    public OpenCGAResult<Sample> resetRgaIndexes(String studyStr, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("token", token);

        OpenCGAResult<Sample> result;
        try {
            authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
            result = sampleDBAdaptor.setRgaIndexes(study.getUid(), new RgaIndex(RgaIndex.Status.NOT_INDEXED, TimeUtils.getTime()));

            auditManager.audit(userId, Enums.Action.RESET_RGA_INDEXES, Enums.Resource.SAMPLE, "ALL", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.RESET_RGA_INDEXES, Enums.Resource.SAMPLE, "ALL", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw new CatalogException("Could not reset all sample RGA indexes", e);
        }

        return result;
    }

    // TODO: This method should be somehow private. This should only be accessible internally.
    public OpenCGAResult<Sample> updateRgaIndexes(String studyStr, List<String> samples, RgaIndex rgaIndex, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("samples", samples)
                .append("rgaIndex", rgaIndex)
                .append("token", token);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        InternalGetDataResult<Sample> sampleResult = null;

        OpenCGAResult<Sample> result;
        try {
            auditManager.initAuditBatch(operationUuid);

            authorizationManager.isOwnerOrAdmin(study.getUid(), userId);

            ParamUtils.checkNotEmptyArray(samples, "samples");
            ParamUtils.checkObj(rgaIndex, "RgaIndex");
            ParamUtils.checkObj(rgaIndex.getStatus(), "RgaIndex status");

            rgaIndex.setDate(TimeUtils.getTime());

            sampleResult = internalGet(study.getUid(), samples, INCLUDE_SAMPLE_IDS, userId, false);
            result = sampleDBAdaptor.setRgaIndexes(study.getUid(),
                    sampleResult.getResults().stream().map(Sample::getUid).collect(Collectors.toList()), rgaIndex);

            for (Sample sample : sampleResult.getResults()) {
                auditManager.audit(operationUuid, userId, Enums.Action.UPDATE_RGA_INDEX, Enums.Resource.SAMPLE, sample.getId(),
                        sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            }
        } catch (CatalogException e) {
            if (sampleResult == null) {
                auditManager.audit(operationUuid, userId, Enums.Action.UPDATE_RGA_INDEX, Enums.Resource.SAMPLE, "", "", study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            } else {
                for (Sample sample : sampleResult.getResults()) {
                    auditManager.audit(operationUuid, userId, Enums.Action.UPDATE_RGA_INDEX, Enums.Resource.SAMPLE, sample.getId(),
                            sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
                }
            }
            throw new CatalogException("Could not reset all sample RGA indexes", e);
        } finally {
            auditManager.finishAuditBatch(operationUuid);
        }

        return result;
    }

    public OpenCGAResult<?> updateSampleInternalVariantIndex(Sample sample, SampleInternalVariantIndex index, String token)
            throws CatalogException {
        return updateSampleInternalVariant(sample, index, SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX.key(), token);
    }

    public OpenCGAResult<?> updateSampleInternalGenotypeIndex(Sample sample, SampleInternalVariantGenotypeIndex index, String token)
            throws CatalogException {
        return updateSampleInternalVariant(sample, index, SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_GENOTYPE_INDEX.key(), token);
    }

    public OpenCGAResult<?> updateSampleInternalVariantAnnotationIndex(Sample sample, SampleInternalVariantAnnotationIndex index,
                                                                       String token) throws CatalogException {
        return updateSampleInternalVariant(sample, index, SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX.key(), token);
    }

    public OpenCGAResult<?> updateSampleInternalVariantSecondaryIndex(Sample sample, SampleInternalVariantSecondaryIndex index,
                                                                      String token) throws CatalogException {
        return updateSampleInternalVariant(sample, index, SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_SECONDARY_INDEX.key(), token);
    }

    private OpenCGAResult<?> updateSampleInternalVariant(Sample sample, Object value, String fieldKey, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyDBAdaptor.get(sample.getStudyUid(), StudyManager.INCLUDE_STUDY_IDS).first();

        ObjectMap auditParams = new ObjectMap()
                .append("sample", sample)
                .append(fieldKey, value)
                .append("token", token);

        authorizationManager.isOwnerOrAdmin(study.getUid(), userId);

        ObjectMap params;
        try {
            params = new ObjectMap(fieldKey, new ObjectMap(getUpdateObjectMapper().writeValueAsString(value)));
        } catch (JsonProcessingException e) {
            throw new CatalogException("Cannot parse SampleInternalVariant object: " + e.getMessage(), e);
        }
        OpenCGAResult<?> update = sampleDBAdaptor.update(sample.getUid(), params, QueryOptions.empty());
        auditManager.audit(userId, Enums.Action.UPDATE_INTERNAL, Enums.Resource.SAMPLE, sample.getId(), sample.getUuid(), study.getId(),
                study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

        return new OpenCGAResult<>(update.getTime(), update.getEvents(), 1, Collections.emptyList(), 1);
    }

    public OpenCGAResult<Sample> updateAnnotationSet(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                     ParamUtils.BasicUpdateAction action, QueryOptions options, String token)
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
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.BasicUpdateAction.ADD, options, token);
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
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.BasicUpdateAction.REMOVE, options, token);
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
                if (!FileInternal.getVariantIndexStatusId(file.getInternal()).equals(VariantIndexStatus.NONE)) {
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
                        && CohortStatus.CALCULATING.equals(cohort.getInternal().getStatus().getId())) {
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
                result.setNumErrors(result.getNumErrors() + 1);

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
            result.setNumErrors(result.getNumErrors() + 1);

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
     * @param studyStr     Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sampleIds    List of Sample ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
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
                result.setNumErrors(result.getNumErrors() + 1);

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
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        SampleUpdateParams updateParamsClone;
        try {
            updateParamsClone = JacksonUtils.copy(updateParams, SampleUpdateParams.class);
        } catch (IOException e) {
            throw new CatalogException("Could not clone SampleUpdateParams object");
        }

        fixQualityControlUpdateParams(updateParamsClone, options);
        ObjectMap parameters = new ObjectMap();

        if (updateParamsClone != null) {
            try {
                parameters = updateParamsClone.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse SampleUpdateParams object: " + e.getMessage(), e);
            }
        }

        if (StringUtils.isNotEmpty(parameters.getString(SampleDBAdaptor.QueryParams.CREATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(SampleDBAdaptor.QueryParams.CREATION_DATE.key()),
                    SampleDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (StringUtils.isNotEmpty(parameters.getString(SampleDBAdaptor.QueryParams.MODIFICATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(SampleDBAdaptor.QueryParams.MODIFICATION_DATE.key()),
                    SampleDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

        ParamUtils.checkUpdateParametersMap(parameters);

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
        if (updateParamsClone != null && updateParamsClone.getAnnotationSets() != null) {
            authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                    SamplePermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                    SamplePermissions.WRITE);
        }

        if (updateParamsClone != null && StringUtils.isNotEmpty(updateParamsClone.getId())) {
            ParamUtils.checkIdentifier(updateParamsClone.getId(), SampleDBAdaptor.QueryParams.ID.key());
        }

        if (updateParamsClone != null && StringUtils.isNotEmpty(updateParamsClone.getIndividualId())) {
            // Check individual id exists
            OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    updateParamsClone.getIndividualId(), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
            if (individualDataResult.getNumResults() == 0) {
                throw new CatalogException("Individual '" + updateParamsClone.getIndividualId() + "' not found.");
            }

            // Overwrite individual id parameter just in case the user used a uuid or other individual identifier
            parameters.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualDataResult.first().getId());
        }

        checkUpdateAnnotations(study, sample, parameters, options, VariableSet.AnnotableDataModels.SAMPLE, sampleDBAdaptor, userId);

        OpenCGAResult<Sample> update = sampleDBAdaptor.update(sample.getUid(), parameters, study.getVariableSets(), options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated sample
            OpenCGAResult<Sample> queryResult = sampleDBAdaptor.get(study.getUid(),
                    new Query(SampleDBAdaptor.QueryParams.UID.key(), sample.getUid()), options, userId);
            update.setResults(queryResult.getResults());
        }
        return update;
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

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.VIEW_SAMPLES);

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
    public OpenCGAResult<AclEntryList<SamplePermissions>> getAcls(String studyId, List<String> sampleList, String member,
                                                                  boolean ignoreException, String token)
            throws CatalogException {
        return getAcls(studyId, sampleList, Collections.singletonList(member), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<SamplePermissions>> getAcls(String studyId, List<String> sampleList,
                                                                  List<String> members, boolean ignoreException,
                                                                  String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("sampleList", sampleList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);

        OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcls = OpenCGAResult.empty();
        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        try {
            auditManager.initAuditBatch(operationId);
            InternalGetDataResult<Sample> queryResult = internalGet(study.getUid(), sampleList, INCLUDE_SAMPLE_IDS, user, ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> sampleUids = queryResult.getResults().stream().map(Sample::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                sampleAcls = authorizationManager.getAcl(user, study.getUid(), sampleUids, members, Enums.Resource.SAMPLE,
                        SamplePermissions.class);
            } else {
                sampleAcls = authorizationManager.getAcl(user, study.getUid(), sampleUids, Enums.Resource.SAMPLE,
                        SamplePermissions.class);
            }

            // Include non-existing samples to the result list
            List<AclEntryList<SamplePermissions>> resultList = new ArrayList<>(sampleList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String sampleId : sampleList) {
                if (!missingMap.containsKey(sampleId)) {
                    Sample sample = queryResult.getResults().get(counter);
                    resultList.add(sampleAcls.getResults().get(counter));
                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.SAMPLE, sample.getId(),
                            sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, sampleId, missingMap.get(sampleId).getErrorMsg()));
                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.SAMPLE, sampleId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(sampleId).getErrorMsg())), new ObjectMap());
                }
            }
            sampleAcls.setResults(resultList);
            sampleAcls.setEvents(eventList);
        } catch (CatalogException e) {
            for (String sampleId : sampleList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.SAMPLE, sampleId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            if (!ignoreException) {
                throw e;
            } else {
                for (String sampleId : sampleList) {
                    Event event = new Event(Event.Type.ERROR, sampleId, e.getMessage());
                    sampleAcls.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.emptyList(), 0));
                }
            }
        } finally {
            auditManager.finishAuditBatch(operationId);
        }

        return sampleAcls;
    }

    public OpenCGAResult<AclEntryList<SamplePermissions>> updateAcl(String studyId, List<String> sampleStringList,
                                                                    String memberList, SampleAclParams sampleAclParams,
                                                                    ParamUtils.AclAction action, String token)
            throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("sampleStringList", sampleStringList)
                .append("memberList", memberList)
                .append("sampleAclParams", sampleAclParams)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        List<String> members;
        List<Sample> sampleList;
        List<String> permissions = Collections.emptyList();
        try {
            auditManager.initAuditBatch(operationId);

            int count = 0;
            count += sampleStringList != null && !sampleStringList.isEmpty() ? 1 : 0;
            count += StringUtils.isNotEmpty(sampleAclParams.getIndividual()) ? 1 : 0;
            count += StringUtils.isNotEmpty(sampleAclParams.getFamily()) ? 1 : 0;
            count += StringUtils.isNotEmpty(sampleAclParams.getCohort()) ? 1 : 0;
            count += StringUtils.isNotEmpty(sampleAclParams.getFile()) ? 1 : 0;

            if (count > 1) {
                throw new CatalogException("Update ACL: Only one of these parameters are allowed: sample, individual, family, file or "
                        + "cohort per query.");
            } else if (count == 0) {
                throw new CatalogException("Update ACL: At least one of these parameters should be provided: sample, individual, family,"
                        + " file or cohort");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            if (StringUtils.isNotEmpty(sampleAclParams.getPermissions())) {
                permissions = Arrays.asList(sampleAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, SamplePermissions::valueOf);
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

            if (StringUtils.isNotEmpty(sampleAclParams.getFamily())) {
                OpenCGAResult<Family> familyDataResult = catalogManager.getFamilyManager().get(studyId,
                        Arrays.asList(sampleAclParams.getFamily().split(",")), FamilyManager.INCLUDE_FAMILY_MEMBERS, token);

                Set<String> sampleSet = new HashSet<>();
                for (Family family : familyDataResult.getResults()) {
                    if (family.getMembers() != null) {
                        for (Individual individual : family.getMembers()) {
                            if (individual.getSamples() != null) {
                                sampleSet.addAll(individual.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()));
                            }
                        }
                    }
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
            auditManager.finishAuditBatch(operationId);
            throw e;
        }

        OpenCGAResult<AclEntryList<SamplePermissions>> aclResultList = OpenCGAResult.empty();
        int numProcessed = 0;
        do {
            List<Sample> batchSampleList = new ArrayList<>();
            while (numProcessed < Math.min(numProcessed + BATCH_OPERATION_SIZE, sampleList.size())) {
                batchSampleList.add(sampleList.get(numProcessed));
                numProcessed += 1;
            }

            List<Long> sampleUids = batchSampleList.stream().map(Sample::getUid).collect(Collectors.toList());
            List<String> sampleIds = batchSampleList.stream().map(Sample::getId).collect(Collectors.toList());
            List<AuthorizationManager.CatalogAclParams> aclParamsList = new ArrayList<>();
            AuthorizationManager.CatalogAclParams.addToList(sampleUids, permissions, Enums.Resource.SAMPLE, aclParamsList);

            try {
                switch (action) {
                    case SET:
                        authorizationManager.setAcls(study.getUid(), members, aclParamsList);
                        break;
                    case ADD:
                        authorizationManager.addAcls(study.getUid(), members, aclParamsList);
                        break;
                    case REMOVE:
                        authorizationManager.removeAcls(members, aclParamsList);
                        break;
                    case RESET:
                        for (AuthorizationManager.CatalogAclParams aclParam : aclParamsList) {
                            aclParam.setPermissions(null);
                        }
                        authorizationManager.removeAcls(members, aclParamsList);
                        break;
                    default:
                        throw new CatalogException("Unexpected error occurred. No valid action found.");
                }

                OpenCGAResult<AclEntryList<SamplePermissions>> queryResults = authorizationManager.getAcls(study.getUid(),
                        sampleUids, members, Enums.Resource.SAMPLE, SamplePermissions.class);

                for (int i = 0; i < queryResults.getResults().size(); i++) {
                    queryResults.getResults().get(i).setId(sampleIds.get(i));
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

                auditManager.finishAuditBatch(operationId);
                throw e;
            }
        } while (numProcessed < sampleList.size());

        auditManager.finishAuditBatch(operationId);
        return aclResultList;
    }

    public DataResult<FacetField> facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
            throws CatalogException {
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

    private List<Long> getIndividualsUidsFromSampleUids(long studyUid, List<Long> sampleUids) throws CatalogException {
        // Look for all the individuals owning the samples
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleUids);

        OpenCGAResult<Individual> individualDataResult = individualDBAdaptor.get(query, IndividualManager.INCLUDE_INDIVIDUAL_IDS);

        return individualDataResult.getResults().stream().map(Individual::getUid).collect(Collectors.toList());
    }

    private void fixQualityControlQuery(Query query) {
        String variableSetId = "opencga_sample_variant_stats";

        List<String> simpleStatsKeys = Arrays.asList(ParamConstants.SAMPLE_VARIANT_STATS_COUNT_PARAM,
                ParamConstants.SAMPLE_VARIANT_STATS_TI_TV_RATIO_PARAM, ParamConstants.SAMPLE_VARIANT_STATS_QUALITY_AVG_PARAM,
                ParamConstants.SAMPLE_VARIANT_STATS_QUALITY_STD_DEV_PARAM, ParamConstants.SAMPLE_VARIANT_STATS_HETEROZYGOSITY_RATE_PARAM);

        List<String> mapStatsKeys = Arrays.asList(ParamConstants.SAMPLE_VARIANT_STATS_CHROMOSOME_COUNT_PARAM,
                ParamConstants.SAMPLE_VARIANT_STATS_TYPE_COUNT_PARAM, ParamConstants.SAMPLE_VARIANT_STATS_GENOTYPE_COUNT_PARAM,
                ParamConstants.SAMPLE_VARIANT_STATS_DEPTH_COUNT_PARAM, ParamConstants.SAMPLE_VARIANT_STATS_BIOTYPE_COUNT_PARAM,
                ParamConstants.SAMPLE_VARIANT_STATS_CLINICAL_SIGNIFICANCE_COUNT_PARAM,
                ParamConstants.SAMPLE_VARIANT_STATS_CONSEQUENCE_TYPE_COUNT_PARAM);

        // Default annotation set id
        String id = query.getString(ParamConstants.SAMPLE_VARIANT_STATS_ID_PARAM, "ALL");

        List<String> annotationList = new LinkedList<>();
        for (String statsKey : simpleStatsKeys) {
            String value = query.getString(statsKey);
            if (StringUtils.isNotEmpty(value)) {
                if (!value.startsWith("!") && !value.startsWith("=") && !value.startsWith(">") && !value.startsWith("<")) {
                    value = "=" + value;
                }

                query.remove(statsKey);

                // Remove prefix stats
                String field = statsKey.replace("stats", "");
                // Convert it to cammel case again
                field = Character.toLowerCase(field.charAt(0)) + field.substring(1);

                annotationList.add(variableSetId + "__" + id + "@" + variableSetId + ":" + field + value);
            }
        }
        for (String statsKey : mapStatsKeys) {
            String value = query.getString(statsKey);
            if (StringUtils.isNotEmpty(value)) {
                query.remove(statsKey);

                // Remove prefix stats
                String field = statsKey.replace("stats", "");
                // Convert it to cammel case again
                field = Character.toLowerCase(field.charAt(0)) + field.substring(1);
                annotationList.add(variableSetId + "__" + id + "@" + variableSetId + ":" + field + "." + value);
            }
        }

        if (!annotationList.isEmpty()) {
            query.remove(ParamConstants.SAMPLE_VARIANT_STATS_ID_PARAM);
            query.put(Constants.ANNOTATION, StringUtils.join(annotationList, ";"));
        }
    }

    private void fixQualityControlUpdateParams(SampleUpdateParams sampleUpdateParams, QueryOptions options) throws CatalogException {
        if (sampleUpdateParams.getQualityControl() == null) {
            return;
        }

        if (CollectionUtils.isNotEmpty(sampleUpdateParams.getAnnotationSets())) {
            throw new CatalogException("Cannot update 'qualityControl' and 'annotationSets' at the same time.");
        }

        String variableSetId = "opencga_sample_variant_stats";

        if (sampleUpdateParams.getQualityControl().getVariant() == null
                || sampleUpdateParams.getQualityControl().getVariant().getVariantStats().isEmpty()) {
            // Add REMOVE Action
            Map<String, Object> map = options.getMap(Constants.ACTIONS);
            if (map == null) {
                map = new HashMap<>();
            }
            map.put(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), ParamUtils.BasicUpdateAction.REMOVE);
            options.put(Constants.ACTIONS, map);

            // Delete all annotation sets of variable set
            sampleUpdateParams.setAnnotationSets(Collections.singletonList(new AnnotationSet().setVariableSetId(variableSetId)));
            return;
        }

        List<AnnotationSet> annotationSetList = new LinkedList<>();
        if (sampleUpdateParams.getQualityControl().getVariant() != null) {

            if (CollectionUtils.isNotEmpty(sampleUpdateParams.getQualityControl().getVariant().getVariantStats())) {
                for (SampleQcVariantStats variantStat : sampleUpdateParams.getQualityControl().getVariant().getVariantStats()) {
                    SampleVariantStats stats = variantStat.getStats();
                    if (stats != null) {
                        Map<String, Integer> indelLengthCount = new HashMap<>();
                        if (stats.getIndelLengthCount() != null) {
                            indelLengthCount.put("lt5", stats.getIndelLengthCount().getLt5());
                            indelLengthCount.put("lt10", stats.getIndelLengthCount().getLt10());
                            indelLengthCount.put("lt15", stats.getIndelLengthCount().getLt15());
                            indelLengthCount.put("lt20", stats.getIndelLengthCount().getLt20());
                            indelLengthCount.put("gte20", stats.getIndelLengthCount().getGte20());
                        }

                        Map<String, Integer> depthCount = new HashMap<>();
                        if (stats.getDepthCount() != null) {
                            depthCount.put("na", stats.getDepthCount().getNa());
                            depthCount.put("lt5", stats.getDepthCount().getLt5());
                            depthCount.put("lt10", stats.getDepthCount().getLt10());
                            depthCount.put("lt15", stats.getDepthCount().getLt15());
                            depthCount.put("lt20", stats.getDepthCount().getLt20());
                            depthCount.put("gte20", stats.getDepthCount().getGte20());
                        }

                        ObjectMap annotations = new ObjectMap();
                        annotations.putIfNotEmpty("id", variantStat.getId());
                        annotations.putIfNotNull("variantCount", stats.getVariantCount());
                        annotations.putIfNotNull("chromosomeCount", stats.getChromosomeCount());
                        annotations.putIfNotNull("typeCount", stats.getTypeCount());
                        annotations.putIfNotNull("genotypeCount", stats.getGenotypeCount());
                        annotations.putIfNotNull("indelLengthCount", indelLengthCount);
                        annotations.putIfNotNull("filterCount", stats.getFilterCount());
                        annotations.putIfNotNull("tiTvRatio", stats.getTiTvRatio());
                        annotations.putIfNotNull("qualityAvg", stats.getQualityAvg());
                        annotations.putIfNotNull("qualityStdDev", stats.getQualityStdDev());
                        annotations.putIfNotNull("heterozygosityRate", stats.getHeterozygosityRate());
                        annotations.putIfNotNull("consequenceTypeCount", stats.getConsequenceTypeCount());
                        annotations.putIfNotNull("biotypeCount", stats.getBiotypeCount());
                        annotations.putIfNotNull("clinicalSignificanceCount", stats.getClinicalSignificanceCount());
                        annotations.putIfNotNull("mendelianErrorCount", stats.getMendelianErrorCount());
                        annotations.putIfNotNull("depthCount", depthCount);

                        annotationSetList.add(new AnnotationSet(variableSetId + "__" + variantStat.getId(), variableSetId, annotations));
                    }
                }
            }
        }

        Map<String, Object> map = options.getMap(Constants.ACTIONS);
        if (map == null) {
            map = new HashMap<>();
        }
        if (!annotationSetList.isEmpty()) {
            // Add SET Action
            map.put(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), ParamUtils.BasicUpdateAction.SET);
            options.put(Constants.ACTIONS, map);

            sampleUpdateParams.setAnnotationSets(annotationSetList);
        } else {
            // Add REMOVE Action
            map.put(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), ParamUtils.BasicUpdateAction.REMOVE);
            options.put(Constants.ACTIONS, map);

            // Delete all annotation sets of variable set
            sampleUpdateParams.setAnnotationSets(Collections.singletonList(new AnnotationSet().setVariableSetId(variableSetId)));
        }
    }
}
