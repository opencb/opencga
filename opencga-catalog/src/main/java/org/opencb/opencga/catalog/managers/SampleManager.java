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
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.*;
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
public class SampleManager extends AnnotationSetManager<Sample, SamplePermissions> {

    public static final QueryOptions INCLUDE_SAMPLE_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.UUID.key(),
            SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.STUDY_UID.key()));
    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);
    private StudyManager studyManager;

    SampleManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                  CatalogManager catalogManager, DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        studyManager = catalogManager.getStudyManager();
    }

    @Override
    protected Enums.Resource getResource() {
        return Enums.Resource.SAMPLE;
    }

    @Override
    InternalGetDataResult<Sample> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
                                              QueryOptions options, String user, boolean ignoreException) throws CatalogException {
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

        OpenCGAResult<Sample> sampleDataResult = getSampleDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        Function<Sample, String> sampleStringFunction = Sample::getId;
        if (idQueryParam.equals(SampleDBAdaptor.QueryParams.UUID)) {
            sampleStringFunction = Sample::getUuid;
        }

        if (ignoreException || sampleDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, sampleStringFunction, sampleDataResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Sample> resultsNoCheck = getSampleDBAdaptor(organizationId).get(queryCopy, queryOptions);

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

    private OpenCGAResult<Sample> getSample(String organizationId, long studyUid, String sampleUuid, QueryOptions options)
            throws CatalogException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.UUID.key(), sampleUuid);
        return getSampleDBAdaptor(organizationId).get(query, options);
    }

    void validateNewSample(String organizationId, Study study, Sample sample, String userId) throws CatalogException {
        ParamUtils.checkIdentifier(sample.getId(), "id");

        // Check the id is not in use
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(SampleDBAdaptor.QueryParams.ID.key(), sample.getId());
        if (getSampleDBAdaptor(organizationId).count(query).getNumMatches() > 0) {
            throw new CatalogException("Sample '" + sample.getId() + "' already exists.");
        }

        if (StringUtils.isNotEmpty(sample.getIndividualId())) {
            // Check individual exists
            OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager().internalGet(organizationId,
                    study.getUid(), sample.getIndividualId(), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
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
        return create(studyStr, sample, options, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId, entryParam) -> {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_SAMPLES);

            validateNewSample(organizationId, study, sample, userId);
            entryParam.setId(sample.getId());
            entryParam.setUuid(sample.getUuid());

            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();

            // We create the sample
            OpenCGAResult<Sample> insert = getSampleDBAdaptor(organizationId).insert(study.getUid(), sample, study.getVariableSets(),
                    queryOptions);
            if (queryOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created sample
                OpenCGAResult<Sample> result = getSample(organizationId, study.getUid(), sample.getUuid(), queryOptions);
                insert.setResults(result.getResults());
            }
            return insert;
        });
    }

    @Override
    public DBIterator<Sample> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return iterator(studyStr, query, options, StudyManager.INCLUDE_VARIABLE_SET, token, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions queryOptions = ParamUtils.defaultObject(options, QueryOptions::new);

            fixQueryObject(organizationId, study, finalQuery, userId);
            AnnotationUtils.fixQueryOptionAnnotation(queryOptions);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getSampleDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, queryOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        return facet(studyStr, query, facet, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(organizationId, study, finalQuery, userId);
            query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getSampleDBAdaptor(organizationId).facet(study.getUid(), finalQuery, facet, userId);
        });
    }

    @Override
    public OpenCGAResult<Sample> search(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return search(studyStr, query, options, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();

            fixQueryObject(organizationId, study, finalQuery, userId);
            AnnotationUtils.fixQueryOptionAnnotation(queryOptions);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getSampleDBAdaptor(organizationId).get(study.getUid(), finalQuery, queryOptions, userId);
        });
    }

    public OpenCGAResult<?> distinct(String field, Query query, String token) throws CatalogException {
        return distinct(Collections.singletonList(field), query, token);
    }

    /**
     * Fetch a list containing all the distinct values of the key {@code field}.
     * Query object may or may not contain a study parameter, so only organization administrators will be able to call to this method.
     *
     * @param fields  Fields for which to return distinct values.
     * @param query   Query object.
     * @param token   Token of the user logged in.
     * @return The list of distinct values.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<?> distinct(List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("fields", fields)
                .append("query", new Query(query))
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            fixQueryObject(organizationId, null, query, userId);

            OpenCGAResult<?> result = getSampleDBAdaptor(organizationId).distinct(fields, query);

            auditManager.auditDistinct(organizationId, userId, Enums.Resource.SAMPLE, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(organizationId, userId, Enums.Resource.SAMPLE, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyStr, List<String> fields, Query query, String token) throws CatalogException {
        return distinct(studyStr, fields, query, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(organizationId, study, finalQuery, userId);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getSampleDBAdaptor(organizationId).distinct(study.getUid(), fields, finalQuery, userId);
        });
    }

    @Override
    public OpenCGAResult<Sample> count(String studyStr, Query query, String token) throws CatalogException {
        return count(studyStr, query, token, StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(organizationId, study, finalQuery, userId);

            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> count = getSampleDBAdaptor(organizationId).count(finalQuery, userId);
            return new OpenCGAResult<>(count.getTime(), count.getEvents(), 0, Collections.emptyList(), count.getNumMatches());
        });
    }

    @Override
    public OpenCGAResult<Sample> delete(String studyStr, List<String> sampleIds, QueryOptions options, String token)
            throws CatalogException {
        return delete(studyStr, sampleIds, options, false, token);
    }

    public OpenCGAResult<Sample> delete(String studyStr, List<String> sampleIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, sampleIds, params, ignoreException, token,
                (organizationId, study, userId, entryParam) -> {
                    if (StringUtils.isEmpty(entryParam.getId())) {
                        throw new CatalogException("Internal error: Missing sample id. This sample id should have been provided"
                                + " internally.");
                    }
                    String sampleId = entryParam.getId();

                    Query query = new Query();
                    authorizationManager.buildAclCheckQuery(userId, SamplePermissions.DELETE.name(), query);
                    InternalGetDataResult<Sample> tmpResult = internalGet(organizationId, study.getUid(), sampleId, query,
                            INCLUDE_SAMPLE_IDS, userId, true);
                    if (tmpResult.getNumResults() == 0) {
                        throw new CatalogException("Sample '" + sampleId + "' not found or user " + userId + " does not have the proper "
                                + "permissions to delete it.");
                    }
                    Sample sample = tmpResult.first();

                    // We set the proper sample ids in the entry param object
                    entryParam.setId(sample.getId());
                    entryParam.setUuid(sample.getUuid());

                    // Check if the sample can be deleted
                    checkSampleCanBeDeleted(organizationId, study.getUid(), sample, params.getBoolean(Constants.FORCE, false));

                    return getSampleDBAdaptor(organizationId).delete(sample);
                });
    }

    @Override
    public OpenCGAResult<Sample> delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult<Sample> delete(String studyStr, Query query, QueryOptions options, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, query, options, ignoreException, token, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            // Obtain sample iterator
            fixQueryObject(organizationId, study, finalQuery, userId);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            authorizationManager.buildAclCheckQuery(userId, SamplePermissions.DELETE.name(), finalQuery);
            return getSampleDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_SAMPLE_IDS, userId);
        }, (organizationId, study, userId, sample) -> {
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            // Check if the sample can be deleted
            checkSampleCanBeDeleted(organizationId, study.getUid(), sample, finalOptions.getBoolean(Constants.FORCE, false));
            return getSampleDBAdaptor(organizationId).delete(sample);
        });
    }

    // TODO: This method should be private. This should only be accessible internally.
    public OpenCGAResult<Sample> resetRgaIndexes(String studyStr, String token) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("token", token);
        return runForSingleEntry(params, Enums.Action.RESET_RGA_INDEXES, studyStr, token,
                (organizationId, study, userId, entryParam) -> {
                    authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);
                    return getSampleDBAdaptor(organizationId).setRgaIndexes(study.getUid(), new RgaIndex(RgaIndex.Status.NOT_INDEXED,
                            TimeUtils.getTime()));
                }, "Could not reset all sample RGA indexes");
    }

    // TODO: This method should be somehow private. This should only be accessible internally.
    public OpenCGAResult<Sample> updateRgaIndexes(String studyStr, List<String> samples, RgaIndex rgaIndex, String token)
            throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("study", studyStr)
                .append("samples", samples)
                .append("rgaIndex", rgaIndex)
                .append("token", token);
        return runForMultiOperation(params, Enums.Action.UPDATE_RGA_INDEX, studyStr, token,
                (organizationId, study, userId, entryParamList) -> {
                    authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);

                    ParamUtils.checkNotEmptyArray(samples, "samples");
                    ParamUtils.checkObj(rgaIndex, "RgaIndex");
                    ParamUtils.checkObj(rgaIndex.getStatus(), "RgaIndex status");

                    rgaIndex.setDate(TimeUtils.getTime());

                    InternalGetDataResult<Sample> sampleResult = internalGet(organizationId, study.getUid(), samples, INCLUDE_SAMPLE_IDS,
                            userId, false);
                    for (Sample sample : sampleResult.getResults()) {
                        entryParamList.add(new EntryParam(sample.getId(), sample.getUuid()));
                    }

                    return getSampleDBAdaptor(organizationId).setRgaIndexes(study.getUid(),
                            sampleResult.getResults().stream().map(Sample::getUid).collect(Collectors.toList()), rgaIndex);
                });
    }

    public OpenCGAResult<?> updateSampleInternalVariantIndex(String studyFqn, Sample sample, SampleInternalVariantIndex index,
                                                             String token) throws CatalogException {
        return updateSampleInternalVariant(studyFqn, sample, index, SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX.key(), token);
    }

    public OpenCGAResult<?> updateSampleInternalVariantSecondarySampleIndex(String studyFqn, Sample sample,
                                                                            SampleInternalVariantSecondarySampleIndex index, String token)
            throws CatalogException {
        return updateSampleInternalVariant(studyFqn, sample, index, Arrays.asList(
                SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_SECONDARY_SAMPLE_INDEX.key(),
                SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_GENOTYPE_INDEX.key()), token);
    }

    public OpenCGAResult<?> updateSampleInternalVariantAnnotationIndex(
            String studyFqn, Sample sample, SampleInternalVariantAnnotationIndex index, String token) throws CatalogException {
        return updateSampleInternalVariant(studyFqn, sample, index,
                SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX.key(), token);
    }

    public OpenCGAResult<?> updateSampleInternalVariantSecondaryAnnotationIndex(String studyFqn, Sample sample,
                                                                                SampleInternalVariantSecondaryAnnotationIndex index,
                                                                                String token) throws CatalogException {
        return updateSampleInternalVariant(studyFqn, sample, index,
                SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_SECONDARY_ANNOTATION_INDEX.key(), token);
    }

    private OpenCGAResult<?> updateSampleInternalVariant(String studyFqn, Sample sample, Object value, String fieldKey, String token)
            throws CatalogException {
        return updateSampleInternalVariant(studyFqn, sample, value, Collections.singletonList(fieldKey), token);
    }

    private OpenCGAResult<?> updateSampleInternalVariant(String studyFqn, Sample sample, Object value, List<String> fieldKeys,
                                                         String token) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append("studyFqn", studyFqn)
                .append("sample", sample.getId())
                .append("token", token);
        for (String fieldKey : fieldKeys) {
            params.append(fieldKey, value);
        }
        return runForSingleEntry(params, Enums.Action.UPDATE_INTERNAL, studyFqn, token,
                (organizationId, study, userId, entryParam) -> {
                    authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);
                    ObjectMap tmpParams;
                    try {
                        tmpParams = new ObjectMap();
                        ObjectMap valueAsObjectMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(value));
                        for (String fieldKey : fieldKeys) {
                            tmpParams.append(fieldKey, valueAsObjectMap);
                        }
                    } catch (JsonProcessingException e) {
                        throw new CatalogException("Cannot parse SampleInternalVariant object: " + e.getMessage(), e);
                    }
                    return getSampleDBAdaptor(organizationId).update(sample.getUid(), tmpParams, QueryOptions.empty());
                });
    }

    public OpenCGAResult<Sample> updateAnnotationSet(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                     ParamUtils.BasicUpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        SampleUpdateParams sampleUpdateParams = new SampleUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, sampleStr, sampleUpdateParams, options, token);
    }

    public OpenCGAResult<Sample> addAnnotationSet(String studyStr, String sampleStr, AnnotationSet annotationSet,
                                                  QueryOptions options, String token) throws CatalogException {
        return addAnnotationSets(studyStr, sampleStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Sample> addAnnotationSets(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.BasicUpdateAction.ADD, options,
                token);
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
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.BasicUpdateAction.REMOVE, options,
                token);
    }

    public OpenCGAResult<Sample> removeAnnotations(String studyStr, String sampleStr, String annotationSetId, List<String> annotations,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, sampleStr, annotationSetId,
                new ObjectMap("remove", StringUtils.join(annotations, ",")), ParamUtils.CompleteUpdateAction.REMOVE, options, token);
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

    public OpenCGAResult<Sample> resetAnnotations(String studyStr, String sampleStr, String annotationSetId, List<String> annotations,
                                                  QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, sampleStr, annotationSetId,
                new ObjectMap("reset", StringUtils.join(annotations, ",")), ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    private void checkSampleCanBeDeleted(String organizationId, long studyId, Sample sample, boolean force) throws CatalogException {
        String msg = "Could not delete sample '" + sample.getId() + "'. ";
        // Look for files related with the sample
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sample.getId())
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        DBIterator<File> fileIterator = getFileDBAdaptor(organizationId).iterator(query, QueryOptions.empty());
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
                throw new CatalogException(msg + "Associated files are used in storage: " + StringUtils.join(errorFiles, ", "));
            } else {
                throw new CatalogException(msg + "Sample associated to the files: " + StringUtils.join(errorFiles, ", "));
            }
        }

        // Look for cohorts containing the sample
        query = new Query()
                .append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        DBIterator<Cohort> cohortIterator = getCohortDBAdaptor(organizationId).iterator(query, QueryOptions.empty());
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
            throw new CatalogException(msg + "Sample in cohort " + StudyEntry.DEFAULT_COHORT);
        }
        if (!errorCohorts.isEmpty()) {
            if (force) {
                throw new CatalogException(msg + "Sample present in cohorts in the process of calculating the stats: "
                        + StringUtils.join(errorCohorts, ", "));
            } else {
                throw new CatalogException(msg + "Sample present in cohorts: " + StringUtils.join(errorCohorts, ", "));
            }
        }

        // Look for individuals containing the sample
        if (!force) {
            query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Individual> individualDataResult = getIndividualDBAdaptor(organizationId).get(query,
                    new QueryOptions(QueryOptions.INCLUDE,
                            Arrays.asList(IndividualDBAdaptor.QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.ID.key())));
            if (individualDataResult.getNumResults() > 0) {
                throw new CatalogException(msg + "Sample is associated with individual '" + individualDataResult.first().getId() + "'.");
            }
        }
    }

    public OpenCGAResult<Sample> update(String studyStr, Query query, SampleUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Sample> update(String studyStr, Query query, SampleUpdateParams updateParams, boolean ignoreException,
                                        QueryOptions options, String token) throws CatalogException {
        return updateMany(studyStr, query, updateParams, ignoreException, options, token,
                StudyManager.INCLUDE_VARIABLE_SET,
                (organizationId, study, userId) -> {
                    Query finalQuery = query != null ? new Query(query) : new Query();
                    fixQueryObject(organizationId, study, finalQuery, userId);
                    finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                    return getSampleDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_SAMPLE_IDS, userId);
                }, (organizationId, study, userId, sample) -> update(organizationId, study, sample, updateParams, options, userId),
                "Could not update sample");
    }

    public OpenCGAResult<Sample> update(String studyStr, String sampleId, SampleUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, sampleId, updateParams, options, token, StudyManager.INCLUDE_VARIABLE_SET,
                (organizationId, study, userId, entryParam) -> {
                    OpenCGAResult<Sample> internalResult = internalGet(organizationId, study.getUid(), sampleId, INCLUDE_SAMPLE_IDS,
                            userId);
                    if (internalResult.getNumResults() == 0) {
                        throw new CatalogException("Sample '" + sampleId + "' not found");
                    }
                    Sample sample = internalResult.first();
                    entryParam.setId(sample.getId());
                    entryParam.setUuid(sample.getUuid());

                    return update(organizationId, study, sample, updateParams, options, userId);
                });
    }

    /**
     * Update Sample from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of
     *                     [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
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

    public OpenCGAResult<Sample> update(String studyStr, List<String> sampleIds, SampleUpdateParams updateParams,
                                        boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        return updateMany(studyStr, sampleIds, updateParams, ignoreException, options, token,
                StudyManager.INCLUDE_VARIABLE_SET, (organizationId, study, userId, entryParam) -> {
                    String sampleId = entryParam.getId();
                    OpenCGAResult<Sample> internalResult = internalGet(organizationId, study.getUid(), sampleId, INCLUDE_SAMPLE_IDS,
                            userId);
                    if (internalResult.getNumResults() == 0) {
                        throw new CatalogException("Sample '" + sampleId + "' not found");
                    }
                    Sample sample = internalResult.first();
                    entryParam.setId(sample.getId());
                    entryParam.setUuid(sample.getUuid());

                    return update(organizationId, study, sample, updateParams, options, userId);
                });
    }

    private OpenCGAResult<Sample> update(String organizationId, Study study, Sample sample, SampleUpdateParams updateParams,
                                         QueryOptions options, String userId) throws CatalogException {
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
            authorizationManager.checkSamplePermission(organizationId, study.getUid(), sample.getUid(), userId,
                    SamplePermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkSamplePermission(organizationId, study.getUid(), sample.getUid(), userId,
                    SamplePermissions.WRITE);
        }

        if (updateParamsClone != null && updateParamsClone.getId() != null) {
            ParamUtils.checkIdentifier(updateParamsClone.getId(), SampleDBAdaptor.QueryParams.ID.key());
        }

        if (updateParamsClone != null && StringUtils.isNotEmpty(updateParamsClone.getIndividualId())) {
            // Check individual id exists
            OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager().internalGet(organizationId,
                    study.getUid(), updateParamsClone.getIndividualId(), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
            if (individualDataResult.getNumResults() == 0) {
                throw new CatalogException("Individual '" + updateParamsClone.getIndividualId() + "' not found.");
            }

            // Overwrite individual id parameter just in case the user used a uuid or other individual identifier
            parameters.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualDataResult.first().getId());
        }

        checkUpdateAnnotations(organizationId, study, sample, parameters, options, VariableSet.AnnotableDataModels.SAMPLE,
                getSampleDBAdaptor(organizationId), userId);

        OpenCGAResult<Sample> update = getSampleDBAdaptor(organizationId).update(sample.getUid(), parameters, study.getVariableSets(),
                options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated sample
            OpenCGAResult<Sample> queryResult = getSampleDBAdaptor(organizationId).get(study.getUid(),
                    new Query(SampleDBAdaptor.QueryParams.UID.key(), sample.getUid()), options, userId);
            update.setResults(queryResult.getResults());
        }
        return update;
    }

    @Override
    public OpenCGAResult<Sample> rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        return rank(studyStr, query, field, numResults, asc, token, (organizationId, study, userId) -> {
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_SAMPLES);

            Query finalQuery = query != null ? new Query(query) : new Query();
            ParamUtils.checkObj(field, "field");
            ParamUtils.checkObj(token, "token");
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, userId, finalQuery, authorizationManager);

            // TODO: In next release, we will have to check the count parameter from the queryOptions object.
            boolean count = true;
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Sample> queryResult = null;
            if (count) {
                // We do not need to check for permissions when we show the count of files
                queryResult = getSampleDBAdaptor(organizationId).rank(finalQuery, field, numResults, asc);
            }

            return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
        });
    }

    @Override
    public OpenCGAResult<Sample> groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        return groupBy(studyStr, query, fields, options, token, (organizationId, study, userId) -> {
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_SAMPLES);

            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CatalogException("Empty fields parameter.");
            }

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, userId, finalQuery, authorizationManager);
            AnnotationUtils.fixQueryOptionAnnotation(queryOptions);

            // Add study id to the query
            finalQuery.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getSampleDBAdaptor(organizationId).groupBy(finalQuery, fields, queryOptions, userId);
        });
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<SamplePermissions>> getAcls(String studyId, List<String> sampleList, String member,
                                                                  boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, sampleList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<SamplePermissions>> getAcls(String studyStr, List<String> sampleList, List<String> members,
                                                                  boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyStr, sampleList, members, ignoreException, token,
                (organizationId, study, userId, entryParamList) -> {
                    OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcls;
                    Map<String, InternalGetDataResult<?>.Missing> missingMap = new HashMap<>();

                    for (String sampleId : sampleList) {
                        entryParamList.add(new EntryParam(sampleId, null));
                    }

                    InternalGetDataResult<Sample> queryResult = internalGet(organizationId, study.getUid(), sampleList, INCLUDE_SAMPLE_IDS,
                            userId, ignoreException);
                    entryParamList.clear();
                    for (Sample result : queryResult.getResults()) {
                        entryParamList.add(new EntryParam(result.getId(), result.getUuid()));
                    }
                    if (queryResult.getMissing() != null) {
                        missingMap = queryResult.getMissing().stream()
                                .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
                    }

                    List<Long> sampleUids = queryResult.getResults().stream().map(Sample::getUid).collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(members)) {
                        sampleAcls = authorizationManager.getAcl(organizationId, study.getUid(), sampleUids, members, Enums.Resource.SAMPLE,
                                SamplePermissions.class, userId);
                    } else {
                        sampleAcls = authorizationManager.getAcl(organizationId, study.getUid(), sampleUids, Enums.Resource.SAMPLE,
                                SamplePermissions.class, userId);
                    }

                    // Include non-existing samples to the result list
                    List<AclEntryList<SamplePermissions>> resultList = new ArrayList<>(sampleList.size());
                    List<Event> eventList = new ArrayList<>(missingMap.size());
                    int counter = 0;
                    for (String sampleId : sampleList) {
                        if (!missingMap.containsKey(sampleId)) {
                            resultList.add(sampleAcls.getResults().get(counter));
                            counter++;
                        } else {
                            resultList.add(new AclEntryList<>());
                            eventList.add(new Event(Event.Type.ERROR, sampleId, missingMap.get(sampleId).getErrorMsg()));
                        }
                    }
                    for (int i = 0; i < queryResult.getResults().size(); i++) {
                        sampleAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
                    }
                    sampleAcls.setResults(resultList);
                    sampleAcls.setEvents(eventList);

                    return sampleAcls;
                });
    }

    public OpenCGAResult<AclEntryList<SamplePermissions>> updateAcl(String studyStr, List<String> sampleStringList, String memberList,
                                                                    SampleAclParams sampleAclParams, ParamUtils.AclAction action,
                                                                    String token) throws CatalogException {
        return updateAcls(studyStr, sampleStringList, memberList, sampleAclParams, action, token,
                (organizationId, study, userId, entryParamList) -> {
                    int count = 0;
                    count += sampleStringList != null && !sampleStringList.isEmpty() ? 1 : 0;
                    count += StringUtils.isNotEmpty(sampleAclParams.getIndividual()) ? 1 : 0;
                    count += StringUtils.isNotEmpty(sampleAclParams.getFamily()) ? 1 : 0;
                    count += StringUtils.isNotEmpty(sampleAclParams.getCohort()) ? 1 : 0;
                    count += StringUtils.isNotEmpty(sampleAclParams.getFile()) ? 1 : 0;

                    if (count > 1) {
                        throw new CatalogException("Update ACL: Only one of these parameters are allowed: sample, individual, family,"
                                + " file or cohort per query.");
                    } else if (count == 0) {
                        throw new CatalogException("Update ACL: At least one of these parameters should be provided: sample, individual,"
                                + " family, file or cohort");
                    }

                    if (action == null) {
                        throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
                    }

                    List<String> permissions = Collections.emptyList();
                    if (StringUtils.isNotEmpty(sampleAclParams.getPermissions())) {
                        permissions = Arrays.asList(sampleAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                        checkPermissions(permissions, SamplePermissions::valueOf);
                    }
                    List<String> sampleStringListCopy = new LinkedList<>();
                    if (CollectionUtils.isNotEmpty(sampleStringList)) {
                        sampleStringListCopy.addAll(sampleStringList);
                    }
                    if (StringUtils.isNotEmpty(sampleAclParams.getIndividual())) {
                        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), sampleAclParams.getIndividual());
                        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key());
                        OpenCGAResult<Individual> indDataResult = catalogManager.getIndividualManager().search(studyStr, query,
                                options, token);

                        Set<String> sampleSet = new HashSet<>();
                        for (Individual individual : indDataResult.getResults()) {
                            sampleSet.addAll(individual.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()));
                        }
                        sampleStringListCopy.addAll(sampleSet);
                    }
                    if (StringUtils.isNotEmpty(sampleAclParams.getFamily())) {
                        OpenCGAResult<Family> familyDataResult = catalogManager.getFamilyManager().get(studyStr,
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
                        sampleStringListCopy.addAll(sampleSet);
                    }
                    if (StringUtils.isNotEmpty(sampleAclParams.getFile())) {
                        // Obtain the samples of the files
                        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.SAMPLE_IDS.key());
                        OpenCGAResult<File> fileDataResult = catalogManager.getFileManager().internalGet(organizationId, study.getUid(),
                                Arrays.asList(StringUtils.split(sampleAclParams.getFile(), ",")), options, userId, false);

                        Set<String> sampleSet = new HashSet<>();
                        for (File file : fileDataResult.getResults()) {
                            sampleSet.addAll(file.getSampleIds());
                        }
                        sampleStringListCopy.addAll(sampleSet);
                    }
                    if (StringUtils.isNotEmpty(sampleAclParams.getCohort())) {
                        Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), sampleAclParams.getCohort());
                        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());
                        OpenCGAResult<Cohort> cohortDataResult = catalogManager.getCohortManager().search(studyStr, query, options,
                                token);

                        Set<String> sampleSet = new HashSet<>();
                        for (Cohort cohort : cohortDataResult.getResults()) {
                            sampleSet.addAll(cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
                        }
                        sampleStringListCopy.addAll(sampleSet);
                    }
                    // Set entry param list with the id's
                    for (String s : sampleStringListCopy) {
                        entryParamList.add(new EntryParam(s, ""));
                    }
                    List<Sample> sampleList = internalGet(organizationId, study.getUid(), sampleStringListCopy, INCLUDE_SAMPLE_IDS,
                            userId, false).getResults();
                    // Set entry param list with the valid id's and uuids
                    entryParamList.clear();
                    for (Sample sample : sampleList) {
                        entryParamList.add(new EntryParam(sample.getId(), sample.getUuid()));
                    }
                    authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

                    // Validate that the members are actually valid members
                    List<String> members;
                    if (memberList != null && !memberList.isEmpty()) {
                        members = Arrays.asList(memberList.split(","));
                    } else {
                        members = Collections.emptyList();
                    }
                    checkMembers(organizationId, study.getUid(), members);
                    authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
                    if (study.getInternal().isFederated()) {
                        try {
                            checkIsNotAFederatedUser(organizationId, members);
                        } catch (CatalogException e) {
                            throw new CatalogException("Cannot provide access to federated users to a federated study.", e);
                        }
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

                        switch (action) {
                            case SET:
                                authorizationManager.setAcls(organizationId, study.getUid(), members, aclParamsList);
                                break;
                            case ADD:
                                authorizationManager.addAcls(organizationId, study.getUid(), members, aclParamsList);
                                break;
                            case REMOVE:
                                authorizationManager.removeAcls(organizationId, members, aclParamsList);
                                break;
                            case RESET:
                                for (AuthorizationManager.CatalogAclParams aclParam : aclParamsList) {
                                    aclParam.setPermissions(null);
                                }
                                authorizationManager.removeAcls(organizationId, members, aclParamsList);
                                break;
                            default:
                                throw new CatalogException("Unexpected error occurred. No valid action found.");
                        }

                        OpenCGAResult<AclEntryList<SamplePermissions>> queryResults = authorizationManager.getAcls(organizationId,
                                study.getUid(), sampleUids, members, Enums.Resource.SAMPLE, SamplePermissions.class);

                        for (int i = 0; i < queryResults.getResults().size(); i++) {
                            queryResults.getResults().get(i).setId(sampleIds.get(i));
                        }
                        aclResultList.append(queryResults);

                    } while (numProcessed < sampleList.size());

                    return aclResultList;
                });
    }

    protected void fixQueryObject(String organizationId, Study study, Query query, String userId) throws CatalogException {
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
        AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, query);

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
                InternalGetDataResult<File> result = catalogManager.getFileManager().internalGet(organizationId, study.getUid(),
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
                InternalGetDataResult<Individual> result = catalogManager.getIndividualManager().internalGet(organizationId, study.getUid(),
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
