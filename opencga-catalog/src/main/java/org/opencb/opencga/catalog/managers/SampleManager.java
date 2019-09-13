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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.StudyEntry;
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
import org.opencb.opencga.catalog.models.update.SampleUpdateParams;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.audit.AuditRecord.ERROR;
import static org.opencb.opencga.catalog.audit.AuditRecord.SUCCESS;
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
            SampleDBAdaptor.QueryParams.VERSION.key()));

    SampleManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                  Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    QueryResult<Sample> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            queryCopy.put(SampleDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(SampleDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//               SampleDBAdaptor.QueryParams.UUID.key(), SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.STUDY_UID.key(),
//               SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.RELEASE.key(), SampleDBAdaptor.QueryParams.VERSION.key(),
//                SampleDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(queryCopy, queryOptions, user);
        if (sampleQueryResult.getNumResults() == 0) {
            sampleQueryResult = sampleDBAdaptor.get(queryCopy, queryOptions);
            if (sampleQueryResult.getNumResults() == 0) {
                throw new CatalogException("Sample " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the sample " + entry);
            }
        } else if (sampleQueryResult.getNumResults() > 1 && !queryCopy.getBoolean(Constants.ALL_VERSIONS)) {
            throw new CatalogException("More than one sample found based on " + entry);
        } else {
            return sampleQueryResult;
        }
    }

    @Override
    InternalGetQueryResult<Sample> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                               String user, boolean silent) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing sample entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<Sample, String> sampleStringFunction = Sample::getId;
        SampleDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            SampleDBAdaptor.QueryParams param = SampleDBAdaptor.QueryParams.ID;
            if (UUIDUtils.isOpenCGAUUID(entry)) {
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

        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(queryCopy, queryOptions, user);

        if (silent || sampleQueryResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, sampleStringFunction, sampleQueryResult, silent,
                    queryCopy.getBoolean(Constants.ALL_VERSIONS));
        }
        // Query without adding the user check
        QueryResult<Sample> resultsNoCheck = sampleDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == sampleQueryResult.getNumResults()) {
            throw CatalogException.notFound("samples", getMissingFields(uniqueList, sampleQueryResult.getResult(), sampleStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the samples.");
        }
    }

    private QueryResult<Sample> getSample(long studyUid, String sampleUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.UUID.key(), sampleUuid);
        return sampleDBAdaptor.get(query, options);
    }

    void validateNewSample(Study study, Sample sample, String userId) throws CatalogException {
        ParamUtils.checkAlias(sample.getId(), "id");
        sample.setSource(ParamUtils.defaultString(sample.getSource(), ""));
        sample.setDescription(ParamUtils.defaultString(sample.getDescription(), ""));
        sample.setType(ParamUtils.defaultString(sample.getType(), ""));
        sample.setIndividualId(ParamUtils.defaultObject(sample.getIndividualId(), ""));
        sample.setPhenotypes(ParamUtils.defaultObject(sample.getPhenotypes(), Collections.emptyList()));
        sample.setAnnotationSets(ParamUtils.defaultObject(sample.getAnnotationSets(), Collections.emptyList()));
        sample.setStats(ParamUtils.defaultObject(sample.getStats(), Collections.emptyMap()));
        sample.setAttributes(ParamUtils.defaultObject(sample.getAttributes(), Collections.emptyMap()));
        sample.setStatus(new Status());
        sample.setCreationDate(TimeUtils.getTime());
        sample.setVersion(1);
        sample.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
        sample.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE));

        // Check the id is not in use
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(SampleDBAdaptor.QueryParams.ID.key(), sample.getId());
        if (sampleDBAdaptor.count(query).first() > 0) {
            throw new CatalogException("Sample '" + sample.getId() + "' already exists.");
        }

        if (StringUtils.isNotEmpty(sample.getIndividualId())) {
            // Check individual exists
            QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    sample.getIndividualId(), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
            if (individualQueryResult.getNumResults() == 0) {
                throw new CatalogException("Individual '" + sample.getIndividualId() + "' not found.");
            }

            // Just in case the user provided a uuid or other kind of individual identifier, we set again the id value
            sample.setIndividualId(individualQueryResult.first().getId());
        }

        validateNewAnnotationSets(study.getVariableSets(), sample.getAnnotationSets());
    }

    @Override
    public QueryResult<Sample> create(String studyStr, Sample sample, QueryOptions options, String token) throws CatalogException {
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
            QueryResult<Sample> queryResult = getSample(study.getUid(), sample.getUuid(), options);
            auditManager.auditCreate(userId, sample.getId(), sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.SAMPLE, SUCCESS);
            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, sample.getId(), "", study.getId(), study.getUuid(), auditParams, AuditRecord.Entity.SAMPLE,
                    ERROR);
            throw e;
        }
    }

    @Override
    public QueryResult<Sample> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        fixQueryObject(study, query, userId);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, options, userId);

        if (sampleQueryResult.getNumResults() == 0 && query.containsKey(SampleDBAdaptor.QueryParams.UID.key())) {
            List<Long> sampleIds = query.getAsLongList(SampleDBAdaptor.QueryParams.UID.key());
            for (Long sampleId : sampleIds) {
                authorizationManager.checkSamplePermission(study.getUid(), sampleId, userId, SampleAclEntry.SamplePermissions.VIEW);
            }
        }

        return sampleQueryResult;
    }

    @Override
    public DBIterator<Sample> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        return sampleDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<Sample> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        fixQueryObject(study, query, userId);

        query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, options, userId);
        return queryResult;
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            QueryResult<Individual> queryResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    query.getAsStringList(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId,
                    false);
            query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), queryResult.getResult().stream().map(Individual::getUid)
                    .collect(Collectors.toList()));
            query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }
    }

    @Override
    public QueryResult<Sample> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        fixQueryObject(study, query, userId);

        query.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = sampleDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_SAMPLES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        params = ParamUtils.defaultObject(params, ObjectMap::new);

        WriteResult writeResult = new WriteResult();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        Query auditQuery = new Query(query);
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("params", params)
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

            iterator = sampleDBAdaptor.iterator(finalQuery, INCLUDE_SAMPLE_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(userId, operationUuid, "", "", study.getId(), study.getUuid(), auditQuery, auditParams,
                    AuditRecord.Entity.SAMPLE, ERROR);
            throw e;
        }

        while (iterator.hasNext()) {
            Sample sample = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                            SampleAclEntry.SamplePermissions.DELETE);
                }

                // Check if the sample can be deleted
                checkSampleCanBeDeleted(study.getUid(), sample, params.getBoolean(Constants.FORCE, false));

                writeResult.concat(sampleDBAdaptor.delete(sample.getUid()));

                auditManager.auditDelete(userId, operationUuid, sample.getId(), sample.getUuid(), study.getId(), study.getUuid(),
                        auditQuery, auditParams, AuditRecord.Entity.SAMPLE, SUCCESS);
            } catch (Exception e) {
                writeResult.getFailed().add(new WriteResult.Fail(sample.getId(), e.getMessage()));
                logger.debug("Cannot delete sample {}: {}", sample.getId(), e.getMessage(), e);

                auditManager.auditDelete(userId, operationUuid, sample.getId(), sample.getUuid(), study.getId(), study.getUuid(),
                        auditQuery, auditParams, AuditRecord.Entity.SAMPLE, ERROR);
            }
        }

        if (!writeResult.getFailed().isEmpty()) {
            writeResult.setWarning(Collections.singletonList("Some samples could not be deleted"));
        }

        return writeResult;
    }

    public QueryResult<Sample> updateAnnotationSet(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                   ParamUtils.UpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        SampleUpdateParams sampleUpdateParams = new SampleUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, sampleStr, sampleUpdateParams, options, token);
    }

    public QueryResult<Sample> addAnnotationSet(String studyStr, String sampleStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return addAnnotationSets(studyStr, sampleStr, Collections.singletonList(annotationSet), options, token);
    }

    public QueryResult<Sample> addAnnotationSets(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.UpdateAction.ADD, options, token);
    }

    public QueryResult<Sample> setAnnotationSet(String studyStr, String sampleStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return setAnnotationSets(studyStr, sampleStr, Collections.singletonList(annotationSet), options, token);
    }

    public QueryResult<Sample> setAnnotationSets(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.UpdateAction.SET, options, token);
    }

    public QueryResult<Sample> removeAnnotationSet(String studyStr, String sampleStr, String annotationSetId, QueryOptions options,
                                                   String token) throws CatalogException {
        return removeAnnotationSets(studyStr, sampleStr, Collections.singletonList(annotationSetId), options, token);
    }

    public QueryResult<Sample> removeAnnotationSets(String studyStr, String sampleStr, List<String> annotationSetIdList,
                                                    QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, sampleStr, annotationSetList, ParamUtils.UpdateAction.REMOVE, options, token);
    }

    public QueryResult<Sample> updateAnnotations(String studyStr, String sampleStr, String annotationSetId, Map<String, Object> annotations,
                                                 ParamUtils.CompleteUpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            return new QueryResult<>(sampleStr, -1, -1, -1, "Nothing to do: The map of annotations is empty", "", Collections.emptyList());
        }
        SampleUpdateParams sampleUpdateParams = new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, null, annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, sampleStr, sampleUpdateParams, options, token);
    }

    public QueryResult<Sample> removeAnnotations(String studyStr, String sampleStr, String annotationSetId, List<String> annotations,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, sampleStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public QueryResult<Sample> resetAnnotations(String studyStr, String sampleStr, String annotationSetId, List<String> annotations,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, sampleStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    private void checkSampleCanBeDeleted(long studyId, Sample sample, boolean force) throws CatalogException {
        // Look for files related with the sample
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        DBIterator<File> fileIterator = fileDBAdaptor.iterator(query, QueryOptions.empty());
        List<String> errorFiles = new ArrayList<>();
        while (fileIterator.hasNext()) {
            File file = fileIterator.next();
            if (force) {
                // Check index status
                if (file.getIndex() != null && file.getIndex().getStatus() != null
                        && !FileIndex.IndexStatus.NONE.equals(file.getIndex().getStatus().getName())) {
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
                if (cohort.getStatus() != null && Cohort.CohortStatus.CALCULATING.equals(cohort.getStatus().getName())) {
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
            QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(IndividualDBAdaptor.QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.ID.key())));
            if (individualQueryResult.getNumResults() > 0) {
                throw new CatalogException("Sample from individual " + individualQueryResult.first().getName() + "("
                        + individualQueryResult.first().getUid() + ")");
            }
        }
    }

    /**
     * Update a Sample from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sampleId   Sample id in string format. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A QueryResult with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public QueryResult<Sample> update(String studyStr, String sampleId, SampleUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("sampleId", sampleId)
                .append("updateParams", updateParams != null ? updateParams.getUpdateMap() : null)
                .append("options", options)
                .append("token", token);
        Sample sample;
        try {
            sample = internalGet(study.getUid(), sampleId, INCLUDE_SAMPLE_IDS, userId).first();
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, sampleId, "", study.getId(), study.getUuid(), auditParams, AuditRecord.Entity.SAMPLE, ERROR);
            throw e;
        }

        try {
            ObjectMap parameters = new ObjectMap();

            if (updateParams != null) {
                parameters = updateParams.getUpdateMap();
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
                QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                        updateParams.getIndividualId(), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
                if (individualQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Individual '" + updateParams.getIndividualId() + "' not found.");
                }

                // Overwrite individual id parameter just in case the user used a uuid or other individual identifier
                parameters.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualQueryResult.first().getId());
            }

            checkUpdateAnnotations(study, sample, parameters, options, VariableSet.AnnotableDataModels.SAMPLE, sampleDBAdaptor, userId);

            if (options.getBoolean(Constants.INCREMENT_VERSION)) {
                // We do need to get the current release to properly create a new version
                options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study));
            }

            WriteResult result = sampleDBAdaptor.update(sample.getUid(), parameters, study.getVariableSets(), options);
            auditManager.auditUpdate(userId, sample.getId(), sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.SAMPLE, SUCCESS);

            QueryResult<Sample> queryResult = sampleDBAdaptor.get(sample.getUid(),
                    new QueryOptions(QueryOptions.INCLUDE, parameters.keySet()));
            queryResult.setDbTime(queryResult.getDbTime() + result.getDbTime());

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, sample.getId(), sample.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.SAMPLE, ERROR);
            throw e;
        }
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
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
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
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
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        // Add study id to the query
        query.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        if (StringUtils.isNotEmpty(query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            String individualStr = query.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
            if (NumberUtils.isCreatable(individualStr) && Long.parseLong(individualStr) <= 0) {
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), -1);
            } else {
                QueryResult<Individual> queryResult = catalogManager.getIndividualManager().internalGet(study.getUid(), individualStr,
                        IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), queryResult.first().getUid());
            }
            query.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        QueryResult queryResult = sampleDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<SampleAclEntry>> getAcls(String studyStr, List<String> sampleList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        List<QueryResult<SampleAclEntry>> sampleAclList = new ArrayList<>(sampleList.size());
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        InternalGetQueryResult<Sample> queryResult = internalGet(study.getUid(), sampleList, INCLUDE_SAMPLE_IDS, user, silent);

        Map<String, InternalGetQueryResult.Missing> missingMap = new HashMap<>();
        if (queryResult.getMissing() != null) {
            missingMap = queryResult.getMissing().stream()
                    .collect(Collectors.toMap(InternalGetQueryResult.Missing::getId, Function.identity()));
        }
        int counter = 0;
        for (String sampleId : sampleList) {
            if (!missingMap.containsKey(sampleId)) {
                try {
                    QueryResult<SampleAclEntry> allSampleAcls;
                    if (StringUtils.isNotEmpty(member)) {
                        allSampleAcls =
                                authorizationManager.getSampleAcl(study.getUid(), queryResult.getResult().get(counter).getUid(), user,
                                        member);
                    } else {
                        allSampleAcls = authorizationManager.getAllSampleAcls(study.getUid(), queryResult.getResult().get(counter).getUid(),
                                user);
                    }
                    allSampleAcls.setId(sampleId);
                    sampleAclList.add(allSampleAcls);
                } catch (CatalogException e) {
                    if (!silent) {
                        throw e;
                    } else {
                        sampleAclList.add(new QueryResult<>(sampleId, queryResult.getDbTime(), 0, 0, "",
                                missingMap.get(sampleId).getErrorMsg(), Collections.emptyList()));
                    }
                }
                counter += 1;
            } else {
                sampleAclList.add(new QueryResult<>(sampleId, queryResult.getDbTime(), 0, 0, "", missingMap.get(sampleId).getErrorMsg(),
                        Collections.emptyList()));
            }
        }
        return sampleAclList;
    }

    public List<QueryResult<SampleAclEntry>> updateAcl(String studyStr, List<String> sampleStringList, String memberIds,
                                                       Sample.SampleAclParams sampleAclParams, String sessionId) throws CatalogException {
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

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

        if (sampleAclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(sampleAclParams.getPermissions())) {
            permissions = Arrays.asList(sampleAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, SampleAclEntry.SamplePermissions::valueOf);
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getIndividual())) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), sampleAclParams.getIndividual());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key());
            QueryResult<Individual> indQueryResult = catalogManager.getIndividualManager().get(studyStr, query, options, sessionId);

            Set<String> sampleSet = new HashSet<>();
            for (Individual individual : indQueryResult.getResult()) {
                sampleSet.addAll(individual.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()));
            }
            sampleStringList = new ArrayList<>();
            sampleStringList.addAll(sampleSet);
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getFile())) {
//            // Obtain the samples of the files
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.SAMPLES.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().internalGet(study.getUid(),
                    Arrays.asList(StringUtils.split(sampleAclParams.getFile(), ",")), options, user, false);

            Set<String> sampleSet = new HashSet<>();
            for (File file : fileQueryResult.getResult()) {
                sampleSet.addAll(file.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            }
            sampleStringList = new ArrayList<>();
            sampleStringList.addAll(sampleSet);
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getCohort())) {
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), sampleAclParams.getCohort());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());
            QueryResult<Cohort> cohortQueryResult = catalogManager.getCohortManager().get(studyStr, query, options, sessionId);

            Set<String> sampleSet = new HashSet<>();
            for (Cohort cohort : cohortQueryResult.getResult()) {
                sampleSet.addAll(cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            }
            sampleStringList = new ArrayList<>();
            sampleStringList.addAll(sampleSet);
        }

        List<Sample> sampleList = internalGet(study.getUid(), sampleStringList, INCLUDE_SAMPLE_IDS, user, false).getResult();
        authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        checkMembers(study.getUid(), members);
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);

        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        StopWatch stopWatch = new StopWatch();

        List<QueryResult<SampleAclEntry>> aclResultList = new ArrayList<>(sampleList.size());
        int numProcessed = 0;
        do {
            List<Sample> batchSampleList = new ArrayList<>();
            while (numProcessed < Math.min(numProcessed + BATCH_OPERATION_SIZE, sampleList.size())) {
                batchSampleList.add(sampleList.get(numProcessed));
                numProcessed += 1;
            }

            List<Long> sampleUids = batchSampleList.stream().map(Sample::getUid).collect(Collectors.toList());

            Entity entity2 = null;
            List<Long> individualUids = null;
            if (sampleAclParams.isPropagate()) {
                entity2 = Entity.INDIVIDUAL;
                individualUids = getIndividualsUidsFromSampleUids(study.getUid(), sampleUids);
            }

            List<QueryResult<SampleAclEntry>> queryResults;
            switch (sampleAclParams.getAction()) {
                case SET:
                    queryResults = authorizationManager.setAcls(study.getUid(), sampleUids, individualUids, members, permissions,
                            Entity.SAMPLE, entity2);
                    break;
                case ADD:
                    queryResults = authorizationManager.addAcls(study.getUid(), sampleUids, individualUids, members, permissions,
                            Entity.SAMPLE, entity2);
                    break;
                case REMOVE:
                    queryResults = authorizationManager.removeAcls(sampleUids, individualUids, members, permissions, Entity.SAMPLE,
                            entity2);
                    break;
                case RESET:
                    queryResults = authorizationManager.removeAcls(sampleUids, individualUids, members, null, Entity.SAMPLE, entity2);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }
            aclResultList.addAll(queryResults);

            AuditRecord.Result result = new AuditRecord.Result(Math.toIntExact(stopWatch.getTime(TimeUnit.MILLISECONDS)),
                    batchSampleList.size(), queryResults.size(), "", "");
            stopWatch.reset();

        } while (numProcessed < sampleList.size());

        return aclResultList;
    }

    private List<Long> getIndividualsUidsFromSampleUids(long studyUid, List<Long> sampleUids) throws CatalogDBException {
        // Look for all the individuals owning the samples
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleUids);

        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, IndividualManager.INCLUDE_INDIVIDUAL_IDS);

        return individualQueryResult.getResult().stream().map(Individual::getUid).collect(Collectors.toList());
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

        return catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION, query, queryOptions, userId);
    }


}
