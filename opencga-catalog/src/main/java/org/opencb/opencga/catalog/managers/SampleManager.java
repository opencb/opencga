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
import org.opencb.commons.datastore.core.result.Error;
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
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetQueryResult;
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
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
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

    @Override
    public QueryResult<Sample> create(String studyStr, Sample sample, QueryOptions options, String token) throws CatalogException {
        ParamUtils.checkAlias(sample.getId(), "name");
        sample.setSource(ParamUtils.defaultString(sample.getSource(), ""));
        sample.setDescription(ParamUtils.defaultString(sample.getDescription(), ""));
        sample.setType(ParamUtils.defaultString(sample.getType(), ""));
        sample.setPhenotypes(ParamUtils.defaultObject(sample.getPhenotypes(), Collections.emptyList()));
        sample.setAnnotationSets(ParamUtils.defaultObject(sample.getAnnotationSets(), Collections.emptyList()));
        sample.setStats(ParamUtils.defaultObject(sample.getStats(), Collections.emptyMap()));
        sample.setAttributes(ParamUtils.defaultObject(sample.getAttributes(), Collections.emptyMap()));
        sample.setStatus(new Status());
        sample.setCreationDate(TimeUtils.getTime());
        sample.setVersion(1);

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        List<VariableSet> variableSetList = validateNewAnnotationSetsAndExtractVariableSets(study.getUid(), sample.getAnnotationSets());

        // 1. We check everything can be done
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_SAMPLES);

        // We will store the individual information if the individual already exists
        Individual individual = null;

        // if 0, it means there is no individual passed with the sample.
        // if 1, it means there is an individual that already exists, and we will need to update the sample array.
        // if 2, it means the individual does not exist and we will have to create it from scratch containing the current sample.
        int individualInfo = 0;

        if (sample.getIndividual() != null && StringUtils.isNotEmpty(sample.getIndividual().getId())) {
            try {
                QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(studyStr,
                        sample.getIndividual().getId(),
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                IndividualDBAdaptor.QueryParams.UID.key(),
                                IndividualDBAdaptor.QueryParams.SAMPLES.key())),
                        token);

                // Check if the user can update the individual
                authorizationManager.checkIndividualPermission(study.getUid(), individualQueryResult.first().getUid(), userId,
                        IndividualAclEntry.IndividualPermissions.UPDATE);

                individual = individualQueryResult.first();
                individualInfo = 1;

            } catch (CatalogException e) {
                if (e instanceof CatalogAuthorizationException) {
                    throw e;
                }

                // The individual does not exist so we check if the user will be able to create it
                authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);
                individualInfo = 2;
            }
        }

        // 2. We create the sample
        sample.setRelease(catalogManager.getStudyManager().getCurrentRelease(study, userId));
        sample.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE));
        QueryResult<Sample> queryResult = sampleDBAdaptor.insert(study.getUid(), sample, variableSetList, options);
        auditManager.recordCreation(AuditRecord.Resource.sample, queryResult.first().getUid(), userId, queryResult.first(), null, null);

        // 3. We update or create an individual if any..
        // We check if we have to update or create a new individual containing the sample
        if (individualInfo > 0) {
            if (individualInfo == 1) { // Update individual info
                List<Sample> sampleList = new ArrayList<>(individual.getSamples().size() + 1);
                sampleList.addAll(individual.getSamples());
                sampleList.add(queryResult.first());

                ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
                try {
                    individualDBAdaptor.update(individual.getUid(), params, QueryOptions.empty());
                } catch (CatalogDBException e) {
                    logger.error("Internal error. The sample was created but the sample could not be associated to the individual. {}",
                            e.getMessage(), e);
                    queryResult.setErrorMsg("Internal error. The sample was created but the sample could not be associated to the "
                            + "individual. " + e.getMessage());
                }
            } else { // = 2 - Create new individual
                sample.getIndividual().setSamples(Collections.singletonList(queryResult.first()));
                try {
                    catalogManager.getIndividualManager().create(studyStr, sample.getIndividual(), QueryOptions.empty(),
                            token);
                } catch (CatalogException e) {
                    logger.error("Internal error. The sample was created but the individual could not be created. {}", e.getMessage(), e);
                    queryResult.setErrorMsg("Internal error. The sample was created but the individual could not be created. "
                            + e.getMessage());
                }
            }
        }

        return queryResult;
    }

    @Deprecated
    public QueryResult<Sample> create(String studyStr, String name, String source, String description, String type, boolean somatic,
                                      Individual individual, Map<String, Object> stats, Map<String, Object> attributes,
                                      QueryOptions options, String sessionId)
            throws CatalogException {
        Sample sample = new Sample(name, source, individual, null, null, -1, 1, description, type, somatic, Collections.emptyList(),
                Collections.emptyList(), attributes).setStats(stats);
        return create(studyStr, sample, options, sessionId);
    }

    @Deprecated
    public QueryResult<Sample> get(Long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(sampleId), options, sessionId);
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
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        params = ParamUtils.defaultObject(params, ObjectMap::new);

        WriteResult writeResult = new WriteResult("delete");

        String userId;
        Study study;

        StopWatch watch = StopWatch.createStarted();

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the samples to be deleted
        DBIterator<Sample> iterator;
        try {
            if (StringUtils.isNotEmpty(params.getString(Constants.EMPTY_FILES_ACTION))) {
                // Validate the action
                String filesAction = params.getString(Constants.EMPTY_FILES_ACTION);
                params.put(Constants.EMPTY_FILES_ACTION, filesAction.toUpperCase());
                if (!"NONE".equals(filesAction) && !"TRASH".equals(filesAction) && !"DELETE".equals(filesAction)) {
                    throw new CatalogException("Unrecognised " + Constants.EMPTY_FILES_ACTION + " value. Accepted actions are NONE, TRASH,"
                            + " DELETE");
                }
            } else {
                params.put(Constants.EMPTY_FILES_ACTION, "NONE");
            }

            userId = catalogManager.getUserManager().getUserId(sessionId);
            study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                    StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = sampleDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error("Delete sample: {}", e.getMessage(), e);
            writeResult.setError(new Error(-1, null, e.getMessage()));
            writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
            return writeResult;
        }

        long numMatches = 0;
        long numModified = 0;
        List<WriteResult.Fail> failedList = new ArrayList<>();

        String suffixName = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();
        List<Error> warningList = new ArrayList<>();

        while (iterator.hasNext()) {
            Sample sample = iterator.next();
            numMatches += 1;

            try {
                if (checkPermissions) {
                    authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                            SampleAclEntry.SamplePermissions.DELETE);
                }

                // Check if the sample can be deleted
                checkSampleCanBeDeleted(study.getUid(), sample, params.getBoolean(Constants.FORCE, false));

                // Update the files
                Query auxQuery = new Query()
                        .append(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                        .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                DBIterator<File> fileIterator = fileDBAdaptor.iterator(auxQuery, QueryOptions.empty());
                while (fileIterator.hasNext()) {
                    File file = fileIterator.next();

                    // Remove the sample from the file
                    List<Sample> remainingSampleList = new ArrayList<>();
                    for (Sample sampleInFile : file.getSamples()) {
                        if (sampleInFile.getUid() != sample.getUid()) {
                            remainingSampleList.add(sampleInFile);
                        }
                    }

                    ObjectMap fileUpdateParams = new ObjectMap()
                            .append(FileDBAdaptor.QueryParams.SAMPLES.key(), remainingSampleList);
                    catalogManager.getFileManager().unsafeUpdate(study, file, fileUpdateParams, QueryOptions.empty(), userId);

                    // If the file has been left orphan, are we intended to delete it as well?
                    if (remainingSampleList.isEmpty() && !"NONE".equals(params.getString(Constants.EMPTY_FILES_ACTION))) {
                        Query fileQuery = new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
                        ObjectMap fileParams = new ObjectMap();
                        if ("DELETE".equals(params.getString(Constants.EMPTY_FILES_ACTION))) {
                            fileParams.put(FileManager.SKIP_TRASH, true);
                        }

                        WriteResult delete = catalogManager.getFileManager().delete(studyStr, fileQuery, fileParams, sessionId);
                        if (delete.getWarning() != null || delete.getError() != null) {
                            logger.warn("File {} could not be deleted after extracting the sample {}. WriteResult: {}",
                                    file.getId(), sample.getId(), delete);
                            warningList.add(new Error(-1, "", "File " + file.getId() + " could not be deleted after extracting the "
                                    + "sample."));
                        }
                    }
                }

                // Update the cohorts
                auxQuery = new Query()
                        .append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                        .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                DBIterator<Cohort> cohortIterator = cohortDBAdaptor.iterator(auxQuery, QueryOptions.empty());
                while (cohortIterator.hasNext()) {
                    Cohort cohort = cohortIterator.next();

                    // Remove the sample from the cohort
                    List<Sample> remainingSampleList = new ArrayList<>();
                    for (Sample sampleInCohort : cohort.getSamples()) {
                        if (sampleInCohort.getUid() != sample.getUid()) {
                            remainingSampleList.add(sampleInCohort);
                        }
                    }

                    ObjectMap cohortUpdateParams = new ObjectMap()
                            .append(FileDBAdaptor.QueryParams.SAMPLES.key(), remainingSampleList);
                    catalogManager.getCohortManager().unsafeUpdate(study, cohort, cohortUpdateParams, false, QueryOptions.empty(),
                            userId);

                    // If the cohort has been left orphan, are we intended to delete it as well?
                    if (remainingSampleList.isEmpty() && params.getBoolean(Constants.DELETE_EMPTY_COHORTS)) {
                        Query cohortQuery = new Query(CohortDBAdaptor.QueryParams.UID.key(), cohort.getUid());
                        WriteResult delete = catalogManager.getCohortManager().delete(studyStr, cohortQuery, new ObjectMap(), sessionId);
                        if (delete.getWarning() != null || delete.getError() != null) {
                            logger.warn("Cohort {} could not be deleted after extracting the sample {}. WriteResult: {}",
                                    cohort.getId(), sample.getId(), delete);
                            warningList.add(new Error(-1, "", "Cohort " + cohort.getId() + "could not be deleted after extracting the "
                                    + "sample."));
                        }
                    }
                }

                // Update the individual
                auxQuery = new Query()
                        .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                        .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(auxQuery, QueryOptions.empty());
                if (individualQueryResult.getNumResults() > 0) {
                    if (individualQueryResult.getNumResults() > 1) {
                        logger.error("Critical error: More than one individual detected pointing to sample {}. The list of individual ids "
                                        + "are: {}", sample.getId(),
                                individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList()));
                    }
                    for (Individual individual : individualQueryResult.getResult()) {
                        // Remove the sample from the individual
                        List<Sample> remainingSampleList = new ArrayList<>();
                        for (Sample sampleInIndividual : individual.getSamples()) {
                            if (sampleInIndividual.getUid() != sample.getUid()) {
                                remainingSampleList.add(sampleInIndividual);
                            }
                        }

                        ObjectMap individualUpdateParams = new ObjectMap()
                                .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(), remainingSampleList);
                        catalogManager.getIndividualManager().unsafeUpdate(study, individual, individualUpdateParams,
                                QueryOptions.empty(), userId);
                    }
                }

                // Delete the sample
                Query updateQuery = new Query()
                        .append(SampleDBAdaptor.QueryParams.UID.key(), sample.getUid())
                        .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                ObjectMap updateParams = new ObjectMap()
                        .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED)
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sample.getId() + suffixName);
                QueryResult<Long> update = sampleDBAdaptor.update(updateQuery, updateParams, QueryOptions.empty());
                if (update.first() > 0) {
                    numModified += 1;
                    auditManager.recordDeletion(AuditRecord.Resource.sample, sample.getUid(), userId, null, updateParams, null, null);
                } else {
                    failedList.add(new WriteResult.Fail(sample.getId(), "Unknown reason"));
                }
            } catch (Exception e) {
                failedList.add(new WriteResult.Fail(sample.getId(), e.getMessage()));
                logger.debug("Cannot delete sample {}: {}", sample.getId(), e.getMessage(), e);
            }
        }

        writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
        writeResult.setNumMatches(numMatches);
        writeResult.setNumModified(numModified);
        writeResult.setFailed(failedList);
        if (!failedList.isEmpty()) {
            warningList.add(new Error(-1, null, "There are samples that could not be deleted"));
        }
        if (!warningList.isEmpty()) {
            writeResult.setWarning(warningList);
        }

        return writeResult;
    }

    public QueryResult<Sample> updateAnnotationSet(String studyStr, String sampleStr, List<AnnotationSet> annotationSetList,
                                                   ParamUtils.UpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, sampleStr, params, options, token);
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
                                  ParamUtils.CompleteUpdateAction action, QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            return new QueryResult<>(sampleStr, -1, -1, -1, "Nothing to do: The map of annotations is empty", "", Collections.emptyList());
        }
        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATIONS, new AnnotationSet(annotationSetId, "", annotations));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, sampleStr, params, options, token);
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

    @Override
    public QueryResult<Sample> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);
        Sample sample = internalGet(study.getUid(), entryStr, INCLUDE_SAMPLE_IDS, userId).first();

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                    SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkSamplePermission(study.getUid(), sample.getUid(), userId,
                    SampleAclEntry.SamplePermissions.UPDATE);
        }

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> SampleDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }
        if (parameters.containsKey(SampleDBAdaptor.UpdateParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(SampleDBAdaptor.UpdateParams.ID.key()), SampleDBAdaptor.UpdateParams.ID.key());
        }

        if (StringUtils.isNotEmpty(parameters.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key()))) {
            Individual individual = null;

            String individualStr = parameters.getString(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());

            // Look for the individual where the sample is assigned
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid())
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            QueryOptions indOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    IndividualDBAdaptor.QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.SAMPLES.key()));
            QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, indOptions);

            if (NumberUtils.isCreatable(individualStr) && Long.parseLong(individualStr) <= 0) {
                // Take out sample from individual

                if (individualQueryResult.getNumResults() == 1) {
                    individual = individualQueryResult.first();

                    authorizationManager.checkIndividualPermission(study.getUid(), individual.getUid(), userId,
                            IndividualAclEntry.IndividualPermissions.UPDATE);

                    List<Sample> sampleList = new ArrayList<>(individual.getSamples().size() - 1);
                    for (Sample tmpSample : individual.getSamples()) {
                        if (tmpSample.getUid() != sample.getUid()) {
                            sampleList.add(tmpSample);
                        }
                    }

                    individual.setSamples(sampleList);
                } // else - nothing to do

            } else {
                // Obtain the individual where the sample is intended to be associated to
                QueryResult<Individual> newIndividualQueryResult = catalogManager.getIndividualManager().get(studyStr, individualStr,
                        indOptions, token);

                if (newIndividualQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Individual " + individualStr + " not found");
                }

                // Check if the sample is not already assigned to other individual
                if (individualQueryResult.getNumResults() == 1) {
                    if (individualQueryResult.first().getUid() != newIndividualQueryResult.first().getUid()) {
                        throw new CatalogException("Cannot update sample. The sample is already associated to other individual ("
                                + individualQueryResult.first().getUid() + "). Please, first remove the sample from the individual.");
                    }
                } else {
                    individual = newIndividualQueryResult.first();

                    authorizationManager.checkIndividualPermission(study.getUid(), individual.getUid(), userId,
                            IndividualAclEntry.IndividualPermissions.UPDATE);

                    // We can freely assign the sample to the individual
                    List<Sample> sampleList = new ArrayList<>(individual.getSamples().size() + 1);
                    sampleList.addAll(individual.getSamples());

                    // Add current sample
                    sampleList.add(sample);

                    individual.setSamples(sampleList);
                }
            }

            if (individual != null) {
                // We need to update the sample array from the individual
                ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), individual.getSamples());
                try {
                    individualDBAdaptor.update(individual.getUid(), params, QueryOptions.empty());
                } catch (CatalogDBException e) {
                    logger.error("Could not update sample information: {}", e.getMessage(), e);
                    throw new CatalogException("Could not update sample information: " + e.getMessage());
                }
            }

            parameters.remove(SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        }

        List<VariableSet> variableSetList = checkUpdateAnnotationsAndExtractVariableSets(study, sample, parameters, options,
                VariableSet.AnnotableDataModels.SAMPLE, sampleDBAdaptor, userId);

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study, userId));
        }

        QueryResult<Sample> queryResult = sampleDBAdaptor.update(sample.getUid(), parameters, variableSetList, options);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sample.getUid(), userId, parameters, null, null);
        return queryResult;
    }

    @Deprecated
    public QueryResult<Sample> update(Long sampleId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        return update(null, String.valueOf(sampleId), parameters, options, sessionId);
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

    public List<QueryResult<SampleAclEntry>> updateAcl(String studyStr, List<String> sampleList, String memberIds,
                                                       Sample.SampleAclParams sampleAclParams, String sessionId) throws CatalogException {
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        int count = 0;
        count += sampleList != null && !sampleList.isEmpty() ? 1 : 0;
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
            sampleList = new ArrayList<>();
            sampleList.addAll(sampleSet);
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
            sampleList = new ArrayList<>();
            sampleList.addAll(sampleSet);
        }

        if (StringUtils.isNotEmpty(sampleAclParams.getCohort())) {
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), sampleAclParams.getCohort());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());
            QueryResult<Cohort> cohortQueryResult = catalogManager.getCohortManager().get(studyStr, query, options, sessionId);

            Set<String> sampleSet = new HashSet<>();
            for (Cohort cohort : cohortQueryResult.getResult()) {
                sampleSet.addAll(cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            }
            sampleList = new ArrayList<>();
            sampleList.addAll(sampleSet);
        }

        QueryResult<Sample> sampleQueryResult = internalGet(study.getUid(), sampleList, INCLUDE_SAMPLE_IDS, user, false);
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

        List<QueryResult<SampleAclEntry>> queryResults;
        switch (sampleAclParams.getAction()) {
            case SET:
                // Todo: Remove this in 1.4
                List<String> allSamplePermissions = EnumSet.allOf(SampleAclEntry.SamplePermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                queryResults = authorizationManager.setAcls(study.getUid(), sampleQueryResult.getResult().stream()
                                .map(Sample::getUid).collect(Collectors.toList()), members, permissions, allSamplePermissions,
                        Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.SET, StringUtils.join(sampleQueryResult.getResult().stream().map(Sample::getId)
                                .collect(Collectors.toList()), ","), false);

                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            case ADD:
                queryResults = authorizationManager.addAcls(study.getUid(), sampleQueryResult.getResult().stream()
                        .map(Sample::getUid).collect(Collectors.toList()), members, permissions, Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.ADD, StringUtils.join(sampleQueryResult.getResult().stream().map(Sample::getId)
                                .collect(Collectors.toList()), ","), false);
                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            case REMOVE:
                queryResults = authorizationManager.removeAcls(sampleQueryResult.getResult().stream().map(Sample::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.REMOVE, StringUtils.join(sampleQueryResult.getResult().stream().map(Sample::getId)
                                .collect(Collectors.toList()), ","), false);

                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            case RESET:
                queryResults = authorizationManager.removeAcls(sampleQueryResult.getResult().stream().map(Sample::getUid)
                        .collect(Collectors.toList()), members, null, Entity.SAMPLE);
                if (sampleAclParams.isPropagate()) {
                    try {
                        Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(sampleAclParams.getPermissions(),
                                AclParams.Action.RESET, StringUtils.join(sampleQueryResult.getResult().stream().map(Sample::getId)
                                .collect(Collectors.toList()), ","), false);

                        catalogManager.getIndividualManager().updateAcl(studyStr, null, memberIds, aclParams, sessionId);
                    } catch (CatalogException e) {
                        logger.warn("Error propagating permissions to individual: {}", e.getMessage(), e);
                        queryResults.get(0).setWarningMsg("Error propagating permissions to individual: " + e.getMessage());
                    }
                }
                break;
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }

        return queryResults;
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


    // **************************   Private methods  ******************************** //

    void checkCanDeleteSamples(MyResourceIds resources) throws CatalogException {
        for (Long sampleId : resources.getResourceIds()) {
            authorizationManager.checkSamplePermission(resources.getStudyId(), sampleId, resources.getUser(),
                    SampleAclEntry.SamplePermissions.DELETE);
        }

        // Check that the samples are not being used in cohorts
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), resources.getStudyId())
                .append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), resources.getResourceIds())
                .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        long count = cohortDBAdaptor.count(query).first();
        if (count > 0) {
            if (resources.getResourceIds().size() == 1) {
                throw new CatalogException("The sample " + resources.getResourceIds().get(0) + " is part of " + count + " cohorts. Please, "
                        + "first update or delete the cohorts");
            } else {
                throw new CatalogException("Some samples are part of " + count + " cohorts. Please, first update or delete the cohorts");
            }
        }
    }

//    private void addIndividualInformation(QueryResult<Sample> sampleQueryResult, String sessionId) {
//        if (sampleQueryResult.getNumResults() == 0) {
//            return;
//        }
//        String individualIdList = sampleQueryResult.getResult().stream()
//                .map(Sample::getIndividual).filter(Objects::nonNull)
//                .map(Individual::getId).filter(id -> id > 0)
//                .map(String::valueOf)
//                .collect(Collectors.joining(","));
//        if (StringUtils.isEmpty(individualIdList)) {
//            return;
//        }
//        try {
//            QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(null, individualIdList,
//                    QueryOptions.empty(), sessionId);
//
//            // We create a map of individualId - Individual
//            Map<Long, Individual> individualMap = new HashMap<>();
//            for (Individual individual : individualQueryResult.getResult()) {
//                individualMap.put(individual.getId(), individual);
//            }
//
//            // And set the individual information in the sample result
//            for (Sample sample : sampleQueryResult.getResult()) {
//                if (sample.getIndividual() != null && sample.getIndividual().getId() > 0) {
//                    sample.setIndividual(individualMap.get(sample.getIndividual().getId()));
//                }
//            }
//
//        } catch (CatalogException e) {
//            logger.warn("Could not retrieve individual information to complete sample object, {}", e.getMessage(), e);
//            sampleQueryResult.setWarningMsg("Could not retrieve individual information to complete sample object" + e.getMessage());
//        }
//    }

    private long getSampleId(boolean silent, String sampleStrAux) throws CatalogException {
        long sampleId = Long.parseLong(sampleStrAux);
        try {
            sampleDBAdaptor.checkId(sampleId);
        } catch (CatalogException e) {
            if (silent) {
                return -1L;
            } else {
                throw e;
            }
        }
        return sampleId;
    }

    public DBIterator<Sample> indexSolr(Query query) throws CatalogException {
        return sampleDBAdaptor.iterator(query, null, null);
    }
}
