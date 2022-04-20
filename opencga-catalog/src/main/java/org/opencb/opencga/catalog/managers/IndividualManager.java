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
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.core.OntologyTermAnnotation;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
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
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
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
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualManager extends AnnotationSetManager<Individual> {

    protected static Logger logger = LoggerFactory.getLogger(IndividualManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    private final String defaultFacet = "creationYear>>creationMonth;status;ethnicity;population;lifeStatus;phenotypes;sex;"
            + "numSamples[0..10]:1";

    public static final QueryOptions INCLUDE_INDIVIDUAL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            IndividualDBAdaptor.QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.UUID.key(),
            IndividualDBAdaptor.QueryParams.VERSION.key(), IndividualDBAdaptor.QueryParams.FATHER.key(),
            IndividualDBAdaptor.QueryParams.MOTHER.key(), IndividualDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final QueryOptions INCLUDE_INDIVIDUAL_DISORDERS_PHENOTYPES = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            IndividualDBAdaptor.QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.UUID.key(),
            IndividualDBAdaptor.QueryParams.VERSION.key(), IndividualDBAdaptor.QueryParams.FATHER.key(),
            IndividualDBAdaptor.QueryParams.MOTHER.key(), IndividualDBAdaptor.QueryParams.DISORDERS.key(),
            IndividualDBAdaptor.QueryParams.PHENOTYPES.key(), IndividualDBAdaptor.QueryParams.SEX.key(),
            IndividualDBAdaptor.QueryParams.STUDY_UID.key()));

    private static final Map<IndividualProperty.KaryotypicSex, IndividualProperty.Sex> KARYOTYPIC_SEX_SEX_MAP;

    static {
        KARYOTYPIC_SEX_SEX_MAP = new HashMap<>();
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.UNKNOWN, IndividualProperty.Sex.UNKNOWN);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XX, IndividualProperty.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XO, IndividualProperty.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XXX, IndividualProperty.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XXXX, IndividualProperty.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XY, IndividualProperty.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XXY, IndividualProperty.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XXYY, IndividualProperty.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XXXY, IndividualProperty.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.XYY, IndividualProperty.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(IndividualProperty.KaryotypicSex.OTHER, IndividualProperty.Sex.UNDETERMINED);
    }

    IndividualManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                      DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.INDIVIDUAL;
    }

//    @Override
//    OpenCGAResult<Individual> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
//            throws CatalogException {
//        ParamUtils.checkIsSingleID(entry);
//        Query queryCopy = query == null ? new Query() : new Query(query);
//        queryCopy.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
//
//        if (UuidUtils.isOpenCgaUuid(entry)) {
//            queryCopy.put(IndividualDBAdaptor.QueryParams.UUID.key(), entry);
//        } else {
//            queryCopy.put(IndividualDBAdaptor.QueryParams.ID.key(), entry);
//        }
//        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//        OpenCGAResult<Individual> individualDataResult = individualDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
//        if (individualDataResult.getNumResults() == 0) {
//            individualDataResult = individualDBAdaptor.get(queryCopy, queryOptions);
//            if (individualDataResult.getNumResults() == 0) {
//                throw new CatalogException("Individual " + entry + " not found");
//            } else {
//                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the individual " + entry);
//            }
//        } else if (individualDataResult.getNumResults() > 1 && !queryCopy.getBoolean(Constants.ALL_VERSIONS)) {
//            throw new CatalogException("More than one individual found based on " + entry);
//        } else {
//            return individualDataResult;
//        }
//    }

    @Override
    InternalGetDataResult<Individual> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                                  String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing individual entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(IndividualDBAdaptor.QueryParams.VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one individual allowed when requesting multiple versions");
        }

        IndividualDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Individual> individualDataResult = individualDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        Function<Individual, String> individualStringFunction = Individual::getId;
        if (idQueryParam.equals(IndividualDBAdaptor.QueryParams.UUID)) {
            individualStringFunction = Individual::getUuid;
        }
        if (ignoreException || individualDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, individualStringFunction, individualDataResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Individual> resultsNoCheck = individualDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == individualDataResult.getNumResults()) {
            throw CatalogException.notFound("individuals",
                    getMissingFields(uniqueList, individualDataResult.getResults(), individualStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the"
                    + " individuals.");
        }
    }

    IndividualDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        IndividualDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            IndividualDBAdaptor.QueryParams param = IndividualDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = IndividualDBAdaptor.QueryParams.UUID;
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

    private OpenCGAResult<Individual> getIndividual(long studyUid, String individualUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(IndividualDBAdaptor.QueryParams.UUID.key(), individualUuid);
        return individualDBAdaptor.get(query, options);
    }

    void validateNewIndividual(Study study, Individual individual, List<String> samples, String userId, boolean linkParents)
            throws CatalogException {
        ParamUtils.checkIdentifier(individual.getId(), "id");
        individual.setName(StringUtils.isEmpty(individual.getName()) ? individual.getId() : individual.getName());
        individual.setLocation(ParamUtils.defaultObject(individual.getLocation(), Location::new));
        individual.setEthnicity(ParamUtils.defaultObject(individual.getEthnicity(), OntologyTermAnnotation::new));
        individual.setPopulation(ParamUtils.defaultObject(individual.getPopulation(), IndividualPopulation::new));
        individual.setLifeStatus(ParamUtils.defaultObject(individual.getLifeStatus(), IndividualProperty.LifeStatus.UNKNOWN));
        individual.setKaryotypicSex(ParamUtils.defaultObject(individual.getKaryotypicSex(), IndividualProperty.KaryotypicSex.UNKNOWN));
        individual.setSex(ParamUtils.defaultObject(individual.getSex(), SexOntologyTermAnnotation::initUnknown));
        individual.setPhenotypes(ParamUtils.defaultObject(individual.getPhenotypes(), Collections.emptyList()));
        individual.setDisorders(ParamUtils.defaultObject(individual.getDisorders(), Collections.emptyList()));
        individual.setAnnotationSets(ParamUtils.defaultObject(individual.getAnnotationSets(), Collections.emptyList()));
        individual.setAttributes(ParamUtils.defaultObject(individual.getAttributes(), Collections.emptyMap()));
        individual.setSamples(ParamUtils.defaultObject(individual.getSamples(), new ArrayList<>()));
        individual.setStatus(ParamUtils.defaultObject(individual.getStatus(), Status::new));
        individual.setQualityControl(ParamUtils.defaultObject(individual.getQualityControl(), IndividualQualityControl::new));

        individual.setInternal(IndividualInternal.init());
        individual.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(individual.getCreationDate(),
                IndividualDBAdaptor.QueryParams.CREATION_DATE.key()));
        individual.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(individual.getModificationDate(),
                IndividualDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        individual.setModificationDate(TimeUtils.getTime());
        individual.setRelease(studyManager.getCurrentRelease(study));
        individual.setVersion(1);
        individual.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.INDIVIDUAL));

        if (CollectionUtils.isNotEmpty(individual.getFamilyIds())) {
            throw new CatalogException("'familyIds' list is not empty");
        } else {
            individual.setFamilyIds(Collections.emptyList());
        }

        // Check the id is not in use
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(IndividualDBAdaptor.QueryParams.ID.key(), individual.getId());
        if (individualDBAdaptor.count(query).getNumMatches() > 0) {
            throw new CatalogException("Individual '" + individual.getId() + "' already exists.");
        }

        validateNewAnnotationSets(study.getVariableSets(), individual.getAnnotationSets());
        validateSamples(study, individual, samples, userId);

        if (linkParents) {
            if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getId())) {
                OpenCGAResult<Individual> fatherResult = internalGet(study.getUid(), individual.getFather().getId(), INCLUDE_INDIVIDUAL_IDS,
                        userId);
                individual.setFather(fatherResult.first());
            }
            if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getId())) {
                OpenCGAResult<Individual> motherResult = internalGet(study.getUid(), individual.getMother().getId(), INCLUDE_INDIVIDUAL_IDS,
                        userId);
                individual.setMother(motherResult.first());
            }
        }
    }

    private void validateSamples(Study study, Individual individual, List<String> samples, String userId) throws CatalogException {
        List<Sample> sampleList = new ArrayList<>();

        if (individual.getSamples() != null && !individual.getSamples().isEmpty()) {
            // Check the user can create new samples
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_SAMPLES);

            // Validate the samples can be created and are valid
            for (Sample sample : individual.getSamples()) {
                catalogManager.getSampleManager().validateNewSample(study, sample, userId);
                sampleList.add(sample);
            }
        }

        if (samples != null && !samples.isEmpty()) {
            // We remove any possible duplicate
            ArrayList<String> deduplicatedSampleIds = new ArrayList<>(new HashSet<>(samples));

            InternalGetDataResult<Sample> sampleDataResult = catalogManager.getSampleManager().internalGet(study.getUid(),
                    deduplicatedSampleIds, SampleManager.INCLUDE_SAMPLE_IDS, userId, false);

            // Check the samples are not attached to other individual
            Set<Long> sampleUidSet = sampleDataResult.getResults().stream().map(Sample::getUid).collect(Collectors.toSet());

            checkSamplesNotInUseInOtherIndividual(sampleUidSet, study.getUid(), null);

            sampleList.addAll(sampleDataResult.getResults());
        }

        individual.setSamples(sampleList);
    }

    @Override
    public OpenCGAResult<Individual> create(String studyStr, Individual individual, QueryOptions options, String token)
            throws CatalogException {
        return create(studyStr, individual, null, options, token);
    }

    public OpenCGAResult<Individual> create(String studyStr, Individual individual, List<String> sampleIds, QueryOptions options,
                                            String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("individual", individual)
                .append("sampleIds", sampleIds)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);
            validateNewIndividual(study, individual, sampleIds, userId, true);

            // Create the individual
            OpenCGAResult<Individual> insert = individualDBAdaptor.insert(study.getUid(), individual, study.getVariableSets(), options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created individual
                OpenCGAResult<Individual> queryResult = getIndividual(study.getUid(), individual.getUuid(), options);
                insert.setResults(queryResult.getResults());
            }
            auditManager.auditCreate(userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.INDIVIDUAL, individual.getId(), "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void checkSamplesNotInUseInOtherIndividual(Set<Long> sampleIds, long studyId, Long individualId)
            throws CatalogException {
        // Check if any of the existing samples already belong to an individual
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleIds)
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                IndividualDBAdaptor.QueryParams.SAMPLES.key(), IndividualDBAdaptor.QueryParams.UID.key()));
        OpenCGAResult<Individual> queryResult = individualDBAdaptor.get(query, options);
        if (queryResult.getNumResults() > 0) {
            // Check which of the samples are already associated to an individual
            List<String> usedSamples = new ArrayList<>();
            for (Individual individual1 : queryResult.getResults()) {
                if (individualId != null && individualId == individual1.getUid()) {
                    continue;
                }
                if (individual1.getSamples() != null) {
                    for (Sample sample : individual1.getSamples()) {
                        if (sampleIds.contains(sample.getUid())) {
                            usedSamples.add(sample.getId());
                        }
                    }
                }
            }

            if (usedSamples.size() > 0) {
                throw new CatalogException("Cannot associate some of the samples to the individual. Samples belonging to other "
                        + "individuals: " + StringUtils.join(usedSamples, ", "));
            }
        }
    }

    @Override
    public DBIterator<Individual> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        Query finalQuery = new Query(query);
        fixQuery(study, finalQuery, userId);
        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        finalQuery.append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return individualDBAdaptor.iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<Individual> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        Query finalQuery = new Query(query);
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
            fixQuery(study, finalQuery, userId);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            AnnotationUtils.fixQueryOptionAnnotation(options);

            finalQuery.append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Individual> queryResult = individualDBAdaptor.get(study.getUid(), finalQuery, options, userId);

            auditManager.auditSearch(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
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
            IndividualDBAdaptor.QueryParams param = IndividualDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQuery(study, query, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);

            query.append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = individualDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Individual> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        Query finalQuery = new Query(query);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", query)
                .append("token", token);
        try {
            fixQuery(study, finalQuery, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);

            finalQuery.append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = individualDBAdaptor.count(finalQuery, userId);

            auditManager.auditCount(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Individual> relatives(String studyId, String individualId, int degree, QueryOptions options, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("individualId", individualId)
                .append("degree", degree)
                .append("options", options)
                .append("token", token);

        String individualUuid = individualId;
        try {
            long startTime = System.currentTimeMillis();

            QueryOptions queryOptions = individualDBAdaptor.fixOptionsForRelatives(options);

            if (degree < 0 || degree > 2) {
                throw new CatalogException("Unsupported degree value. Degree must be 0, 1 or 2");
            }

            List<Individual> individualList = new LinkedList<>();
            Individual proband = internalGet(study.getUid(), individualId, queryOptions, userId).first();
            individualDBAdaptor.addRelativeToList(proband, Family.FamiliarRelationship.PROBAND, 0, individualList);

            individualId = proband.getId();
            individualUuid = proband.getUuid();

            if (degree == 0) {
                auditManager.audit(userId, Enums.Action.RELATIVES, Enums.Resource.INDIVIDUAL, individualId, individualUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), individualList.size(),
                        individualList, individualList.size());
            }

            individualList.addAll(individualDBAdaptor.calculateRelationship(study.getUid(), proband, degree, userId));

            auditManager.audit(userId, Enums.Action.RELATIVES, Enums.Resource.INDIVIDUAL, individualId, individualUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), individualList.size(),
                    individualList, individualList.size());

        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.RELATIVES, Enums.Resource.INDIVIDUAL, individualId, individualUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> individualIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, individualIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> individualIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("individualIds", individualIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.INDIVIDUAL, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        OpenCGAResult result = OpenCGAResult.empty();
        for (String id : individualIds) {
            String individualId = id;
            String individualUuid = "";

            try {
                OpenCGAResult<Individual> internalResult = internalGet(study.getUid(), id, INCLUDE_INDIVIDUAL_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Individual '" + id + "' not found");
                }

                Individual individual = internalResult.first();
                // We set the proper values for the audit
                individualId = individual.getId();
                individualUuid = individual.getUuid();

                OpenCGAResult deleteResult = delete(study, individual, params, userId, checkPermissions);

                // Add the results to the current write result
                result.append(deleteResult);

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete individual " + individualId + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, individualId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg, e);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.INDIVIDUAL, individualId, individualUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
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

        // We try to get an iterator containing all the individuals to be deleted
        DBIterator<Individual> iterator;
        try {
            // Fix query if it contains any annotation
            fixQuery(study, finalQuery, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);

            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = individualDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_INDIVIDUAL_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.INDIVIDUAL, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            Individual individual = iterator.next();

            try {
                OpenCGAResult deleteResult = delete(study, individual, params, userId, checkPermissions);

                // Add the results to the current write result
                result.append(deleteResult);

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete individual " + individual.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, individual.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult delete(Study study, Individual individual, ObjectMap params, String userId, boolean checkPermissions)
            throws CatalogException {
        if (checkPermissions) {
            authorizationManager.checkIndividualPermission(study.getUid(), individual.getUid(), userId,
                    IndividualAclEntry.IndividualPermissions.DELETE);
        }

        // Get the families the individual is a member of
        Query tmpQuery = new Query()
                .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individual.getUid())
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        OpenCGAResult<Family> familyDataResult = familyDBAdaptor.get(tmpQuery, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.ID.key(),
                        FamilyDBAdaptor.QueryParams.MEMBERS.key())));

        // Check if the individual can be deleted
        if (!params.getBoolean(Constants.FORCE, false)) {
            if (familyDataResult.getNumResults() > 0) {
                throw new CatalogException("Individual found in the families: " + familyDataResult.getResults()
                        .stream()
                        .map(Family::getId)
                        .collect(Collectors.joining(", ")));
            }
        } else {
            logger.info("Forcing deletion of individuals belonging to families");
        }

        return individualDBAdaptor.delete(individual);
    }

    public OpenCGAResult<Individual> updateAnnotationSet(String studyStr, String individualStr, List<AnnotationSet> annotationSetList,
                                                         ParamUtils.BasicUpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        IndividualUpdateParams individualUpdateParams = new IndividualUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, individualStr, individualUpdateParams, options, token);
    }

    public OpenCGAResult<Individual> addAnnotationSet(String studyStr, String individualStr, AnnotationSet annotationSet,
                                                      QueryOptions options, String token) throws CatalogException {
        return addAnnotationSets(studyStr, individualStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Individual> addAnnotationSets(String studyStr, String individualStr, List<AnnotationSet> annotationSetList,
                                                       QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, individualStr, annotationSetList, ParamUtils.BasicUpdateAction.ADD, options, token);
    }

    public OpenCGAResult<Individual> setAnnotationSet(String studyStr, String individualStr, AnnotationSet annotationSet,
                                                      QueryOptions options, String token) throws CatalogException {
        return setAnnotationSets(studyStr, individualStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Individual> setAnnotationSets(String studyStr, String individualStr, List<AnnotationSet> annotationSetList,
                                                       QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, individualStr, annotationSetList, ParamUtils.BasicUpdateAction.SET, options, token);
    }

    public OpenCGAResult<Individual> removeAnnotationSet(String studyStr, String individualStr, String annotationSetId,
                                                         QueryOptions options, String token) throws CatalogException {
        return removeAnnotationSets(studyStr, individualStr, Collections.singletonList(annotationSetId), options, token);
    }

    public OpenCGAResult<Individual> removeAnnotationSets(String studyStr, String individualStr, List<String> annotationSetIdList,
                                                          QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, individualStr, annotationSetList, ParamUtils.BasicUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Individual> updateAnnotations(String studyStr, String individualStr, String annotationSetId,
                                                       Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                       QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            throw new CatalogException("Missing array of annotations.");
        }
        IndividualUpdateParams individualUpdateParams = new IndividualUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, "", annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, individualStr, individualUpdateParams, options, token);
    }

    public OpenCGAResult<Individual> removeAnnotations(String studyStr, String individualStr, String annotationSetId,
                                                       List<String> annotations, QueryOptions options, String token)
            throws CatalogException {
        return updateAnnotations(studyStr, individualStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Individual> resetAnnotations(String studyStr, String individualStr, String annotationSetId,
                                                      List<String> annotations, QueryOptions options, String token)
            throws CatalogException {
        return updateAnnotations(studyStr, individualStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    public OpenCGAResult<Individual> update(String studyStr, Query query, IndividualUpdateParams updateParams, QueryOptions options,
                                            String token) throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Individual> update(String studyStr, Query query, IndividualUpdateParams updateParams, boolean ignoreException,
                                            QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse IndividualUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        DBIterator<Individual> iterator;
        try {
            fixQuery(study, finalQuery, userId);

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);

            finalQuery.append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = individualDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_INDIVIDUAL_IDS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.INDIVIDUAL, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Individual> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Individual individual = iterator.next();
            try {
                OpenCGAResult updateResult = update(study, individual, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, individual.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot update individual {}: {}", individual.getId(), e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Individual> update(String studyStr, String individualId, IndividualUpdateParams updateParams, QueryOptions options,
                                            String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse IndividualUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("individualId", individualId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Individual> result = OpenCGAResult.empty();
        String individualUuid = "";

        try {
            OpenCGAResult<Individual> internalResult = internalGet(study.getUid(), individualId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Individual '" + individualId + "' not found");
            }
            Individual individual = internalResult.first();

            // We set the proper values for the audit
            individualId = individual.getId();
            individualUuid = individual.getUuid();

            OpenCGAResult updateResult = update(study, individual, updateParams, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, individualId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Cannot update individual {}: {}", individualId, e.getMessage());
            auditManager.auditUpdate(operationId, userId, Enums.Resource.INDIVIDUAL, individualId, individualUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    /**
     * Update an Individual from catalog.
     *
     * @param studyStr      Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param individualIds List of individual ids. Could be either the id or uuid.
     * @param updateParams  Data model filled only with the parameters to be updated.
     * @param options       QueryOptions object.
     * @param token         Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Individual> update(String studyStr, List<String> individualIds, IndividualUpdateParams updateParams,
                                            QueryOptions options, String token) throws CatalogException {
        return update(studyStr, individualIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Individual> update(String studyStr, List<String> individualIds, IndividualUpdateParams updateParams,
                                            boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse IndividualUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("individualIds", individualIds)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Individual> result = OpenCGAResult.empty();
        for (String id : individualIds) {
            String individualId = id;
            String individualUuid = "";

            try {
                OpenCGAResult<Individual> internalResult = internalGet(study.getUid(), id, QueryOptions.empty(), userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Individual '" + id + "' not found");
                }
                Individual individual = internalResult.first();

                // We set the proper values for the audit
                individualId = individual.getId();
                individualUuid = individual.getUuid();

                OpenCGAResult updateResult = update(study, individual, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(userId, Enums.Resource.INDIVIDUAL, individual.getId(), individual.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot update individual {}: {}", individualId, e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.INDIVIDUAL, individualId, individualUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult update(Study study, Individual individual, IndividualUpdateParams updateParams, QueryOptions options,
                                 String userId) throws CatalogException {
        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse IndividualUpdateParams object: " + e.getMessage(), e);
            }
        }

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (StringUtils.isNotEmpty(parameters.getString(IndividualDBAdaptor.QueryParams.CREATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(IndividualDBAdaptor.QueryParams.CREATION_DATE.key()),
                    IndividualDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (StringUtils.isNotEmpty(parameters.getString(IndividualDBAdaptor.QueryParams.MODIFICATION_DATE.key()))) {
            ParamUtils.checkDateFormat(parameters.getString(IndividualDBAdaptor.QueryParams.MODIFICATION_DATE.key()),
                    IndividualDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

        if (parameters.isEmpty() && !options.getBoolean(Constants.INCREMENT_VERSION, false)) {
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

        long studyUid = study.getUid();
        long individualUid = individual.getUid();

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (updateParams != null && updateParams.getAnnotationSets() != null) {
            authorizationManager.checkIndividualPermission(studyUid, individualUid, userId,
                    IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkIndividualPermission(studyUid, individualUid, userId,
                    IndividualAclEntry.IndividualPermissions.WRITE);
        }

        if (updateParams != null && StringUtils.isNotEmpty(updateParams.getId())) {
            ParamUtils.checkIdentifier(updateParams.getId(), "id");

            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(IndividualDBAdaptor.QueryParams.ID.key(), updateParams.getId());
            if (individualDBAdaptor.count(query).getNumMatches() > 0) {
                throw new CatalogException("Individual name " + updateParams.getId() + " already in use");
            }
        }
        if (updateParams != null && updateParams.getDateOfBirth() != null) {
            if (StringUtils.isEmpty(updateParams.getDateOfBirth())) {
                parameters.put(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), "");
            } else {
                if (!TimeUtils.isValidFormat("yyyyMMdd", updateParams.getDateOfBirth())) {
                    throw new CatalogException("Invalid date of birth format. Valid format yyyyMMdd");
                }
            }
        }
        if (updateParams != null && CollectionUtils.isNotEmpty(updateParams.getSamples())) {
            // Check those samples can be used
            List<String> idList = new ArrayList<>(updateParams.getSamples().size());
            for (SampleReferenceParam sample : updateParams.getSamples()) {
                if (StringUtils.isNotEmpty(sample.getId())) {
                    idList.add(sample.getId());
                } else if (StringUtils.isNotEmpty(sample.getUuid())) {
                    idList.add(sample.getUuid());
                } else {
                    throw new CatalogException("Missing id or uuid in 'samples'");
                }
            }
            List<Sample> sampleList = catalogManager.getSampleManager().internalGet(studyUid, idList, SampleManager.INCLUDE_SAMPLE_IDS,
                    userId, false).getResults();

            // Check those samples are not in use by other individuals
            checkSamplesNotInUseInOtherIndividual(sampleList.stream().map(Sample::getUid).collect(Collectors.toSet()), studyUid,
                    individualUid);

            // Pass the DBAdaptor the corresponding list of Sample objects
            parameters.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
        }

        if (updateParams != null && updateParams.getFather() != null) {
            if (StringUtils.isNotEmpty(updateParams.getFather().getId()) || StringUtils.isNotEmpty(updateParams.getFather().getUuid())) {
                String fatherId = StringUtils.isNotEmpty(updateParams.getFather().getId())
                        ? updateParams.getFather().getId() : updateParams.getFather().getUuid();
                OpenCGAResult<Individual> queryResult = internalGet(studyUid, fatherId, INCLUDE_INDIVIDUAL_IDS, userId);
                parameters.put(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), queryResult.first().getUid());
            } else {
                parameters.put(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), -1L);
            }
            parameters.remove(IndividualDBAdaptor.QueryParams.FATHER.key());
        }
        if (updateParams != null && updateParams.getMother() != null) {
            if (StringUtils.isNotEmpty(updateParams.getMother().getId()) || StringUtils.isNotEmpty(updateParams.getMother().getUuid())) {
                String motherId = StringUtils.isNotEmpty(updateParams.getMother().getId())
                        ? updateParams.getMother().getId() : updateParams.getMother().getUuid();
                OpenCGAResult<Individual> queryResult = internalGet(studyUid, motherId, INCLUDE_INDIVIDUAL_IDS, userId);
                parameters.put(IndividualDBAdaptor.QueryParams.MOTHER_UID.key(), queryResult.first().getUid());
            } else {
                parameters.put(IndividualDBAdaptor.QueryParams.MOTHER_UID.key(), -1L);
            }
            parameters.remove(IndividualDBAdaptor.QueryParams.MOTHER.key());
        }

        checkUpdateAnnotations(study, individual, parameters, options, VariableSet.AnnotableDataModels.INDIVIDUAL, individualDBAdaptor,
                userId);

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study));
        }

        OpenCGAResult<Individual> update = individualDBAdaptor.update(individual.getUid(), parameters, study.getVariableSets(), options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated individual
            OpenCGAResult<Individual> result = individualDBAdaptor.get(study.getUid(),
                    new Query(IndividualDBAdaptor.QueryParams.UID.key(), individual.getUid()), options, userId);
            update.setResults(result.getResults());
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

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogIndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = individualDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        Query finalQuery = new Query(query);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, finalQuery, authorizationManager);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        try {
            fixQuery(study, finalQuery, userId);
        } catch (CatalogException e) {
            // Any of mother, father or sample ids or names do not exist or were not found
            return OpenCGAResult.empty();
        }

        // Add study id to the query
        finalQuery.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = individualDBAdaptor.groupBy(finalQuery, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }


    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> individualList, String member,
                                                            boolean ignoreException, String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("individualList", individualList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);
        try {
            OpenCGAResult<Map<String, List<String>>> individualAclList = OpenCGAResult.empty();
            InternalGetDataResult<Individual> queryResult = internalGet(study.getUid(), individualList, INCLUDE_INDIVIDUAL_IDS, user,
                    ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String individualId : individualList) {
                if (!missingMap.containsKey(individualId)) {
                    Individual individual = queryResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> allIndividualAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allIndividualAcls = authorizationManager.getIndividualAcl(study.getUid(), individual.getUid(), user, member);
                        } else {
                            allIndividualAcls = authorizationManager.getAllIndividualAcls(study.getUid(), individual.getUid(), user);
                        }
                        individualAclList.append(allIndividualAcls);

                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.INDIVIDUAL,
                                individual.getId(), individual.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.INDIVIDUAL,
                                individual.getId(), individual.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, individualId, missingMap.get(individualId).getErrorMsg());
                            individualAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, individualId, missingMap.get(individualId).getErrorMsg());
                    individualAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.INDIVIDUAL, individualId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(individualId).getErrorMsg())), new ObjectMap());
                }
            }
            return individualAclList;
        } catch (CatalogException e) {
            for (String individualId : individualList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.INDIVIDUAL, individualId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, List<String> individualStrList, String memberList,
                                                              IndividualAclParams aclParams, ParamUtils.AclAction action, boolean propagate,
                                                              String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, StudyManager.INCLUDE_STUDY_UID);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("individualStrList", individualStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("propagate", propagate)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            int count = 0;
            count += individualStrList != null && !individualStrList.isEmpty() ? 1 : 0;
            count += StringUtils.isNotEmpty(aclParams.getSample()) ? 1 : 0;

            if (count > 1) {
                throw new CatalogException("Update ACL: Only one of these parameters are allowed: individual or sample per query.");
            } else if (count == 0) {
                throw new CatalogException("Update ACL: At least one of these parameters should be provided: individual or sample");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);
            }

            if (StringUtils.isNotEmpty(aclParams.getSample())) {
                Query query = new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), aclParams.getSample());
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
                OpenCGAResult<Individual> indDataResult = catalogManager.getIndividualManager().search(studyId, query, options, token);

                individualStrList = indDataResult.getResults().stream().map(Individual::getId).collect(Collectors.toList());
            }

            // Obtain the resource ids
            List<Individual> individualList = internalGet(study.getUid(), individualStrList, INCLUDE_INDIVIDUAL_IDS, userId, false)
                    .getResults();

            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), userId);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(study.getUid(), members);

            List<Long> individualUids = individualList.stream().map(Individual::getUid).collect(Collectors.toList());
            List<AuthorizationManager.CatalogAclParams> aclParamsList = new LinkedList<>();
            aclParamsList.add(new AuthorizationManager.CatalogAclParams(individualUids, permissions, Enums.Resource.INDIVIDUAL));

            if (propagate) {
                List<Long> sampleUids = getSampleUidsFromIndividuals(study.getUid(), individualUids);
                aclParamsList.add(new AuthorizationManager.CatalogAclParams(sampleUids, permissions, Enums.Resource.SAMPLE));
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
                    for (AuthorizationManager.CatalogAclParams catalogAclParams : aclParamsList) {
                        catalogAclParams.setPermissions(null);
                    }
                    queryResults = authorizationManager.removeAcls(members, aclParamsList);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (Individual individual : individualList) {
                auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.INDIVIDUAL, individual.getId(),
                        individual.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResults;
        } catch (CatalogException e) {
            if (individualStrList != null) {
                for (String individualId : individualStrList) {
                    auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.INDIVIDUAL, individualId,
                            "", study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

    public DataResult<FacetField> facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
            throws CatalogException, IOException {
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

            try (CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager)) {
                AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

                DataResult<FacetField> result = catalogSolrManager.facetedQuery(study, CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION, query,
                        options, userId);
                auditManager.auditFacet(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                return result;
            }
        } catch (CatalogException e) {
            auditManager.auditFacet(userId, Enums.Resource.INDIVIDUAL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage())));
            throw e;
        }
    }

    // **************************   Private methods  ******************************** //

    private List<Long> getSampleUidsFromIndividuals(long studyUid, List<Long> individualUidList) throws CatalogException {
        // Look for all the samples belonging to the individual
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(IndividualDBAdaptor.QueryParams.UID.key(), individualUidList);

        OpenCGAResult<Individual> individualDataResult = individualDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key()));

        List<Long> sampleUids = new ArrayList<>();
        for (Individual individual : individualDataResult.getResults()) {
            sampleUids.addAll(individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()));
        }

        return sampleUids;
    }

    // Checks if father or mother are in query and transforms them into father.id and mother.id respectively

    private void fixQuery(Study study, Query query, String userId) throws CatalogException {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.INDIVIDUAL_SEX_PARAM, IndividualDBAdaptor.QueryParams.SEX_ID.key());
        changeQueryId(query, ParamConstants.INDIVIDUAL_ETHNICITY_PARAM, IndividualDBAdaptor.QueryParams.ETHNICITY_ID.key());
        changeQueryId(query, ParamConstants.INDIVIDUAL_POPULATION_NAME_PARAM, IndividualDBAdaptor.QueryParams.POPULATION_NAME.key());
        changeQueryId(query, ParamConstants.INDIVIDUAL_POPULATION_SUBPOPULATION_PARAM,
                IndividualDBAdaptor.QueryParams.POPULATION_SUBPOPULATION.key());
        changeQueryId(query, ParamConstants.INTERNAL_STATUS_PARAM, IndividualDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());
        changeQueryId(query, ParamConstants.STATUS_PARAM, IndividualDBAdaptor.QueryParams.STATUS_ID.key());

        if (query.containsKey(IndividualDBAdaptor.QueryParams.FATHER.key())) {
            if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.FATHER.key()))) {
                Individual ind = internalGet(study.getUid(), query.getString(IndividualDBAdaptor.QueryParams.FATHER.key()),
                        INCLUDE_INDIVIDUAL_IDS, userId).first();
                query.append(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), ind.getUid());
            }
            query.remove(IndividualDBAdaptor.QueryParams.FATHER.key());
        }
        if (query.containsKey(IndividualDBAdaptor.QueryParams.MOTHER.key())) {
            if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.MOTHER.key()))) {
                Individual ind = internalGet(study.getUid(), query.getString(IndividualDBAdaptor.QueryParams.MOTHER.key()),
                        INCLUDE_INDIVIDUAL_IDS, userId).first();
                query.append(IndividualDBAdaptor.QueryParams.MOTHER_UID.key(), ind.getUid());
            }
            query.remove(IndividualDBAdaptor.QueryParams.MOTHER.key());
        }
        if (query.containsKey(IndividualDBAdaptor.QueryParams.SAMPLES.key())) {
            if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.SAMPLES.key()))) {
                OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().internalGet(study.getUid(),
                        query.getAsStringList(IndividualDBAdaptor.QueryParams.SAMPLES.key()), SampleManager.INCLUDE_SAMPLE_IDS, userId,
                        true);
                query.append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleDataResult.getResults().stream().map(Sample::getUid)
                        .collect(Collectors.toList()));
            }
            query.remove(IndividualDBAdaptor.QueryParams.SAMPLES.key());
        }
    }

}
