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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
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
public class IndividualManager extends AnnotationSetManager<Individual> {

    protected static Logger logger = LoggerFactory.getLogger(IndividualManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    private static final Map<Individual.KaryotypicSex, Individual.Sex> KARYOTYPIC_SEX_SEX_MAP;

    static {
        KARYOTYPIC_SEX_SEX_MAP = new HashMap<>();
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.UNKNOWN, Individual.Sex.UNKNOWN);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XX, Individual.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XO, Individual.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XXX, Individual.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XXXX, Individual.Sex.FEMALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XY, Individual.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XXY, Individual.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XXYY, Individual.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XXXY, Individual.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.XYY, Individual.Sex.MALE);
        KARYOTYPIC_SEX_SEX_MAP.put(Individual.KaryotypicSex.OTHER, Individual.Sex.UNDETERMINED);
    }

    IndividualManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                      DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                      Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Deprecated
    public QueryResult<Individual> create(long studyId, String name, String family, long fatherId, long motherId, Individual.Sex sex,
                                          String ethnicity, String populationName, String populationSubpopulation,
                                          String populationDescription, String dateOfBirth, Individual.KaryotypicSex karyotypicSex,
                                          Individual.LifeStatus lifeStatus, Individual.AffectationStatus affectationStatus,
                                          QueryOptions options, String sessionId) throws CatalogException {
        Individual individual = new Individual(-1, name, null, null, null, fatherId, motherId, family, sex, karyotypicSex, ethnicity,
                null, new Individual.Population(populationName, populationSubpopulation, populationDescription), dateOfBirth, -1, 1, null,
                null, lifeStatus, affectationStatus, null, null, false, null, null);
        return create(String.valueOf(studyId), individual, options, sessionId);
    }

    @Override
    public QueryResult<Individual> create(String studyStr, Individual individual, QueryOptions options, String sessionId)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ParamUtils.checkAlias(individual.getName(), "name", configuration.getCatalog().getOffset());
        individual.setFamily(ParamUtils.defaultObject(individual.getFamily(), ""));
        individual.setEthnicity(ParamUtils.defaultObject(individual.getEthnicity(), ""));
        individual.setSpecies(ParamUtils.defaultObject(individual.getSpecies(), Individual.Species::new));
        individual.setPopulation(ParamUtils.defaultObject(individual.getPopulation(), Individual.Population::new));
        individual.setLifeStatus(ParamUtils.defaultObject(individual.getLifeStatus(), Individual.LifeStatus.UNKNOWN));
        individual.setKaryotypicSex(ParamUtils.defaultObject(individual.getKaryotypicSex(), Individual.KaryotypicSex.UNKNOWN));
        individual.setSex(ParamUtils.defaultObject(individual.getSex(), Individual.Sex.UNKNOWN));
        individual.setAffectationStatus(ParamUtils.defaultObject(individual.getAffectationStatus(), Individual.AffectationStatus.UNKNOWN));
        individual.setPhenotypes(ParamUtils.defaultObject(individual.getPhenotypes(), Collections.emptyList()));
        individual.setAnnotationSets(ParamUtils.defaultObject(individual.getAnnotationSets(), Collections.emptyList()));
        individual.setAnnotationSets(validateAnnotationSets(individual.getAnnotationSets()));
        individual.setAttributes(ParamUtils.defaultObject(individual.getAttributes(), Collections.emptyMap()));
        individual.setSamples(ParamUtils.defaultObject(individual.getSamples(), Collections.emptyList()));
        individual.setStatus(new Status());
        individual.setCreationDate(TimeUtils.getTime());

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);

        individual.setRelease(studyManager.getCurrentRelease(studyId));

        // Check samples exist and can be used or can be created by the user
        Set<Long> existingSampleIds = new HashSet<>();
        List<Sample> nonExistingSamples = new ArrayList<>();
        if (individual.getSamples().size() > 0) {
            for (Sample sample : individual.getSamples()) {
                try {
                    MyResourceId resource = catalogManager.getSampleManager().getId(sample.getName(), String.valueOf(studyId), sessionId);
                    existingSampleIds.add(resource.getResourceId());
                } catch (CatalogException e) {
                    // Sample does not exist so we need to check if the user has permissions to create the samples
                    nonExistingSamples.add(sample);
                }
            }
            if (!existingSampleIds.isEmpty()) {
                checkSamplesNotInUseInOtherIndividual(existingSampleIds, studyId, null);
            }
            if (!nonExistingSamples.isEmpty()) {
                // Check the user can create new samples
                authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_SAMPLES);
            }
        }

        // Fetch the sample id and version necessary to point the individual to the proper samples
        List<Sample> sampleList = new ArrayList<>(existingSampleIds.size() + nonExistingSamples.size());
        if (!existingSampleIds.isEmpty()) {
            // We need to obtain the latest version of the samples
            Query sampleQuery = new Query().append(SampleDBAdaptor.QueryParams.ID.key(), existingSampleIds);
            QueryOptions sampleOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.VERSION.key()));

            QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(sampleQuery, sampleOptions);
            if (sampleQueryResult.getNumResults() < existingSampleIds.size()) {
                throw new CatalogException("Internal error. Could not obtain the current version of all the existing samples.");
            }
            sampleList.addAll(sampleQueryResult.getResult());
        }
        if (!nonExistingSamples.isEmpty()) {
            for (Sample sample : nonExistingSamples) {
                QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create(String.valueOf(studyId), sample,
                        QueryOptions.empty(), sessionId);
                if (sampleQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Internal error. Could not obtain created sample");
                }
                sampleList.add(sampleQueryResult.first());
            }
        }
        individual.setSamples(sampleList);

        // Create the individual
        QueryResult<Individual> queryResult = individualDBAdaptor.insert(individual, studyId, options);
        auditManager.recordCreation(AuditRecord.Resource.individual, queryResult.first().getId(), userId, queryResult.first(), null, null);

        addSampleInformation(queryResult, studyId, userId);

        return queryResult;
    }

    private void checkSamplesNotInUseInOtherIndividual(Set<Long> sampleIds, long studyId, Long individualId) throws CatalogException {
        // Check if any of the existing samples already belong to an individual
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.SAMPLES_ID.key(), sampleIds)
                .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                IndividualDBAdaptor.QueryParams.SAMPLES.key(), IndividualDBAdaptor.QueryParams.ID.key()));
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, options);
        if (queryResult.getNumResults() > 0) {
            // Check which of the samples are already associated to an individual
            List<Long> usedSamples = new ArrayList<>();
            for (Individual individual1 : queryResult.getResult()) {
                if (individualId != null && individualId == individual1.getId()) {
                    // It already belongs to the proper individual. Nothing to do
                    continue;
                }
                if (individual1.getSamples() != null) {
                    for (Sample sample : individual1.getSamples()) {
                        if (sampleIds.contains(sample.getId())) {
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
    public QueryResult<Individual> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, query);
        fixQueryOptionAnnotation(options);

        query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        if (query.containsKey(IndividualDBAdaptor.QueryParams.SAMPLES.key())) {
            MyResourceIds ids = catalogManager.getSampleManager().getIds(
                    query.getAsStringList(IndividualDBAdaptor.QueryParams.SAMPLES.key()), String.valueOf(studyId), sessionId);
            query.put(IndividualDBAdaptor.QueryParams.SAMPLES_ID.key(), ids.getResourceIds());
            query.remove(IndividualDBAdaptor.QueryParams.SAMPLES.key());
        }

        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options, userId);
        // Add sample information
        addSampleInformation(individualQueryResult, studyId, userId);

        if (individualQueryResult.getNumResults() == 0 && query.containsKey("id")) {
            List<Long> idList = query.getAsLongList("id");
            for (Long myId : idList) {
                authorizationManager.checkIndividualPermission(studyId, myId, userId, IndividualAclEntry.IndividualPermissions.VIEW);
            }
        }

        return individualQueryResult;
    }

    public QueryResult<Individual> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return get(String.valueOf(studyId), query, options, sessionId);
    }

    @Override
    public DBIterator<Individual> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
        query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        return individualDBAdaptor.iterator(query, options, userId);
    }


    public List<QueryResult<Individual>> restore(String individualIdStr, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(individualIdStr, "id");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceIds resource = getIds(Arrays.asList(StringUtils.split(individualIdStr, ",")), null, sessionId);

        List<QueryResult<Individual>> queryResultList = new ArrayList<>(resource.getResourceIds().size());
        for (Long individualId : resource.getResourceIds()) {
            QueryResult<Individual> queryResult = null;
            try {
                authorizationManager.checkIndividualPermission(resource.getStudyId(), individualId, resource.getUser(),
                        IndividualAclEntry.IndividualPermissions.DELETE);
                queryResult = individualDBAdaptor.restore(individualId, options);

                auditManager.recordRestore(AuditRecord.Resource.individual, individualId, resource.getUser(), Status.DELETED,
                        Status.READY, "Individual restore", null);
            } catch (CatalogAuthorizationException e) {
                auditManager.recordRestore(AuditRecord.Resource.individual, individualId, resource.getUser(), null, null, e.getMessage(),
                        null);
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

    public List<QueryResult<Individual>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
        List<Long> individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
        String individualStr = StringUtils.join(individualIds, ",");
        return restore(individualStr, options, sessionId);
    }

    @Override
    public Long getStudyId(long individualId) throws CatalogException {
        return individualDBAdaptor.getStudyId(individualId);
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
            userId = userManager.getUserId(sessionId);
        } else {
            if (individualStr.contains(",")) {
                throw new CatalogException("More than one individual found");
            }

            userId = userManager.getUserId(sessionId);
            studyId = studyManager.getId(userId, studyStr);

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
    MyResourceIds getIds(List<String> individualList, @Nullable String studyStr, boolean silent, String sessionId) throws CatalogException {
        if (individualList == null || individualList.isEmpty()) {
            throw new CatalogException("Missing individual parameter");
        }

        String userId;
        long studyId;
        List<Long> individualIds = new ArrayList<>();

        if (individualList.size() == 1 && StringUtils.isNumeric(individualList.get(0))
                && Long.parseLong(individualList.get(0)) > configuration.getCatalog().getOffset()) {
            individualIds.add(Long.parseLong(individualList.get(0)));
            individualDBAdaptor.checkId(individualIds.get(0));
            studyId = individualDBAdaptor.getStudyId(individualIds.get(0));
            userId = userManager.getUserId(sessionId);
        } else {
            userId = userManager.getUserId(sessionId);
            studyId = studyManager.getId(userId, studyStr);

            Map<String, Long> myIds = new HashMap<>();
            for (String individualstrAux : individualList) {
                if (StringUtils.isNumeric(individualstrAux)) {
                    long individualId = getIndividualId(silent, individualstrAux);
                    myIds.put(individualstrAux, individualId);
                }
            }

            if (myIds.size() < individualList.size()) {
                Query query = new Query()
                        .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(IndividualDBAdaptor.QueryParams.NAME.key(), individualList);

                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        IndividualDBAdaptor.QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.NAME.key()));
                QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);

                if (individualQueryResult.getNumResults() > 0) {
                    myIds.putAll(individualQueryResult.getResult().stream()
                            .collect(Collectors.toMap(Individual::getName, Individual::getId)));
                }
            }
            if (myIds.size() < individualList.size() && !silent) {
                throw new CatalogException("Found only " + myIds.size() + " out of the " + individualList.size()
                        + " individuals looked for in study " + studyStr);
            }
            for (String individualstrAux : individualList) {
                individualIds.add(myIds.getOrDefault(individualstrAux, -1L));
            }
        }

        return new MyResourceIds(userId, studyId, individualIds);
    }

    @Override
    public QueryResult<Individual> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

        Query finalQuery = new Query(query);
        fixQuery(studyId, finalQuery, sessionId);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, finalQuery);
        fixQueryOptionAnnotation(options);

        finalQuery.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<Individual> queryResult = individualDBAdaptor.get(finalQuery, options, userId);
//        authorizationManager.filterIndividuals(userId, studyId, queryResultAux.getResult());

        // Add sample information
        addSampleInformation(queryResult, studyId, userId);
        return queryResult;
    }

    @Override
    public QueryResult<Individual> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

        Query finalQuery = new Query(query);
        fixQuery(studyId, finalQuery, sessionId);

        // Fix query if it contains any annotation
        fixQueryAnnotationSearch(studyId, finalQuery);


        finalQuery.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = individualDBAdaptor.count(finalQuery, userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public QueryResult<Individual> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceId resource = getId(entryStr, studyStr, sessionId);
        String userId = resource.getUser();
        long studyId = resource.getStudyId();
        long individualId = resource.getResourceId();

        authorizationManager.checkIndividualPermission(studyId, individualId, userId, IndividualAclEntry.IndividualPermissions.UPDATE);

        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            IndividualDBAdaptor.QueryParams queryParam = IndividualDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case NAME:
                    ParamUtils.checkAlias(parameters.getString(queryParam.key()), "name", configuration.getCatalog().getOffset());

                    String myName = parameters.getString(IndividualDBAdaptor.QueryParams.NAME.key());
                    Query query = new Query()
                            .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                            .append(IndividualDBAdaptor.QueryParams.NAME.key(), myName);
                    if (individualDBAdaptor.count(query).first() > 0) {
                        throw new CatalogException("Individual name " + myName + " already in use");
                    }
                    break;
                case DATE_OF_BIRTH:
                    if (StringUtils.isEmpty((String) param.getValue())) {
                        parameters.put(param.getKey(), "");
                    } else {
                        if (!TimeUtils.isValidFormat("yyyyMMdd", (String) param.getValue())) {
                            throw new CatalogException("Invalid date of birth format. Valid format yyyyMMdd");
                        }
                    }
                    break;
                case KARYOTYPIC_SEX:
                    try {
                        Individual.KaryotypicSex.valueOf(String.valueOf(param.getValue()));
                    } catch (IllegalArgumentException e) {
                        logger.error("Invalid karyotypic sex found: {}", e.getMessage(), e);
                        throw new CatalogException("Invalid karyotypic sex detected");
                    }
                    break;
                case SEX:
                    try {
                        Individual.Sex.valueOf(String.valueOf(param.getValue()));
                    } catch (IllegalArgumentException e) {
                        logger.error("Invalid sex found: {}", e.getMessage(), e);
                        throw new CatalogException("Invalid sex detected");
                    }
                    break;
                case MULTIPLES:
                    // Check individual names exist
                    Map<String, Object> multiples = (Map<String, Object>) param.getValue();
                    List<String> siblingList = (List<String>) multiples.get("siblings");
                    query = new Query()
                            .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                            .append(IndividualDBAdaptor.QueryParams.NAME.key(), StringUtils.join(siblingList, ","));
                    QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
                    QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
                    if (individualQueryResult.getNumResults() < siblingList.size()) {
                        int missing = siblingList.size() - individualQueryResult.getNumResults();
                        throw new CatalogDBException("Missing " + missing + " siblings in the database.");
                    }
                    break;
                case SAMPLES:
                    // Check those samples can be used
                    List<String> samples = parameters.getAsStringList(param.getKey());
                    MyResourceIds sampleResource = catalogManager.getSampleManager().getIds(samples, String.valueOf(studyId), sessionId);
                    checkSamplesNotInUseInOtherIndividual(new HashSet<>(sampleResource.getResourceIds()), studyId, individualId);

                    // Fetch the samples to obtain the latest version as well
                    Query sampleQuery = new Query()
                            .append(SampleDBAdaptor.QueryParams.ID.key(), sampleResource.getResourceIds());
                    QueryOptions sampleOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                            SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.VERSION.key()));
                    QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(sampleQuery, sampleOptions);

                    if (sampleQueryResult.getNumResults() < sampleResource.getResourceIds().size()) {
                        throw new CatalogException("Internal error: Could not obtain all the samples to be updated.");
                    }

                    // Update the parameters with the proper list of samples
                    parameters.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleQueryResult.getResult());
                    break;
                case FATHER:
                case MOTHER:
                case ETHNICITY:
                case POPULATION_DESCRIPTION:
                case POPULATION_NAME:
                case POPULATION_SUBPOPULATION:
                case PHENOTYPES:
                case LIFE_STATUS:
                case AFFECTATION_STATUS:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        if (StringUtils.isNotEmpty(parameters.getString(IndividualDBAdaptor.QueryParams.FATHER.key()))) {
            MyResourceId tmpResource =
                    getId(parameters.getString(IndividualDBAdaptor.QueryParams.FATHER.key()), String.valueOf(studyId), sessionId);
            parameters.remove(IndividualDBAdaptor.QueryParams.FATHER.key());
            parameters.put(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), tmpResource.getResourceId());
        }
        if (StringUtils.isNotEmpty(parameters.getString(IndividualDBAdaptor.QueryParams.MOTHER.key()))) {
            MyResourceId tmpResource =
                    getId(parameters.getString(IndividualDBAdaptor.QueryParams.MOTHER.key()), String.valueOf(studyId), sessionId);
            parameters.remove(IndividualDBAdaptor.QueryParams.MOTHER.key());
            parameters.put(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), tmpResource.getResourceId());
        }

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(resource.getStudyId()));
        }

        QueryResult<Individual> queryResult = individualDBAdaptor.update(individualId, parameters, options);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, parameters, null, null);

        // Add sample information
        addSampleInformation(queryResult, studyId, userId);

        return queryResult;
    }

    public List<QueryResult<Individual>> delete(@Nullable String studyStr, String individualIdStr, ObjectMap options, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(individualIdStr, "id");
        options = ParamUtils.defaultObject(options, ObjectMap::new);

        MyResourceIds resourceId = getIds(Arrays.asList(StringUtils.split(individualIdStr, ",")), studyStr, sessionId);
        List<Long> individualIds = resourceId.getResourceIds();
        String userId = resourceId.getUser();

        List<QueryResult<Individual>> queryResultList = new ArrayList<>(individualIds.size());
        for (Long individualId : individualIds) {
            QueryResult<Individual> queryResult = null;
            try {
                authorizationManager.checkIndividualPermission(resourceId.getStudyId(), individualId, userId,
                        IndividualAclEntry.IndividualPermissions.DELETE);

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
                    catalogManager.getSampleManager().delete(Long.toString(resourceId.getStudyId()), StringUtils.join(sampleIds, ","),
                            QueryOptions.empty(), sessionId);
                }

                // Get the individual info before the update
                QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId, QueryOptions.empty());

                String newIndividualName = individualQueryResult.first().getName() + ".DELETED_" + TimeUtils.getTime();
                ObjectMap updateParams = new ObjectMap()
                        .append(IndividualDBAdaptor.QueryParams.NAME.key(), newIndividualName)
                        .append(IndividualDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);
                queryResult = individualDBAdaptor.update(individualId, updateParams, QueryOptions.empty());

                auditManager.recordDeletion(AuditRecord.Resource.individual, individualId, resourceId.getUser(),
                        individualQueryResult.first(), queryResult.first(), null, null);

            } catch (CatalogAuthorizationException e) {
                auditManager.recordDeletion(AuditRecord.Resource.individual, individualId, resourceId.getUser(), null, e.getMessage(),
                        null);

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

    public List<QueryResult<Individual>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
        List<Long> individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
        String individualStr = StringUtils.join(individualIds, ",");
        return delete(null, individualStr, options, sessionId);
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

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

        String userId = userManager.getUserId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);

        // Add study id to the query
        query.put(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult queryResult = individualDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }


    @Override
    public QueryResult<AnnotationSet> createAnnotationSet(String id, @Nullable String studyStr, String variableSetId,
                                                          String annotationSetName, Map<String, Object> annotations,
                                                          Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        MyResourceId resource = catalogManager.getIndividualManager().getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);
        MyResourceId variableSetResource = studyManager.getVariableSetId(variableSetId,
                Long.toString(resource.getStudyId()), sessionId);

        QueryResult<VariableSet> variableSet = studyDBAdaptor.getVariableSet(variableSetResource.getResourceId(), null,
                resource.getUser());
        if (variableSet.getNumResults() == 0) {
            // Variable set must be confidential and the user does not have those permissions
            throw new CatalogAuthorizationException("Permission denied: User " + resource.getUser() + " cannot create annotations over "
                    + "that variable set");
        }

        QueryResult<AnnotationSet> annotationSet = createAnnotationSet(resource.getResourceId(), variableSet.first(), annotationSetName,
                annotations, studyManager.getCurrentRelease(resource.getStudyId()), attributes, individualDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.individual, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr, String annotationSetName,
                                                          Map<String, Object> newAnnotations, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);

        // Update the annotation
        QueryResult<AnnotationSet> queryResult = updateAnnotationSet(resource, annotationSetName, newAnnotations, individualDBAdaptor);

        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("There was an error with the update");
        }

        AnnotationSet annotationSet = queryResult.first();

        // Audit the changes
        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations, annotationSet.getCreationDate(), 1, null);
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
        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> annotationSet = individualDBAdaptor.getAnnotationSet(resource.getResourceId(), annotationSetName, null);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }
        // We make this query because it will check the proper permissions in case the variable set is confidential
        studyDBAdaptor.getVariableSet(annotationSet.first().getVariableSetId(), new QueryOptions(), resource.getUser());

        individualDBAdaptor.deleteAnnotationSet(resource.getResourceId(), annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.individual, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }


    // **************************   ACLs  ******************************** //
    public List<QueryResult<IndividualAclEntry>> getAcls(String studyStr, List<String> individualList, String member,
                                                         boolean silent, String sessionId) throws CatalogException {
        MyResourceIds resource = getIds(individualList, studyStr, silent, sessionId);

        List<QueryResult<IndividualAclEntry>> individualAclList = new ArrayList<>(resource.getResourceIds().size());
        List<Long> resourceIds = resource.getResourceIds();
        for (int i = 0; i < resourceIds.size(); i++) {
            Long individualId = resourceIds.get(i);
            try {
                QueryResult<IndividualAclEntry> allIndividualAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allIndividualAcls = authorizationManager.getIndividualAcl(resource.getStudyId(), individualId,
                            resource.getUser(), member);
                } else {
                    allIndividualAcls = authorizationManager.getAllIndividualAcls(resource.getStudyId(), individualId, resource.getUser());
                }
                allIndividualAcls.setId(String.valueOf(individualId));
                individualAclList.add(allIndividualAcls);
            } catch (CatalogException e) {
                if (silent) {
                    individualAclList.add(new QueryResult<>(individualList.get(i), 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return individualAclList;
    }

    public List<QueryResult<IndividualAclEntry>> updateAcl(String studyStr, List<String> individualList, String memberIds,
                                                           Individual.IndividualAclParams aclParams, String sessionId)
            throws CatalogException {
        int count = 0;
        count += individualList != null && !individualList.isEmpty() ? 1 : 0;
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
            MyResourceIds ids = catalogManager.getSampleManager().getIds(Arrays.asList(StringUtils.split(aclParams.getSample(), ",")),
                    studyStr, sessionId);

            Query query = new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
            QueryResult<Individual> indQueryResult = catalogManager.getIndividualManager().get(ids.getStudyId(), query, options, sessionId);

            individualList = indQueryResult.getResult().stream().map(Individual::getId).map(String::valueOf).collect(Collectors.toList());

            studyStr = Long.toString(ids.getStudyId());
        }

        // Obtain the resource ids
        MyResourceIds resourceIds = getIds(individualList, studyStr, sessionId);

        authorizationManager.checkCanAssignOrSeePermissions(resourceIds.getStudyId(), resourceIds.getUser());

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resourceIds.getStudyId(), members);
//        studyManager.membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        List<QueryResult<IndividualAclEntry>> queryResults;
        switch (aclParams.getAction()) {
            case SET:
                queryResults = authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.INDIVIDUAL);
                if (aclParams.isPropagate()) {
                    List<String> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (sampleIds.size() > 0) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.SET, null, null, null);
                        catalogManager.getSampleManager().updateAcl(studyStr, sampleIds, memberIds, sampleAclParams, sessionId);
                    }
                }
                break;
            case ADD:
                queryResults = authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.INDIVIDUAL);
                if (aclParams.isPropagate()) {
                    List<String> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (sampleIds.size() > 0) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.ADD, null, null, null);
                        catalogManager.getSampleManager().updateAcl(studyStr, sampleIds, memberIds, sampleAclParams, sessionId);
                    }
                }
                break;
            case REMOVE:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, Entity.INDIVIDUAL);
                if (aclParams.isPropagate()) {
                    List<String> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (CollectionUtils.isNotEmpty(sampleIds)) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.REMOVE, null, null, null);
                        catalogManager.getSampleManager().updateAcl(studyStr, sampleIds, memberIds, sampleAclParams, sessionId);
                    }
                }
                break;
            case RESET:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, Entity.INDIVIDUAL);
                if (aclParams.isPropagate()) {
                    List<String> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (CollectionUtils.isNotEmpty(sampleIds)) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.RESET, null, null, null);
                        catalogManager.getSampleManager().updateAcl(studyStr, sampleIds, memberIds, sampleAclParams, sessionId);
                    }
                }
                break;
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }

        return queryResults;
    }


    // **************************   Private methods  ******************************** //

    private List<String> getSamplesFromIndividuals(MyResourceIds resourceIds) throws CatalogDBException {
        // Look for all the samples belonging to the individual
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                .append(IndividualDBAdaptor.QueryParams.ID.key(), resourceIds.getResourceIds());

        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key()));

        List<String> sampleIds = new ArrayList<>();
        for (Individual individual : individualQueryResult.getResult()) {
            sampleIds.addAll(individual.getSamples().stream().map(Sample::getId).map(String::valueOf).collect(Collectors.toList()));
        }

        return sampleIds;
    }

    // Checks if father or mother are in query and transforms them into father.id and mother.id respectively

    private void fixQuery(long studyId, Query query, String sessionId) throws CatalogException {
        if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.FATHER.key()))) {
            MyResourceId resource =
                    getId(query.getString(IndividualDBAdaptor.QueryParams.FATHER.key()), String.valueOf(studyId), sessionId);
            query.remove(IndividualDBAdaptor.QueryParams.FATHER.key());
            query.append(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), resource.getResourceId());
        }
        if (StringUtils.isNotEmpty(query.getString(IndividualDBAdaptor.QueryParams.MOTHER.key()))) {
            MyResourceId resource =
                    getId(query.getString(IndividualDBAdaptor.QueryParams.MOTHER.key()), String.valueOf(studyId), sessionId);
            query.remove(IndividualDBAdaptor.QueryParams.MOTHER.key());
            query.append(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), resource.getResourceId());
        }
    }

    private void addSampleInformation(QueryResult<Individual> individualQueryResult, long studyId, String userId) {
        if (individualQueryResult.getNumResults() == 0) {
            return;
        }

        List<String> errorMessages = new ArrayList<>();
        for (Individual individual : individualQueryResult.getResult()) {
            if (individual.getSamples() == null || individual.getSamples().isEmpty()) {
                continue;
            }

            List<Sample> sampleList = new ArrayList<>();
            for (Sample sample : individual.getSamples()) {
                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sample.getId())
                        .append(SampleDBAdaptor.QueryParams.VERSION.key(), sample.getVersion());
                try {
                    QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, QueryOptions.empty(), userId);
                    if (sampleQueryResult.getNumResults() == 0) {
                        throw new CatalogException("Could not get information from sample " + sample.getId());
                    } else {
                        sampleList.add(sampleQueryResult.first());
                    }
                } catch (CatalogException e) {
                    logger.warn("Could not retrieve sample information to complete individual {}, {}", individual.getName(), e.getMessage(),
                            e);
                    errorMessages.add("Could not retrieve sample information to complete individual " + individual.getName() + ", "
                            + e.getMessage());
                }
            }
            individual.setSamples(sampleList);
        }

        if (errorMessages.size() > 0) {
            individualQueryResult.setWarningMsg(StringUtils.join(errorMessages, "\n"));
        }
    }

    private long getIndividualId(boolean silent, String individualStr) throws CatalogException {
        long individualId = Long.parseLong(individualStr);
        try {
            individualDBAdaptor.checkId(individualId);
        } catch (CatalogException e) {
            if (silent) {
                return -1L;
            } else {
                throw e;
            }
        }
        return individualId;
    }

}
