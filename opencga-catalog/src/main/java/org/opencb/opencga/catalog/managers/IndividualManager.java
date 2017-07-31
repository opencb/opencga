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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.AnnotationManager;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
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
public class IndividualManager extends AbstractManager implements IIndividualManager {

    protected static Logger logger = LoggerFactory.getLogger(IndividualManager.class);
    private IUserManager userManager;

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

    @Deprecated
    public IndividualManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                             DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                             Properties catalogProperties) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public IndividualManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                             DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                             Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
        this.userManager = catalogManager.getUserManager();
    }

    @Override
    public QueryResult<Individual> create(long studyId, String name, String family, long fatherId, long motherId, Individual.Sex sex,
                                          String ethnicity, String populationName, String populationSubpopulation,
                                          String populationDescription, String dateOfBirth, Individual.KaryotypicSex karyotypicSex,
                                          Individual.LifeStatus lifeStatus, Individual.AffectationStatus affectationStatus,
                                          QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        sex = ParamUtils.defaultObject(sex, Individual.Sex.UNKNOWN);
        logger.info(Long.toString(configuration.getCatalog().getOffset()));
        ParamUtils.checkAlias(name, "name", configuration.getCatalog().getOffset());
        family = ParamUtils.defaultObject(family, "");
        ethnicity = ParamUtils.defaultObject(ethnicity, "");
        populationName = ParamUtils.defaultObject(populationName, "");
        populationSubpopulation = ParamUtils.defaultObject(populationSubpopulation, "");
        populationDescription = ParamUtils.defaultObject(populationDescription, "");
        karyotypicSex = ParamUtils.defaultObject(karyotypicSex, Individual.KaryotypicSex.UNKNOWN);
        lifeStatus = ParamUtils.defaultObject(lifeStatus, Individual.LifeStatus.UNKNOWN);
        affectationStatus = ParamUtils.defaultObject(affectationStatus, Individual.AffectationStatus.UNKNOWN);
        if (StringUtils.isEmpty(dateOfBirth)) {
            dateOfBirth = "";
        } else {
            if (!TimeUtils.isValidFormat("yyyyMMdd", dateOfBirth)) {
                throw new CatalogException("Invalid date of birth format. Valid format yyyyMMdd");
            }
        }

        String userId = userManager.getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);

        Individual individual = new Individual(0, name, fatherId, motherId, family, sex, karyotypicSex, ethnicity,
                new Individual.Population(populationName, populationSubpopulation, populationDescription), lifeStatus, affectationStatus,
                dateOfBirth, false, catalogManager.getStudyManager().getCurrentRelease(studyId), Collections.emptyList(),
                new ArrayList<>());

        QueryResult<Individual> queryResult = individualDBAdaptor.insert(individual, studyId, options);
//      auditManager.recordCreation(AuditRecord.Resource.individual, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Individual> get(Long individualId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(individualId, "individualId");
        ParamUtils.checkObj(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        long studyId = individualDBAdaptor.getStudyId(individualId);

        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.ID.key(), individualId)
                .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options, userId);
        if (individualQueryResult.getNumResults() <= 0) {
            throw CatalogAuthorizationException.deny(userId, "view", "individual", individualId, "");
        }
        addChildrenInformation(userId, studyId, individualQueryResult);
        individualQueryResult.setNumResults(individualQueryResult.getResult().size());
        return individualQueryResult;
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        return get(query.getInt("studyId"), query, options, sessionId);
    }

    @Override
    public QueryResult<Individual> get(long studyId, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, options, userId);
//        authorizationManager.filterIndividuals(userId, studyId, queryResult.getResult());
        addChildrenInformation(userId, studyId, queryResult);
        return queryResult;
    }

    @Override
    public DBIterator<Individual> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        return individualDBAdaptor.iterator(query, options, userId);
    }

    private void addChildrenInformation(String userId, long studyId, QueryResult<Individual> queryResult) {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FamilyDBAdaptor.QueryParams.FATHER.key(), FamilyDBAdaptor.QueryParams.MOTHER.key()));
        for (Individual individual : queryResult.getResult()) {
            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FamilyDBAdaptor.QueryParams.CHILDREN_IDS.key(), individual.getId());
            try {
                QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, queryOptions, userId);
                if (familyQueryResult.getNumResults() == 0) {
                    continue;
                }
                Map<String, Object> attributes = individual.getAttributes();
                if (attributes == null) {
                    individual.setAttributes(new HashMap<>());
                    attributes = individual.getAttributes();
                }
                attributes.put(FamilyDBAdaptor.QueryParams.MOTHER.key(), familyQueryResult.first().getMother());
                attributes.put(FamilyDBAdaptor.QueryParams.FATHER.key(), familyQueryResult.first().getFather());

            } catch (CatalogException e) {
                logger.warn("Error occurred when trying to fetch parents of individual: {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public List<QueryResult<Individual>> restore(String individualIdStr, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(individualIdStr, "id");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceIds resource = getIds(individualIdStr, null, sessionId);

        List<QueryResult<Individual>> queryResultList = new ArrayList<>(resource.getResourceIds().size());
        for (Long individualId : resource.getResourceIds()) {
            QueryResult<Individual> queryResult = null;
            try {
                authorizationManager.checkIndividualPermission(resource.getStudyId(), individualId, resource.getUser(),
                        IndividualAclEntry.IndividualPermissions.DELETE);
                queryResult = individualDBAdaptor.restore(individualId, options);
                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.restore, AuditRecord.Magnitude.medium,
                        individualId, resource.getUser(), Status.DELETED, Status.READY, "Individual restore", new ObjectMap());
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.restore, AuditRecord.Magnitude.high,
                        individualId, resource.getUser(), null, null, e.getMessage(), null);
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

    @Override
    public List<QueryResult<Individual>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
        List<Long> individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
        String individualStr = StringUtils.join(individualIds, ",");
        return restore(individualStr, options, sessionId);
    }

    @Override
    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        throw new NotImplementedException("Project: Operation not yet supported");
    }

    @Override
    public Long getStudyId(long individualId) throws CatalogException {
        return individualDBAdaptor.getStudyId(individualId);
    }

    @Override
    public Long getId(String userId, String individualStr) throws CatalogException {
        if (StringUtils.isNumeric(individualStr)) {
            return Long.parseLong(individualStr);
        }

        // Resolve the studyIds and filter the individualName
        ObjectMap parsedSampleStr = parseFeatureId(userId, individualStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String individualName = parsedSampleStr.getString("featureName");

        Query query = new Query(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(IndividualDBAdaptor.QueryParams.NAME.key(), individualName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.individuals.id");
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one individual id found based on " + individualName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Deprecated
    @Override
    public Long getId(String id) throws CatalogDBException {
        if (StringUtils.isNumeric(id)) {
            return Long.parseLong(id);
        }

        Query query = new Query(IndividualDBAdaptor.QueryParams.NAME.key(), id);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());

        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options);
        if (individualQueryResult.getNumResults() == 1) {
            return individualQueryResult.first().getId();
        } else {
            return -1L;
        }
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
            userId = userManager.getId(sessionId);
        } else {
            if (individualStr.contains(",")) {
                throw new CatalogException("More than one individual found");
            }

            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

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
    public MyResourceIds getIds(String individualStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(individualStr)) {
            throw new CatalogException("Missing individual parameter");
        }

        String userId;
        long studyId;
        List<Long> individualIds;

        if (StringUtils.isNumeric(individualStr) && Long.parseLong(individualStr) > configuration.getCatalog().getOffset()) {
            individualIds = Arrays.asList(Long.parseLong(individualStr));
            individualDBAdaptor.exists(individualIds.get(0));
            studyId = individualDBAdaptor.getStudyId(individualIds.get(0));
            userId = userManager.getId(sessionId);
        } else {
            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            List<String> individualSplit = Arrays.asList(individualStr.split(","));
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(IndividualDBAdaptor.QueryParams.NAME.key(), individualSplit);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
            QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
            if (individualQueryResult.getNumResults() == individualSplit.size()) {
                individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
            } else {
                throw new CatalogException("Found only " + individualQueryResult.getNumResults() + " out of the " + individualSplit.size()
                        + " individuals looked for in study " + studyStr);
            }
        }

        return new MyResourceIds(userId, studyId, individualIds);
    }

    @Override
    public QueryResult<Individual> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, options, userId);
//        authorizationManager.filterIndividuals(userId, studyId, queryResultAux.getResult());

        return queryResult;
    }

    @Override
    public QueryResult<Individual> count(String studyStr, Query query, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        query.append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = individualDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
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
        individual.setAffectationStatus(ParamUtils.defaultObject(individual.getAffectationStatus(), Individual.AffectationStatus.UNKNOWN));
        individual.setOntologyTerms(ParamUtils.defaultObject(individual.getOntologyTerms(), Collections.emptyList()));
        individual.setAnnotationSets(ParamUtils.defaultObject(individual.getAnnotationSets(), Collections.emptyList()));
        individual.setAnnotationSets(AnnotationManager.validateAnnotationSets(individual.getAnnotationSets(), studyDBAdaptor));
        individual.setAttributes(ParamUtils.defaultObject(individual.getAttributes(), Collections.emptyMap()));
        individual.setSamples(Collections.emptyList());
        individual.setAcl(Collections.emptyList());
        individual.setStatus(new Status());
        individual.setCreationDate(TimeUtils.getTime());

        // Validate sex and karyotypic sex
        if (individual.getSex() != null && individual.getKaryotypicSex() != null) {
            if (individual.getSex() != KARYOTYPIC_SEX_SEX_MAP.get(individual.getKaryotypicSex())) {
                throw new CatalogException("Sex and karyotypic sex are not consistent");
            }
        } else if (individual.getSex() != null) {
            switch (individual.getSex()) {
                case MALE:
                    individual.setKaryotypicSex(Individual.KaryotypicSex.XY);
                    break;
                case FEMALE:
                    individual.setKaryotypicSex(Individual.KaryotypicSex.XX);
                    break;
                case UNDETERMINED:
                    individual.setKaryotypicSex(Individual.KaryotypicSex.OTHER);
                    break;
                default:
                    individual.setKaryotypicSex(Individual.KaryotypicSex.UNKNOWN);
                    break;

            }
        } else if (individual.getKaryotypicSex() != null) {
            individual.setSex(KARYOTYPIC_SEX_SEX_MAP.get(individual.getKaryotypicSex()));
        } else {
            individual.setSex(Individual.Sex.UNKNOWN);
            individual.setKaryotypicSex(Individual.KaryotypicSex.UNKNOWN);
        }

        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyId(studyStr, sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS);

        individual.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));

        QueryResult<Individual> queryResult = individualDBAdaptor.insert(individual, studyId, options);
        auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Individual> update(Long individualId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.defaultObject(parameters, QueryOptions::new);
        ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getId(sessionId);
        long studyId = individualDBAdaptor.getStudyId(individualId);
        authorizationManager.checkIndividualPermission(studyId, individualId, userId,
                IndividualAclEntry.IndividualPermissions.UPDATE);

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
                    Individual.KaryotypicSex karyo = Individual.KaryotypicSex.valueOf(String.valueOf(param.getValue()));

                    if (parameters.containsKey(IndividualDBAdaptor.QueryParams.SEX.key())) {
                        Individual.Sex sex = Individual.Sex.valueOf(parameters.getString(IndividualDBAdaptor.QueryParams.SEX.key()));
                        if (sex != KARYOTYPIC_SEX_SEX_MAP.get(karyo)) {
                            throw new CatalogException("Sex and karyotypic sex are not consistent");
                        }
                    } else {
                        // Get sex of the individual and check it is compatible
                        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId,
                                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SEX.key()));
                        if (individualQueryResult.getNumResults() < 1) {
                            throw new CatalogException("Internal error occurred. Could not obtain individual information. Update not "
                                    + "performed");
                        } else {
                            if (individualQueryResult.first().getSex() != KARYOTYPIC_SEX_SEX_MAP.get(karyo)) {
                                throw new CatalogException("Cannot update karyotypic sex to " + karyo + ". That's inconsistent with "
                                        + "existing sex " + individualQueryResult.first().getSex());
                            }
                        }
                    }
                    break;
                case SEX:
                    if (!parameters.containsKey(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key())) {
                        Individual.Sex sex = Individual.Sex.valueOf(String.valueOf(param.getValue()));
                        // Get karyotype and check it is compatible
                        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId,
                                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key()));
                        if (individualQueryResult.getNumResults() < 1) {
                            throw new CatalogException("Internal error occurred. Could not obtain individual information. Update not "
                                    + "performed");
                        } else {
                            if (sex != KARYOTYPIC_SEX_SEX_MAP.get(individualQueryResult.first().getKaryotypicSex())) {
                                throw new CatalogException("Cannot update sex to " + sex + ". That's inconsistent with "
                                        + "existing karyotypic sex " + individualQueryResult.first().getKaryotypicSex());
                            }
                        }
                    }
                    break;
                case FATHER_ID:
                case MOTHER_ID:
                case FAMILY:
                case ETHNICITY:
                case POPULATION_DESCRIPTION:
                case POPULATION_NAME:
                case POPULATION_SUBPOPULATION:
                case ONTOLOGY_TERMS:
                case LIFE_STATUS:
                case AFFECTATION_STATUS:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

//        options.putAll(parameters); //FIXME: Use separated params and options, or merge
        QueryResult<Individual> queryResult = individualDBAdaptor.update(individualId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.individual, individualId, userId, parameters, null, null);
        return queryResult;

    }

    @Override
    public List<QueryResult<Individual>> delete(String individualIdStr, @Nullable String studyStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.checkParameter(individualIdStr, "id");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceIds resourceId = getIds(individualIdStr, studyStr, sessionId);
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
                    catalogManager.getSampleManager().delete(StringUtils.join(sampleIds, ","),
                            Long.toString(resourceId.getStudyId()), QueryOptions.empty(), sessionId);
                }

                // Get the individual info before the update
                QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(individualId, QueryOptions.empty());

                String newIndividualName = individualQueryResult.first().getName() + ".DELETED_" + TimeUtils.getTime();
                ObjectMap updateParams = new ObjectMap()
                        .append(IndividualDBAdaptor.QueryParams.NAME.key(), newIndividualName)
                        .append(IndividualDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);
                queryResult = individualDBAdaptor.update(individualId, updateParams);

                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.delete, AuditRecord.Magnitude.high,
                        individualId, resourceId.getUser(), individualQueryResult.first(), queryResult.first(), "", null);
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.individual, AuditRecord.Action.delete, AuditRecord.Magnitude.high,
                        individualId, userId, null, null, e.getMessage(), null);
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

    @Override
    public List<QueryResult<Individual>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key());
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, queryOptions);
        List<Long> individualIds = individualQueryResult.getResult().stream().map(Individual::getId).collect(Collectors.toList());
        String individualStr = StringUtils.join(individualIds, ",");
        return delete(individualStr, null, options, sessionId);
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getId(sessionId);
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

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);

        // Add study id to the query
        query.put(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = individualDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public List<QueryResult<IndividualAclEntry>> updateAcl(String individual, String studyStr, String memberIds,
                                                           Individual.IndividualAclParams aclParams, String sessionId)
            throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(individual) ? 1 : 0;
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
            MyResourceIds ids = catalogManager.getSampleManager().getIds(aclParams.getSample(), studyStr, sessionId);

            Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
            QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(ids.getStudyId(), query, options, sessionId);

            Set<Long> individualSet = sampleQueryResult.getResult().stream().map(sample -> sample.getIndividual().getId())
                    .collect(Collectors.toSet());
            individual = StringUtils.join(individualSet, ",");

            studyStr = Long.toString(ids.getStudyId());
        }

        // Obtain the resource ids
        MyResourceIds resourceIds = getIds(individual, studyStr, sessionId);

        // Check the user has the permissions needed to change permissions over those individuals
        for (Long individualId : resourceIds.getResourceIds()) {
            authorizationManager.checkIndividualPermission(resourceIds.getStudyId(), individualId, resourceIds.getUser(),
                    IndividualAclEntry.IndividualPermissions.SHARE);
        }

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        CatalogMemberValidator.checkMembers(catalogDBAdaptorFactory, resourceIds.getStudyId(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        String collectionName = MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION;

        List<QueryResult<IndividualAclEntry>> queryResults;
        switch (aclParams.getAction()) {
            case SET:
                queryResults = authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (sampleIds.size() > 0) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.SET, null, null, null);
                        catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds, sampleAclParams,
                                sessionId);
                    }
                }
                break;
            case ADD:
                queryResults = authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (sampleIds.size() > 0) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.ADD, null, null, null);
                        catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds, sampleAclParams,
                                sessionId);
                    }
                }
                break;
            case REMOVE:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (sampleIds.size() > 0) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.REMOVE, null, null, null);
                        catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds,
                                sampleAclParams, sessionId);
                    }
                }
                break;
            case RESET:
                queryResults = authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, collectionName);
                if (aclParams.isPropagate()) {
                    List<Long> sampleIds = getSamplesFromIndividuals(resourceIds);
                    if (sampleIds.size() > 0) {
                        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams(aclParams.getPermissions(),
                                AclParams.Action.RESET, null, null, null);
                        catalogManager.getSampleManager().updateAcl(StringUtils.join(sampleIds, ","), studyStr, memberIds,
                                sampleAclParams, sessionId);
                    }
                }
                break;
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }

        return queryResults;
    }

    private List<Long> getSamplesFromIndividuals(MyResourceIds resourceIds) throws CatalogDBException {
        // Look for all the samples belonging to the individual
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), resourceIds.getResourceIds());

        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()));

        return sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
    }

    private MyResourceId commonGetAllInvidualSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        return getId(id, studyStr, sessionId);
//        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
//                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);
//        return resource.getResourceId();
    }

    private MyResourceId commonGetAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        ParamUtils.checkAlias(annotationSetName, "annotationSetName", configuration.getCatalog().getOffset());
        return getId(id, studyStr, sessionId);
//        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
//                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);
//        return resource.getResourceId();
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
        MyResourceId variableSetResource = catalogManager.getStudyManager().getVariableSetId(variableSetId,
                Long.toString(resource.getStudyId()), sessionId);

        QueryResult<VariableSet> variableSet = studyDBAdaptor.getVariableSet(variableSetResource.getResourceId(), null,
                resource.getUser(), null);
        if (variableSet.getNumResults() == 0) {
            // Variable set must be confidential and the user does not have those permissions
            throw new CatalogAuthorizationException("Permission denied: User " + resource.getUser() + " cannot create annotations over "
                    + "that variable set");
        }

        QueryResult<AnnotationSet> annotationSet = AnnotationManager.createAnnotationSet(resource.getResourceId(), variableSet.first(),
                annotationSetName, annotations, catalogManager.getStudyManager().getCurrentRelease(resource.getStudyId()), attributes,
                individualDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.individual, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> getAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        MyResourceId resource = commonGetAllInvidualSets(id, studyStr, sessionId);
        return individualDBAdaptor.getAnnotationSet(resource, null,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAllInvidualSets(id, studyStr, sessionId);
        return individualDBAdaptor.getAnnotationSetAsMap(resource, null,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return individualDBAdaptor.getAnnotationSet(resource, annotationSetName,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<ObjectMap> getAnnotationSetAsMap(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return individualDBAdaptor.getAnnotationSetAsMap(resource, annotationSetName,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, Map<String,
            Object> newAnnotations, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
                IndividualAclEntry.IndividualPermissions.WRITE_ANNOTATIONS);

        // Update the annotation
        QueryResult<AnnotationSet> queryResult =
                AnnotationManager.updateAnnotationSet(resource, annotationSetName, newAnnotations, individualDBAdaptor, studyDBAdaptor);

        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("There was an error with the update");
        }

        AnnotationSet annotationSet = queryResult.first();

        // Audit the changes
        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getCreationDate(), 1, null);
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

        QueryResult<AnnotationSet> annotationSet = individualDBAdaptor.getAnnotationSet(resource.getResourceId(), annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }
        // We make this query because it will check the proper permissions in case the variable set is confidential
        studyDBAdaptor.getVariableSet(annotationSet.first().getVariableSetId(), new QueryOptions(), resource.getUser(), null);

        individualDBAdaptor.deleteAnnotationSet(resource.getResourceId(), annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.individual, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }

    @Override
    public QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, @Nullable String studyStr, String variableSetStr,
                                                           @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        MyResourceId resource = getId(id, studyStr, sessionId);
//        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
//                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);

        long variableSetId = -1;
        if (StringUtils.isNotEmpty(variableSetStr)) {
            variableSetId = catalogManager.getStudyManager().getVariableSetId(variableSetStr, Long.toString(resource.getStudyId()),
                    sessionId).getResourceId();
        }

        return individualDBAdaptor.searchAnnotationSetAsMap(resource, variableSetId, annotation,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> searchAnnotationSet(String id, @Nullable String studyStr, String variableSetStr,
                                                          @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");

        MyResourceId resource = getId(id, studyStr, sessionId);
//        authorizationManager.checkIndividualPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
//                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS);

        long variableSetId = -1;
        if (StringUtils.isNotEmpty(variableSetStr)) {
            variableSetId = catalogManager.getStudyManager().getVariableSetId(variableSetStr, Long.toString(resource.getStudyId()),
                    sessionId).getResourceId();
        }

        return individualDBAdaptor.searchAnnotationSet(resource, variableSetId, annotation,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.toString());
    }

}
