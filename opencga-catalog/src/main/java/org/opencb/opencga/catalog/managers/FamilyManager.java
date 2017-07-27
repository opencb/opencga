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
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IAnnotationSetManager;
import org.opencb.opencga.catalog.managers.api.ResourceManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.FamilyAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.AnnotationManager;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.db.api.FamilyDBAdaptor.QueryParams.ONTOLOGIES;
import static org.opencb.opencga.catalog.db.api.FamilyDBAdaptor.QueryParams.ONTOLOGY_TERMS;

/**
 * Created by pfurio on 02/05/17.
 */
public class FamilyManager extends AbstractManager implements ResourceManager<Long, Family>, IAnnotationSetManager {

    public FamilyManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                         DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param familyStr Family id in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one family id is found.
     */
    public MyResourceId getId(String familyStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(familyStr)) {
            throw new CatalogException("Missing family parameter");
        }

        String userId;
        long studyId;
        long familyId;

        if (StringUtils.isNumeric(familyStr) && Long.parseLong(familyStr) > configuration.getCatalog().getOffset()) {
            familyId = Long.parseLong(familyStr);
            familyDBAdaptor.exists(familyId);
            studyId = familyDBAdaptor.getStudyId(familyId);
            userId = catalogManager.getUserManager().getId(sessionId);
        } else {
            if (familyStr.contains(",")) {
                throw new CatalogException("More than one family found");
            }

            userId = catalogManager.getUserManager().getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FamilyDBAdaptor.QueryParams.NAME.key(), familyStr);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.ID.key());
            QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, queryOptions);
            if (familyQueryResult.getNumResults() == 1) {
                familyId = familyQueryResult.first().getId();
            } else {
                if (familyQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Family " + familyStr + " not found in study " + studyStr);
                } else {
                    throw new CatalogException("More than one family found under " + familyStr + " in study " + studyStr);
                }
            }
        }

        return new MyResourceId(userId, studyId, familyId);
    }

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param familyStr Family id in string format. Could be either the id or name.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    public MyResourceIds getIds(String familyStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(familyStr)) {
            throw new CatalogException("Missing family parameter");
        }

        String userId;
        long studyId;
        List<Long> familyIds = new ArrayList<>();

        if (StringUtils.isNumeric(familyStr) && Long.parseLong(familyStr) > configuration.getCatalog().getOffset()) {
            familyIds = Arrays.asList(Long.parseLong(familyStr));
            familyDBAdaptor.checkId(familyIds.get(0));
            studyId = familyDBAdaptor.getStudyId(familyIds.get(0));
            userId = catalogManager.getUserManager().getId(sessionId);
        } else {
            userId = catalogManager.getUserManager().getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            List<String> familySplit = Arrays.asList(familyStr.split(","));
            for (String familyStrAux : familySplit) {
                if (StringUtils.isNumeric(familyStrAux)) {
                    long familyId = Long.parseLong(familyStrAux);
                    familyDBAdaptor.exists(familyId);
                    familyIds.add(familyId);
                }
            }

            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FamilyDBAdaptor.QueryParams.NAME.key(), familySplit);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.ID.key());
            QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, queryOptions);

            if (familyQueryResult.getNumResults() > 0) {
                familyIds.addAll(familyQueryResult.getResult().stream().map(Family::getId).collect(Collectors.toList()));
            }

            if (familyIds.size() < familySplit.size()) {
                throw new CatalogException("Found only " + familyIds.size() + " out of the " + familySplit.size()
                        + " families looked for in study " + studyStr);
            }
        }

        return new MyResourceIds(userId, studyId, familyIds);
    }

    public QueryResult<Family> create(String studyStr, Family family, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_FAMILIES);

        ParamUtils.checkObj(family, "family");
        ParamUtils.checkAlias(family.getName(), "name", configuration.getCatalog().getOffset());
        family.setCreationDate(TimeUtils.getTime());
        family.setDescription(ParamUtils.defaultString(family.getDescription(), ""));
        family.setStatus(new Status());
        family.setOntologyTerms(ParamUtils.defaultObject(family.getOntologyTerms(), Collections.emptyList()));
        family.setAnnotationSets(ParamUtils.defaultObject(family.getAnnotationSets(), Collections.emptyList()));
        family.setAnnotationSets(AnnotationManager.validateAnnotationSets(family.getAnnotationSets(), studyDBAdaptor));
        family.setAcl(Collections.emptyList());
        family.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));
        family.setAttributes(ParamUtils.defaultObject(family.getAttributes(), Collections.emptyMap()));

        checkAndCreateAllIndividualsFromFamily(studyId, family, sessionId);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        QueryResult<Family> queryResult = familyDBAdaptor.insert(family, studyId, options);
        auditManager.recordAction(AuditRecord.Resource.family, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    private void checkAndCreateAllIndividualsFromFamily(long studyId, Family family, String sessionId) throws CatalogException {
        if (family.getMother() == null) {
            family.setMother(new Individual().setId(-1));
        }
        if (family.getFather() == null) {
            family.setFather(new Individual().setId(-1));
        }

        // Check all individuals exist or can be created
        checkAndCreateIndividual(studyId, family.getMother(), Individual.Sex.FEMALE, false, sessionId);
        checkAndCreateIndividual(studyId, family.getFather(), Individual.Sex.MALE, false, sessionId);
        if (family.getChildren() != null) {
            for (Individual individual : family.getChildren()) {
                checkAndCreateIndividual(studyId, individual, null, false, sessionId);
            }
        } else {
            family.setChildren(Collections.emptyList());
        }

        // Create the ones that did not exist
        checkAndCreateIndividual(studyId, family.getMother(), null, true, sessionId);
        checkAndCreateIndividual(studyId, family.getFather(), null, true, sessionId);
        for (Individual individual : family.getChildren()) {
            checkAndCreateIndividual(studyId, individual, null, true, sessionId);
        }
    }


    /**
     * This method should be called two times. First time with !create to check if every individual is fine or can be created and a
     * second time with create to create the individual if is needed.
     *
     * @param studyId studyId.
     * @param individual individual.
     * @param sex When !create, it will check whether the individual sex corresponds with the sex given. If null, this will not be checked.
     * @param create Boolean indicating whether to make only checks or to create the individual.
     * @param sessionId sessionID.
     * @throws CatalogException catalogException.
     */
    private void checkAndCreateIndividual(long studyId, Individual individual, Individual.Sex sex, boolean create, String sessionId)
            throws CatalogException {
        if (!create) {
            // Just check everything is fine

            if (individual.getId() > 0 || (StringUtils.isNotEmpty(individual.getName()) && StringUtils.isNumeric(individual.getName()))
                    && Long.parseLong(individual.getName()) > 0) {
                if (individual.getId() <= 0) {
                    individual.setId(Long.parseLong(individual.getName()));
                }
                individualDBAdaptor.checkId(individual.getId());

                // Check studyId of the individual
                long studyIdIndividual = individualDBAdaptor.getStudyId(individual.getId());
                if (studyId != studyIdIndividual) {
                    throw new CatalogException("Cannot create family in a different study than the one corresponding to the individuals.");
                }

                if (sex != null) {
                    // Check the sex
                    Query query = new Query()
                            .append(IndividualDBAdaptor.QueryParams.ID.key(), individual.getId())
                            .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                    QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SEX.key());

                    QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options);
                    if (individualQueryResult.getNumResults() != 1) {
                        throw new CatalogException("Internal error. Found " + individualQueryResult.getNumResults() + " results when it was"
                                + " expected to get 1 individual result");
                    }
                    if (individualQueryResult.first().getSex() != sex) {
                        throw new CatalogException("The sex of the individual " + individual.getId() + " does not correspond with "
                                + "the expected sex: " + sex);
                    }
                }
            } else {
                if (StringUtils.isNotEmpty(individual.getName())) {
                    Query query = new Query()
                            .append(IndividualDBAdaptor.QueryParams.NAME.key(), individual.getName())
                            .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                    QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                            Arrays.asList(IndividualDBAdaptor.QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.SEX.key()));

                    QueryResult<Individual> individualQueryResult = individualDBAdaptor.get(query, options);
                    if (individualQueryResult.getNumResults() == 1) {
                        // Check the sex
                        if (sex != null && individualQueryResult.first().getSex() != sex) {
                            throw new CatalogException("The sex of the individual " + individual.getName() + " does not correspond with "
                                    + "the expected sex: " + sex);
                        }

                        individual.setId(individualQueryResult.first().getId());
                    } else {
                        // The individual has to be created.
                        if (sex != null && sex != individual.getSex()) {
                            throw new CatalogException("The sex of the individual " + individual.getName() + " does not correspond with "
                                    + "the expected sex: " + sex);
                        }
                    }
                }
            }
        } else {
            // Create if it was not already created
            if (individual.getId() <= 0 && StringUtils.isNotEmpty(individual.getName())) {
                // We create the individual
                QueryResult<Individual> individualQueryResult =
                        catalogManager.getIndividualManager().create(Long.toString(studyId), individual, new QueryOptions(), sessionId);
                if (individualQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Unexpected error occurred when creating the individual");
                } else {
                    // We set the id
                    individual.setId(individualQueryResult.first().getId());
                }
            }
        }

    }

    @Override
    public QueryResult<Family> get(Long id, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = familyDBAdaptor.getStudyId(id);

        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.ID.key(), id)
                .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(query, options, userId);
        if (familyQueryResult.getNumResults() <= 0) {
            throw CatalogAuthorizationException.deny(userId, "view", "family", id, "");
        }
        return familyQueryResult;
    }

    public QueryResult<Family> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getId(sessionId);

        query.append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Family> queryResult = familyDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    @Override
    public QueryResult<Family> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        return get(query.getLong(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), -1), query, options, sessionId);
    }


    public QueryResult<Family> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        // We change the FATHER, MOTHER and CHILDREN parameters for FATHER_ID, MOTHER_ID and CHILDREN_IDS which is what the DBAdaptor
        // understands
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.FATHER.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getString(FamilyDBAdaptor.QueryParams.FATHER.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.FATHER_ID.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.FATHER.key());
        }
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MOTHER.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getString(FamilyDBAdaptor.QueryParams.MOTHER.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.MOTHER_ID.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.MOTHER.key());
        }
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.CHILDREN.key()))) {
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getString(FamilyDBAdaptor.QueryParams.CHILDREN.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.CHILDREN_IDS.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.CHILDREN.key());
        }

        query.append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<Family> queryResult = familyDBAdaptor.get(query, options, userId);
//            authorizationManager.filterFamilies(userId, studyId, queryResultAux.getResult());
        return queryResult;
    }

    public QueryResult<Family> count(String studyStr, Query query, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // The individuals introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        // We change the FATHER, MOTHER and CHILDREN parameters for FATHER_ID, MOTHER_ID and CHILDREN_IDS which is what the DBAdaptor
        // understands
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.FATHER.key()))) {
//            String studyStrAux = studyIds.size() == 1 ? Long.toString(studyIds.get(0)) : null;
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getString(FamilyDBAdaptor.QueryParams.FATHER.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.FATHER_ID.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.FATHER.key());
        }
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.MOTHER.key()))) {
//            String studyStrAux = studyIds.size() == 1 ? Long.toString(studyIds.get(0)) : null;
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getString(FamilyDBAdaptor.QueryParams.MOTHER.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.MOTHER_ID.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.MOTHER.key());
        }
        if (StringUtils.isNotEmpty(query.getString(FamilyDBAdaptor.QueryParams.CHILDREN.key()))) {
//            String studyStrAux = studyIds.size() == 1 ? Long.toString(studyIds.get(0)) : null;
            MyResourceIds resourceIds = catalogManager.getIndividualManager()
                    .getIds(query.getString(FamilyDBAdaptor.QueryParams.CHILDREN.key()), Long.toString(studyId), sessionId);
            query.put(FamilyDBAdaptor.QueryParams.CHILDREN_IDS.key(), resourceIds.getResourceIds());
            query.remove(FamilyDBAdaptor.QueryParams.CHILDREN.key());
        }

        query.append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = familyDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_FAMILIES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public QueryResult<Family> update(Long id, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        parameters = ParamUtils.defaultObject(parameters, ObjectMap::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceId resource = getId(Long.toString(id), null, sessionId);
        authorizationManager.checkFamilyPermission(resource.getStudyId(), id, resource.getUser(), FamilyAclEntry.FamilyPermissions.UPDATE);

        QueryResult<Family> familyQueryResult = familyDBAdaptor.get(id, new QueryOptions());
        if (familyQueryResult.getNumResults() == 0) {
            throw new CatalogException("Family " + id + " not found");
        }

        long individual;
        Iterator<Map.Entry<String, Object>> iterator = parameters.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> param = iterator.next();
            FamilyDBAdaptor.QueryParams queryParam = FamilyDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case NAME:
                    ParamUtils.checkAlias(parameters.getString(queryParam.key()), "name", configuration.getCatalog().getOffset());
                    break;
                case MOTHER_ID:
                    individual = parameters.getLong(param.getKey());
                    if (familyQueryResult.first().getMother().getId() > 0) {
                        if (individual != familyQueryResult.first().getMother().getId()) {
                            throw new CatalogException("Cannot update mother parameter of family. The family " + id + " already has "
                                    + "the mother defined");
                        } else {
                            iterator.remove();
                        }
                    }
                    individualDBAdaptor.checkId(individual);
                    break;
                case FATHER_ID:
                    individual = parameters.getLong(param.getKey());
                    if (familyQueryResult.first().getFather().getId() > 0) {
                        if (individual != familyQueryResult.first().getFather().getId()) {
                            throw new CatalogException("Cannot update mother parameter of family. The family " + id + " already has "
                                    + "the father defined");
                        } else {
                            iterator.remove();
                        }
                    }
                    individualDBAdaptor.checkId(individual);
                    break;
                case CHILDREN_IDS:
                    if (familyQueryResult.first().getChildren().size() > 0) {
                        throw new CatalogException("Cannot update children parameter of family. The family " + id + " already has "
                                + "children defined");
                    }
                    List<Long> individualList = parameters.getAsLongList(param.getKey());
                    for (Long individualId : individualList) {
                        individualDBAdaptor.checkId(individualId);
                    }
                    break;
                case ONTOLOGIES:
                case ONTOLOGY_TERMS:
                    try {
                        List<OntologyTerm> ontologyTerms = (List<OntologyTerm>) parameters.get(param.getKey());
                    } catch (RuntimeException e) {
                        throw new CatalogException("Invalid list of ontology terms.");
                    }
                    break;
                case DESCRIPTION:
                case ATTRIBUTES:
                case PARENTAL_CONSANGUINITY:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        if (parameters.containsKey(ONTOLOGIES.key())) {
            parameters.put(ONTOLOGY_TERMS.key(), parameters.get(ONTOLOGIES.key()));
            parameters.remove(ONTOLOGIES.key());
        }

        QueryResult<Family> queryResult = familyDBAdaptor.update(id, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.family, id, resource.getUser(), parameters, null, null);
        return queryResult;
    }

    @Override
    public List<QueryResult<Family>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        return null;
    }

    @Override
    public List<QueryResult<Family>> restore(String ids, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public List<QueryResult<Family>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public void setStatus(String id, @Nullable String status, @Nullable String message, String sessionId) throws CatalogException {

    }

    @Override
    public QueryResult<AnnotationSet> createAnnotationSet(String id, @Nullable String studyStr, String variableSetId,
                                                          String annotationSetName, Map<String, Object> annotations,
                                                          Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        MyResourceId resourceId = getId(id, studyStr, sessionId);
        authorizationManager.checkFamilyPermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
                FamilyAclEntry.FamilyPermissions.WRITE_ANNOTATIONS);
        MyResourceId variableSetResource = catalogManager.getStudyManager().getVariableSetId(variableSetId,
                Long.toString(resourceId.getStudyId()), sessionId);

        QueryResult<VariableSet> variableSet = studyDBAdaptor.getVariableSet(variableSetResource.getResourceId(), null,
                resourceId.getUser(), null);
        if (variableSet.getNumResults() == 0) {
            // Variable set must be confidential and the user does not have those permissions
            throw new CatalogAuthorizationException("Permission denied: User " + resourceId.getUser() + " cannot create annotations over "
                    + "that variable set");
        }

        QueryResult<AnnotationSet> annotationSet = AnnotationManager.createAnnotationSet(resourceId.getResourceId(), variableSet.first(),
                annotationSetName, annotations, catalogManager.getStudyManager().getCurrentRelease(resourceId.getStudyId()), attributes,
                familyDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.family, resourceId.getResourceId(), resourceId.getUser(),
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> getAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        MyResourceId resource = commonGetAllAnnotationSets(id, studyStr, sessionId);
        return familyDBAdaptor.getAnnotationSet(resource, null,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, @Nullable String studyStr, String sessionId) throws
            CatalogException {
        MyResourceId resource = commonGetAllAnnotationSets(id, studyStr, sessionId);
        return familyDBAdaptor.getAnnotationSetAsMap(resource, null,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return familyDBAdaptor.getAnnotationSet(resource, annotationSetName,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<ObjectMap> getAnnotationSetAsMap(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        MyResourceId resource = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return familyDBAdaptor.getAnnotationSetAsMap(resource, annotationSetName,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, Map<String,
            Object> newAnnotations, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        MyResourceId resourceId = getId(id, studyStr, sessionId);
        authorizationManager.checkFamilyPermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
                FamilyAclEntry.FamilyPermissions.WRITE_ANNOTATIONS);

        // Update the annotation
        QueryResult<AnnotationSet> queryResult =
                AnnotationManager.updateAnnotationSet(resourceId, annotationSetName, newAnnotations, familyDBAdaptor, studyDBAdaptor);

        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("There was an error with the update");
        }

        AnnotationSet annotationSet = queryResult.first();

        // Audit the changes
        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getCreationDate(), 1, null);
        auditManager.recordUpdate(AuditRecord.Resource.family, resourceId.getResourceId(), resourceId.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSetUpdate)), "Update annotation", null);

        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String
            sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");

        MyResourceId resourceId = getId(id, studyStr, sessionId);
        authorizationManager.checkFamilyPermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
                FamilyAclEntry.FamilyPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> annotationSet = familyDBAdaptor.getAnnotationSet(resourceId.getResourceId(), annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }
        // We make this query because it will check the proper permissions in case the variable set is confidential
        studyDBAdaptor.getVariableSet(annotationSet.first().getVariableSetId(), new QueryOptions(), resourceId.getUser(), null);

        familyDBAdaptor.deleteAnnotationSet(resourceId.getResourceId(), annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.family, resourceId.getResourceId(), resourceId.getUser(),
                new ObjectMap("annotationSets", Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }

    @Override
    public QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, @Nullable String studyStr, String variableSetStr,
                                                           @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");

        AbstractManager.MyResourceId resourceId = getId(id, studyStr, sessionId);
//        authorizationManager.checkFamilyPermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS);

        long variableSetId = -1;
        if (StringUtils.isNotEmpty(variableSetStr)) {
            variableSetId = catalogManager.getStudyManager().getVariableSetId(variableSetStr, Long.toString(resourceId.getStudyId()),
                    sessionId).getResourceId();
        }

        return familyDBAdaptor.searchAnnotationSetAsMap(resourceId, variableSetId, annotation,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.toString());
    }

    @Override
    public QueryResult<AnnotationSet> searchAnnotationSet(String id, @Nullable String studyStr, String variableSetStr,
                                                          @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");

        AbstractManager.MyResourceId resourceId = getId(id, studyStr, sessionId);
//        authorizationManager.checkFamilyPermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS);

        long variableSetId = -1;
        if (StringUtils.isNotEmpty(variableSetStr)) {
            variableSetId = catalogManager.getStudyManager().getVariableSetId(variableSetStr, Long.toString(resourceId.getStudyId()),
                    sessionId).getResourceId();
        }

        return familyDBAdaptor.searchAnnotationSet(resourceId, variableSetId, annotation,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.toString());
    }

    private MyResourceId commonGetAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        return getId(id, studyStr, sessionId);
//        authorizationManager.checkFamilyPermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS);
//        return resourceId.getResourceId();
    }

    private MyResourceId commonGetAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkAlias(annotationSetName, "annotationSetName", configuration.getCatalog().getOffset());
        return getId(id, studyStr, sessionId);
//        authorizationManager.checkFamilyPermission(resourceId.getStudyId(), resourceId.getResourceId(), resourceId.getUser(),
//                FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS);
//        return resourceId.getResourceId();
    }

    public List<QueryResult<FamilyAclEntry>> updateAcl(String family, String studyStr, String memberIds, AclParams familyAclParams,
                                                       String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(family)) {
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

        MyResourceIds resourceIds = getIds(family, studyStr, sessionId);

        String collectionName = MongoDBAdaptorFactory.FAMILY_COLLECTION;
        // Check the user has the permissions needed to change permissions over those families
        for (Long familyId : resourceIds.getResourceIds()) {
            authorizationManager.checkFamilyPermission(resourceIds.getStudyId(), familyId, resourceIds.getUser(),
                    FamilyAclEntry.FamilyPermissions.SHARE);
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

        switch (familyAclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
            case ADD:
                return authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
            case REMOVE:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
            case RESET:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, collectionName);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }
}
