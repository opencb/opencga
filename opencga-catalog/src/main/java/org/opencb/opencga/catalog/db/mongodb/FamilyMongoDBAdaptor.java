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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.FamilyConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.FamilyCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.IndividualManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.PedigreeGraphUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyPermissions;
import org.opencb.opencga.core.models.family.FamilyStatus;
import org.opencb.opencga.core.models.family.PedigreeGraph;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 03/05/17.
 */
public class FamilyMongoDBAdaptor extends AnnotationMongoDBAdaptor<Family> implements FamilyDBAdaptor {

    private final MongoDBCollection familyCollection;
    private final MongoDBCollection archiveFamilyCollection;
    private final MongoDBCollection deletedFamilyCollection;
    private final FamilyConverter familyConverter;
    private final VersionedMongoDBAdaptor versionedMongoDBAdaptor;

    public FamilyMongoDBAdaptor(MongoDBCollection familyCollection, MongoDBCollection archiveFamilyCollection,
                                MongoDBCollection deletedFamilyCollection, Configuration configuration,
                                MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(FamilyMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.familyCollection = familyCollection;
        this.archiveFamilyCollection = archiveFamilyCollection;
        this.deletedFamilyCollection = deletedFamilyCollection;
        this.familyConverter = new FamilyConverter();
        this.versionedMongoDBAdaptor = new VersionedMongoDBAdaptor(familyCollection, archiveFamilyCollection, deletedFamilyCollection);
    }

    /**
     * @return MongoDB connection to the family collection.
     */
    public MongoDBCollection getFamilyCollection() {
        return familyCollection;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> family, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(family, "family");
        return new OpenCGAResult(familyCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult<Family> insert(long studyId, Family family, List<Individual> members, List<VariableSet> variableSetList,
                                        QueryOptions options) throws CatalogDBException, CatalogParameterException,
            CatalogAuthorizationException {
        try {
            AtomicReference<Family> familyCopy = new AtomicReference<>();
            OpenCGAResult<Family> result = runTransaction(clientSession -> {
                // Family is modified during insert. As the transaction might be re-run in case of
                // transient transaction error, we need to take a new copy of the input family every time.
                // Only if the transaction gets executed correctly, the input transaction is updated with
                // the modified copy.
                familyCopy.set(JacksonUtils.copySafe(family, Family.class));
                long tmpStartTime = startQuery();
                logger.debug("Starting family insert transaction for family id '{}'", familyCopy.get().getId());
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                insert(clientSession, studyId, familyCopy.get(), members, variableSetList);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
            // Do we really need to propagate these changes?
            JacksonUtils.updateSafe(family, familyCopy.get());
            return result;
        } catch (Exception e) {
            logger.error("Could not create family {}: {}", family.getId(), e.getMessage(), e);
            throw e;
        }
    }

    Family insert(ClientSession clientSession, long studyUid, Family family, List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return insert(clientSession, studyUid, family, null, variableSetList);
    }

    private Family insert(ClientSession clientSession, long studyUid, Family family, List<Individual> members,
                          List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (StringUtils.isEmpty(family.getId())) {
            throw new CatalogDBException("Missing family id");
        }

        Query tmpQuery = new Query()
                .append(QueryParams.ID.key(), family.getId())
                .append(QueryParams.STUDY_UID.key(), studyUid);
        if (!get(clientSession, tmpQuery, new QueryOptions()).getResults().isEmpty()) {
            throw CatalogDBException.alreadyExists("Family", "id", family.getId());
        }

        List<Individual> createIndividuals = family.getMembers();
        List<Individual> allIndividuals = new LinkedList<>();
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            allIndividuals.addAll(family.getMembers());
        }
        if (CollectionUtils.isNotEmpty(members)) {
            allIndividuals.addAll(members);
        }
        family.setMembers(allIndividuals);

        // First we check if we need to create any individuals
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            // In order to keep parent relations, we need to create parents before their children.

            // We initialise a map containing all the individuals
            Map<String, Individual> individualMap = new HashMap<>();
            for (Individual individual : family.getMembers()) {
                individualMap.put(individual.getId(), individual);
            }

            // We link father and mother to individual objects
            for (Map.Entry<String, Individual> entry : individualMap.entrySet()) {
                if (entry.getValue().getFather() != null && StringUtils.isNotEmpty(entry.getValue().getFather().getId())) {
                    entry.getValue().setFather(individualMap.get(entry.getValue().getFather().getId()));
                }
                if (entry.getValue().getMother() != null && StringUtils.isNotEmpty(entry.getValue().getMother().getId())) {
                    entry.getValue().setMother(individualMap.get(entry.getValue().getMother().getId()));
                }
            }

            // We start creating missing individuals
            for (Individual individual : createIndividuals) {
                createMissingIndividual(clientSession, studyUid, individual, individualMap, variableSetList);
            }
        }

        long familyUid = getNewUid();

        family.setUid(familyUid);
        family.setStudyUid(studyUid);
        family.setVersion(1);
        if (StringUtils.isEmpty(family.getUuid())) {
            family.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FAMILY));
        }
        Map<String, Map<String, Family.FamiliarRelationship>> roles = calculateRoles(clientSession, studyUid, family);
        family.setRoles(roles);

        // Pedigree graph
        try {
            PedigreeGraph pedigreeGraph = PedigreeGraphUtils.getPedigreeGraph(family, Paths.get(configuration.getWorkspace()).getParent(),
                    Paths.get(configuration.getAnalysis().getScratchDir()));
            family.setPedigreeGraph(pedigreeGraph);
        } catch (IOException e) {
            throw new CatalogDBException("Error computing pedigree graph for family " + family.getId(), e);
        }

        Document familyDocument = familyConverter.convertToStorageType(family, variableSetList);

        // Versioning private parameters
        familyDocument.put(RELEASE_FROM_VERSION, Arrays.asList(family.getRelease()));
        familyDocument.put(LAST_OF_VERSION, true);
        familyDocument.put(LAST_OF_RELEASE, true);
        familyDocument.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(family.getCreationDate()) ? TimeUtils.toDate(family.getCreationDate()) : TimeUtils.getDate());
        familyDocument.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(family.getModificationDate())
                ? TimeUtils.toDate(family.getModificationDate()) : TimeUtils.getDate());
        familyDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting family '{}' ({})...", family.getId(), family.getUid());
        versionedMongoDBAdaptor.insert(clientSession, familyDocument);
        logger.debug("Family '{}' successfully inserted", family.getId());

        // Add family reference to the members
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            List<String> individualIds = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().updateFamilyReferences(clientSession, studyUid, individualIds, family.getId(),
                    ParamUtils.BasicUpdateAction.ADD);
        }

        return family;
    }

    private void createMissingIndividual(ClientSession clientSession, long studyUid, Individual individual,
                                         Map<String, Individual> individualMap, List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (individual == null || individual.getUid() > 0 || individualMap.get(individual.getId()).getUid() > 0) {
            return;
        }
        if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getId())) {
            createMissingIndividual(clientSession, studyUid, individual.getFather(), individualMap, variableSetList);
            individual.setFather(individualMap.get(individual.getFather().getId()));
        }
        if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getId())) {
            createMissingIndividual(clientSession, studyUid, individual.getMother(), individualMap, variableSetList);
            individual.setMother(individualMap.get(individual.getMother().getId()));
        }
        Individual newIndividual = dbAdaptorFactory.getCatalogIndividualDBAdaptor().insert(clientSession, studyUid, individual,
                variableSetList);
        individualMap.put(individual.getId(), newIndividual);
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(familyCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query, user);
    }

    public OpenCGAResult<Long> count(ClientSession clientSession, final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Family count: query : {}", bson.toBsonDocument());
        return new OpenCGAResult<>(familyCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();

        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        OpenCGAResult<Family> familyDataResult = get(id, queryOptions);
        if (familyDataResult.first().getAnnotationSets().isEmpty()) {
            return new OpenCGAResult<>(familyDataResult.getTime(), familyDataResult.getEvents(), 0, Collections.emptyList(), 0);
        } else {
            List<AnnotationSet> annotationSets = familyDataResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new OpenCGAResult<>(familyDataResult.getTime(), familyDataResult.getEvents(), size, annotationSets, size);
        }
    }

    @Override
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(long familyUid, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key(),
                        QueryParams.MEMBERS.key() + "." + IndividualDBAdaptor.QueryParams.ID.key()));
        OpenCGAResult<Family> familyDataResult = get(familyUid, options);

        if (familyDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update family. Family uid '" + familyUid + "' not found.");
        }

        try {
            return runTransaction(clientSession
                    -> privateUpdate(clientSession, familyDataResult.first(), parameters, variableSetList, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update family {}: {}", familyDataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update family " + familyDataResult.first().getId() + ": " + e.getMessage(),
                    e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single family
            if (count(query).getNumMatches() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one family");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key(),
                        QueryParams.MEMBERS.key() + "." + IndividualDBAdaptor.QueryParams.ID.key()));
        DBIterator<Family> iterator = iterator(query, options);

        OpenCGAResult<Cohort> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Family family = iterator.next();
            try {
                result.append(runTransaction(clientSession ->
                        privateUpdate(clientSession, family, parameters, variableSetList, queryOptions)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update family {}: {}", family.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, family.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Family family, ObjectMap parameters, List<VariableSet> variableSetList,
                                        QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), family.getStudyUid())
                .append(QueryParams.UID.key(), family.getUid());

        Bson bsonQuery = parseQuery(tmpQuery);
        return versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
                    DataResult result = updateAnnotationSets(clientSession, family.getUid(), parameters, variableSetList, queryOptions,
                            true);
                    List<String> familyMemberIds = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
                    boolean updateRoles = queryOptions.getBoolean(ParamConstants.FAMILY_UPDATE_ROLES_PARAM);
                    boolean updatePedigree = queryOptions.getBoolean(ParamConstants.FAMILY_UPDATE_PEDIGREEE_GRAPH_PARAM);
                    if (CollectionUtils.isNotEmpty(parameters.getAsList(QueryParams.MEMBERS.key()))) {
                        List<Map> newIndividuals = parameters.getAsList(QueryParams.MEMBERS.key(), Map.class);
                        Set<String> newIndividualIds = newIndividuals.stream().map(i -> (String) i.get(IndividualDBAdaptor.QueryParams.ID
                                .key())).collect(Collectors.toSet());

                        Set<String> currentIndividualIds = family.getMembers().stream().map(Individual::getId).collect(Collectors.toSet());

                        // Obtain new members to be added to the family
                        List<String> missingIndividualIds = new ArrayList<>();
                        for (String newIndividualId : newIndividualIds) {
                            if (!currentIndividualIds.contains(newIndividualId)) {
                                missingIndividualIds.add(newIndividualId);
                            }
                        }

                        // Obtain members to remove from family
                        List<String> oldIndividualIds = new ArrayList<>();
                        for (String currentIndividualId : currentIndividualIds) {
                            if (!newIndividualIds.contains(currentIndividualId)) {
                                oldIndividualIds.add(currentIndividualId);
                            }
                        }

                        updateFamilyReferenceInIndividuals(clientSession, family, missingIndividualIds, oldIndividualIds);
                        updateRoles = true;
                        familyMemberIds = new ArrayList<>(newIndividualIds);
                    }

                    if (updateRoles) {
                        // CALCULATE ROLES
                        if (!familyMemberIds.isEmpty()) {
                            // Fetch individuals with relevant information to guess the relationship
                            Query individualQuery = new Query()
                                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), family.getStudyUid())
                                    .append(IndividualDBAdaptor.QueryParams.ID.key(), familyMemberIds);
                            QueryOptions relationshipOptions = dbAdaptorFactory.getCatalogIndividualDBAdaptor().fixOptionsForRelatives(
                                    null);
                            OpenCGAResult<Individual> memberResult = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(clientSession,
                                    individualQuery, relationshipOptions);
                            family.setMembers(memberResult.getResults());
                            Map<String, Map<String, Family.FamiliarRelationship>> roles = calculateRoles(clientSession, family
                                    .getStudyUid(), family);
                            parameters.put(QueryParams.ROLES.key(), roles);
                        } else {
                            parameters.put(QueryParams.ROLES.key(), Collections.emptyMap());
                        }
                    }

                    if (updatePedigree && !updateRoles && !parameters.containsKey(QueryParams.DISORDERS.key())) {
                        PedigreeGraph pedigreeGraph = computePedigreeGraph(clientSession, family);
                        parameters.put(QueryParams.PEDIGREE_GRAPH.key(), pedigreeGraph);
                    }

                    Document familyUpdate = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery).toFinalUpdateDocument();

                    if (familyUpdate.isEmpty() && result.getNumUpdated() == 0) {
                        if (!parameters.isEmpty()) {
                            logger.error("Non-processed update parameters: {}", parameters.keySet());
                        }
                        throw new CatalogDBException("Nothing to be updated");
                    }

                    List<Event> events = new ArrayList<>();
                    if (!familyUpdate.isEmpty()) {
                        Bson finalQuery = parseQuery(tmpQuery);

                        logger.debug("Family update: query : {}, update: {}", finalQuery.toBsonDocument(), familyUpdate.toBsonDocument());
                        result = familyCollection.update(clientSession, finalQuery, familyUpdate, new QueryOptions("multi", true));

                        // Compute pedigree graph
                        if (updateRoles || parameters.containsKey(QueryParams.DISORDERS.key())) {
                            PedigreeGraph pedigreeGraph = computePedigreeGraph(clientSession, family);
                            Document pedigreeGraphDoc = getMongoDBDocument(pedigreeGraph, "PedigreeGraph");

                            UpdateDocument updateDocument = new UpdateDocument()
                                    .setSet(new Document(QueryParams.PEDIGREE_GRAPH.key(), pedigreeGraphDoc));
                            familyUpdate = updateDocument.toFinalUpdateDocument();
                            familyCollection.update(clientSession, finalQuery, familyUpdate, new QueryOptions("multi", true));
                        }

                        if (parameters.containsKey(QueryParams.ID.key())) {
                            String newFamilyId = parameters.getString(QueryParams.ID.key());

                            // Fetch members (we don't trust those from the Family object because they could have been updated previously)
                            Query query = new Query()
                                    .append(IndividualDBAdaptor.QueryParams.FAMILY_IDS.key(), family.getId())
                                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), family.getStudyUid());
                            OpenCGAResult<Individual> individualResult = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(clientSession,
                                    query, IndividualManager.INCLUDE_INDIVIDUAL_IDS);
                            List<String> memberIds = individualResult.getResults().stream().map(Individual::getId)
                                    .collect(Collectors.toList());

                            // Remove familyId from all members
                            updateFamilyReferenceInIndividuals(clientSession, family, null, memberIds);
                            family.setId(newFamilyId);
                            updateFamilyReferenceInIndividuals(clientSession, family, memberIds, null);
                        }

                        if (result.getNumMatches() == 0) {
                            throw new CatalogDBException("Family " + family.getId() + " not found");
                        }
                        if (result.getNumUpdated() == 0) {
                            events.add(new Event(Event.Type.WARNING, family.getId(), "Family was already updated"));
                        }
                        logger.debug("Family {} successfully updated", family.getId());
                    }

                    return endWrite(tmpStartTime, 1, 1, events);
                }, Arrays.asList(QueryParams.MEMBERS_ID.key(), QueryParams.MEMBERS_SAMPLES_ID.key()),
                this::iterator, (DBIterator<Family> iterator) -> updateReferencesAfterFamilyVersionIncrement(clientSession, iterator));
    }

    private PedigreeGraph computePedigreeGraph(ClientSession clientSession, Family family)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.UID.key(), family.getUid())
                .append(QueryParams.STUDY_UID.key(), family.getStudyUid());
        Family tmpFamily = get(clientSession, query, QueryOptions.empty()).first();

        try {
            return PedigreeGraphUtils.getPedigreeGraph(tmpFamily,
                    Paths.get(configuration.getWorkspace()).getParent(),
                    Paths.get(configuration.getAnalysis().getScratchDir()));
        } catch (IOException e) {
            String msg = "Error computing/updating the pedigree graph for the family " + family.getId();
            logger.error(msg);
            throw new CatalogDBException(msg, e);
        }
    }

    private void updateReferencesAfterFamilyVersionIncrement(ClientSession clientSession, DBIterator<Family> iterator)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        while (iterator.hasNext()) {
            Family f = iterator.next();
            dbAdaptorFactory.getClinicalAnalysisDBAdaptor().updateClinicalAnalysisFamilyReferences(clientSession, f);
        }
    }

    private void updateFamilyReferenceInIndividuals(ClientSession clientSession, Family family, List<String> newIndividuals,
                                                    List<String> removeIndividuals)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        if (CollectionUtils.isNotEmpty(newIndividuals)) {
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().updateFamilyReferences(clientSession, family.getStudyUid(), newIndividuals,
                    family.getId(), ParamUtils.BasicUpdateAction.ADD);
        }
        if (CollectionUtils.isNotEmpty(removeIndividuals)) {
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().updateFamilyReferences(clientSession, family.getStudyUid(),
                    removeIndividuals, family.getId(), ParamUtils.BasicUpdateAction.REMOVE);
        }
    }

    /**
     * Update Individual references from any Family where they were used.
     *
     * @param clientSession Client session.
     * @param studyUid      Study uid.
     * @param individualMap Map containing all individuals that have changed containing their latest version.
     * @throws CatalogDBException CatalogDBException.
     * @throws CatalogParameterException CatalogParameterException.
     * @throws CatalogAuthorizationException CatalogAuthorizationException.
     */
    void updateIndividualReferencesInFamily(ClientSession clientSession, long studyUid, Map<Long, Individual> individualMap)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), new ArrayList<>(individualMap.keySet()));

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.UID.key(),
                        FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
                        FamilyDBAdaptor.QueryParams.MEMBERS.key() + "." + IndividualDBAdaptor.QueryParams.ID.key()));

        DBIterator<Family> iterator = dbAdaptorFactory.getCatalogFamilyDBAdaptor().iterator(clientSession, query, options);

        while (iterator.hasNext()) {
            Family family = iterator.next();
            boolean changed = false;

            List<Map<String, Object>> members = new ArrayList<>(family.getMembers().size());
            for (Individual member : family.getMembers()) {
                if (individualMap.containsKey(member.getUid())) {
                    Individual individual = individualMap.get(member.getUid());
                    if (member.getVersion() < individual.getVersion()) {
                        member.setVersion(individual.getVersion());
                        changed = true;
                    }
                }
                members.add(getMongoDBDocument(member, "Individual"));
            }

            if (changed) {
                Query tmpQuery = new Query()
                        .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                        .append(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid());

                ObjectMap params = new ObjectMap(FamilyDBAdaptor.QueryParams.MEMBERS.key(), members);

                UpdateDocument updateDocument = parseAndValidateUpdateParams(clientSession, params, tmpQuery);
                Document bsonUpdate = updateDocument.toFinalUpdateDocument();
                Bson bsonQuery = parseQuery(tmpQuery);
                versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
                            DataResult<?> result = familyCollection.update(clientSession, bsonQuery, bsonUpdate, QueryOptions.empty());
                            if (result.getNumUpdated() != 1) {
                                throw new CatalogDBException("Family '" + family.getId() + "' could not be updated to the latest member"
                                        + " versions");
                            }
                            return result;
                        }, Arrays.asList(QueryParams.MEMBERS_ID.key(), QueryParams.MEMBERS_SAMPLES_ID.key()),
                        this::iterator,
                        (DBIterator<Family> fIterator) -> updateReferencesAfterFamilyVersionIncrement(clientSession, fIterator));
            }
        }
    }

    void updateIndividualIdFromFamilies(ClientSession clientSession, long studyUid, long memberUid, String oldIndividualId,
                                        String newIndividualId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (StringUtils.isEmpty(oldIndividualId)) {
            throw new CatalogDBException("Empty old individual ID");
        }
        if (StringUtils.isEmpty(newIndividualId)) {
            throw new CatalogDBException("Empty new individual ID");
        }

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.MEMBER_UID.key(), memberUid);

        // We need to update the roles so it reflects the new individual id
        try (DBIterator<Family> iterator = iterator(clientSession, query,
                new QueryOptions(QueryParams.UID.key(), QueryParams.ROLES.key()))) {
            while (iterator.hasNext()) {
                Family family = iterator.next();

                if (family.getRoles() != null) {
                    boolean changed = false;
                    Map<String, Map<String, Family.FamiliarRelationship>> roles = new HashMap<>();

                    for (Map.Entry<String, Map<String, Family.FamiliarRelationship>> entry : family.getRoles().entrySet()) {
                        if (oldIndividualId.equals(entry.getKey())) {
                            roles.put(newIndividualId, entry.getValue());
                            changed = true;
                        } else {
                            if (entry.getValue() == null) {
                                roles.put(entry.getKey(), entry.getValue());
                            } else {
                                Map<String, Family.FamiliarRelationship> relationshipMap = new HashMap<>();
                                for (Map.Entry<String, Family.FamiliarRelationship> entry2 : entry.getValue().entrySet()) {
                                    if (oldIndividualId.equals(entry2.getKey())) {
                                        relationshipMap.put(newIndividualId, entry2.getValue());
                                        changed = true;
                                    } else {
                                        relationshipMap.put(entry.getKey(), entry2.getValue());
                                    }
                                }
                                roles.put(entry.getKey(), relationshipMap);
                            }
                        }
                    }

                    if (changed) {
                        Bson bsonQuery = parseQuery(new Query()
                                .append(QueryParams.STUDY_UID.key(), studyUid)
                                .append(QueryParams.UID.key(), family.getUid())
                        );
                        versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
                                    Bson update = Updates.set(QueryParams.ROLES.key(), getMongoDBDocument(roles, QueryParams.ROLES.key()));
                                    return familyCollection.update(clientSession, bsonQuery, update, QueryOptions.empty());
                                }, Arrays.asList(QueryParams.MEMBERS_ID.key(), QueryParams.MEMBERS_SAMPLES_ID.key()),
                                this::iterator,
                                (DBIterator<Family> fIterator) -> updateReferencesAfterFamilyVersionIncrement(clientSession, fIterator));
                    }
                }
            }
        }
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.MEMBERS.key(), QueryParams.PHENOTYPES.key(), QueryParams.DISORDERS.key(),
                QueryParams.STATUS.key(), QueryParams.QUALITY_CONTROL.key(), QueryParams.ROLES.key(), QueryParams.INTERNAL_STATUS.key(),
                QueryParams.PEDIGREE_GRAPH.key(), };
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        final String[] acceptedIntParams = {QueryParams.EXPECTED_SIZE.key()};
        filterIntParams(parameters, document.getSet(), acceptedIntParams);

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one family...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results from the same family...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STUDY_UID.key());

            OpenCGAResult<Family> familyDataResult = get(clientSession, tmpQuery, queryOptions);
            if (familyDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update family: No family found to be updated");
            }
            if (familyDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update family: Cannot set the same name parameter for different families");
            }

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), familyDataResult.first().getStudyUid());
            OpenCGAResult<Long> count = count(clientSession, tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot set '" + QueryParams.ID.key() + "' for family. A family with { '"
                        + QueryParams.ID.key() + "': '" + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        familyConverter.validateDocumentToUpdate(document.getSet());

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    @Override
    public OpenCGAResult delete(Family family) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), family.getUid())
                    .append(QueryParams.STUDY_UID.key(), family.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find family " + family.getId() + " with uid " + family.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete family {}: {}", family.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete family " + family.getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<Document> iterator = nativeIterator(query, new QueryOptions());

        OpenCGAResult<Family> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Document family = iterator.next();
            String familyId = family.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, family)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete family {}: {}", familyId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, familyId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document familyDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        String familyId = familyDocument.getString(QueryParams.ID.key());
        long familyUid = familyDocument.getLong(PRIVATE_UID);
        long studyUid = familyDocument.getLong(PRIVATE_STUDY_UID);

        logger.debug("Deleting family {} ({})", familyId, familyUid);

        // Check if family is in use in a case
        Query queryCheck = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(), familyUid)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        OpenCGAResult<Long> count = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().count(clientSession, queryCheck);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Could not delete family. Family is in use in " + count.getNumMatches() + " cases");
        }

        // Look for all the different family versions
        Query familyQuery = new Query()
                .append(QueryParams.UID.key(), familyUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(familyQuery);
        versionedMongoDBAdaptor.delete(clientSession, bson);
        // Remove family references
        removeFamilyReferences(clientSession, familyDocument);
        logger.debug("Family {}({}) deleted", familyId, familyUid);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    private void removeFamilyReferences(ClientSession clientSession, Document familyDocument)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        // Remove family reference from individuals
        Family family = familyConverter.convertToDataModelType(familyDocument);
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            List<String> members = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
            updateFamilyReferenceInIndividuals(clientSession, family, Collections.emptyList(), members);
        }
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult<Family> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    public OpenCGAResult<Family> get(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Family> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Family> get(long familyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(familyId);
        Query query = new Query(QueryParams.UID.key(), familyId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(familyId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Family> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return get(null, studyUid, query, options, user);
    }

    public OpenCGAResult<Family> get(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Family> dbIterator = iterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Family> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    DBIterator<Family> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new FamilyCatalogMongoDBIterator<>(mongoCursor, clientSession, familyConverter, null,
                dbAdaptorFactory.getCatalogIndividualDBAdaptor(), options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new FamilyCatalogMongoDBIterator(mongoCursor, clientSession, null, null, dbAdaptorFactory.getCatalogIndividualDBAdaptor(),
                options);
    }

    @Override
    public DBIterator<Family> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return iterator(null, studyUid, query, options, user);
    }

    public DBIterator<Family> iterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new FamilyCatalogMongoDBIterator<>(mongoCursor, null, familyConverter, iteratorFilter,
                dbAdaptorFactory.getCatalogIndividualDBAdaptor(), studyUid, user, options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    DBIterator nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new FamilyCatalogMongoDBIterator(mongoCursor, clientSession, null, iteratorFilter,
                dbAdaptorFactory.getCatalogIndividualDBAdaptor(), studyUid, user, options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeInnerProjections(qOptions, QueryParams.MEMBERS.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);
        fixAclProjection(qOptions);

        logger.debug("Family query : {}", bson.toBsonDocument());
        MongoDBCollection collection = getQueryCollection(query, familyCollection, archiveFamilyCollection, deletedFamilyCollection);
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return rank(familyCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(familyCollection, bsonQuery, field, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(familyCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(familyCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult<>(familyCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        StopWatch stopWatch = StopWatch.createStarted();
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        Set<String> results = new LinkedHashSet<>();
        for (String field : fields) {
            results.addAll(familyCollection.distinct(field, bson, String.class).getResults());
        }

        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), results.size(),
                new ArrayList<>(results), -1);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<Family> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    @Override
    protected MongoDBCollection getCollection() {
        return this.familyCollection;
    }

    public MongoDBCollection getArchiveFamilyCollection() {
        return archiveFamilyCollection;
    }

    @Override
    public long getStudyId(long familyId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, familyId);
        Bson projection = Projections.include(PRIVATE_STUDY_UID);
        DataResult<Document> queryResult = familyCollection.find(query, projection, null);

        if (!queryResult.getResults().isEmpty()) {
            Object studyId = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Family", familyId);
        }
    }

    @Override
    public OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        return versionedMongoDBAdaptor.updateWithoutVersionIncrement(bson, () -> {
            Document update = new Document()
                    .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

            QueryOptions queryOptions = new QueryOptions("multi", true);

            return new OpenCGAResult(familyCollection.update(bson, update, queryOptions));
        });
    }

    Map<String, Map<String, Family.FamiliarRelationship>> calculateRoles(ClientSession clientSession, long studyUid, Family family)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (family.getMembers() == null || family.getMembers().size() <= 1) {
            family.setRoles(Collections.emptyMap());
            // Nothing to calculate
            return Collections.emptyMap();
        }

        Set<String> individualIds = family.getMembers().stream().map(Individual::getId).collect(Collectors.toSet());

        Map<String, Map<String, Family.FamiliarRelationship>> roles = new HashMap<>();
        for (Individual member : family.getMembers()) {
            List<Individual> individualList = dbAdaptorFactory.getCatalogIndividualDBAdaptor().calculateRelationship(clientSession,
                    studyUid, member, 2, null);
            Map<String, Family.FamiliarRelationship> memberRelation = new HashMap<>();
            for (Individual individual : individualList) {
                if (individualIds.contains(individual.getId())) {
                    memberRelation.put(individual.getId(), extractIndividualRelation(individual));
                }
            }
            roles.put(member.getId(), memberRelation);
        }

        return roles;
    }

    /**
     * Assuming the individual entry contains the OPENCGA_RELATIVE attributes, it will extract the relation.
     *
     * @param individual Individual entry.
     * @return The relation out of OPENCGA_RELATIVE attributes.
     */
    private static Family.FamiliarRelationship extractIndividualRelation(Individual individual) {
        if (individual.getAttributes() != null && individual.getAttributes().containsKey("OPENCGA_RELATIVE")) {
            return (Family.FamiliarRelationship) ((ObjectMap) individual.getAttributes().get("OPENCGA_RELATIVE")).get("RELATION");
        }
        return Family.FamiliarRelationship.UNKNOWN;
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(familyCollection, studyId, permissionRuleId);
    }

    protected Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, null);
    }

    protected Bson parseQuery(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, null);
    }

    protected Bson parseQuery(Query query, Document extraQuery, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        if (query.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(QueryParams.STUDY_UID.key()));

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.FAMILY, user,
                        configuration));
            } else {
                if (containsAnnotationQuery(query)) {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                            FamilyPermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.FAMILY, configuration));
                } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FamilyPermissions.VIEW.name(),
                            Enums.Resource.FAMILY, configuration));
                }
            }

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        if ("all".equalsIgnoreCase(queryCopy.getString(QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(QueryParams.VERSION.key());
        }

        boolean uidVersionQueryFlag = versionedMongoDBAdaptor.generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.ALL_VERSIONS.equals(entry.getKey()) || Constants.PRIVATE_ANNOTATION_PARAM_TYPES.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PHENOTYPES:
                    case DISORDERS:
                        addDefaultOrQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(queryCopy.getString(QueryParams.ANNOTATION.key()),
                                    queryCopy.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(QueryParams.STATUS_ID.key(), queryParam.key(), query, QueryParams.STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(FamilyStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), query,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case MEMBER_UID:
                    case UUID:
                    case ID:
                    case NAME:
                    case EXPECTED_SIZE:
                    case RELEASE:
                    case VERSION:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + queryCopy.toJson(), e);
                }
            }
        }

        // If the user doesn't look for a concrete version...
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())
                && queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
            // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
            andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
        }

        if (annotationDocument != null && !annotationDocument.isEmpty()) {
            andBsonList.add(annotationDocument);
        }
        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
        }
        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
