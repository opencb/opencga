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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.FamilyConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.FamilyMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyAclEntry;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 03/05/17.
 */
public class FamilyMongoDBAdaptor extends AnnotationMongoDBAdaptor<Family> implements FamilyDBAdaptor {

    private final MongoDBCollection familyCollection;
    private final MongoDBCollection deletedFamilyCollection;
    private FamilyConverter familyConverter;

    public FamilyMongoDBAdaptor(MongoDBCollection familyCollection, MongoDBCollection deletedFamilyCollection,
                                MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(FamilyMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.familyCollection = familyCollection;
        this.deletedFamilyCollection = deletedFamilyCollection;
        this.familyConverter = new FamilyConverter();
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
    public OpenCGAResult insert(long studyId, Family family, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting family insert transaction for family id '{}'", family.getId());

                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                insert(clientSession, studyId, family, variableSetList);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create family {}: {}", family.getId(), e.getMessage(), e);
            throw e;
        }
    }

    Family insert(ClientSession clientSession, long studyId, Family family, List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // First we check if we need to create any individuals
        if (family.getMembers() != null && !family.getMembers().isEmpty()) {
            // In order to keep parent relations, we need to create parents before their children.

            // We initialise a map containing all the individuals
            Map<String, Individual> individualMap = new HashMap<>();
            List<Individual> individualsToCreate = new ArrayList<>();
            for (Individual individual : family.getMembers()) {
                individualMap.put(individual.getId(), individual);
                if (individual.getUid() <= 0) {
                    individualsToCreate.add(individual);
                }
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
            for (Individual individual : individualsToCreate) {
                createMissingIndividual(clientSession, studyId, individual, individualMap, variableSetList);
            }

            family.setMembers(new ArrayList<>(individualMap.values()));
        }

        if (StringUtils.isEmpty(family.getId())) {
            throw new CatalogDBException("Missing family id");
        }
        Query tmpQuery = new Query()
                .append(QueryParams.ID.key(), family.getId())
                .append(QueryParams.STUDY_UID.key(), studyId);
        if (!get(clientSession, tmpQuery, new QueryOptions()).getResults().isEmpty()) {
            throw CatalogDBException.alreadyExists("Family", "id", family.getId());
        }

        long familyUid = getNewUid(clientSession);

        family.setUid(familyUid);
        family.setStudyUid(studyId);
        family.setVersion(1);
        if (StringUtils.isEmpty(family.getUuid())) {
            family.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FAMILY));
        }
        if (StringUtils.isEmpty(family.getCreationDate())) {
            family.setCreationDate(TimeUtils.getTime());
        }

        Document familyDocument = familyConverter.convertToStorageType(family, variableSetList);

        // Versioning private parameters
        familyDocument.put(RELEASE_FROM_VERSION, Arrays.asList(family.getRelease()));
        familyDocument.put(LAST_OF_VERSION, true);
        familyDocument.put(LAST_OF_RELEASE, true);
        familyDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(family.getCreationDate()));
        familyDocument.put(PRIVATE_MODIFICATION_DATE, familyDocument.get(PRIVATE_CREATION_DATE));
        familyDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting family '{}' ({})...", family.getId(), family.getUid());
        familyCollection.insert(clientSession, familyDocument, null);
        logger.debug("Family '{}' successfully inserted", family.getId());

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
        logger.debug("Family count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(familyCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult distinct(Query query, String field)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult(familyCollection.distinct(field, bson));
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
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
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
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
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

        if (queryOptions.getBoolean(Constants.REFRESH)) {
            getLastVersionOfMembers(clientSession, tmpQuery, parameters);
        }

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(clientSession, family.getStudyUid(), family.getUid());
        }

        DataResult result = updateAnnotationSets(clientSession, family.getUid(), parameters, variableSetList, queryOptions, true);
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

            logger.debug("Family update: query : {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    familyUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            result = familyCollection.update(clientSession, finalQuery, familyUpdate, new QueryOptions("multi", true));

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Family " + family.getId() + " not found");
            }
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, family.getId(), "Family was already updated"));
            }
            logger.debug("Family {} successfully updated", family.getId());
        }

        return endWrite(tmpStartTime, 1, 1, events);
    }

    private void getLastVersionOfMembers(ClientSession clientSession, Query query, ObjectMap parameters)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.MEMBERS.key())) {
            throw new CatalogDBException("Invalid option: Cannot update to the last version of members and update to different members at "
                    + "the same time.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.MEMBERS.key());
        OpenCGAResult<Family> queryResult = get(clientSession, query, options);

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Family not found.");
        }
        if (queryResult.getNumResults() > 1) {
            throw new CatalogDBException("Update to the last version of members for multiple families at once is not supported.");
        }

        Family family = queryResult.first();
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            // Nothing to do
            return;
        }

        List<Long> individualIds = family.getMembers().stream().map(Individual::getUid).collect(Collectors.toList());
        Query individualQuery = new Query()
                .append(IndividualDBAdaptor.QueryParams.UID.key(), individualIds);
        options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                IndividualDBAdaptor.QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.VERSION.key()
        ));
        OpenCGAResult<Individual> individualDataResult = dbAdaptorFactory.getCatalogIndividualDBAdaptor()
                .get(clientSession, individualQuery, options);
        parameters.put(QueryParams.MEMBERS.key(), individualDataResult.getResults());
    }

    private void createNewVersion(ClientSession clientSession, long studyUid, long familyUid)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), familyUid);
        OpenCGAResult<Document> queryResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find family '" + familyUid + "'");
        }

        createNewVersion(clientSession, familyCollection, queryResult.first());
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.MEMBERS.key(), QueryParams.PHENOTYPES.key(), QueryParams.DISORDERS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

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

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            document.getSet().put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        familyConverter.validateDocumentToUpdate(document.getSet());

        if (!document.toFinalUpdateDocument().isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        return document;
    }

    @Override
    public OpenCGAResult removeMembersFromFamily(Query query, List<Long> individualUids)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        Document update = new Document("$pull", new Document(QueryParams.MEMBERS.key(),
                new Document(IndividualDBAdaptor.QueryParams.UID.key(), new Document("$in", individualUids))));
        return new OpenCGAResult(familyCollection.update(bson, update, new QueryOptions(MongoDBCollection.MULTI, true)));
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

        // Look for all the different family versions
        Query familyQuery = new Query()
                .append(QueryParams.UID.key(), familyUid)
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(Constants.ALL_VERSIONS, true);
        DBIterator<Document> familyDbIterator = nativeIterator(clientSession, familyQuery, new QueryOptions());

        // Delete any documents that might have been already deleted with that id
        Bson query = new Document()
                .append(QueryParams.ID.key(), familyId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedFamilyCollection.remove(clientSession, query, new QueryOptions(MongoDBCollection.MULTI, true));

        while (familyDbIterator.hasNext()) {
            Document tmpFamily = familyDbIterator.next();

            // Set status to DELETED
            tmpFamily.put(QueryParams.STATUS.key(), getMongoDBDocument(new Status(Status.DELETED), "status"));

            int sampleVersion = tmpFamily.getInteger(QueryParams.VERSION.key());

            // Insert the document in the DELETE collection
            deletedFamilyCollection.insert(clientSession, tmpFamily, null);
            logger.debug("Inserted family uid '{}' version '{}' in DELETE collection", familyUid, sampleVersion);

            // Remove the document from the main SAMPLE collection
            query = parseQuery(new Query()
                    .append(QueryParams.UID.key(), familyUid)
                    .append(QueryParams.VERSION.key(), sampleVersion));
            DataResult remove = familyCollection.remove(clientSession, query, null);
            if (remove.getNumMatches() == 0) {
                throw new CatalogDBException("Family " + familyId + " not found");
            }
            if (remove.getNumDeleted() == 0) {
                throw new CatalogDBException("Family " + familyId + " could not be deleted");
            }

            logger.debug("Family uid '{}' version '{}' deleted from main SAMPLE collection", familyUid, sampleVersion);
        }

        logger.debug("Family {}({}) deleted", familyId, familyUid);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
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
        List<Family> documentList = new ArrayList<>();
        try (DBIterator<Family> dbIterator = iterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    OpenCGAResult nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    OpenCGAResult nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
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
        List<Family> documentList = new ArrayList<>();
        try (DBIterator<Family> dbIterator = iterator(clientSession, studyUid, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
    }

    @Override
    public DBIterator<Family> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    DBIterator<Family> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new FamilyMongoDBIterator<>(mongoCursor, clientSession, familyConverter, null,
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

        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new FamilyMongoDBIterator(mongoCursor, clientSession, null, null, dbAdaptorFactory.getCatalogIndividualDBAdaptor(), options);
    }

    @Override
    public DBIterator<Family> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return iterator(null, studyUid, query, options, user);
    }

    public DBIterator<Family> iterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new FamilyMongoDBIterator<>(mongoCursor, null, familyConverter, iteratorFilter,
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
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new FamilyMongoDBIterator(mongoCursor, clientSession, null, iteratorFilter, dbAdaptorFactory.getCatalogIndividualDBAdaptor(),
                studyUid, user, options);
    }

    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
    }

    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
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

        logger.debug("Family query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return familyCollection.nativeQuery().find(clientSession, bson, qOptions).iterator();
        } else {
            return deletedFamilyCollection.nativeQuery().find(clientSession, bson, qOptions).iterator();
        }
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
    protected AnnotableConverter<? extends Annotable> getConverter() {
        return this.familyConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return this.familyCollection;
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

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        return new OpenCGAResult(familyCollection.update(bson, update, queryOptions));
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
            if (containsAnnotationQuery(query)) {
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                        FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.FAMILY));
            } else {
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FamilyAclEntry.FamilyPermissions.VIEW.name(),
                        Enums.Resource.FAMILY));
            }

            andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.FAMILY, user));

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        boolean uidVersionQueryFlag = generateUidVersionQuery(queryCopy, andBsonList);

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
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PHENOTYPES:
                    case DISORDERS:
                        addOntologyQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
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
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Family.FamilyStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.STATUS_NAME.key(), queryParam.key(), query, QueryParams.STATUS_NAME.type(), andBsonList);
                        break;
                    case MEMBER_UID:
                    case UUID:
                    case ID:
                    case NAME:
                    case DESCRIPTION:
                    case EXPECTED_SIZE:
                    case RELEASE:
                    case VERSION:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
                    case STATUS_MSG:
                    case STATUS_DATE:
//                    case ANNOTATION_SETS:
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
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())) {
            if (queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
                // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
                andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
            } else {
                // Otherwise, we will always look for the latest version
                andBsonList.add(Filters.eq(LAST_OF_VERSION, true));
            }
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
