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
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.FamilyConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.FamilyMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.FamilyAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
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
    private FamilyConverter familyConverter;

    public FamilyMongoDBAdaptor(MongoDBCollection familyCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(FamilyMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.familyCollection = familyCollection;
        this.familyConverter = new FamilyConverter();
    }

    /**
     * @return MongoDB connection to the family collection.
     */
    public MongoDBCollection getFamilyCollection() {
        return familyCollection;
    }

    @Override
    public WriteResult nativeInsert(Map<String, Object> family, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(family, "family");
        return familyCollection.insert(document, null);
    }

    @Override
    public WriteResult insert(long studyId, Family family, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody<WriteResult> txnBody = () -> {
            long tmpStartTime = startQuery();

            logger.debug("Starting family insert transaction for family id '{}'", family.getId());

            try {
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                insert(clientSession, studyId, family, variableSetList);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null, null);
            } catch (CatalogDBException e) {
                logger.error("Could not create family {}: {}", family.getId(), e.getMessage(), e);
                clientSession.abortTransaction();
                return endWrite(tmpStartTime, 1, 0, null,
                        Collections.singletonList(new WriteResult.Fail(family.getId(), e.getMessage())));
            }
        };
        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumInserted() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
        return result;
    }

    Family insert(ClientSession clientSession, long studyId, Family family, List<VariableSet> variableSetList) throws CatalogDBException {
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
        if (!get(clientSession, tmpQuery, new QueryOptions()).getResult().isEmpty()) {
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
        familyDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting family '{}' ({})...", family.getId(), family.getUid());
        familyCollection.insert(clientSession, familyDocument, null);
        logger.debug("Family '{}' successfully inserted", family.getId());

        return family;
    }

    private void createMissingIndividual(ClientSession clientSession, long studyUid, Individual individual,
                                         Map<String, Individual> individualMap, List<VariableSet> variableSetList)
            throws CatalogDBException {
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
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    QueryResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return familyCollection.count(clientSession, bson);
    }

    @Override
    public QueryResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        return count(null, query, user, studyPermissions);
    }

    public QueryResult<Long> count(ClientSession clientSession, final Query query, final String user,
                                   final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_FAMILIES : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getFamilyPermission().name(), Entity.FAMILY.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("Family count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return familyCollection.count(clientSession, bson);
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return familyCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public WriteResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();

        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        QueryResult<Family> familyQueryResult = get(id, queryOptions);
        if (familyQueryResult.first().getAnnotationSets().isEmpty()) {
            return new QueryResult<>("Get annotation set", familyQueryResult.getDbTime(), 0, 0, familyQueryResult.getWarningMsg(),
                    familyQueryResult.getErrorMsg(), Collections.emptyList());
        } else {
            List<AnnotationSet> annotationSets = familyQueryResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new QueryResult<>("Get annotation set", familyQueryResult.getDbTime(), size, size, familyQueryResult.getWarningMsg(),
                    familyQueryResult.getErrorMsg(), annotationSets);
        }
    }

    @Override
    public WriteResult update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        WriteResult update = update(new Query(QueryParams.UID.key(), id), parameters, variableSetList, queryOptions);
        if (update.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not update family with id " + id + ": " + update.getFailed().get(0).getMessage());
        }
        return update;
    }

    @Override
    public WriteResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public WriteResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single family
            if (count(query).first() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one family");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Family> iterator = iterator(query, options);

        int numMatches = 0;
        int numModified = 0;
        List<WriteResult.Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Family family = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody<WriteResult> txnBody = () -> {
                long tmpStartTime = startQuery();
                try {
                    Query tmpQuery = new Query()
                            .append(QueryParams.STUDY_UID.key(), family.getStudyUid())
                            .append(QueryParams.UID.key(), family.getUid());

                    if (queryOptions.getBoolean(Constants.REFRESH)) {
                        getLastVersionOfMembers(clientSession, tmpQuery, parameters);
                    }

                    if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
                        createNewVersion(clientSession, family.getStudyUid(), family.getUid());
                    }

                    updateAnnotationSets(clientSession, family.getUid(), parameters, variableSetList, queryOptions, true);

                    Document familyUpdate = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery).toFinalUpdateDocument();
                    if (!familyUpdate.isEmpty()) {
                        Bson finalQuery = parseQuery(tmpQuery);

                        logger.debug("Family update: query : {}, update: {}",
                                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                                familyUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                        familyCollection.update(clientSession, finalQuery, familyUpdate, new QueryOptions("multi", true));
                    }

                    return endWrite(tmpStartTime, 1, 1, null, null);
                } catch (CatalogDBException e) {
                    logger.error("Error updating family {}({}). {}", family.getId(), family.getUid(), e.getMessage(), e);
                    clientSession.abortTransaction();
                    return endWrite(tmpStartTime, 1, 0, null,
                            Collections.singletonList(new WriteResult.Fail(family.getId(), e.getMessage())));
                }
            };

            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumUpdated() == 1) {
                logger.info("Family {} successfully updated", family.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not update family {}: {}", family.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not update family {}", family.getId());
                }
            }
        }

        return endWrite(startTime, numMatches, numModified, null, failList);
    }

    private void getLastVersionOfMembers(ClientSession clientSession, Query query, ObjectMap parameters) throws CatalogDBException {
        if (parameters.containsKey(QueryParams.MEMBERS.key())) {
            throw new CatalogDBException("Invalid option: Cannot update to the last version of members and update to different members at "
                    + "the same time.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.MEMBERS.key());
        QueryResult<Family> queryResult = get(clientSession, query, options);

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
        QueryResult<Individual> individualQueryResult = dbAdaptorFactory.getCatalogIndividualDBAdaptor()
                .get(clientSession, individualQuery, options);
        parameters.put(QueryParams.MEMBERS.key(), individualQueryResult.getResult());
    }

    private void createNewVersion(ClientSession clientSession, long studyUid, long familyUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), familyUid);
        QueryResult<Document> queryResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find family '" + familyUid + "'");
        }

        createNewVersion(clientSession, familyCollection, queryResult.first());
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query) throws CatalogDBException {
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

            QueryResult<Family> familyQueryResult = get(clientSession, tmpQuery, queryOptions);
            if (familyQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update family: No family found to be updated");
            }
            if (familyQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update family: Cannot set the same name parameter for different families");
            }

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), familyQueryResult.first().getStudyUid());
            QueryResult<Long> count = count(clientSession, tmpQuery);
            if (count.getResult().get(0) > 0) {
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
    public WriteResult removeMembersFromFamily(Query query, List<Long> individualUids) throws CatalogDBException {
        Bson bson = parseQuery(query);
        Document update = new Document("$pull", new Document(QueryParams.MEMBERS.key(),
                new Document(IndividualDBAdaptor.QueryParams.UID.key(), new Document("$in", individualUids))));
        return familyCollection.update(bson, update, new QueryOptions(MongoDBCollection.MULTI, true));
    }

    @Override
    public WriteResult delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        WriteResult delete = delete(query);
        if (delete.getNumMatches() == 0) {
            throw new CatalogDBException("Could not delete family. Uid " + id + " not found.");
        } else if (delete.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not delete family. " + delete.getFailed().get(0).getMessage());
        }
        return delete;
    }

    @Override
    public WriteResult delete(Query query) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Family> iterator = iterator(query, options);

        long startTime = startQuery();
        int numMatches = 0;
        int numModified = 0;
        List<WriteResult.Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Family family = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody<WriteResult> txnBody = () -> {
                long tmpStartTime = startQuery();
                try {
                    logger.info("Deleting family {} ({})", family.getId(), family.getUid());

                    // Look for all the different family versions
                    Query familyQuery = new Query()
                            .append(QueryParams.UID.key(), family.getUid())
                            .append(QueryParams.STUDY_UID.key(), family.getStudyUid())
                            .append(Constants.ALL_VERSIONS, true);
                    DBIterator<Family> familyDbIterator = iterator(clientSession, familyQuery, options);

                    String deleteSuffix = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

                    while (familyDbIterator.hasNext()) {
                        Family tmpFamily = familyDbIterator.next();

                        familyQuery = new Query()
                                .append(QueryParams.UID.key(), tmpFamily.getUid())
                                .append(QueryParams.VERSION.key(), tmpFamily.getVersion())
                                .append(QueryParams.STUDY_UID.key(), tmpFamily.getStudyUid());
                        // Mark the family as deleted
                        ObjectMap updateParams = new ObjectMap()
                                .append(QueryParams.STATUS_NAME.key(), Status.DELETED)
                                .append(QueryParams.STATUS_DATE.key(), TimeUtils.getTime())
                                .append(QueryParams.ID.key(), tmpFamily.getId() + deleteSuffix);

                        Bson bsonQuery = parseQuery(familyQuery);
                        Document updateDocument = parseAndValidateUpdateParams(clientSession, updateParams, familyQuery)
                                .toFinalUpdateDocument();

                        logger.debug("Delete version {} of family: Query: {}, update: {}", tmpFamily.getVersion(),
                                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                        WriteResult result = familyCollection.update(clientSession, bsonQuery, updateDocument,
                                QueryOptions.empty());
                        if (result.getNumUpdated() == 1) {
                            logger.debug("Family version {} successfully deleted", tmpFamily.getVersion());
                        } else {
                            logger.error("Family version {} could not be deleted", tmpFamily.getVersion());
                        }
                    }

                    logger.info("Family {}({}) deleted", family.getId(), family.getUid());

                    return endWrite(tmpStartTime, 1, 1, null, null);
                } catch (CatalogDBException e) {
                    logger.error("Error deleting family {}({}). {}", family.getId(), family.getUid(), e.getMessage(), e);
                    clientSession.abortTransaction();
                    return endWrite(tmpStartTime, 1, 0, null,
                            Collections.singletonList(new WriteResult.Fail(family.getId(), e.getMessage())));
                }
            };
            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumUpdated() == 1) {
                logger.info("Family {} successfully deleted", family.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not delete family {}: {}", family.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not delete family {}", family.getId());
                }
            }
        }

        return endWrite(startTime, numMatches, numModified, null, failList);
    }

    @Override
    public WriteResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        checkId(id);
        // Check if the family is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The family {" + id + "} is not deleted");
        }

        // Change the status of the family to deleted
        return setStatus(id, Status.READY);
    }

    @Override
    public WriteResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return setStatus(query, Status.READY);
    }

    @Override
    public QueryResult<Family> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    public QueryResult<Family> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Family> documentList = new ArrayList<>();
        QueryResult<Family> queryResult;
        try (DBIterator<Family> dbIterator = iterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);
//        addMemberInfoToFamily(queryResult);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(clientSession, query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        return nativeGet(null, query, options);
    }

    QueryResult nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(clientSession, query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        return nativeGet(null, query, options, user);
    }

    QueryResult nativeGet(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(clientSession, query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Family> get(long familyId, QueryOptions options) throws CatalogDBException {
        checkId(familyId);
        Query query = new Query(QueryParams.UID.key(), familyId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED)
                .append(QueryParams.STUDY_UID.key(), getStudyId(familyId));
        return get(query, options);
    }

    @Override
    public QueryResult<Family> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return get(null, query, options, user);
    }

    public QueryResult<Family> get(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Family> documentList = new ArrayList<>();
        QueryResult<Family> queryResult;
        try (DBIterator<Family> dbIterator = iterator(clientSession, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(clientSession, query, user, StudyAclEntry.StudyPermissions.VIEW_FAMILIES);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<Family> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    DBIterator<Family> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new FamilyMongoDBIterator<>(mongoCursor, clientSession, familyConverter, null,
                dbAdaptorFactory.getCatalogIndividualDBAdaptor(), query.getLong(PRIVATE_STUDY_UID), null, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new FamilyMongoDBIterator(mongoCursor, clientSession, null, null, dbAdaptorFactory.getCatalogIndividualDBAdaptor(),
                query.getLong(PRIVATE_STUDY_UID), null, options);
    }

    @Override
    public DBIterator<Family> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return iterator(null, query, options, user);
    }

    public DBIterator<Family> iterator(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(clientSession, query);
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new FamilyMongoDBIterator<>(mongoCursor, null, familyConverter, iteratorFilter,
                dbAdaptorFactory.getCatalogIndividualDBAdaptor(), query.getLong(PRIVATE_STUDY_UID), user, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return nativeIterator(null, query, options, user);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(clientSession, query);
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new FamilyMongoDBIterator(mongoCursor, clientSession, null, iteratorFilter, dbAdaptorFactory.getCatalogIndividualDBAdaptor(),
                query.getLong(PRIVATE_STUDY_UID), user, options);
    }

    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(clientSession, query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, Document studyDocument,
                                                 String user) throws CatalogDBException, CatalogAuthorizationException {
        Document queryForAuthorisedEntries = null;
        if (studyDocument != null && user != null) {
            // Get the document query needed to check the permissions as well
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FAMILIES.name(), FamilyAclEntry.FamilyPermissions.VIEW.name(),
                    Entity.FAMILY.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeInnerProjections(qOptions, QueryParams.MEMBERS.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);

        logger.debug("Family query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return familyCollection.nativeQuery().find(clientSession, bson, qOptions).iterator();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return rank(familyCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(familyCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(familyCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(),
                    FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name(), Entity.FAMILY.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FAMILIES.name(), FamilyAclEntry.FamilyPermissions.VIEW.name(),
                    Entity.FAMILY.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(familyCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(),
                    FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name(), Entity.FAMILY.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FAMILIES.name(), FamilyAclEntry.FamilyPermissions.VIEW.name(),
                    Entity.FAMILY.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(familyCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
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
        QueryResult<Document> queryResult = familyCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Family", familyId);
        }
    }

    @Override
    public WriteResult updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        return familyCollection.update(bson, update, queryOptions);
    }

    @Override
    public WriteResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(familyCollection, studyId, permissionRuleId);
    }

    private WriteResult setStatus(long familyId, String status) throws CatalogDBException {
        return update(familyId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    private WriteResult setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    Bson parseQuery(Query query, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        Query queryCopy = new Query(query);

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
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Family.FamilyStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
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
        if (authorisation != null && authorisation.size() > 0) {
            andBsonList.add(authorisation);
        }
        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
