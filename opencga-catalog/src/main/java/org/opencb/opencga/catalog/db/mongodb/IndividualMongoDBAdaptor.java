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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.datastore.core.result.WriteResult.Fail;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.IndividualConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.IndividualMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
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
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualMongoDBAdaptor extends AnnotationMongoDBAdaptor<Individual> implements IndividualDBAdaptor {

    private final MongoDBCollection individualCollection;
    private IndividualConverter individualConverter;

    public IndividualMongoDBAdaptor(MongoDBCollection individualCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(IndividualMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.individualCollection = individualCollection;
        this.individualConverter = new IndividualConverter();
    }

    @Override
    protected AnnotableConverter<? extends Annotable> getConverter() {
        return individualConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return individualCollection;
    }

    public boolean exists(ClientSession clientSession, long individualId) {
        return individualCollection.count(clientSession, new Document(PRIVATE_UID, individualId)).first() != 0;
    }

    @Override
    public WriteResult nativeInsert(Map<String, Object> individual, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(individual, "individual");
        return individualCollection.insert(document, null);
    }

    @Override
    public WriteResult insert(long studyId, Individual individual, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody<WriteResult> txnBody = () -> {
            long tmpStartTime = startQuery();

            logger.debug("Starting individual insert transaction for individual id '{}'", individual.getId());
            // TODO: Add loggers to every action. Test it.

            try {
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
                insert(clientSession, studyId, individual, variableSetList);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null, null);
            } catch (CatalogDBException e) {
                logger.error("Could not create individual {}: {}", individual.getId(), e.getMessage(), e);
                clientSession.abortTransaction();
                return endWrite(tmpStartTime, 1, 0, null,
                        Collections.singletonList(new Fail(individual.getId(), e.getMessage())));
            }
        };
        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumInserted() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
        return result;
    }

    Individual insert(ClientSession clientSession, long studyId, Individual individual, List<VariableSet> variableSetList)
            throws CatalogDBException {
        // First we check if we need to create any samples and update current list of samples with the ones created
        if (individual.getSamples() != null && !individual.getSamples().isEmpty()) {
            List<Sample> sampleList = new ArrayList<>(individual.getSamples().size());

            SampleMongoDBAdaptor sampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

            for (Sample sample : individual.getSamples()) {
                if (sample.getUid() <= 0) {
                    logger.debug("Sample '{}' needs to be created. Inserting sample...", sample.getId());
                    // Sample needs to be created
                    Sample newSample = sampleDBAdaptor.insert(clientSession, studyId, sample, variableSetList);
                    sampleList.add(newSample);
                } else {
                    logger.debug("Sample '{}' was already registered. No need to create it.", sample.getId());
                    sampleList.add(sample);
                }
                individual.setSamples(sampleList);
            }
        }

        if (StringUtils.isEmpty(individual.getId())) {
            throw new CatalogDBException("Missing individual id");
        }
        if (!get(clientSession, new Query(QueryParams.ID.key(), individual.getId())
                .append(QueryParams.STUDY_UID.key(), studyId), new QueryOptions()).getResult().isEmpty()) {
            throw CatalogDBException.alreadyExists("Individual", "id", individual.getId());
        }
        if (individual.getFather() != null && individual.getFather().getUid() > 0
                && !exists(clientSession, individual.getFather().getUid())) {
            throw CatalogDBException.idNotFound("Individual", individual.getFather().getId());
        }
        if (individual.getMother() != null && individual.getMother().getUid() > 0
                && !exists(clientSession, individual.getMother().getUid())) {
            throw CatalogDBException.idNotFound("Individual", individual.getMother().getId());
        }

        long individualUid = getNewUid(clientSession);

        individual.setUid(individualUid);
        individual.setStudyUid(studyId);
        individual.setVersion(1);
        if (StringUtils.isEmpty(individual.getUuid())) {
            individual.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.INDIVIDUAL));
        }
        if (StringUtils.isEmpty(individual.getCreationDate())) {
            individual.setCreationDate(TimeUtils.getTime());
        }

        Document individualDocument = individualConverter.convertToStorageType(individual, variableSetList);

        // Versioning private parameters
        individualDocument.put(RELEASE_FROM_VERSION, Arrays.asList(individual.getRelease()));
        individualDocument.put(LAST_OF_VERSION, true);
        individualDocument.put(LAST_OF_RELEASE, true);
        individualDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(individual.getCreationDate()));
        individualDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting individual '{}' ({})...", individual.getId(), individual.getUid());
        individualCollection.insert(clientSession, individualDocument, null);
        logger.debug("Individual '{}' successfully inserted", individual.getId());

        if (individual.getSamples() != null && !individual.getSamples().isEmpty()) {
            for (Sample sample : individual.getSamples()) {
                // We associate the samples to the individual just created
                updateIndividualFromSampleCollection(clientSession, studyId, sample.getUid(), individual.getId());
            }
        }

        return individual;
    }

    private void updateIndividualFromSampleCollection(ClientSession clientSession, long studyId, long sampleUid, String individualId)
            throws CatalogDBException {
        SampleMongoDBAdaptor sampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

        ObjectMap params = new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
        Document update = sampleDBAdaptor.parseAndValidateUpdateParams(clientSession, null, params).toFinalUpdateDocument();
        Bson query = sampleDBAdaptor.parseQuery(new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleUid), null);

        sampleDBAdaptor.getCollection().update(clientSession, query, update, null);
    }

    @Override
    public long getStudyId(long individualId) throws CatalogDBException {
        QueryResult<Document> result =
                individualCollection.find(new Document(PRIVATE_UID, individualId), new Document(PRIVATE_STUDY_UID, 1), null);

        if (!result.getResult().isEmpty()) {
            return (long) result.getResult().get(0).get(PRIVATE_STUDY_UID);
        } else {
            throw CatalogDBException.uidNotFound("Individual", individualId);
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

        return individualCollection.update(bson, update, queryOptions);
    }

    @Override
    public WriteResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(individualCollection, studyId, permissionRuleId);
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    public QueryResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return individualCollection.count(clientSession, bson);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        return count(null, query, user, studyPermissions);
    }

    QueryResult<Long> count(ClientSession clientSession, Query query, String user, StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getIndividualPermission().name(), Entity.INDIVIDUAL.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("Individual count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()));
        return individualCollection.count(clientSession, bson);
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return individualCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public WriteResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, null, queryOptions);
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

        QueryResult<Individual> individualQueryResult = get(id, queryOptions);
        if (individualQueryResult.first().getAnnotationSets().isEmpty()) {
            return new QueryResult<>("Get annotation set", individualQueryResult.getDbTime(), 0, 0, individualQueryResult.getWarningMsg(),
                    individualQueryResult.getErrorMsg(), Collections.emptyList());
        } else {
            List<AnnotationSet> annotationSets = individualQueryResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new QueryResult<>("Get annotation set", individualQueryResult.getDbTime(), size, size,
                    individualQueryResult.getWarningMsg(), individualQueryResult.getErrorMsg(), annotationSets);
        }
    }

    @Override
    public WriteResult update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        WriteResult update = update(new Query(QueryParams.UID.key(), id), parameters, variableSetList, queryOptions);
        if (update.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not update individual with id " + id + ": " + update.getFailed().get(0).getMessage());
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
            // We need to check that the update is only performed over 1 single individual
            if (count(query).first() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key()
                        + "' can only be updated for one individual");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Individual> iterator = iterator(query, options);

        // TODO: We might need to add one check, and if the parameters being updated can be performed globally, perform the updateMany
        //  instead of updating one by one.

        int numMatches = 0;
        int numModified = 0;
        List<Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Individual individual = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody<WriteResult> txnBody = () -> {
                long tmpStartTime = startQuery();
                try {
                    update(clientSession, individual, parameters, variableSetList, queryOptions);
                    return endWrite(tmpStartTime, 1, 1, null, null);
                } catch (CatalogDBException e) {
                    logger.error("Error updating individual {}({}). {}", individual.getId(), individual.getUid(), e.getMessage(), e);
                    clientSession.abortTransaction();
                    return endWrite(tmpStartTime, 1, 0, null,
                            Collections.singletonList(new Fail(individual.getId(), e.getMessage())));
                }
            };
            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumUpdated() == 1) {
                logger.info("Individual {} successfully updated", individual.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not update individual {}: {}", individual.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not update individual {}", individual.getId());
                }
            }
        }

        return endWrite(startTime, numMatches, numModified, null, failList);


        // TODO: Review. This condition might not be needed any more? This condition made sense when we deleted individuals preparing the
        //  update in the IndividualManager which is no longer the case. Now we have a proper individualDBADaptor.delete();
//        if (updateDocument.getSet().containsKey(QueryParams.STATUS_NAME.key())) {
//            updateAnnotationSets(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList, queryOptions, true);
//            query.put(Constants.ALL_VERSIONS, true);
//
//            Bson finalQuery = parseQuery(query);
//            Document finalUpdateDocument = updateDocument.toFinalUpdateDocument();
//            logger.debug("Individual update: query : {}, update: {}",
//                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
//                    finalUpdateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
//
//            QueryResult<UpdateResult> update = individualCollection.update(finalQuery, finalUpdateDocument,
//                    new QueryOptions("multi", true));
//            return endQuery("Update individual", startTime, Arrays.asList(update.getNumTotalResults()));
//        }
    }

    private void update(ClientSession clientSession, Individual individual, ObjectMap parameters, List<VariableSet> variableSetList,
                        QueryOptions queryOptions) throws CatalogDBException {
        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), individual.getStudyUid())
                .append(QueryParams.UID.key(), individual.getUid());

        if (queryOptions.getBoolean(Constants.REFRESH)) {
            // Add the latest sample versions in the parameters object
            updateToLastSampleVersions(clientSession, tmpQuery, parameters, queryOptions);
        }

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(clientSession, individual.getStudyUid(), individual.getUid());
        }

        updateAnnotationSets(clientSession, individual.getUid(), parameters, variableSetList, queryOptions, true);

        UpdateDocument updateDocument = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery, queryOptions);
        Document individualUpdate = updateDocument.toFinalUpdateDocument();

        if (!individualUpdate.isEmpty()) {
            Bson finalQuery = parseQuery(tmpQuery);

            logger.debug("Individual update: query : {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    individualUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            individualCollection.update(clientSession, finalQuery, individualUpdate, new QueryOptions("multi", true));

            if (!updateDocument.getAttributes().isEmpty()) {
                List<Long> addedSamples = updateDocument.getAttributes().getAsLongList("ADDED_SAMPLES");
                List<Long> removedSamples = updateDocument.getAttributes().getAsLongList("REMOVED_SAMPLES");

                for (long sampleUid : addedSamples) {
                    // Set new individual reference
                    updateIndividualFromSampleCollection(clientSession, individual.getStudyUid(), sampleUid, individual.getId());
                }

                for (long sampleUid : removedSamples) {
                    // Set individual reference to ""
                    updateIndividualFromSampleCollection(clientSession, individual.getStudyUid(), sampleUid, "");
                }
            }

            // If the list of disorders or phenotypes is altered, we will need to update the corresponding effective lists
            // of the families associated (if any)
            if (parameters.containsKey(QueryParams.DISORDERS.key()) || parameters.containsKey(QueryParams.PHENOTYPES.key())) {
                FamilyMongoDBAdaptor familyDBAdaptor = dbAdaptorFactory.getCatalogFamilyDBAdaptor();

                Query familyQuery = new Query()
                        .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individual.getUid())
                        .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), individual.getStudyUid());
                QueryOptions familyOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.MEMBERS.key()));

                DBIterator<Family> familyIterator = familyDBAdaptor.iterator(clientSession, familyQuery, familyOptions);

                while (familyIterator.hasNext()) {
                    Family family = familyIterator.next();

                    // Update the list of disorders and phenotypes
                    ObjectMap params = new ObjectMap()
                            .append(FamilyDBAdaptor.QueryParams.DISORDERS.key(),
                                    familyDBAdaptor.getAllDisorders(family.getMembers()))
                            .append(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(),
                                    familyDBAdaptor.getAllPhenotypes(family.getMembers()));

                    Bson bsonQuery = familyDBAdaptor.parseQuery(new Query(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid()));
                    Document update = familyDBAdaptor.parseAndValidateUpdateParams(clientSession, params, null)
                            .toFinalUpdateDocument();

                    familyDBAdaptor.getFamilyCollection().update(clientSession, bsonQuery, update, QueryOptions.empty());
                }
            }
        }
    }

    private void createNewVersion(ClientSession clientSession, long studyUid, long individualUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.UID.key(), individualUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);
        QueryResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find individual '" + individualUid + "'");
        }

        createNewVersion(clientSession, individualCollection, queryResult.first());
    }

    private void updateToLastSampleVersions(ClientSession clientSession, Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException {
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            throw new CatalogDBException("Invalid option: Cannot update to the last version of samples and update to different samples at "
                    + "the same time.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.SAMPLES.key());
        QueryResult<Individual> queryResult = get(query, options);

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Individual not found.");
        }
        if (queryResult.getNumResults() > 1) {
            throw new CatalogDBException("Update to the last version of samples in multiple individuals at once not supported.");
        }

        Individual individual = queryResult.first();
        if (individual.getSamples() == null || individual.getSamples().isEmpty()) {
            // Nothing to do
            return;
        }

        List<Long> sampleIds = individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toList());
        Query sampleQuery = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleIds);
        options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.VERSION.key()
        ));
        QueryResult<Sample> sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(clientSession, sampleQuery, options);
        parameters.put(QueryParams.SAMPLES.key(), sampleQueryResult.getResult());

        // Add SET action for samples
        queryOptions.putIfAbsent(Constants.ACTIONS, new HashMap<>());
        queryOptions.getMap(Constants.ACTIONS).put(QueryParams.SAMPLES.key(), SET);
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Individual individual = checkOnlyOneIndividualMatches(clientSession, query);

            // Check that the new individual name is still unique
            long studyId = getStudyId(individual.getUid());

            Query tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId)
                    .append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
            QueryResult<Long> count = count(clientSession, tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot set id for individual. An individual with { id: '"
                        + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.ETHNICITY.key(), QueryParams.SEX.key(),
                QueryParams.POPULATION_NAME.key(), QueryParams.POPULATION_SUBPOPULATION.key(), QueryParams.POPULATION_DESCRIPTION.key(),
                QueryParams.KARYOTYPIC_SEX.key(), QueryParams.LIFE_STATUS.key(), QueryParams.AFFECTATION_STATUS.key(),
                QueryParams.DATE_OF_BIRTH.key(), };
        filterStringParams(parameters, document.getSet(), acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap((QueryParams.SEX.key()), IndividualProperty.Sex.class);
        filterEnumParams(parameters, document.getSet(), acceptedEnums);

        String[] acceptedIntParams = {QueryParams.FATHER_UID.key(), QueryParams.MOTHER_UID.key()};
        filterLongParams(parameters, document.getSet(), acceptedIntParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] acceptedObjectParams = {QueryParams.PHENOTYPES.key(), QueryParams.DISORDERS.key(), QueryParams.MULTIPLES.key(),
                QueryParams.LOCATION.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            document.getSet().put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        //Check individualIds exist
        String[] individualIdParams = {QueryParams.FATHER_UID.key(), QueryParams.MOTHER_UID.key()};
        for (String individualIdParam : individualIdParams) {
            if (document.getSet().containsKey(individualIdParam)) {
                Long individualId1 = (Long) document.getSet().get(individualIdParam);
                if (individualId1 > 0 && !exists(individualId1)) {
                    throw CatalogDBException.uidNotFound("Individual " + individualIdParam, individualId1);
                }
            }
        }

        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            // That can only be done to one individual...
            Individual individual = checkOnlyOneIndividualMatches(clientSession, query);

            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            String operation = (String) actionMap.getOrDefault(QueryParams.SAMPLES.key(), ParamUtils.UpdateAction.ADD.name());

            getSampleChanges(individual, parameters, document, operation);

            acceptedObjectParams = new String[]{QueryParams.SAMPLES.key()};
            switch (operation) {
                case "SET":
                    filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
                    individualConverter.validateSamplesToUpdate(document.getSet());
                    break;
                case "REMOVE":
                    filterObjectParams(parameters, document.getPullAll(), acceptedObjectParams);
                    individualConverter.validateSamplesToUpdate(document.getPullAll());
                    break;
                case "ADD":
                default:
                    filterObjectParams(parameters, document.getAddToSet(), acceptedObjectParams);
                    individualConverter.validateSamplesToUpdate(document.getAddToSet());
                    break;
            }
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        return document;
    }

    private void getSampleChanges(Individual individual, ObjectMap parameters, UpdateDocument updateDocument, String operation) {
        List<Sample> sampleList = parameters.getAsList(QueryParams.SAMPLES.key(), Sample.class);

        Set<Long> currentSampleUidList = new HashSet<>();
        if (individual.getSamples() != null) {
            currentSampleUidList = individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet());
        }

        if ("SET".equals(operation) || "ADD".equals(operation)) {
            // We will see which of the samples are actually new
            List<Long> samplesToAdd = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (!currentSampleUidList.contains(sample.getUid())) {
                    samplesToAdd.add(sample.getUid());
                }
            }

            if (!samplesToAdd.isEmpty()) {
                updateDocument.getAttributes().put("ADDED_SAMPLES", samplesToAdd);
            }

            if ("SET".equals(operation) && individual.getSamples() != null) {
                // We also need to see which samples existed and are not currently in the new list provided by the user to take them out
                Set<Long> newSampleUids = sampleList.stream().map(Sample::getUid).collect(Collectors.toSet());

                List<Long> samplesToRemove = new ArrayList<>();
                for (Sample sample : individual.getSamples()) {
                    if (!newSampleUids.contains(sample.getUid())) {
                        samplesToRemove.add(sample.getUid());
                    }
                }

                if (!samplesToRemove.isEmpty()) {
                    updateDocument.getAttributes().put("REMOVED_SAMPLES", samplesToRemove);
                }
            }
        } else if ("REMOVE".equals(operation)) {
            // We will only store the samples to be removed that are already associated to the individual
            List<Long> samplesToRemove = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (currentSampleUidList.contains(sample.getUid())) {
                    samplesToRemove.add(sample.getUid());
                }
            }

            if (!samplesToRemove.isEmpty()) {
                updateDocument.getAttributes().put("REMOVED_SAMPLES", samplesToRemove);
            }
        }
    }

    private Individual checkOnlyOneIndividualMatches(ClientSession clientSession, Query query) throws CatalogDBException {
        Query tmpQuery = new Query(query);
        // We take out ALL_VERSION from query just in case we get multiple results from the same individual...
        tmpQuery.remove(Constants.ALL_VERSIONS);

        QueryResult<Individual> individualQueryResult = get(clientSession, tmpQuery, new QueryOptions());
        if (individualQueryResult.getNumResults() == 0) {
            throw new CatalogDBException("Update individual: No individual found to be updated");
        }
        if (individualQueryResult.getNumResults() > 1) {
            throw CatalogDBException.cannotUpdateMultipleEntries(QueryParams.ID.key(), "individual");
        }
        return individualQueryResult.first();
    }

    @Override
    public WriteResult delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        WriteResult delete = delete(query);
        if (delete.getNumMatches() == 0) {
            throw new CatalogDBException("Could not delete individual. Uid " + id + " not found.");
        } else if (delete.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not delete individual. " + delete.getFailed().get(0).getMessage());
        }
        return delete;
    }

    @Override
    public WriteResult delete(Query query) throws CatalogDBException {
        FamilyMongoDBAdaptor familyDBAdaptor = dbAdaptorFactory.getCatalogFamilyDBAdaptor();

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key(),
                        QueryParams.SAMPLES.key() + "." + SampleDBAdaptor.QueryParams.UID.key()));
        DBIterator<Individual> iterator = iterator(query, options);

        long startTime = startQuery();
        int numMatches = 0;
        int numModified = 0;
        List<Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Individual individual = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody<WriteResult> txnBody = () -> {
                long tmpStartTime = startQuery();
                try {
                    logger.info("Deleting individual {} ({})", individual.getId(), individual.getUid());

                    // Remove individual reference from the list of samples
                    if (individual.getSamples() != null) {
                        for (Sample sample : individual.getSamples()) {
                            // We set the individual id for those samples to ""
                            updateIndividualFromSampleCollection(clientSession, individual.getStudyUid(), sample.getUid(), "");
                        }
                    }

                    // Remove individual from any list of members it might be part of
                    Query familyQuery = new Query()
                            .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individual.getUid())
                            .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), individual.getStudyUid())
                            .append(Constants.ALL_VERSIONS, true);

                    QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                            Arrays.asList(FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.VERSION.key(),
                                    FamilyDBAdaptor.QueryParams.MEMBERS.key()));
                    DBIterator<Family> familyIterator = familyDBAdaptor.iterator(familyQuery, queryOptions);

                    while (familyIterator.hasNext()) {
                        Family family = familyIterator.next();

                        List<Individual> members = new ArrayList<>();
                        for (Individual member : family.getMembers()) {
                            if (member.getUid() != individual.getUid()) {
                                members.add(member);
                            }
                        }
                        // Remove the member and update the list of disorders and phenotypes
                        ObjectMap params = new ObjectMap()
                                .append(FamilyDBAdaptor.QueryParams.MEMBERS.key(), members)
                                .append(FamilyDBAdaptor.QueryParams.DISORDERS.key(), familyDBAdaptor.getAllDisorders(members))
                                .append(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), familyDBAdaptor.getAllPhenotypes(members));

                        Bson bsonQuery = familyDBAdaptor.parseQuery(new Query()
                                .append(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid())
                                .append(FamilyDBAdaptor.QueryParams.VERSION.key(), family.getVersion())
                        );
                        Document update = familyDBAdaptor.parseAndValidateUpdateParams(clientSession, params, null).toFinalUpdateDocument();

                        logger.debug("Remove individual references from family: Query: {}, update: {}",
                                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                        WriteResult result = familyDBAdaptor.getFamilyCollection().update(clientSession, bsonQuery, update,
                                new QueryOptions(MongoDBCollection.MULTI, true));
                        logger.debug("Families found: {}, families updated: {}", result.getNumMatches(), result.getNumUpdated());
                    }

                    // Look for all the different individual versions
                    Query individualQuery = new Query()
                            .append(QueryParams.UID.key(), individual.getUid())
                            .append(QueryParams.STUDY_UID.key(), individual.getStudyUid())
                            .append(Constants.ALL_VERSIONS, true);
                    DBIterator<Individual> individualDBIterator = iterator(individualQuery, options);

                    String deleteSuffix = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

                    while (individualDBIterator.hasNext()) {
                        Individual tmpIndividual = individualDBIterator.next();

                        individualQuery = new Query()
                                .append(QueryParams.UID.key(), tmpIndividual.getUid())
                                .append(QueryParams.VERSION.key(), tmpIndividual.getVersion())
                                .append(QueryParams.STUDY_UID.key(), tmpIndividual.getStudyUid());
                        // Mark the individual as deleted
                        ObjectMap updateParams = new ObjectMap()
                                .append(QueryParams.STATUS_NAME.key(), Status.DELETED)
                                .append(QueryParams.STATUS_DATE.key(), TimeUtils.getTime())
                                .append(QueryParams.ID.key(), tmpIndividual.getId() + deleteSuffix);

                        Bson bsonQuery = parseQuery(individualQuery);
                        Document updateDocument = parseAndValidateUpdateParams(clientSession, updateParams, individualQuery,
                                QueryOptions.empty()).toFinalUpdateDocument();

                        logger.debug("Delete version {} of individual: Query: {}, update: {}", tmpIndividual.getVersion(),
                                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                        WriteResult result = individualCollection.update(clientSession, bsonQuery, updateDocument, QueryOptions.empty());
                        if (result.getNumUpdated() == 1) {
                            logger.debug("Individual version {} successfully deleted", tmpIndividual.getVersion());
                        } else {
                            logger.error("Individual version {} could not be deleted", tmpIndividual.getVersion());
                        }
                    }

                    logger.info("Individual {}({}) deleted", individual.getId(), individual.getUid());

                    return endWrite(tmpStartTime, 1, 1, null, null);
                } catch (CatalogDBException e) {
                    logger.error("Error deleting individual {}({}). {}", individual.getId(), individual.getUid(), e.getMessage(), e);
                    clientSession.abortTransaction();
                    return endWrite(tmpStartTime, 1, 0, null,
                            Collections.singletonList(new Fail(individual.getId(), e.getMessage())));
                }
            };
            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumUpdated() == 1) {
                logger.info("Individual {} successfully deleted", individual.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not delete individual {}: {}", individual.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not delete individual {}", individual.getId());
                }
            }
        }

        return endWrite(startTime, numMatches, numModified, null, failList);
    }

    private WriteResult remove(int id, boolean force) throws CatalogDBException {
        checkId(id);
        QueryResult<Individual> individual = get(id, new QueryOptions());
        Bson bson = Filters.eq(QueryParams.UID.key(), id);
        return individualCollection.remove(bson, null);
    }

    @Override
    public WriteResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return setStatus(query, Status.READY);
    }

    @Override
    public WriteResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The individual {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        return setStatus(id, File.FileStatus.READY);
    }

    @Override
    public QueryResult<Individual> get(long individualId, QueryOptions options) throws CatalogDBException {
        checkId(individualId);
        Query query = new Query(QueryParams.UID.key(), individualId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED)
                .append(QueryParams.STUDY_UID.key(), getStudyId(individualId));
        return get(query, options);
    }

    @Override
    public QueryResult<Individual> get(long individualId, QueryOptions options, String userId)
            throws CatalogDBException, CatalogAuthorizationException {
        long studyId = getStudyId(individualId);
        Query query = new Query()
                .append(QueryParams.UID.key(), individualId)
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options, userId);
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return get(null, query, options, user);
    }

    QueryResult<Individual> get(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Individual> documentList = new ArrayList<>();
        QueryResult<Individual> queryResult;
        try (DBIterator<Individual> dbIterator = iterator(clientSession, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(clientSession, query, user, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    public QueryResult<Individual> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Individual> documentList = new ArrayList<>();
        QueryResult<Individual> queryResult;
        try (DBIterator<Individual> dbIterator = iterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

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
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        return nativeGet(null, query, options);
    }

    public QueryResult nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
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

    public QueryResult nativeGet(ClientSession clientSession, Query query, QueryOptions options, String user)
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
    public DBIterator<Individual> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    DBIterator<Individual> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new IndividualMongoDBIterator<>(mongoCursor, individualConverter, null, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_UID),
                null, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new IndividualMongoDBIterator(mongoCursor, null, null, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_UID), null, options);
    }

    @Override
    public DBIterator<Individual> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return iterator(null, query, options, user);
    }

    DBIterator<Individual> iterator(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(clientSession, query);
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualMongoDBIterator<>(mongoCursor, individualConverter, iteratorFilter, dbAdaptorFactory,
                query.getLong(PRIVATE_STUDY_UID), user, options);
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
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualMongoDBIterator(mongoCursor, null, iteratorFilter, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_UID), user,
                options);
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
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name(),
                    Entity.INDIVIDUAL.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, queryForAuthorisedEntries);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        qOptions = removeInnerProjections(qOptions, QueryParams.SAMPLES.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);

        // FIXME we should be able to remove this now safely
        qOptions = filterOptions(qOptions, FILTER_ROUTE_INDIVIDUALS);

        logger.debug("Individual get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return individualCollection.nativeQuery().find(clientSession, bson, qOptions).iterator();
    }


    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return rank(individualCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                    IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name(), Entity.INDIVIDUAL.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name(),
                    Entity.INDIVIDUAL.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(individualCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                    IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name(), Entity.INDIVIDUAL.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name(),
                    Entity.INDIVIDUAL.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(individualCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Individual> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    protected Bson parseQuery(Query query, Document authorisation) throws CatalogDBException {
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
                                Status.getPositiveStatus(Status.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case FATHER_UID:
                    case MOTHER_UID:
                    case DATE_OF_BIRTH:
                    case SEX:
                    case ETHNICITY:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case POPULATION_NAME:
                    case POPULATION_SUBPOPULATION:
                    case POPULATION_DESCRIPTION:
                    case KARYOTYPIC_SEX:
                    case LIFE_STATUS:
                    case AFFECTATION_STATUS:
                    case RELEASE:
                    case VERSION:
                    case SAMPLE_UIDS:
//                    case ANNOTATION_SETS:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
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
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    void removeSampleReferences(ClientSession clientSession, long studyUid, long sampleUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.SAMPLE_UIDS.key(), sampleUid);

        ObjectMap params = new ObjectMap()
                .append(QueryParams.SAMPLES.key(), Collections.singletonList(new Sample().setUid(sampleUid)));
        // Add the the Remove action for the sample provided
        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS,
                new ObjectMap(QueryParams.SAMPLES.key(), ParamUtils.UpdateAction.REMOVE.name()));

        Bson update;
        try {
            update = parseAndValidateUpdateParams(clientSession, params, query, queryOptions).toFinalUpdateDocument();
        } catch (CatalogDBException e) {
            if (e.getMessage().contains("No individual found to be updated")) {
                return;
            } else {
                throw e;
            }
        }

        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);

        Bson bsonQuery = parseQuery(query);

        logger.debug("Sample references extraction. Query: {}, update: {}",
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        WriteResult updateResult = individualCollection.update(clientSession, bsonQuery, update, multi);
        logger.debug("Sample uid '" + sampleUid + "' references removed from " + updateResult.getNumUpdated() + " out of "
                + updateResult.getNumMatches() + " individuals");
    }

    public MongoDBCollection getIndividualCollection() {
        return individualCollection;
    }

    WriteResult setStatus(long individualId, String status) throws CatalogDBException {
        return update(individualId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    WriteResult setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

}
