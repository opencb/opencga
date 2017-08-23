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
import com.mongodb.WriteResult;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.IndividualConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

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
public class IndividualMongoDBAdaptor extends AnnotationMongoDBAdaptor implements IndividualDBAdaptor {

    private final MongoDBCollection individualCollection;
    private IndividualConverter individualConverter;

    public IndividualMongoDBAdaptor(MongoDBCollection individualCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(IndividualMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.individualCollection = individualCollection;
        this.individualConverter = new IndividualConverter();
    }

    @Override
    protected GenericDocumentComplexConverter<? extends Annotable> getConverter() {
        return individualConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return individualCollection;
    }

    @Override
    public boolean exists(long individualId) {
        return individualCollection.count(new Document(PRIVATE_ID, individualId)).first() != 0;
    }

    @Override
    public QueryResult<Individual> insert(Individual individual, long studyId, QueryOptions options) throws CatalogDBException {
        long startQuery = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        if (!get(new Query(QueryParams.NAME.key(), individual.getName())
                .append(QueryParams.STUDY_ID.key(), studyId), new QueryOptions()).getResult().isEmpty()) {
            throw CatalogDBException.alreadyExists("Individual", "name", individual.getName());
        }
        if (individual.getFatherId() > 0 && !exists(individual.getFatherId())) {
            throw CatalogDBException.idNotFound("Individual", individual.getFatherId());
        }
        if (individual.getMotherId() > 0 && !exists(individual.getMotherId())) {
            throw CatalogDBException.idNotFound("Individual", individual.getMotherId());
        }

        long individualId = getNewId();

        individual.setId(individualId);

        Document individualDocument = individualConverter.convertToStorageType(individual);
        individualDocument.put(PRIVATE_ID, individualId);
        individualDocument.put(PRIVATE_STUDY_ID, studyId);
        QueryResult<WriteResult> insert = individualCollection.insert(individualDocument, null);

        return endQuery("createIndividual", startQuery, Collections.singletonList(individual));
    }

    @Override
    public QueryResult<AnnotationSet> annotate(long individualId, AnnotationSet annotationSet, boolean overwrite)
            throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = individualCollection.count(
                new Document("annotationSets.name", annotationSet.getName()).append(PRIVATE_ID, individualId));

        if (overwrite) {
            if (count.first() == 0) {
                throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getName());
            }
        } else {
            if (count.first() > 0) {
                throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
            }
        }

        Document document = getMongoDBDocument(annotationSet, "AnnotationSet");

        Bson query;
        Bson individualQuery = Filters.eq(PRIVATE_ID, individualId);
        if (overwrite) {
//            query.put("annotationSets.id", annotationSet.getId());
            query = Filters.and(individualQuery, Filters.eq("annotationSets.name", annotationSet.getName()));
        } else {
//            query.put("annotationSets.id", new BasicDBObject("$ne", annotationSet.getId()));
            query = Filters.and(individualQuery, Filters.eq("annotationSets.name", new Document("$ne", annotationSet.getName())));
        }

        Bson update;
        if (overwrite) {
            update = new Document("$set", new Document("annotationSets.$", document));
        } else {
            update = new Document("$push", new Document("annotationSets", document));
        }

        QueryResult<UpdateResult> queryResult = individualCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
        }

        return endQuery("", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(long individualId, String annotationId) throws CatalogDBException {

        long startTime = startQuery();

        Individual individual =
                get(individualId, new QueryOptions("include", "projects.studies.individuals.annotationSets")).first();
        AnnotationSet annotationSet = null;
        for (AnnotationSet as : individual.getAnnotationSets()) {
            if (as.getName().equals(annotationId)) {
                annotationSet = as;
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        Bson eq = Filters.eq(PRIVATE_ID, individualId);
        Bson pull = Updates.pull("annotationSets", new Document("name", annotationId));
        QueryResult<UpdateResult> update = individualCollection.update(eq, pull, null);
        if (update.first().getModifiedCount() < 1) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        return endQuery("Delete annotation", startTime, Collections.singletonList(annotationSet));
    }

    public void checkInUse(long individualId) throws CatalogDBException {
        long studyId = getStudyId(individualId);
        QueryResult<Individual> individuals = get(new Query(QueryParams.FATHER_ID.key(), individualId)
                .append(QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"fatherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        individuals = get(new Query(QueryParams.MOTHER_ID.key(), individualId)
                .append(QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"motherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getId() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(
                new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId), new QueryOptions());
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"individualId\" of sample : [";
            for (Sample sample : samples.getResult()) {
                msg += " { id: " + sample.getId() + ", name: \"" + sample.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }

    }

    @Override
    public long getStudyId(long individualId) throws CatalogDBException {
        QueryResult<Document> result =
                individualCollection.find(new Document(PRIVATE_ID, individualId), new Document(PRIVATE_STUDY_ID, 1), null);

        if (!result.getResult().isEmpty()) {
            return (long) result.getResult().get(0).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("Individual", individualId);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return individualCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        if (studyPermission == null) {
            studyPermission = StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS;
        }

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.ID.key(), query.getLong(QueryParams.STUDY_ID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_ID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getIndividualPermission().name());
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("Individual count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()));
        return individualCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return individualCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Individual> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Bson query = parseQuery(new Query(QueryParams.ID.key(), id), false);
        Map<String, Object> myParams = getValidatedUpdateParams(parameters);

        if (myParams.isEmpty()) {
            logger.debug("The map of parameters to update individual is empty. Originally it contained {}", parameters.safeToString());
            throw new CatalogDBException("Nothing to update");
        }

        logger.debug("Update individual. Query: {}, Update: {}",
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), myParams);

        QueryResult<UpdateResult> update = individualCollection.update(query, new Document("$set", myParams),
                new QueryOptions("multi", true));
        if (update.first().getMatchedCount() == 0) {
            throw new CatalogDBException("Individual " + id + " not found.");
        }

        QueryResult<Individual> queryResult = individualCollection.find(query, individualConverter, QueryOptions.empty());
        return endQuery("Update individual", startTime, queryResult);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> individualParameters = getValidatedUpdateParams(parameters);

        if (!individualParameters.isEmpty()) {
            QueryResult<UpdateResult> update = individualCollection.update(parseQuery(query, false), new Document("$set",
                            individualParameters), null);
            return endQuery("Update individual", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update individual", startTime, new QueryResult<Long>());
    }

    private Map<String, Object> getValidatedUpdateParams(ObjectMap parameters) throws CatalogDBException {
        Map<String, Object> individualParameters = new HashMap<>();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.FAMILY.key(), QueryParams.ETHNICITY.key(), QueryParams.SEX.key(),
                QueryParams.POPULATION_NAME.key(), QueryParams.POPULATION_SUBPOPULATION.key(), QueryParams.POPULATION_DESCRIPTION.key(),
                QueryParams.KARYOTYPIC_SEX.key(), QueryParams.LIFE_STATUS.key(), QueryParams.AFFECTATION_STATUS.key(),
                QueryParams.DATE_OF_BIRTH.key(), };
        filterStringParams(parameters, individualParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap((QueryParams.SEX.key()), Individual.Sex.class);
        filterEnumParams(parameters, individualParameters, acceptedEnums);

        String[] acceptedIntParams = {QueryParams.FATHER_ID.key(), QueryParams.MOTHER_ID.key()};
        filterIntParams(parameters, individualParameters, acceptedIntParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, individualParameters, acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.ONTOLOGY_TERMS.key()};
        filterObjectParams(parameters, individualParameters, acceptedObjectParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            individualParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            individualParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        //Check individualIds exists
        String[] individualIdParams = {"fatherId", "motherId"};
        for (String individualIdParam : individualIdParams) {
            if (individualParameters.containsKey(individualIdParam)) {
                Integer individualId1 = (Integer) individualParameters.get(individualIdParam);
                if (individualId1 > 0 && !exists(individualId1)) {
                    throw CatalogDBException.idNotFound("Individual " + individualIdParam, individualId1);
                }
            }
        }

        return individualParameters;
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = individualCollection.remove(parseQuery(query, false), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("Individual");
        }
    }

    private QueryResult<Individual> remove(int id, boolean force) throws CatalogDBException {
        long startTime = startQuery();
        checkId(id);
        QueryResult<Individual> individual = get(id, new QueryOptions());
        Bson bson = Filters.eq(QueryParams.ID.key(), id);
        QueryResult<DeleteResult> remove = individualCollection.remove(bson, null);
        return endQuery("Delete individual", startTime, individual);
    }

    @Override
    public QueryResult<Individual> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        checkId(id);
        // Check the file is active
        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
                + File.FileStatus.DELETED);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED + "," + Status.DELETED);
            QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.STATUS_NAME.key());
            Individual individual = get(query, options).first();
            throw new CatalogDBException("The individual {" + id + "} was already " + individual.getStatus().getName());
        }

        // If we don't find the force parameter, we check first if the file could be deleted.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            deleteReferences(id);
        }

        // Change the status of the project to deleted
        setStatus(id, Status.TRASHED);

        query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);

        return endQuery("Delete individual", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        // This will be the query that will be updated.
        Query updateQuery = new Query();

        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            Query subQuery = new Query(query).append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";" + Status.DELETED);
            List<Individual> individuals = get(subQuery, null).getResult();
            List<Long> individualIdsToDelete = new ArrayList<>();

            if (individuals.size() == 0) {
                // Check if we do not get any results because they are already deleted.
                if (count(query).first() > 0) {
                    throw CatalogDBException.alreadyDeletedOrRemoved("Individual");
                } else {
                    throw CatalogDBException.queryNotFound("Individual");
                }
            }

            // Check that the individualIds could be deleted because there is no dependency between them.
            // The map will contain a map family -> list of ids of the family
            Map<String, List<Individual>> familyIndividuals = new HashMap<>();
            for (Individual individual : individuals) {
                if (!familyIndividuals.containsKey(individual.getFamily())) {
                    List<Individual> individualList = new ArrayList<>();
                    familyIndividuals.put(individual.getFamily(), individualList);
                }
                familyIndividuals.get(individual.getFamily()).add(individual);
            }

            // Check family by family
            Query queryNoDeletedNoRemoved = new Query(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";" + Status.DELETED);
            for (Map.Entry<String, List<Individual>> indFamily : familyIndividuals.entrySet()) {
                Query tmpQuery = new Query(queryNoDeletedNoRemoved).append(QueryParams.FAMILY.key(), indFamily.getKey());
                List<Individual> individualListDatabase = get(tmpQuery, new QueryOptions()).getResult();
                if (individualListDatabase.size() == indFamily.getValue().size()) {
                    individualIdsToDelete.addAll(indFamily.getValue().stream()
                            .map(Individual::getId).collect(Collectors.toList()));
                } else {
                    // Remove in order from children to parents
                    List<Individual> indCopyDatabase = new ArrayList<>(individualListDatabase);
                    Set<Long> desiredIdsToDelete = indFamily.getValue().stream()
                            .map(Individual::getId).collect(Collectors.toSet());
                    boolean changed = true;
                    // While we still have some individuals to remove
                    while (indCopyDatabase.size() > 0 && changed) {
                        changed = false;
                        Set<Long> parents = new HashSet<>();
                        Set<Long> children = new HashSet<>();
                        for (Individual individual : indCopyDatabase) {
                            // Add to parents
                            parents.add(individual.getFatherId());
                            parents.add(individual.getMotherId());

                            // Add child
                            if (!parents.contains(individual.getId())) {
                                children.add(individual.getId());
                            }

                            // Check whether any of the new parents were already inserted in children
                            if (children.contains(individual.getFatherId())) {
                                children.remove(individual.getFatherId());
                            }
                            if (children.contains(individual.getMotherId())) {
                                children.remove(individual.getMotherId());
                            }
                        }

                        Set<Long> newIdsToDelete = new HashSet<>();
                        Iterator<Long> iterator = children.iterator();
                        while (iterator.hasNext()) {
                            Long individualId = iterator.next();
                            if (desiredIdsToDelete.contains(individualId)) {
                                individualIdsToDelete.add(individualId);
                                newIdsToDelete.add(individualId);
                                changed = true;
                            }
                        }

                        if (changed) {
                            // Update indCopyDatabase removing the ids that have been marked to be deleted.
                            List<Individual> temporalIndCopyDatabase = new ArrayList<>(indCopyDatabase);
                            for (Individual individual : temporalIndCopyDatabase) {
                                if (newIdsToDelete.contains(individual.getId())) {
                                    indCopyDatabase.remove(individual);
                                }
                            }
                        }
                    }
                }
            }

            updateQuery.append(QueryParams.ID.key(), individualIdsToDelete)
                    .append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";" + Status.DELETED);

        } else {
            updateQuery = new Query(query).append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";" + Status.DELETED);
        }

        long count = setStatus(updateQuery, Status.TRASHED).first();

        return endQuery("Delete individual", startTime, Collections.singletonList(count));
    }

    @Override
    public QueryResult<Individual> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore individuals", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Individual> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The individual {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, File.FileStatus.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore individual", startTime, get(query, null));
    }

    @Override
    public QueryResult<Individual> get(long individualId, QueryOptions options) throws CatalogDBException {
        checkId(individualId);
        Query query = new Query(QueryParams.ID.key(), individualId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Individual> documentList = new ArrayList<>();
        QueryResult<Individual> queryResult;
        try (DBIterator<Individual> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);
        addSamples(queryResult, user);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Individual> documentList = new ArrayList<>();
        QueryResult<Individual> queryResult;
        try (DBIterator<Individual> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    private void addSamples(QueryResult<Individual> queryResult, String user) throws CatalogAuthorizationException {
        if (queryResult.getNumResults() == 0) {
            return;
        }

        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL.key());
        for (Individual individual : queryResult.getResult()) {
            if (individual == null || individual.getId() <= 0) {
                continue;
            }
            Query query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individual.getId());
            try {
                QueryResult<Sample> sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, options, user);
                individual.setSamples(sampleQueryResult.getResult());
            } catch (CatalogDBException e) {
                logger.warn("Could not retrieve samples for individual {}", individual.getId(), e);
            }
        }
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<Individual> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor, individualConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor);
    }

    @Override
    public DBIterator<Individual> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new MongoDBIterator<>(mongoCursor, individualConverter, iteratorFilter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new MongoDBIterator<>(mongoCursor, iteratorFilter);
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options, Document studyDocument, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document queryForAuthorisedEntries = null;
        if (studyDocument != null && user != null) {
            // Get the document query needed to check the permissions as well
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name());
        }

        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_INDIVIDUALS);

        return individualCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.ID.key(), query.getLong(QueryParams.STUDY_ID.key()));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_ID.key()) + " not found");
        }
        return queryResult.first();
    }


    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return rank(individualCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(individualCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(individualCollection, bsonQuery, fields, "name", options);
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

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        return parseQuery(query, isolated, null);
    }

    private Bson parseQuery(Query query, boolean isolated, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        List<Bson> annotationList = new ArrayList<>();
        // We declare variableMap here just in case we have different annotation queries
        Map<String, Variable> variableMap = null;

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        fixComplexQueryParam(QueryParams.ANNOTATION.key(), query);
        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                continue;
            }
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case ONTOLOGIES:
                    case ONTOLOGY_TERMS:
                        addOntologyQueryFilter(QueryParams.ONTOLOGY_TERMS.key(), queryParam.key(), query, andBsonList);
                        break;
                    case VARIABLE_SET_ID:
                        addOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    case ANNOTATION:
                        if (variableMap == null) {
                            long variableSetId = query.getLong(QueryParams.VARIABLE_SET_ID.key());
                            variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                                    .getVariables().stream().collect(Collectors.toMap(Variable::getName, Function.identity()));
                        }
                        addAnnotationQueryFilter(entry.getKey(), query, variableMap, annotationList);
                        break;
                    case ANNOTATION_SET_NAME:
                        addOrQuery("name", queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    case NAME:
                    case FATHER_ID:
                    case MOTHER_ID:
                    case FAMILY:
                    case DATE_OF_BIRTH:
                    case SEX:
                    case ETHNICITY:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case POPULATION_NAME:
                    case POPULATION_SUBPOPULATION:
                    case POPULATION_DESCRIPTION:
                    case KARYOTYPIC_SEX:
                    case LIFE_STATUS:
                    case AFFECTATION_STATUS:
                    case CREATION_DATE:
                    case RELEASE:
                    case ANNOTATION_SETS:
                    case ONTOLOGY_TERMS_ID:
                    case ONTOLOGY_TERMS_NAME:
                    case ONTOLOGY_TERMS_SOURCE:
                    case ONTOLOGY_TERMS_AGE_OF_ONSET:
                    case ONTOLOGY_TERMS_MODIFIERS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (annotationList.size() > 0) {
            Bson projection = Projections.elemMatch(QueryParams.ANNOTATION_SETS.key(), Filters.and(annotationList));
            andBsonList.add(projection);
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

    public MongoDBCollection getIndividualCollection() {
        return individualCollection;
    }

    QueryResult<Individual> setStatus(long individualId, String status) throws CatalogDBException {
        return update(individualId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    /**
     * Checks whether the individualId is parent of any other individual.
     *
     * @param individualId individual id.
     * @throws CatalogDBException when the individualId is parent of other individual.
     */
    private void checkCanDelete(long individualId) throws CatalogDBException {
        // Check if the individual is father
        Query query = new Query()
                .append(QueryParams.FATHER_ID.key(), individualId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + File.FileStatus.DELETED);
        Long count = count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The individual " + individualId + " cannot be deleted/removed because it is the father of "
                    + count + " individuals. Please, consider deleting first the children or using the force parameter.");
        }

        // Check if the individual is mother
        query = new Query()
                .append(QueryParams.MOTHER_ID.key(), individualId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + File.FileStatus.DELETED);
        count = count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The individual " + individualId + " cannot be deleted/removed because it is the mother of "
                    + count + " individuals. Please, consider deleting first the children or using the force parameter.");
        }

        // Check if the individual is being used in a sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + File.FileStatus.DELETED);
        count = dbAdaptorFactory.getCatalogSampleDBAdaptor().count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The individual " + individualId + " cannot be deleted/removed because it is being referenced by "
                    + count + " samples.");
        }

    }

    /**
     * Remove the possible references from other individuals or samples.
     *
     * @param individualId individual Id.
     * @throws CatalogDBException when there is any kind of error.
     */
    private void deleteReferences(long individualId) throws CatalogDBException {
        Query query = new Query(QueryParams.FATHER_ID.key(), individualId);
        Long count = update(query, new ObjectMap(QueryParams.FATHER_ID.key(), -1)).first();
        logger.debug("Individual id {} extracted as father from {} individuals", individualId, count);

        query = new Query(QueryParams.MOTHER_ID.key(), individualId);
        count = update(query, new ObjectMap(QueryParams.MOTHER_ID.key(), -1)).first();
        logger.debug("Individual id {} extracted as mother from {} individuals", individualId, count);

        query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
        count = dbAdaptorFactory.getCatalogSampleDBAdaptor()
                .update(query, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), -1)).first();
        logger.debug("Individual id {} extracted from {} samples", individualId, count);
    }

}
