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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
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
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.FamilyConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.FamilyAclEntry;
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
 * Created by pfurio on 03/05/17.
 */
public class FamilyMongoDBAdaptor extends AnnotationMongoDBAdaptor implements FamilyDBAdaptor {

    private final MongoDBCollection familyCollection;
    private FamilyConverter familyConverter;

    public FamilyMongoDBAdaptor(MongoDBCollection familyCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(SampleMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.familyCollection = familyCollection;
        this.familyConverter = new FamilyConverter();
    }

    /**
     *
     * @return MongoDB connection to the family collection.
     */
    public MongoDBCollection getFamilyCollection() {
        return familyCollection;
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return familyCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        if (studyPermission == null) {
            studyPermission = StudyAclEntry.StudyPermissions.VIEW_FAMILIES;
        }

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.ID.key(), query.getLong(QueryParams.STUDY_ID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_ID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getFamilyPermission().name());
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("Family count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return familyCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return familyCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    public void addMemberInfoToFamily(QueryResult<Family> familyQueryResult) {
        if (familyQueryResult.getResult() == null || familyQueryResult.getResult().size() == 0) {
            return;
        }
        for (Family family : familyQueryResult.getResult()) {
            family.setFather(getIndividual(family.getFather()));
            family.setMother(getIndividual(family.getMother()));
            if (family.getChildren() != null && family.getChildren().size() > 0) {
                family.setChildren(family.getChildren().stream().map(this::getIndividual).collect(Collectors.toList()));
            }
        }
    }

    @Override
    public QueryResult<Family> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters);
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update family with id " + id);
        }
        Query query = new Query()
                .append(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), "!=EMPTY");
        return endQuery("Update family", startTime, get(query, new QueryOptions()));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> familyParameters = new HashMap<>();

        final String[] acceptedBooleanParams = {QueryParams.PARENTAL_CONSANGUINITY.key()};
        filterBooleanParams(parameters, familyParameters, acceptedBooleanParams);

        final String[] acceptedParams = {QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, familyParameters, acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, familyParameters, acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.ONTOLOGY_TERMS.key()};
        filterObjectParams(parameters, familyParameters, acceptedObjectParams);

        if (parameters.containsKey(QueryParams.NAME.key())) {
            // That can only be done to one family...
            QueryResult<Family> familyQueryResult = get(query, new QueryOptions());
            if (familyQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update family: No family found to be updated");
            }
            if (familyQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update family: Cannot set the same name parameter for different families");
            }

            // Check that the new sample name is still unique
            long studyId = getStudyId(familyQueryResult.first().getId());

            Query tmpQuery = new Query()
                    .append(QueryParams.NAME.key(), parameters.get(QueryParams.NAME.key()))
                    .append(QueryParams.STUDY_ID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot set name for family. A family with { name: '"
                        + parameters.get(QueryParams.NAME.key()) + "'} already exists.");
            }

            familyParameters.put(QueryParams.NAME.key(), parameters.get(QueryParams.NAME.key()));
        }

        if (parameters.containsKey(QueryParams.MOTHER_ID.key())) {
            long motherId = parameters.getLong(QueryParams.MOTHER_ID.key());
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().checkId(motherId);
            familyParameters.put("mother.id", motherId);
        }

        if (parameters.containsKey(QueryParams.FATHER_ID.key())) {
            long fatherId = parameters.getLong(QueryParams.FATHER_ID.key());
            dbAdaptorFactory.getCatalogIndividualDBAdaptor().checkId(fatherId);
            familyParameters.put("father.id", fatherId);
        }

        if (parameters.containsKey(QueryParams.CHILDREN_IDS.key())) {
            List<Long> individualIds = parameters.getAsLongList(QueryParams.CHILDREN_IDS.key());
            List<ObjectMap> individualIdList = new ArrayList<>(individualIds.size());
            for (Long individualId : individualIds) {
                dbAdaptorFactory.getCatalogIndividualDBAdaptor().checkId(individualId);
                individualIdList.add(new ObjectMap("id", individualId));
            }
            familyParameters.put("children", individualIdList);
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            familyParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            familyParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        logger.info(familyParameters.toString());

        if (!familyParameters.isEmpty()) {
            QueryResult<UpdateResult> update = familyCollection.update(parseQuery(query, false),
                    new Document("$set", familyParameters), null);
            return endQuery("Update family", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update family", startTime, new QueryResult<>());
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = familyCollection.remove(parseQuery(query, false), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("Family");
        }
    }

    @Override
    public QueryResult<Family> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the family is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The family {" + id + "} is not deleted");
        }

        // Change the status of the family to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore family", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore families", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Family> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Family> documentList = new ArrayList<>();
        QueryResult<Family> queryResult;
        try (DBIterator<Family> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);
        addMemberInfoToFamily(queryResult);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
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
    public QueryResult<Family> get(long familyId, QueryOptions options) throws CatalogDBException {
        checkId(familyId);
        Query query = new Query(QueryParams.ID.key(), familyId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
    }

    @Override
    public QueryResult<Family> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Family> documentList = new ArrayList<>();
        QueryResult<Family> queryResult;
        try (DBIterator<Family> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);
        addMemberInfoToFamily(queryResult);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_FAMILIES);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<Family> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor, familyConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor);
    }

    @Override
    public DBIterator<Family> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new MongoDBIterator<>(mongoCursor, familyConverter, iteratorFilter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

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
                    StudyAclEntry.StudyPermissions.VIEW_FAMILIES.name(), FamilyAclEntry.FamilyPermissions.VIEW.name());
        }

        if (!query.containsKey(CohortDBAdaptor.QueryParams.STATUS_NAME.key())) {
            query.append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }

        return familyCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query()
                .append(StudyDBAdaptor.QueryParams.ID.key(), query.getLong(FamilyDBAdaptor.QueryParams.STUDY_ID.key()));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(FamilyDBAdaptor.QueryParams.STUDY_ID.key()) + " not found");
        }
        return queryResult.first();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return rank(familyCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(familyCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(familyCollection, bsonQuery, fields, "name", options);
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
    protected GenericDocumentComplexConverter<? extends Annotable> getConverter() {
        return this.familyConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return this.familyCollection;
    }

    @Override
    public QueryResult<Family> insert(Family family, long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.NAME.key(), family.getName()));
        filterList.add(Filters.eq(PRIVATE_STUDY_ID, studyId));
        filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        QueryResult<Long> count = familyCollection.count(bson);
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Cannot create family. A family with { name: '" + family.getName() + "'} already exists.");
        }

        long familyId = getNewId();
        family.setId(familyId);

        Document familyObject = familyConverter.convertToStorageType(family);
        familyObject.put(PRIVATE_STUDY_ID, studyId);
        familyObject.put(PRIVATE_ID, familyId);
        familyCollection.insert(familyObject, null);

        return endQuery("createFamily", startTime, get(familyId, options));
    }

    @Override
    public long getStudyId(long familyId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_ID, familyId);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = familyCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("Family", familyId);
        }
    }

    private QueryResult<Family> setStatus(long familyId, String status) throws CatalogDBException {
        return update(familyId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    private QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
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

        if (query.containsKey(QueryParams.ANNOTATION.key())) {
            fixAnnotationQuery(query);
        }

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam =  QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
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
                            if (variableSetId > 0) {
                                variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                                        .getVariables().stream().collect(Collectors.toMap(Variable::getName, Function.identity()));
                            }
                        }
                        addAnnotationQueryFilter(entry.getKey(), query, variableMap, annotationList);
                        break;
                    case ANNOTATION_SET_NAME:
                        addOrQuery("name", queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    case FATHER_ID:
                        addAutoOrQuery("father.id", queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case MOTHER_ID:
                        addAutoOrQuery("mother.id", queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case CHILDREN_IDS:
                        addAutoOrQuery("children.id", queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case NAME:
                    case DESCRIPTION:
                    case RELEASE:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case ONTOLOGY_TERMS_ID:
                    case ONTOLOGY_TERMS_NAME:
                    case ONTOLOGY_TERMS_SOURCE:
                    case ONTOLOGY_TERMS_AGE_OF_ONSET:
                    case ONTOLOGY_TERMS_MODIFIERS:
                    case ANNOTATION_SETS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + query.toJson(), e);
                }
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
}
