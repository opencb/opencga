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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.FamilyConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.AnnotableMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.FamilyAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
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
    public void nativeInsert(Map<String, Object> family, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(family, "family");
        familyCollection.insert(document, null);
    }

    @Override
    public QueryResult<Family> insert(long studyId, Family family, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
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
        family.setVersion(1);

        Document familyObject = familyConverter.convertToStorageType(family, variableSetList);
        familyObject.put(PRIVATE_STUDY_ID, studyId);

        // Versioning private parameters
        familyObject.put(RELEASE_FROM_VERSION, Arrays.asList(family.getRelease()));
        familyObject.put(LAST_OF_VERSION, true);
        familyObject.put(LAST_OF_RELEASE, true);
        if (StringUtils.isNotEmpty(family.getCreationDate())) {
            familyObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(family.getCreationDate()));
        } else {
            familyObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        familyObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        familyCollection.insert(familyObject, null);

        return endQuery("createFamily", startTime, get(familyId, options));
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return familyCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_FAMILIES : studyPermissions);

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

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
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

    @Override
    public QueryResult<Family> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<Family> update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters, variableSetList, queryOptions);
        if (update.getNumTotalResults() != 1 && parameters.size() > 0 && !(parameters.size() <= 3
                && (parameters.containsKey(QueryParams.ANNOTATION_SETS.key()) || parameters.containsKey(Constants.DELETE_ANNOTATION_SET)
                || parameters.containsKey(Constants.DELETE_ANNOTATION)))) {
            throw new CatalogDBException("Could not update family with id " + id);
        }
        Query query = new Query()
                .append(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), "!=EMPTY");
        return endQuery("Update family", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        if (queryOptions.getBoolean(Constants.REFRESH)) {
            updateToLastIndividualVersions(query, parameters);
        }

        Document familyParameters = parseAndValidateUpdateParams(parameters, query);
        ObjectMap annotationUpdateMap = prepareAnnotationUpdate(query.getLong(QueryParams.ID.key(), -1L), parameters, variableSetList);
        if (familyParameters.containsKey(QueryParams.STATUS_NAME.key())) {
            query.put(Constants.ALL_VERSIONS, true);
            QueryResult<UpdateResult> update = familyCollection.update(parseQuery(query, false),
                    new Document("$set", familyParameters), new QueryOptions("multi", true));

            applyAnnotationUpdates(query.getLong(QueryParams.ID.key(), -1L), annotationUpdateMap, true);
            return endQuery("Update family", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        if (!queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            applyAnnotationUpdates(query.getLong(QueryParams.ID.key(), -1L), annotationUpdateMap, true);
            if (!familyParameters.isEmpty()) {
                QueryResult<UpdateResult> update = familyCollection.update(parseQuery(query, false),
                        new Document("$set", familyParameters), new QueryOptions("multi", true));
                return endQuery("Update family", startTime, Arrays.asList(update.getNumTotalResults()));
            }
        } else {
            return updateAndCreateNewVersion(query, familyParameters, annotationUpdateMap, queryOptions);
        }

        return endQuery("Update family", startTime, new QueryResult<>());
    }

    private void updateToLastIndividualVersions(Query query, ObjectMap parameters) throws CatalogDBException {
        if (parameters.containsKey(QueryParams.MEMBERS.key())) {
            throw new CatalogDBException("Invalid option: Cannot update to the last version of members and update to different members at "
                    + "the same time.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.MEMBERS.key());
        QueryResult<Family> queryResult = get(query, options);

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Family not found.");
        }
        if (queryResult.getNumResults() > 1) {
            throw new CatalogDBException("Update to the last version of members in multiple families at once not supported.");
        }

        Family family = queryResult.first();
        if (family.getMembers() == null || family.getMembers().isEmpty()) {
            // Nothing to do
            return;
        }

        List<Long> individualIds = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        Query individualQuery = new Query()
                .append(IndividualDBAdaptor.QueryParams.ID.key(), individualIds);
        options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                IndividualDBAdaptor.QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.VERSION.key()
        ));
        QueryResult<Individual> individualQueryResult = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(individualQuery, options);
        parameters.put(QueryParams.MEMBERS.key(), individualQueryResult.getResult());
    }

    private QueryResult<Long> updateAndCreateNewVersion(Query query, Document familyParameters, ObjectMap annotationUpdateMap,
                                                        QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));
        int release = queryOptions.getInt(Constants.CURRENT_RELEASE, -1);
        if (release == -1) {
            throw new CatalogDBException("Internal error. Mandatory " + Constants.CURRENT_RELEASE + " parameter not passed to update "
                    + "method");
        }

        for (Document familyDocument : queryResult.getResult()) {
            Document updateOldVersion = new Document();

            List<Integer> supportedReleases = (List<Integer>) familyDocument.get(RELEASE_FROM_VERSION);
            if (supportedReleases.size() > 1) {
                // If it contains several releases, it means this is the first update on the current release, so we just need to take the
                // current release number out
                supportedReleases.remove(supportedReleases.size() - 1);
            } else {
                // If it is 1, it means that the previous version being checked was made on this same release as well, so it won't be the
                // last version of the release
                updateOldVersion.put(LAST_OF_RELEASE, false);
            }
            updateOldVersion.put(RELEASE_FROM_VERSION, supportedReleases);
            updateOldVersion.put(LAST_OF_VERSION, false);

            // Perform the update on the previous version
            Document queryDocument = new Document()
                    .append(PRIVATE_STUDY_ID, familyDocument.getLong(PRIVATE_STUDY_ID))
                    .append(QueryParams.VERSION.key(), familyDocument.getInteger(QueryParams.VERSION.key()))
                    .append(PRIVATE_ID, familyDocument.getLong(PRIVATE_ID));
            QueryResult<UpdateResult> updateResult = familyCollection.update(queryDocument, new Document("$set", updateOldVersion), null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("Internal error: Could not update family");
            }

            // We update the information for the new version of the document
            familyDocument.put(LAST_OF_RELEASE, true);
            familyDocument.put(LAST_OF_VERSION, true);
            familyDocument.put(RELEASE_FROM_VERSION, Arrays.asList(release));
            familyDocument.put(QueryParams.VERSION.key(), familyDocument.getInteger(QueryParams.VERSION.key()) + 1);

            // We apply the updates the user wanted to apply (if any)
            mergeDocument(familyDocument, familyParameters);

            // Insert the new version document
            familyCollection.insert(familyDocument, QueryOptions.empty());

            applyAnnotationUpdates(query.getLong(QueryParams.ID.key(), -1L), annotationUpdateMap, true);
        }

        return endQuery("Update family", startTime, Arrays.asList(queryResult.getNumTotalResults()));
    }

    private Document parseAndValidateUpdateParams(ObjectMap parameters, Query query) throws CatalogDBException {
        Document familyParameters = new Document();

        final String[] acceptedParams = {QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, familyParameters, acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, familyParameters, acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.MEMBERS.key(), QueryParams.PHENOTYPES.key()};
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

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            familyParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            familyParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        familyConverter.validateDocumentToUpdate(familyParameters);

        return familyParameters;
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
//        addMemberInfoToFamily(queryResult);

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
//        addMemberInfoToFamily(queryResult);

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
        return new AnnotableMongoDBIterator<>(mongoCursor, familyConverter, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new AnnotableMongoDBIterator<>(mongoCursor, options);
    }

    @Override
    public DBIterator<Family> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new AnnotableMongoDBIterator<>(mongoCursor, familyConverter, iteratorFilter, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS.name(), FamilyAclEntry.FamilyPermissions.VIEW_ANNOTATIONS.name());

        return new AnnotableMongoDBIterator<>(mongoCursor, iteratorFilter, options);
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

        filterOutDeleted(query);
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("Family get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeAnnotationProjectionOptions(qOptions);

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
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return rank(familyCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(familyCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(familyCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user, StudyAclEntry.StudyPermissions.VIEW_FAMILIES
                .name(), FamilyAclEntry.FamilyPermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(familyCollection, bsonQuery, field, QueryParams.NAME.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user, StudyAclEntry.StudyPermissions.VIEW_FAMILIES
                .name(), FamilyAclEntry.FamilyPermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(familyCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
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

    @Override
    public void updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_ID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query, false);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        familyCollection.update(bson, update, queryOptions);
    }

    @Override
    public void unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        unmarkPermissionRule(familyCollection, studyId, permissionRuleId);
    }

    private QueryResult<Family> setStatus(long familyId, String status) throws CatalogDBException {
        return update(familyId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    private QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        return parseQuery(query, isolated, null);
    }

    protected Bson parseQuery(Query query, boolean isolated, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

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
                    case PHENOTYPES:
                        addOntologyQueryFilter(queryParam.key(), queryParam.key(), query, andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(query.getString(QueryParams.ANNOTATION.key()),
                                    query.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case FATHER_ID:
                    case MOTHER_ID:
                    case MEMBER_ID:
                    case NAME:
                    case DESCRIPTION:
                    case RELEASE:
                    case VERSION:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
//                    case ANNOTATION_SETS:
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

        // If the user doesn't look for a concrete version...
        if (!query.getBoolean(Constants.ALL_VERSIONS) && !query.containsKey(QueryParams.VERSION.key())) {
            if (query.containsKey(QueryParams.SNAPSHOT.key())) {
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
