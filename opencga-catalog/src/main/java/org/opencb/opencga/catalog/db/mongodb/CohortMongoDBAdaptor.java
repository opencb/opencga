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
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
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
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.CohortConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CohortMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.CohortAclEntry;
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

public class CohortMongoDBAdaptor extends AnnotationMongoDBAdaptor<Cohort> implements CohortDBAdaptor {

    private final MongoDBCollection cohortCollection;
    private CohortConverter cohortConverter;

    public CohortMongoDBAdaptor(MongoDBCollection cohortCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CohortMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.cohortCollection = cohortCollection;
        this.cohortConverter = new CohortConverter();
    }

    @Override
    protected AnnotableConverter<? extends Annotable> getConverter() {
        return cohortConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return cohortCollection;
    }

    @Override
    public void nativeInsert(Map<String, Object> cohort, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(cohort, "cohort");
        cohortCollection.insert(document, null);
    }

    @Override
    public QueryResult<Cohort> insert(long studyId, Cohort cohort, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        checkCohortNameExists(studyId, cohort.getId());

        long newId = dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
        cohort.setUid(newId);
        cohort.setStudyUid(studyId);

        Document cohortObject = cohortConverter.convertToStorageType(cohort, variableSetList);
        if (StringUtils.isNotEmpty(cohort.getCreationDate())) {
            cohortObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(cohort.getCreationDate()));
        } else {
            cohortObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        cohortObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        try {
            cohortCollection.insert(cohortObject, null);
        } catch (MongoWriteException e) {
            throw ifDuplicateKeyException(() -> CatalogDBException.alreadyExists("Cohort", studyId, "name", cohort.getId(), e), e);
        }

        Query query = new Query()
                .append(QueryParams.UID.key(), newId)
                .append(QueryParams.STUDY_UID.key(), studyId);

        return endQuery("createCohort", startTime, get(query, options));
    }

    @Override
    public QueryResult<Cohort> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        return get(new Query(QueryParams.STUDY_UID.key(), studyId), options);
    }

    @Override
    public QueryResult<Cohort> update(long cohortId, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(cohortId, parameters, Collections.emptyList(), queryOptions);
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

        QueryResult<Cohort> cohortQueryResult = get(id, queryOptions);
        if (cohortQueryResult.first().getAnnotationSets().isEmpty()) {
            return new QueryResult<>("Get annotation set", cohortQueryResult.getDbTime(), 0, 0, cohortQueryResult.getWarningMsg(),
                    cohortQueryResult.getErrorMsg(), Collections.emptyList());
        } else {
            List<AnnotationSet> annotationSets = cohortQueryResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new QueryResult<>("Get annotation set", cohortQueryResult.getDbTime(), size, size, cohortQueryResult.getWarningMsg(),
                    cohortQueryResult.getErrorMsg(), annotationSets);
        }
    }

    @Override
    public QueryResult<Cohort> update(long cohortId, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        update(new Query(QueryParams.UID.key(), cohortId), parameters, variableSetList, queryOptions);
        Query query = new Query()
                .append(QueryParams.UID.key(), cohortId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(cohortId));
        return endQuery("Update cohort", startTime, get(query, queryOptions));
    }

    @Override
    public long getStudyId(long cohortId) throws CatalogDBException {
        checkId(cohortId);
        QueryResult queryResult = nativeGet(new Query(QueryParams.UID.key(), cohortId),
                new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_ID));
        if (queryResult.getResult().isEmpty()) {
            throw CatalogDBException.uidNotFound("Cohort", cohortId);
        } else {
            return ((Document) queryResult.first()).getLong(PRIVATE_STUDY_ID);
        }
    }

    @Override
    public void unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        unmarkPermissionRule(cohortCollection, studyId, permissionRuleId);
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        long startTime = startQuery();
        return endQuery("Count cohort", startTime, cohortCollection.count(parseQuery(query, false)));
    }

    @Override
    public QueryResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);
        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_COHORTS : studyPermissions);

        // Get the study document
        Document studyDocument = getStudyDocument(query);

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user, studyPermission.name(),
                studyPermission.getCohortPermission().name());
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("Cohort count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return cohortCollection.count(bson);
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return cohortCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> cohortParams = new HashMap<>();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key(), QueryParams.CREATION_DATE.key()};
        filterStringParams(parameters, cohortParams, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(QueryParams.TYPE.key(), Study.Type.class);
        filterEnumParams(parameters, cohortParams, acceptedEnums);

        if (parameters.containsKey(CohortDBAdaptor.QueryParams.ID.key())) {
            // That can only be done to one cohort...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            QueryResult<Cohort> cohortQueryResult = get(tmpQuery, new QueryOptions());
            if (cohortQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update cohort: No cohort found to be updated");
            }
            if (cohortQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update cohort: Cannot update " + QueryParams.ID.key() + " parameter. More than one cohort "
                        + "found to be updated.");
            }

            // Check that the new sample name is still unique
            long studyId = cohortQueryResult.first().getStudyUid();

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Cohort "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            cohortParams.put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        // Check if the samples exist.
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            List<Long> objectSampleList = parameters.getAsLongList(QueryParams.SAMPLES.key());
            List<Sample> sampleList = new ArrayList<>();
            for (Long sampleId : objectSampleList) {
                if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().exists((sampleId))) {
                    throw CatalogDBException.uidNotFound("Sample", (sampleId));
                }
                Sample sample = new Sample();
                sample.setUid(sampleId);
                sampleList.add(sample);

            }
            cohortParams.put(QueryParams.SAMPLES.key(), cohortConverter.convertSamplesToDocument(sampleList));
        }
        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, cohortParams, acceptedMapParams);

        //Map<String, Class<? extends Enum>> acceptedEnumParams = Collections.singletonMap(QueryParams.STATUS_NAME.key(),
        //        Cohort.CohortStatus.class);
        //filterEnumParams(parameters, cohortParams, acceptedEnumParams);
        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            cohortParams.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            cohortParams.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey("status")) {
            throw new CatalogDBException("Unable to modify cohort. Use parameter \"" + QueryParams.STATUS_NAME.key()
                    + "\" instead of \"status\"");
        }

//        ObjectMap annotationUpdateMap = prepareAnnotationUpdate(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList);

        if (!cohortParams.isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            cohortParams.put(MODIFICATION_DATE, time);
            cohortParams.put(PRIVATE_MODIFICATION_DATE, date);

            QueryResult<UpdateResult> update = cohortCollection.update(parseQuery(query, false), new Document("$set", cohortParams), null);
            return endQuery("Update cohort", startTime, Arrays.asList(update.getNumTotalResults()));
        }

//        applyAnnotationUpdates(query.getLong(QueryParams.UID.key(), -1L), annotationUpdateMap, false);
        updateAnnotationSets(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList, queryOptions, false);

        return endQuery("Update cohort", startTime, new QueryResult<>());
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = cohortCollection.remove(parseQuery(query, false), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("Cohort");
        }
    }

    @Override
    public QueryResult<Cohort> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
            QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.STATUS_NAME.key());
            Cohort cohort = get(query, options).first();
            throw new CatalogDBException("The cohort {" + id + "} was already " + cohort.getStatus().getName());
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.UID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.DELETED);

        return endQuery("Delete cohort", startTime, get(query, null));
    }

//    @Override
//    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
//        long startTime = startQuery();
//        query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
//        QueryResult<Cohort> cohortQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
//        for (Cohort cohort : cohortQueryResult.getResult()) {
//            delete(cohort.getId(), queryOptions);
//        }
//        return endQuery("Delete cohort", startTime, Collections.singletonList(cohortQueryResult.getNumTotalResults()));
//    }

    QueryResult<Cohort> setStatus(long cohortId, String status) throws CatalogDBException {
        return update(cohortId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    @Override
    public QueryResult<Cohort> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
    }

    @Override
    public QueryResult<Cohort> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The cohort {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Cohort.CohortStatus.NONE);
        query = new Query(QueryParams.UID.key(), id);

        return endQuery("Restore cohort", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return endQuery("Restore cohorts", startTime, setStatus(query, Cohort.CohortStatus.NONE));
    }

    @Override
    public QueryResult<Cohort> get(long cohortId, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.UID.key(), cohortId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(cohortId))
                .append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
    }

    @Override
    public QueryResult<Cohort> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Cohort> documentList = new ArrayList<>();
        QueryResult<Cohort> queryResult;
        try (DBIterator<Cohort> dbIterator = iterator(query, options, user)) {
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
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_COHORTS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Cohort> documentList = new ArrayList<>();
        try (DBIterator<Cohort> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        QueryResult<Cohort> queryResult = endQuery("Get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

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

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
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
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<Cohort> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new CohortMongoDBIterator(mongoCursor, cohortConverter, null, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), null, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new CohortMongoDBIterator(mongoCursor, null, null, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), null, options);
    }

    @Override
    public DBIterator<Cohort> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS.name(), CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name());

        return new CohortMongoDBIterator<>(mongoCursor, cohortConverter, iteratorFilter, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), user, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS.name(), CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name());

        return new CohortMongoDBIterator(mongoCursor, null, iteratorFilter, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), user, options);
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
                    StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), CohortAclEntry.CohortPermissions.VIEW.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeInnerProjections(qOptions, QueryParams.SAMPLES.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);
        qOptions = filterOptions(qOptions, FILTER_ROUTE_COHORTS);

        logger.debug("Cohort query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return cohortCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }
        return queryResult.first();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return rank(cohortCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(cohortCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(cohortCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS.name(),
                    CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), CohortAclEntry.CohortPermissions.VIEW.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(cohortCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS.name(),
                    CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), CohortAclEntry.CohortPermissions.VIEW.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(cohortCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Cohort> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private void checkCohortNameExists(long studyId, String cohortName) throws CatalogDBException {
        QueryResult<Long> count = cohortCollection.count(Filters.and(
                Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.eq(QueryParams.ID.key(), cohortName)));
        if (count.getResult().get(0) > 0) {
            throw CatalogDBException.alreadyExists("Cohort", "name", cohortName);
        }
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
                if (Constants.PRIVATE_ANNOTATION_PARAM_TYPES.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addOrQuery(PRIVATE_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
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
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(query.getString(QueryParams.ANNOTATION.key()),
                                    query.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
//                            annotationDocument = createAnnotationQuery(query.getString(QueryParams.ANNOTATION.key()),
//                                    query.getLong(QueryParams.VARIABLE_SET_UID.key()),
//                                    query.getString(QueryParams.ANNOTATION_SET_NAME.key()));
                        }
                        break;
                    case SAMPLE_UIDS:
                        addQueryFilter(queryParam.key(), queryParam.key(), query, queryParam.type(),
                                MongoDBQueryUtils.ComparisonOperator.IN, MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case UUID:
                    case ID:
                    case NAME:
                    case TYPE:
                    case RELEASE:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case DESCRIPTION:
                    case ANNOTATION_SETS:
//                    case VARIABLE_NAME:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
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

    public MongoDBCollection getCohortCollection() {
        return cohortCollection;
    }

    public QueryResult<Long> extractSamplesFromCohorts(Query query, List<Long> sampleIds) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Cohort> cohortQueryResult = get(query, new QueryOptions(QueryOptions.INCLUDE, QueryParams.UID.key()));
        if (cohortQueryResult.getNumResults() > 0) {
            Bson bsonQuery = parseQuery(query, false);
            Bson update = new Document("$pull", new Document(QueryParams.SAMPLES.key(),
                    new Document("id", new Document("$in", sampleIds))));
            QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
            QueryResult<UpdateResult> updateQueryResult = cohortCollection.update(bsonQuery, update, multi);

            // Now we set all the cohorts where a sample has been taken out to status INVALID
            List<Long> ids = cohortQueryResult.getResult().stream().map(Cohort::getUid).collect(Collectors.toList());
            setStatus(new Query(QueryParams.UID.key(), ids), Cohort.CohortStatus.INVALID);

            return endQuery("Extract samples from cohorts", startTime,
                    Collections.singletonList(updateQueryResult.first().getModifiedCount()));
        }
        return endQuery("Extract samples from cohorts", startTime, Collections.singletonList(0L));
    }

    private boolean excludeSamples(QueryOptions options) {
        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
            for (String include : includeList) {
                if (include.startsWith(QueryParams.SAMPLES.key())) {
                    // Samples should be included
                    return false;
                }
            }
            // Samples are not included
            return true;
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            for (String exclude : excludeList) {
                if (exclude.equals(QueryParams.SAMPLES.key())) {
                    // Samples should be excluded
                    return true;
                }
            }
            // Samples are included
            return false;
        }
        // Samples are included by default
        return false;
    }

}
