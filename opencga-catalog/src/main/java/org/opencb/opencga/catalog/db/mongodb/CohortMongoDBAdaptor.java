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
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.WriteResult;
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
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
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
        long startQuery = startQuery();

        ClientSession clientSession = getClientSession();
        TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
            long startTime = startQuery();
            try {
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
                checkCohortIdExists(studyId, cohort.getId());

                long newId = getNewId(clientSession);
                cohort.setUid(newId);
                cohort.setStudyUid(studyId);
                if (StringUtils.isEmpty(cohort.getUuid())) {
                    cohort.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.COHORT));
                }
                if (StringUtils.isEmpty(cohort.getCreationDate())) {
                    cohort.setCreationDate(TimeUtils.getTime());
                }

                Document cohortObject = cohortConverter.convertToStorageType(cohort, variableSetList);

                cohortObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(cohort.getCreationDate()));
                cohortObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

                logger.debug("Inserting cohort '{}' ({})...", cohort.getId(), cohort.getUid());
                cohortCollection.insert(clientSession, cohortObject, null);
                logger.debug("Cohort '{}' successfully inserted", cohort.getId());

                return endWrite(String.valueOf(newId), startTime, 1, 1, null);
            } catch (CatalogDBException e) {
                logger.error("Could not create cohort '{}': {}", cohort.getId(), e.getMessage(), e);
                return endWrite(cohort.getId(), startTime, 1, 0,
                        Collections.singletonList(new WriteResult.Fail(cohort.getId(), e.getMessage())));
            }
        };

        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 1) {
            Query query = new Query()
                    .append(QueryParams.STUDY_UID.key(), studyId)
                    .append(QueryParams.UID.key(), Long.parseLong(result.getId()));
            return endQuery("createIndividual", startQuery, get(query, options));
        } else {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
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
        return endQuery("Count cohort", startTime, cohortCollection.count(parseQuery(query)));
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
                studyPermission.getCohortPermission().name(), Entity.COHORT.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
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
        Bson bson = parseQuery(query);
        return cohortCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public WriteResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public WriteResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();

        Document cohortUpdate = parseAndValidateUpdateParams(parameters, query).toFinalUpdateDocument();

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Cohort> iterator = iterator(query, options);

        int numMatches = 0;
        int numModified = 0;
        List<WriteResult.Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
                long tmpStartTime = startQuery();

                try {
                    if (!cohortUpdate.isEmpty()) {
                        Query tmpQuery = new Query()
                                .append(QueryParams.STUDY_UID.key(), cohort.getStudyUid())
                                .append(QueryParams.UID.key(), cohort.getUid());
                        Bson finalQuery = parseQuery(tmpQuery);
                        logger.debug("Cohort update: query : {}, update: {}",
                                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                                cohortUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                        cohortCollection.update(clientSession, finalQuery, cohortUpdate, null);
                    }

                    updateAnnotationSets(clientSession, cohort.getUid(), parameters, variableSetList, queryOptions, false);
                } catch (CatalogDBException e) {
                    logger.error("Error updating cohort {}({}). {}", cohort.getId(), cohort.getUid(), e.getMessage(), e);
                    return endWrite(cohort.getId(), tmpStartTime, 1, 0,
                            Collections.singletonList(new WriteResult.Fail(cohort.getId(), e.getMessage())));
                }

                return endWrite(cohort.getId(), tmpStartTime, 1, 1, null);
            };

            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumModified() == 1) {
                logger.info("Cohort {} successfully updated", cohort.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not update cohort {}: {}", cohort.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not update cohort {}", cohort.getId());
                }
            }
        }

        Error error = null;
        if (!failList.isEmpty()) {
            error = new Error(-1, "update", (numModified == 0
                    ? "None of the cohorts could be updated"
                    : "Some of the cohorts could not be updated"));
        }

        return endWrite("update", startTime, numMatches, numModified, failList, null, error);
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, Query query) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(CohortDBAdaptor.QueryParams.ID.key())) {
            // That can only be done to one cohort...
            Query tmpQuery = new Query(query);

            QueryResult<Cohort> cohortQueryResult = get(tmpQuery, new QueryOptions());
            if (cohortQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update cohort: No cohort found to be updated");
            }
            if (cohortQueryResult.getNumResults() > 1) {
                throw CatalogDBException.cannotUpdateMultipleEntries(QueryParams.ID.key(), "cohort");
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

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key(), QueryParams.CREATION_DATE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(QueryParams.TYPE.key(), Study.Type.class);
        filterEnumParams(parameters, document.getSet(), acceptedEnums);

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
            document.getSet().put(QueryParams.SAMPLES.key(), cohortConverter.convertSamplesToDocument(sampleList));
        }
        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            document.getSet().put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey(QueryParams.STATUS_MSG.key())) {
            document.getSet().put(QueryParams.STATUS_MSG.key(), parameters.get(QueryParams.STATUS_MSG.key()));
            document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey(QueryParams.STATUS.key())) {
            throw new CatalogDBException("Unable to modify cohort. Use parameter '" + QueryParams.STATUS_NAME.key()
                    + "' instead of '" + QueryParams.STATUS.key() + "'");
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

    @Override
    public WriteResult delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        return delete(query);
    }

    @Override
    public WriteResult delete(Query query) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key(), QueryParams.STATUS.key()));
        DBIterator<Cohort> iterator = iterator(query, options);

        long startTime = startQuery();
        int numMatches = 0;
        int numModified = 0;
        List<WriteResult.Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
                long tmpStartTime = startQuery();
                try {
                    logger.info("Deleting cohort {} ({})", cohort.getId(), cohort.getUid());

                    checkCohortCanBeDeleted(cohort);

                    String deleteSuffix = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

                    Query tmpQuery = new Query()
                            .append(QueryParams.UID.key(), cohort.getUid())
                            .append(QueryParams.STUDY_UID.key(), cohort.getStudyUid());
                    // Mark the cohort as deleted
                    ObjectMap updateParams = new ObjectMap()
                            .append(QueryParams.STATUS_NAME.key(), Status.DELETED)
                            .append(QueryParams.STATUS_DATE.key(), TimeUtils.getTime())
                            .append(QueryParams.ID.key(), cohort.getId() + deleteSuffix);

                    Bson bsonQuery = parseQuery(tmpQuery);
                    Document updateDocument = parseAndValidateUpdateParams(updateParams, tmpQuery).toFinalUpdateDocument();

                    logger.debug("Delete cohort {} ({}): Query: {}, update: {}", cohort.getId(), cohort.getUid(),
                            bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                    UpdateResult cohortResult = cohortCollection.update(clientSession, bsonQuery, updateDocument,
                            QueryOptions.empty()).first();

                    if (cohortResult.getModifiedCount() == 0) {
                        logger.error("Cohort {} could not be deleted", cohort.getId());
                        throw new CatalogDBException("Cohort " + cohort.getId() + " could not be deleted");
                    }

                    logger.debug("Cohort {} successfully deleted", cohort.getId());
                    return endWrite(cohort.getId(), tmpStartTime, 1, 1, null);

                } catch (CatalogDBException e) {
                    logger.error("Error deleting cohort {}({}). {}", cohort.getId(), cohort.getUid(), e.getMessage(), e);
                    return endWrite(cohort.getId(), tmpStartTime, 1, 0,
                            Collections.singletonList(new WriteResult.Fail(cohort.getId(), e.getMessage())));
                }
            };

            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumModified() == 1) {
                logger.info("Cohort {} successfully deleted", cohort.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not delete cohort {}: {}", cohort.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not delete cohort {}", cohort.getId());
                }
            }
        }

        Error error = null;
        if (!failList.isEmpty()) {
            error = new Error(-1, "delete", (numModified == 0
                    ? "None of the cohorts could be deleted"
                    : "Some of the cohorts could not be deleted"));
        }

        return endWrite("delete", startTime, numMatches, numModified, failList, null, error);
    }

    private void checkCohortCanBeDeleted(Cohort cohort) throws CatalogDBException {
        // Check if the cohort is different from DEFAULT_COHORT
        if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
            throw new CatalogDBException("Cohort " + StudyEntry.DEFAULT_COHORT + " cannot be deleted.");
        }

        // Check if the cohort can be deleted
        if (cohort.getStatus() != null && cohort.getStatus().getName() != null
                && !cohort.getStatus().getName().equals(Cohort.CohortStatus.NONE)) {
            throw new CatalogDBException("Cohort in use in storage.");
        }
    }


    QueryResult<Cohort> setStatus(long cohortId, String status) throws CatalogDBException {
        return update(cohortId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        WriteResult update = update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
        return new QueryResult<>(update.getId(), update.getDbTime(), (int) update.getNumMatches(), update.getNumMatches(), "",
                "", Collections.singletonList(update.getNumModified()));
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
                    StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), CohortAclEntry.CohortPermissions.VIEW.name(), Entity.COHORT.name());
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
        Bson bsonQuery = parseQuery(query);
        return rank(cohortCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(cohortCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
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
                    CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name(), Entity.COHORT.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), CohortAclEntry.CohortPermissions.VIEW.name(), Entity.COHORT.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
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
                    CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name(), Entity.COHORT.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), CohortAclEntry.CohortPermissions.VIEW.name(), Entity.COHORT.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
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

    private void checkCohortIdExists(long studyId, String cohortId) throws CatalogDBException {
        QueryResult<Long> count = cohortCollection.count(Filters.and(
                Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.eq(QueryParams.ID.key(), cohortId)));
        if (count.getResult().get(0) > 0) {
            throw CatalogDBException.alreadyExists("Cohort", "id", cohortId);
        }
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    protected Bson parseQuery(Query query, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

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
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Cohort.CohortStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case UUID:
                    case ID:
                    case NAME:
                    case TYPE:
                    case RELEASE:
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
            Bson bsonQuery = parseQuery(query);
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
