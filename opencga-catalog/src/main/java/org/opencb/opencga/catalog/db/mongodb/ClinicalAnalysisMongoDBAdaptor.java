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
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ClinicalAnalysisMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.permissions.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisMongoDBAdaptor extends MongoDBAdaptor implements ClinicalAnalysisDBAdaptor {

    private final MongoDBCollection clinicalCollection;
    private ClinicalAnalysisConverter clinicalConverter;

    public ClinicalAnalysisMongoDBAdaptor(MongoDBCollection clinicalCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(ClinicalAnalysisMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.clinicalCollection = clinicalCollection;
        this.clinicalConverter = new ClinicalAnalysisConverter();
    }

    public MongoDBCollection getClinicalCollection() {
        return clinicalCollection;
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return clinicalCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = studyPermissions;

        if (studyPermission == null) {
            studyPermission = StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS;
        }

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getClinicalAnalysisPermission().name());
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("Clinical count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return clinicalCollection.count(bson);
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<ClinicalAnalysis> update(long id, ObjectMap parameters, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (parameters.containsKey(QueryParams.ID.key())) {
            // Check that the new sample name is still unique
            long studyId = getStudyId(id);

            QueryResult<Long> count = clinicalCollection.count(
                    new Document(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                            .append(PRIVATE_STUDY_ID, studyId));
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Clinical analysis { name: '" + parameters.get(QueryParams.ID.key())
                        + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedObjectParams = {QueryParams.FAMILY.key(), QueryParams.SUBJECTS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        clinicalConverter.validateFamilyToUpdate(document.getSet());
        clinicalConverter.validateSubjectsToUpdate(document.getSet());

        Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
        String operation = (String) actionMap.getOrDefault(QueryParams.INTERPRETATIONS.key(), "ADD");
        acceptedObjectParams = new String[]{QueryParams.INTERPRETATIONS.key()};
        switch (operation) {
            case "SET":
                filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
                clinicalConverter.validateInterpretationToUpdate(document.getSet());
                break;
            case "REMOVE":
                List<ObjectMap> interpretations = (List<ObjectMap>) parameters.get(QueryParams.INTERPRETATIONS.key());
                if (interpretations != null) {
                    List<String> interpretationIds = new ArrayList<>(interpretations.size());
                    for (ObjectMap interpretation : interpretations) {
                        String interpretationId = interpretation.getString("id");
                        if (StringUtils.isNotEmpty(interpretationId)) {
                            interpretationIds.add(interpretationId);
                        }
                    }
                    if (interpretationIds.size() > 0) {
                        document.getPullAll().put(QueryParams.INTERPRETATIONS.key(), new Document("id", interpretationIds));
                    }
                }
                break;
            case "ADD":
            default:
                filterObjectParams(parameters, document.getAddToSet(), acceptedObjectParams);
                clinicalConverter.validateInterpretationToUpdate(document.getAddToSet());
                break;
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            document.getSet().put(MODIFICATION_DATE, time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        Document updateOperation = document.toFinalUpdateDocument();

        if (!updateOperation.isEmpty()) {
            clinicalConverter.validateDocumentToUpdate(updateOperation);

            Bson query = Filters.eq(PRIVATE_UID, id);
            QueryResult<UpdateResult> update = clinicalCollection.update(query, updateOperation, null);

            if (update.getResult().isEmpty() || update.getResult().get(0).getMatchedCount() == 0) {
                throw CatalogDBException.uidNotFound("Clinical Analysis", id);
            }
        }

        return endQuery("Modify clinical analysis", startTime, get(id, options));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> setInterpretations(long clinicalAnalysisId, List<ClinicalAnalysis.ClinicalInterpretation> interpretationList) {
        return null;
    }

    @Override
    public QueryResult<Long> addInterpretation(long clinicalAnalysisId, ClinicalAnalysis.ClinicalInterpretation interpretation)
            throws CatalogDBException {
        long startTime = startQuery();

        Document clinicalInterpretation = getMongoDBDocument(interpretation, "ClinicalInterpretation");
        clinicalConverter.validateInterpretation(clinicalInterpretation);

        Document match = new Document()
                .append(PRIVATE_UID, clinicalAnalysisId)
                .append(QueryParams.INTERPRETATIONS_ID.key(), new Document("$ne", interpretation.getId()));
        Document update = new Document("$push", new Document(QueryParams.INTERPRETATIONS.key(), clinicalInterpretation));

        QueryResult<UpdateResult> updateResult = clinicalCollection.update(match, update, QueryOptions.empty());

        return endQuery("addInterpretation", startTime, Arrays.asList(updateResult.first().getModifiedCount()));
    }

    @Override
    public QueryResult<Long> removeInterpretation(long clinicalAnalysisId, String interpretationId) throws CatalogDBException {
        long startTime = startQuery();

        Document match = new Document()
                .append(PRIVATE_UID, clinicalAnalysisId)
                .append(QueryParams.INTERPRETATIONS_ID.key(), interpretationId);
        Document update = new Document("$pull", new Document(QueryParams.INTERPRETATIONS.key(), new Document("id", interpretationId)));
        QueryResult<UpdateResult> updateResult = clinicalCollection.update(match, update, QueryOptions.empty());

        return endQuery("removeInterpretation", startTime, Arrays.asList(updateResult.first().getModifiedCount()));
    }

    @Override
    public void unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        unmarkPermissionRule(clinicalCollection, studyId, permissionRuleId);
    }

    @Override
    public void delete(long id) throws CatalogDBException {

    }

    @Override
    public void delete(Query query) throws CatalogDBException {

    }

    @Override
    public QueryResult<ClinicalAnalysis> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<ClinicalAnalysis> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<ClinicalAnalysis> documentList = new ArrayList<>();
        try (DBIterator<ClinicalAnalysis> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        QueryResult<ClinicalAnalysis> queryResult = endQuery("Get", startTime, documentList);

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
    public DBIterator<ClinicalAnalysis> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new ClinicalAnalysisMongoDBIterator(mongoCursor, clinicalConverter, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID), null,
                options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new ClinicalAnalysisMongoDBIterator(mongoCursor, null, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID), null,
                options);
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        return new ClinicalAnalysisMongoDBIterator(mongoCursor, clinicalConverter, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID), user,
                options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions, studyDocument, user);
        return new ClinicalAnalysisMongoDBIterator(mongoCursor, null, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID), user,
                options);
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
                    StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS.name(),
                    ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeInnerProjections(qOptions, QueryParams.SUBJECTS.key());
        qOptions = removeInnerProjections(qOptions, QueryParams.FAMILY.key());

        logger.debug("Clinical analysis query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return clinicalCollection.nativeQuery().find(bson, qOptions).iterator();
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
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(clinicalCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(clinicalCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS.name(),
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(clinicalCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS.name(),
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(clinicalCollection, bsonQuery, fields, SampleDBAdaptor.QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {

    }

    @Override
    public void nativeInsert(Map<String, Object> clinicalAnalysis, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(clinicalAnalysis, "clinicalAnalysis");
        clinicalCollection.insert(document, null);
    }

    @Override
    public QueryResult<ClinicalAnalysis> insert(long studyId, ClinicalAnalysis clinicalAnalysis, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), clinicalAnalysis.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_ID, studyId));
        filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        QueryResult<Long> count = clinicalCollection.count(bson);
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Cannot create clinical analysis. A clinical analysis with { name: '"
                    + clinicalAnalysis.getId() + "'} already exists.");
        }

        long clinicalAnalysisId = getNewId();
        clinicalAnalysis.setUid(clinicalAnalysisId);
        clinicalAnalysis.setStudyUid(studyId);
        if (StringUtils.isEmpty(clinicalAnalysis.getUuid())) {
            clinicalAnalysis.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.CLINICAL));
        }

        Document clinicalObject = clinicalConverter.convertToStorageType(clinicalAnalysis);
        if (StringUtils.isNotEmpty(clinicalAnalysis.getCreationDate())) {
            clinicalObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(clinicalAnalysis.getCreationDate()));
        } else {
            clinicalObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        clinicalObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        clinicalCollection.insert(clinicalObject, null);

        return endQuery("createClinicalAnalysis", startTime, get(clinicalAnalysisId, options));
    }

    @Override
    public QueryResult<ClinicalAnalysis> get(long clinicalAnalysisId, QueryOptions options) throws CatalogDBException {
        checkId(clinicalAnalysisId);
        return get(new Query(QueryParams.UID.key(), clinicalAnalysisId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED)
                .append(QueryParams.STUDY_UID.key(), getStudyId(clinicalAnalysisId)), options);
    }

    @Override
    public QueryResult<ClinicalAnalysis> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS.name(),
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name());

        filterOutDeleted(query);
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }

        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = clinicalCollection.find(bson, clinicalConverter, qOptions);
//        addReferencesInfoToClinicalAnalysis(clinicalAnalysisQueryResult);

        logger.debug("Clinical Analysis get: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()), qOptions.toJson(), clinicalAnalysisQueryResult.getDbTime());
        return endQuery("Get clinical analysis", startTime, clinicalAnalysisQueryResult);
    }

    @Override
    public long getStudyId(long clinicalAnalysisId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, clinicalAnalysisId);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = clinicalCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("ClinicalAnalysis", clinicalAnalysisId);
        }
    }

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        return parseQuery(query, isolated, null);
    }

    protected Bson parseQuery(Query query, boolean isolated, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Status.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case TYPE:
                    case SAMPLE_UID:
                    case SUBJECT_UID:
                    case FAMILY_UID:
                    case GERMLINE_UID:
                    case SOMATIC_UID:
                    case DESCRIPTION:
                    case RELEASE:
                    case STATUS:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case ACL:
                    case ACL_MEMBER:
                    case ACL_PERMISSIONS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
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
