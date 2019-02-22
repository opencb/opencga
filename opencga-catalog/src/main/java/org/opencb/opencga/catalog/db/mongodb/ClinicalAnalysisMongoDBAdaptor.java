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
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.permissions.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.ANALYST_ASSIGNEE;
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
                studyPermission.name(), studyPermission.getClinicalAnalysisPermission().name(), Entity.CLINICAL_ANALYSIS.name());
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

        Query query = new Query(QueryParams.UID.key(), id);
        UpdateDocument updateDocument = parseAndValidateUpdateParams(parameters, query, options);

        Document updateOperation = updateDocument.toFinalUpdateDocument();

        if (!updateOperation.isEmpty()) {
            Bson bsonQuery = Filters.eq(PRIVATE_UID, id);

            logger.debug("Update clinical analysis. Query: {}, Update: {}", bsonQuery.toBsonDocument(Document.class,
                    MongoClient.getDefaultCodecRegistry()), updateDocument);
            QueryResult<UpdateResult> update = clinicalCollection.update(bsonQuery, updateOperation, null);

            if (update.getResult().isEmpty() || update.getResult().get(0).getMatchedCount() == 0) {
                throw CatalogDBException.uidNotFound("Clinical Analysis", id);
            }
        }

        return endQuery("Modify clinical analysis", startTime, get(id, options));
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Query tmpQuery = new Query(query);

            QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = get(tmpQuery, new QueryOptions());
            if (clinicalAnalysisQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update clinical analysis: No clinical analysis found to be updated");
            }
            if (clinicalAnalysisQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update clinical analysis: Cannot set the same id parameter for different clinical analyses");
            }

            // Check that the new clinical analysis id will be unique
            long studyId = getStudyId(clinicalAnalysisQueryResult.first().getUid());

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot set id for clinical analysis. A clinical analysis with { id: '"
                        + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedParams = {QueryParams.DESCRIPTION.key(), QueryParams.PRIORITY.key(), QueryParams.DUE_DATE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        String[] acceptedListParams = {QueryParams.FLAGS.key()};
        filterStringListParams(parameters, document.getSet(), acceptedListParams);

        String[] acceptedObjectParams = {QueryParams.FILES.key(), QueryParams.FAMILY.key(), QueryParams.DISORDER.key(),
                QueryParams.PROBAND.key(), QueryParams.COMMENTS.key(), QueryParams.STATUS.key(), QueryParams.ANALYST.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        clinicalConverter.validateFamilyToUpdate(document.getSet());
        clinicalConverter.validateProbandToUpdate(document.getSet());

        if (parameters.containsKey(QueryParams.INTERPRETATIONS.key())) {
            List<Object> objectInterpretationList = parameters.getAsList(QueryParams.INTERPRETATIONS.key());
            List<Interpretation> interpretationList = new ArrayList<>();
            for (Object interpretation : objectInterpretationList) {
                if (interpretation instanceof Interpretation) {
                    if (!dbAdaptorFactory.getInterpretationDBAdaptor().exists(((Interpretation) interpretation).getUid())) {
                        throw CatalogDBException.uidNotFound("Interpretation", ((Interpretation) interpretation).getUid());
                    }
                    interpretationList.add((Interpretation) interpretation);
                }
            }

            if (!interpretationList.isEmpty()) {
                Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
                String operation = (String) actionMap.getOrDefault(QueryParams.INTERPRETATIONS.key(), "ADD");
                switch (operation) {
                    case "SET":
                        document.getSet().put(QueryParams.INTERPRETATIONS.key(),
                                clinicalConverter.convertInterpretations(interpretationList));
                        break;
                    case "REMOVE":
                        document.getPullAll().put(QueryParams.INTERPRETATIONS.key(),
                                clinicalConverter.convertInterpretations(interpretationList));
                        break;
                    case "ADD":
                    default:
                        document.getAddToSet().put(QueryParams.INTERPRETATIONS.key(),
                                clinicalConverter.convertInterpretations(interpretationList));
                        break;
                }
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

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return null;
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
        return new ClinicalAnalysisMongoDBIterator<>(mongoCursor, clinicalConverter, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID),
                null, options);
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
        return new ClinicalAnalysisMongoDBIterator(mongoCursor, null, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID), user, options);
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
                    ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name(), Entity.CLINICAL_ANALYSIS.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeInnerProjections(qOptions, QueryParams.INTERPRETATIONS.key());

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
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name(), Entity.CLINICAL_ANALYSIS.name());
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
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name(), Entity.CLINICAL_ANALYSIS.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(clinicalCollection, bsonQuery, fields, SampleDBAdaptor.QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<ClinicalAnalysis> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
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

        Bson bson = Filters.and(filterList);
        QueryResult<Long> count = clinicalCollection.count(bson);
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Cannot create clinical analysis. A clinical analysis with { id: '"
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
        List<ClinicalAnalysis> documentList = new ArrayList<>();
        QueryResult<ClinicalAnalysis> queryResult;
        try (DBIterator<ClinicalAnalysis> dbIterator = iterator(query, options, user)) {
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
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
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
                    case DISORDER:
                        addOntologyQueryFilter(queryParam.key(), queryParam.key(), query, andBsonList);
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
                                Status.getPositiveStatus(ClinicalAnalysis.ClinicalStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ANALYST:
                    case ANALYST_ASSIGNEE:
                        addAutoOrQuery(ANALYST_ASSIGNEE.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case TYPE:
                    case DUE_DATE:
                    case PRIORITY:
                    case SAMPLE_UID:
                    case PROBAND_UID:
                    case FAMILY_UID:
                    case DESCRIPTION:
                    case RELEASE:
                    case STATUS:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case FLAGS:
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
