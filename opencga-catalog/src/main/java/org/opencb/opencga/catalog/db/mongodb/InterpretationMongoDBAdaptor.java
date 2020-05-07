/*
 * Copyright 2015-2020 OpenCB
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.InterpretationConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class InterpretationMongoDBAdaptor extends MongoDBAdaptor implements InterpretationDBAdaptor {

    private final MongoDBCollection interpretationCollection;
    private final MongoDBCollection deletedInterpretationCollection;
    private final ClinicalAnalysisMongoDBAdaptor clinicalDBAdaptor;
    private InterpretationConverter interpretationConverter;

    public InterpretationMongoDBAdaptor(MongoDBCollection interpretationCollection, MongoDBCollection deletedInterpretationCollection,
                                        MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(InterpretationMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.clinicalDBAdaptor = dbAdaptorFactory.getClinicalAnalysisDBAdaptor();
        this.interpretationCollection = interpretationCollection;
        this.deletedInterpretationCollection = deletedInterpretationCollection;
        this.interpretationConverter = new InterpretationConverter();
    }

    public MongoDBCollection getInterpretationCollection() {
        return interpretationCollection;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> interpretation, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(interpretation, "clinicalAnalysis");
        return new OpenCGAResult(interpretationCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, Interpretation interpretation, boolean primary)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting interpretation insert transaction for interpretation id '{}'", interpretation.getId());
            insert(clientSession, studyId, interpretation, primary);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create interpretation {}: {}", interpretation.getId(), e.getMessage()));
    }

    private void insert(ClientSession clientSession, long studyId, Interpretation interpretation, boolean primary)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), interpretation.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));
        filterList.add(Filters.eq(QueryParams.STATUS.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = interpretationCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Cannot create interpretation. An interpretation with { id: '"
                    + interpretation.getId() + "'} already exists.");
        }

        long interpretationUid = getNewUid(clientSession);
        interpretation.setUid(interpretationUid);
        interpretation.setStudyUid(studyId);
        if (StringUtils.isEmpty(interpretation.getUuid())) {
            interpretation.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.INTERPRETATION));
        }

        Document interpretationObject = interpretationConverter.convertToStorageType(interpretation);
        if (StringUtils.isNotEmpty(interpretation.getCreationDate())) {
            interpretationObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(interpretation.getCreationDate()));
        } else {
            interpretationObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        interpretationObject.put(PRIVATE_MODIFICATION_DATE, interpretationObject.get(PRIVATE_CREATION_DATE));
        interpretationCollection.insert(clientSession, interpretationObject, null);

        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), interpretation.getClinicalAnalysisId());
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key());
        // Check clinical analysis does not have any primary interpretation already
        OpenCGAResult<ClinicalAnalysis> result = clinicalDBAdaptor.get(clientSession, query, options);
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Clinical analysis '" + interpretation.getClinicalAnalysisId() + "' does not exist.");
        }

        if (primary) {
            if (result.first().getInterpretation() != null && StringUtils.isNotEmpty(result.first().getInterpretation().getId())) {
                throw new CatalogDBException("Clinical analysis '" + interpretation.getClinicalAnalysisId() + "' already has a "
                        + "primary interpretation.");
            }

            ObjectMap updateParams = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), interpretation);
            UpdateDocument updateDocument = clinicalDBAdaptor.parseAndValidateUpdateParams(updateParams, query, QueryOptions.empty());
            clinicalDBAdaptor.getClinicalCollection().update(clientSession, clinicalDBAdaptor.parseQuery(query),
                    updateDocument.toFinalUpdateDocument(), null);
        } else {
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.UpdateAction.ADD.name());
            options = new QueryOptions(Constants.ACTIONS, actionMap);
            ObjectMap updateParams = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                            Collections.singletonList(interpretation));

            // Update array of interpretations from clinical analysis
            UpdateDocument updateDocument = clinicalDBAdaptor.parseAndValidateUpdateParams(updateParams, query, options);
            clinicalDBAdaptor.getClinicalCollection().update(clientSession, clinicalDBAdaptor.parseQuery(query),
                    updateDocument.toFinalUpdateDocument(), null);
        }
    }

    @Override
    public OpenCGAResult<Interpretation> get(long interpretationUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(interpretationUid);
        return get(new Query(QueryParams.UID.key(), interpretationUid).append(QueryParams.STUDY_UID.key(),
                getStudyId(interpretationUid)), options);
    }

    @Override
    public OpenCGAResult<Interpretation> get(long studyUid, String interpretationId, QueryOptions options) throws CatalogDBException {
        return get(new Query(QueryParams.ID.key(), interpretationId).append(QueryParams.STUDY_UID.key(), studyUid), options);
    }

    @Override
    public long getStudyId(long interpretationId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, interpretationId);
        Bson projection = Projections.include(PRIVATE_STUDY_UID);
        DataResult<Document> queryResult = interpretationCollection.find(query, projection, null);

        if (!queryResult.getResults().isEmpty()) {
            Object studyId = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Interpretation", interpretationId);
        }
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        logger.debug("Interpretation count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(interpretationCollection.count(bson));
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException {
        return count(query);
    }

    @Override
    public OpenCGAResult distinct(Query query, String field) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<Interpretation> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Interpretation> dbIterator = iterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Interpretation> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return get(query, options);
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user) throws CatalogDBException {
        return nativeGet(query, options);
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Query tmpQuery = new Query(query);

            OpenCGAResult<Interpretation> interpretationDataResult = get(tmpQuery, new QueryOptions());
            if (interpretationDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update interpretation: No interpretation found to be updated");
            }
            if (interpretationDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update interpretation: Cannot set the same id parameter for different interpretations");
            }

            // Check that the new clinical analysis id will be unique
            long studyId = getStudyId(interpretationDataResult.first().getUid());

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Long> count = count(tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot set id for interpretation. A interpretation with { id: '"
                        + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedParams = {QueryParams.DESCRIPTION.key(), QueryParams.STATUS.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.FILTERS.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] objectAcceptedParams = {QueryParams.PANELS.key(), QueryParams.SOFTWARE.key(), QueryParams.ANALYST.key(),
                QueryParams.DEPENDENCIES.key(), QueryParams.REPORTED_VARIANTS.key(), QueryParams.REPORTED_LOW_COVERAGE.key()};
        filterObjectParams(parameters, document.getSet(), objectAcceptedParams);

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        String operation = (String) actionMap.getOrDefault(QueryParams.COMMENTS.key(), "ADD");
        objectAcceptedParams = new String[]{QueryParams.COMMENTS.key()};
        switch (operation) {
            case "SET":
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case "REMOVE":
                filterObjectParams(parameters, document.getPullAll(), objectAcceptedParams);
                break;
            case "ADD":
            default:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
        }

        operation = (String) actionMap.getOrDefault(QueryParams.REPORTED_VARIANTS.key(), "ADD");
        objectAcceptedParams = new String[]{QueryParams.REPORTED_VARIANTS.key()};
        switch (operation) {
            case "SET":
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case "REMOVE":
                filterObjectParams(parameters, document.getPullAll(), objectAcceptedParams);
                break;
            case "ADD":
            default:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
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

    /**
     * Creates a new version for all the interpretations matching the query.
     *
     * @param query Query object.
     */
    private void createNewVersion(Query query) throws CatalogDBException {
        OpenCGAResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        for (Document document : queryResult.getResults()) {
            Document updateOldVersion = new Document();

            updateOldVersion.put(LAST_OF_VERSION, false);

            // Perform the update on the previous version
            Document queryDocument = new Document()
                    .append(PRIVATE_STUDY_UID, document.getLong(PRIVATE_STUDY_UID))
                    .append(QueryParams.VERSION.key(), document.getInteger(QueryParams.VERSION.key()))
                    .append(PRIVATE_UID, document.getLong(PRIVATE_UID));
            DataResult result = interpretationCollection.update(queryDocument, new Document("$set", updateOldVersion), null);
            if (result.getNumUpdated() == 0) {
                throw new CatalogDBException("Internal error: Could not update interpretation");
            }

            // We update the information for the new version of the document
            document.put(LAST_OF_VERSION, true);
            document.put(QueryParams.VERSION.key(), document.getInteger(QueryParams.VERSION.key()) + 1);

            // Insert the new version document
            interpretationCollection.insert(document, QueryOptions.empty());
        }
    }

    @Override
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        UpdateDocument updateDocument = parseAndValidateUpdateParams(parameters, query, queryOptions);

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(query);
        }

        Document updateOperation = updateDocument.toFinalUpdateDocument();

        if (!updateOperation.isEmpty()) {
            Bson bsonQuery = Filters.eq(PRIVATE_UID, id);

            logger.debug("Update interpretation. Query: {}, Update: {}", bsonQuery.toBsonDocument(Document.class,
                    MongoClient.getDefaultCodecRegistry()), updateDocument);
            DataResult update = interpretationCollection.update(bsonQuery, updateOperation, null);

            if (update.getNumMatches() == 0) {
                throw CatalogDBException.uidNotFound("Interpretation", id);
            }
            return new OpenCGAResult(update);
        }

        return OpenCGAResult.empty();
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult delete(Interpretation interpretation) throws CatalogDBException {
        throw new NotImplementedException("Delete not implemented");
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        throw new NotImplementedException("Delete not implemented");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public DBIterator<Interpretation> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, interpretationConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new CatalogMongoDBIterator(mongoCursor);
    }

    @Override
    public DBIterator<Interpretation> iterator(long studyUid, Query query, QueryOptions options, String user) throws CatalogDBException {
        return iterator(query, options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException {
        return nativeIterator(query, options);
    }


    private MongoDBIterator<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        logger.debug("Interpretation query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return interpretationCollection.iterator(bson, qOptions);
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Interpretation> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    protected Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

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
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Enums.ExecutionStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.STATUS_NAME.key(), queryParam.key(), query, QueryParams.STATUS_NAME.type(),
                                andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case CLINICAL_ANALYSIS:
                    case DESCRIPTION:
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

        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
