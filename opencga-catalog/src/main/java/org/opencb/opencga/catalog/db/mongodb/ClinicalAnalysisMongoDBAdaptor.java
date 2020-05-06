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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ClinicalAnalysisCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisStatus;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.response.OpenCGAResult;
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
    private final MongoDBCollection deletedClinicalCollection;
    private ClinicalAnalysisConverter clinicalConverter;

    public ClinicalAnalysisMongoDBAdaptor(MongoDBCollection clinicalCollection, MongoDBCollection deletedClinicalCollection,
                                          MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(ClinicalAnalysisMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.clinicalCollection = clinicalCollection;
        this.deletedClinicalCollection = deletedClinicalCollection;
        this.clinicalConverter = new ClinicalAnalysisConverter();
    }

    public MongoDBCollection getClinicalCollection() {
        return clinicalCollection;
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(clinicalCollection.count(bson));
    }

    @Override
    public OpenCGAResult<Long> count(final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Clinical count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(clinicalCollection.count(bson));
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
    public OpenCGAResult update(long uid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), uid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<Document> documentResult = nativeGet(query, options);
        if (documentResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update clinical analysis. Clinical Analysis uid '" + uid + "' not found.");
        }
        String clinicalAnalysisId = documentResult.first().getString(QueryParams.ID.key());

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, documentResult.first(), parameters, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update clinical analysis {}: {}", clinicalAnalysisId, e.getMessage(), e);
            throw new CatalogDBException("Could not update clinical analysis " + clinicalAnalysisId + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    private OpenCGAResult privateUpdate(ClientSession clientSession, Document first, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        String clinicalAnalysisId = first.getString(QueryParams.ID.key());
        long clinicalAnalysisUid = first.getLong(QueryParams.UID.key());

        Query query = new Query(QueryParams.UID.key(), clinicalAnalysisUid);
        UpdateDocument updateDocument = parseAndValidateUpdateParams(parameters, query, queryOptions);

        Document updateOperation = updateDocument.toFinalUpdateDocument();

        List<Event> events = new ArrayList<>();
        if (!updateOperation.isEmpty()) {
            Bson bsonQuery = Filters.eq(PRIVATE_UID, clinicalAnalysisUid);

            logger.debug("Update clinical analysis. Query: {}, Update: {}", bsonQuery.toBsonDocument(Document.class,
                    MongoClient.getDefaultCodecRegistry()), updateDocument);
            DataResult result = clinicalCollection.update(clientSession, bsonQuery, updateOperation, null);

            if (result.getNumMatches() == 0) {
                throw CatalogDBException.uidNotFound("Clinical Analysis", clinicalAnalysisUid);
            }

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Clinical Analysis " + clinicalAnalysisId + " not found");
            }
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, clinicalAnalysisId, "Clinical Analysis was already updated"));
            }

            logger.debug("Clinical Analysis {} successfully updated", clinicalAnalysisId);
        } else {
            throw new CatalogDBException("Nothing to update");
        }

        return endWrite(tmpStartTime, 1, 1, events);
    }

    UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one clinical analysis...
            Query tmpQuery = new Query(query);

            OpenCGAResult<ClinicalAnalysis> clinicalAnalysisDataResult = get(tmpQuery, new QueryOptions());
            if (clinicalAnalysisDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update clinical analysis: No clinical analysis found to be updated");
            }
            if (clinicalAnalysisDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update clinical analysis: Cannot set the same id parameter for different clinical analyses");
            }

            // Check that the new clinical analysis id will be unique
            long studyId = getStudyId(clinicalAnalysisDataResult.first().getUid());

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Long> count = count(tmpQuery);
            if (count.getNumMatches() > 0) {
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
                QueryParams.PROBAND.key(), QueryParams.COMMENTS.key(), QueryParams.ALERTS.key(), QueryParams.INTERNAL_STATUS.key(),
                QueryParams.ANALYST.key(), QueryParams.CONSENT.key(), QueryParams.STATUS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        String[] acceptedMapParams = {QueryParams.ROLE_TO_PROBAND.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        clinicalConverter.validateFamilyToUpdate(document.getSet());
        clinicalConverter.validateProbandToUpdate(document.getSet());

        // Interpretation (primary)
        if (parameters.containsKey(QueryParams.INTERPRETATION.key())) {
            document.getSet().put(QueryParams.INTERPRETATION.key(),
                    clinicalConverter.convertInterpretation((Interpretation) parameters.get(QueryParams.INTERPRETATION.key())));
        }

        // Secondary interpretations
        if (parameters.containsKey(QueryParams.SECONDARY_INTERPRETATIONS.key())) {
            List<Object> objectInterpretationList = parameters.getAsList(QueryParams.SECONDARY_INTERPRETATIONS.key());
            List<Interpretation> interpretationList = new ArrayList<>();
            for (Object interpretation : objectInterpretationList) {
                if (interpretation instanceof Interpretation) {
                    if (!dbAdaptorFactory.getInterpretationDBAdaptor().exists(((Interpretation) interpretation).getUid())) {
                        throw CatalogDBException.uidNotFound("Secondary interpretation", ((Interpretation) interpretation).getUid());
                    }
                    interpretationList.add((Interpretation) interpretation);
                }
            }

            if (!interpretationList.isEmpty()) {
                Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
                String operation = (String) actionMap.getOrDefault(QueryParams.SECONDARY_INTERPRETATIONS.key(), "ADD");
                switch (operation) {
                    case "SET":
                        document.getSet().put(QueryParams.SECONDARY_INTERPRETATIONS.key(),
                                clinicalConverter.convertInterpretations(interpretationList));
                        break;
                    case "REMOVE":
                        document.getPullAll().put(QueryParams.SECONDARY_INTERPRETATIONS.key(),
                                clinicalConverter.convertInterpretations(interpretationList));
                        break;
                    case "ADD":
                    default:
                        document.getAddToSet().put(QueryParams.SECONDARY_INTERPRETATIONS.key(),
                                clinicalConverter.convertInterpretations(interpretationList));
                        break;
                }
            }
        }

        if (parameters.containsKey(QueryParams.QUALITY_CONTROL.key())) {
            // Check all quality control parameters
            String[] qualityObjectParams = {QueryParams.QUALITY_CONTROL_VARIANT.key(), QueryParams.QUALITY_CONTROL_ALIGNMENT.key(),
                    QueryParams.QUALITY_CONTROL_ANALYST.key()};
            filterObjectParams(parameters, document.getSet(), qualityObjectParams, true);

            String[] qualityStringParams = {QueryParams.QUALITY_CONTROL_QUALITY.key()};
            filterStringParams(parameters, document.getSet(), qualityStringParams, true);

            // We don't set the list of comments, we always add to the current list
            String[] qualityStringListParams = {QueryParams.QUALITY_CONTROL_COMMENTS.key()};
            filterStringListParams(parameters, document.getAddToSet(), qualityStringListParams, true);

            // We always update the date
            nestedPut(QueryParams.QUALITY_CONTROL_DATE.key(), TimeUtils.getTime(), document.getSet());
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
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(clinicalCollection, studyId, permissionRuleId);
    }

    @Override
    public OpenCGAResult delete(ClinicalAnalysis clinicalAnalysis)
            throws CatalogParameterException, CatalogAuthorizationException, CatalogDBException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), clinicalAnalysis.getUid())
                    .append(QueryParams.STUDY_UID.key(), clinicalAnalysis.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find Clinical Analysis '" + clinicalAnalysis.getId() + "' with uid '"
                        + clinicalAnalysis.getUid() + "'.");
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete Clinical Analysis {}: {}", clinicalAnalysis.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete Clinical Analysis " + clinicalAnalysis.getId() + ": " + e.getMessage(),
                    e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<Document> iterator = nativeIterator(query, new QueryOptions());

        OpenCGAResult<ClinicalAnalysis> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Document clinicalAnalysis = iterator.next();
            String clinicalAnalysisId = clinicalAnalysis.getString(QueryParams.ID.key());

            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, clinicalAnalysis)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete Clinical Analysis {}: {}", clinicalAnalysisId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, clinicalAnalysisId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document clinicalDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        String clinicalId = clinicalDocument.getString(QueryParams.ID.key());
        long clinicalUid = clinicalDocument.getLong(PRIVATE_UID);
        long studyUid = clinicalDocument.getLong(PRIVATE_STUDY_UID);

        logger.debug("Deleting Clinical Analysis {} ({})", clinicalId, clinicalUid);

        // Delete any documents that might have been already deleted with that id
        Bson query = new Document()
                .append(QueryParams.ID.key(), clinicalId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedClinicalCollection.remove(clientSession, query, new QueryOptions(MongoDBCollection.MULTI, true));

        // Set status to DELETED
        nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new Status(Status.DELETED), "status"), clinicalDocument);

        // Insert the document in the DELETE collection
        deletedClinicalCollection.insert(clientSession, clinicalDocument, null);
        logger.debug("Inserted Clinical Analysis uid '{}' in DELETE collection", clinicalUid);

        // Remove the document from the main Clinical collection
        query = parseQuery(new Query(QueryParams.UID.key(), clinicalUid));
        DataResult remove = clinicalCollection.remove(clientSession, query, null);
        if (remove.getNumMatches() == 0) {
            throw new CatalogDBException("Clinical Analysis " + clinicalId + " not found");
        }
        if (remove.getNumDeleted() == 0) {
            throw new CatalogDBException("Clinical Analysis " + clinicalId + " could not be deleted");
        }

        logger.debug("Clinical Analysis {}({}) deleted", clinicalId, clinicalUid);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
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
    public OpenCGAResult<ClinicalAnalysis> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    public OpenCGAResult<ClinicalAnalysis> get(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<ClinicalAnalysis> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    public DBIterator<ClinicalAnalysis> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options, null);
        return new ClinicalAnalysisCatalogMongoDBIterator<>(mongoCursor, clinicalConverter, dbAdaptorFactory, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new ClinicalAnalysisCatalogMongoDBIterator(mongoCursor, null, dbAdaptorFactory, options);
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, options, user);
        return new ClinicalAnalysisCatalogMongoDBIterator(mongoCursor, clinicalConverter, dbAdaptorFactory, studyUid, user, options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, queryOptions, user);
        return new ClinicalAnalysisCatalogMongoDBIterator(mongoCursor, null, dbAdaptorFactory, studyUid, user, options);
    }

    private MongoDBIterator<Document> getMongoCursor(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(null, query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(null, query, options, user);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeInnerProjections(qOptions, QueryParams.SECONDARY_INTERPRETATIONS.key());

        logger.debug("Clinical analysis query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return clinicalCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedClinicalCollection.iterator(clientSession, bson, null, null, qOptions);
        }
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(clinicalCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(clinicalCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(clinicalCollection, bsonQuery, fields, SampleDBAdaptor.QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<ClinicalAnalysis> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> clinicalAnalysis, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(clinicalAnalysis, "clinicalAnalysis");
        return new OpenCGAResult(clinicalCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, ClinicalAnalysis clinicalAnalysis, QueryOptions options) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), clinicalAnalysis.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = clinicalCollection.count(bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Cannot create clinical analysis. A clinical analysis with { id: '"
                    + clinicalAnalysis.getId() + "'} already exists.");
        }

        long clinicalAnalysisId = getNewUid();
        clinicalAnalysis.setUid(clinicalAnalysisId);
        clinicalAnalysis.setStudyUid(studyId);
        if (StringUtils.isEmpty(clinicalAnalysis.getUuid())) {
            clinicalAnalysis.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL));
        }

        Document clinicalObject = clinicalConverter.convertToStorageType(clinicalAnalysis);
        if (StringUtils.isNotEmpty(clinicalAnalysis.getCreationDate())) {
            clinicalObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(clinicalAnalysis.getCreationDate()));
        } else {
            clinicalObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        clinicalObject.put(PRIVATE_MODIFICATION_DATE, clinicalObject.get(PRIVATE_CREATION_DATE));
        clinicalObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        return new OpenCGAResult(clinicalCollection.insert(clinicalObject, null));
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> get(long clinicalAnalysisUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(clinicalAnalysisUid);
        return get(new Query(QueryParams.UID.key(), clinicalAnalysisUid)
                .append(QueryParams.STUDY_UID.key(), getStudyId(clinicalAnalysisUid)), options);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> get(long studyUid, String clinicalAnalysisId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, studyUid, clinicalAnalysisId, options);
    }

    public OpenCGAResult<ClinicalAnalysis> get(ClientSession clientSession, long studyUid, String clinicalAnalysisId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.ID.key(), clinicalAnalysisId);
        return get(clientSession, query, options);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<ClinicalAnalysis> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public long getStudyId(long clinicalAnalysisId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, clinicalAnalysisId);
        Bson projection = Projections.include(PRIVATE_STUDY_UID);
        DataResult<Document> queryResult = clinicalCollection.find(query, projection, null);

        if (!queryResult.getResults().isEmpty()) {
            Object studyId = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("ClinicalAnalysis", clinicalAnalysisId);
        }
    }

    protected Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, null);
    }

    private Bson parseQuery(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, null);
    }

    private Bson parseQuery(Query query, Document extraQuery, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        if (queryCopy.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || queryCopy.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, queryCopy.getLong(QueryParams.STUDY_UID.key()));

            // Get the document query needed to check the permissions as well
            andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                    ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name(), Enums.Resource.CLINICAL_ANALYSIS));

            andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, queryCopy, Enums.Resource.CLINICAL_ANALYSIS, user));

            queryCopy.remove(ParamConstants.ACL_PARAM);
        }

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
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
                    case DISORDER:
                        addOntologyQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(ClinicalAnalysisStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ANALYST:
                    case ANALYST_ASSIGNEE:
                        addAutoOrQuery(ANALYST_ASSIGNEE.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
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
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_DATE:
                    case FLAGS:
                    case ACL:
                    case ACL_MEMBER:
                    case ACL_PERMISSIONS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (extraQuery != null && !extraQuery.isEmpty()) {
            andBsonList.add(extraQuery);
        }
        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
