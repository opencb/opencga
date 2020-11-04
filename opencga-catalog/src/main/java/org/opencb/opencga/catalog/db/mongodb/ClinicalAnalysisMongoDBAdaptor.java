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
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ClinicalAnalysisCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.common.FlagValue;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.*;
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
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(clinicalCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query, user);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Clinical count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(clinicalCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing ClinicalAudit parameter");
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing ClinicalAudit parameter");
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), uid);
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()))
                .append(NATIVE_QUERY, true);
        OpenCGAResult<ClinicalAnalysis> result = get(query, options);
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Could not update clinical analysis. Clinical Analysis uid '" + uid + "' not found.");
        }
        String clinicalAnalysisId = result.first().getId();

        try {
            return runTransaction(clientSession -> update(clientSession, result.first(), parameters, clinicalAuditList, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update clinical analysis {}: {}", clinicalAnalysisId, e.getMessage(), e);
            throw new CatalogDBException("Could not update clinical analysis " + clinicalAnalysisId + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException {
        return null;
    }

    OpenCGAResult update(ClientSession clientSession, ClinicalAnalysis clinical, ObjectMap parameters,
                         List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        String clinicalAnalysisId = clinical.getId();
        long clinicalAnalysisUid = clinical.getUid();

        Query query = new Query(QueryParams.UID.key(), clinicalAnalysisUid);
        UpdateDocument updateDocument = parseAndValidateUpdateParams(parameters, clinicalAuditList, query, queryOptions);

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

    UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, Query query,
                                                QueryOptions queryOptions)
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

        String[] acceptedBooleanParams = {LOCKED.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        String[] acceptedParams = {QueryParams.DESCRIPTION.key(), QueryParams.DUE_DATE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        String[] acceptedObjectParams = {QueryParams.FAMILY.key(), QueryParams.DISORDER.key(), QUALITY_CONTROL.key(),
                QueryParams.PROBAND.key(), QueryParams.ALERTS.key(), QueryParams.INTERNAL_STATUS.key(), QueryParams.PRIORITY.key(),
                QueryParams.ANALYST.key(), QueryParams.CONSENT.key(), QueryParams.STATUS.key(), QueryParams.INTERPRETATION.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (parameters.containsKey(INTERPRETATION.key()) && parameters.get(INTERPRETATION.key()) == null) {
            // User wants to remove current interpretation
            document.getSet().put(INTERPRETATION.key(), null);
        }

        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        clinicalConverter.validateInterpretationToUpdate(document.getSet());
        clinicalConverter.validateFamilyToUpdate(document.getSet());
        clinicalConverter.validateProbandToUpdate(document.getSet());

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());

        String[] objectAcceptedParams = new String[]{QueryParams.COMMENTS.key()};
        ParamUtils.BasicUpdateAction basicOperation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.COMMENTS.key(),
                ParamUtils.BasicUpdateAction.ADD);
        switch (basicOperation) {
            case REMOVE:
                fixCommentsForRemoval(parameters);
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + basicOperation);
        }

        objectAcceptedParams = new String[]{QueryParams.FILES.key()};
        ParamUtils.UpdateAction operation = ParamUtils.UpdateAction.from(actionMap, QueryParams.FILES.key(), ParamUtils.UpdateAction.ADD);
        switch (operation) {
            case SET:
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case REMOVE:
                filterObjectParams(parameters, document.getPullAll(), objectAcceptedParams);
                break;
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + basicOperation);
        }

        objectAcceptedParams = new String[]{FLAGS.key()};
        operation = ParamUtils.UpdateAction.from(actionMap, QueryParams.FLAGS.key(), ParamUtils.UpdateAction.ADD);
        switch (operation) {
            case SET:
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case REMOVE:
                fixFlagsForRemoval(parameters);
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + basicOperation);
        }

        // Secondary interpretations
        if (parameters.containsKey(QueryParams.SECONDARY_INTERPRETATIONS.key())) {
            operation = ParamUtils.UpdateAction.from(actionMap, QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.UpdateAction.ADD);
            String[] secondaryInterpretationParams = {QueryParams.SECONDARY_INTERPRETATIONS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), secondaryInterpretationParams);
                    clinicalConverter.validateSecondaryInterpretationsToUpdate(document.getSet());
                    break;
                case REMOVE:
                    filterObjectParams(parameters, document.getPullAll(), secondaryInterpretationParams);
                    clinicalConverter.validateSecondaryInterpretationsToUpdate(document.getPullAll());
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), secondaryInterpretationParams);
                    clinicalConverter.validateSecondaryInterpretationsToUpdate(document.getAddToSet());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        if (clinicalAuditList != null && !clinicalAuditList.isEmpty()) {
            List<Document> documentAuditList = new ArrayList<>(clinicalAuditList.size());
            for (ClinicalAudit clinicalAudit : clinicalAuditList) {
                documentAuditList.add(getMongoDBDocument(clinicalAudit, "ClinicalAudit"));
            }
            document.getPush().put(AUDIT.key(), documentAuditList);
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

    static void fixCommentsForRemoval(ObjectMap parameters) {
        if (parameters.get(COMMENTS.key()) == null) {
            return;
        }

        List<ClinicalCommentParam> commentParamList = new LinkedList<>();
        for (Object comment : parameters.getAsList(COMMENTS.key())) {
            if (comment instanceof ClinicalComment) {
                commentParamList.add(ClinicalCommentParam.of((ClinicalComment) comment));
            }
        }
        parameters.put(COMMENTS.key(), commentParamList);
    }

    static void fixFlagsForRemoval(ObjectMap parameters) {
        if (parameters.get(FLAGS.key()) == null) {
            return;
        }

        List<FlagValueParam> flagParamList = new LinkedList<>();
        for (Object comment : parameters.getAsList(FLAGS.key())) {
            if (comment instanceof FlagAnnotation) {
                flagParamList.add(FlagValueParam.of((FlagAnnotation) comment));
            }
        }
        parameters.put(FLAGS.key(), flagParamList);
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(clinicalCollection, studyId, permissionRuleId);
    }

    @Override
    public OpenCGAResult<?> delete(ClinicalAnalysis clinicalAnalysis, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateDelete(clientSession, clinicalAnalysis, clinicalAuditList));
        } catch (CatalogDBException e) {
            logger.error("Could not delete Clinical Analysis {}: {}", clinicalAnalysis.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete Clinical Analysis " + clinicalAnalysis.getId() + ": " + e.getMessage(),
                    e.getCause());
        }
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> delete(Query query, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<ClinicalAnalysis> iterator = iterator(query, ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATION_IDS);

        OpenCGAResult<ClinicalAnalysis> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();

            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, clinicalAnalysis, clinicalAuditList)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete Clinical Analysis {}: {}", clinicalAnalysis.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, clinicalAnalysis.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    @Override
    public OpenCGAResult delete(ClinicalAnalysis clinicalAnalysis)
            throws CatalogParameterException, CatalogAuthorizationException, CatalogDBException {
        throw new NotImplementedException("Use other delete method passing ClinicalAudit");
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other delete method passing ClinicalAudit");
    }

    OpenCGAResult<?> privateDelete(ClientSession clientSession, ClinicalAnalysis clinicalAnalysis, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        // Check and delete any associated interpretation
        if (clinicalAnalysis.getInterpretation() != null && clinicalAnalysis.getInterpretation().getUid() > 0) {
            Query query = new Query()
                    .append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), clinicalAnalysis.getStudyUid())
                    .append(InterpretationDBAdaptor.QueryParams.UID.key(), clinicalAnalysis.getInterpretation().getUid());

            OpenCGAResult<Interpretation> result = dbAdaptorFactory.getInterpretationDBAdaptor().get(clientSession, query,
                    QueryOptions.empty());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Internal error: Interpretation '" + clinicalAnalysis.getInterpretation().getId()
                        + "' not found.");
            }

            ClinicalAudit clinicalAudit = new ClinicalAudit(clinicalAuditList.get(0).getAuthor(),
                    ClinicalAudit.Action.DELETE_INTERPRETATION, "Delete interpretation '" + result.first().getId() + "'",
                    TimeUtils.getTime());

            // Delete primary interpretation
            dbAdaptorFactory.getInterpretationDBAdaptor().delete(clientSession, result.first(), Collections.singletonList(clinicalAudit),
                    clinicalAnalysis);
        }

        if (clinicalAnalysis.getSecondaryInterpretations() != null && !clinicalAnalysis.getSecondaryInterpretations().isEmpty()) {
            for (Interpretation interpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                Query query = new Query()
                        .append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), clinicalAnalysis.getStudyUid())
                        .append(InterpretationDBAdaptor.QueryParams.UID.key(), interpretation.getUid());
                OpenCGAResult<Interpretation> result = dbAdaptorFactory.getInterpretationDBAdaptor().get(clientSession, query,
                        QueryOptions.empty());
                if (result.getNumResults() == 0) {
                    throw new CatalogDBException("Internal error: Interpretation '" + interpretation.getId() + "' not found.");
                }

                ClinicalAudit clinicalAudit = new ClinicalAudit(clinicalAuditList.get(0).getAuthor(),
                        ClinicalAudit.Action.DELETE_INTERPRETATION, "Delete interpretation '" + result.first().getId() + "'",
                        TimeUtils.getTime());

                // Delete secondary interpretation
                dbAdaptorFactory.getInterpretationDBAdaptor().delete(clientSession, result.first(),
                        Collections.singletonList(clinicalAudit), clinicalAnalysis);
            }
        }

        Query query = new Query()
                .append(STUDY_UID.key(), clinicalAnalysis.getStudyUid())
                .append(UID.key(), clinicalAnalysis.getUid());
        OpenCGAResult<Document> result = nativeGet(clientSession, query, QueryOptions.empty());
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Internal error: Clinical Analysis '" + clinicalAnalysis.getId() + "' not found.");
        }

        String clinicalId = result.first().getString(QueryParams.ID.key());
        long clinicalUid = result.first().getLong(PRIVATE_UID);
        long studyUid = result.first().getLong(PRIVATE_STUDY_UID);

        logger.debug("Deleting Clinical Analysis {} ({})", clinicalId, clinicalUid);

        // Delete any documents that might have been already deleted with that id
        Bson bsonQuery = new Document()
                .append(QueryParams.ID.key(), clinicalId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedClinicalCollection.remove(clientSession, bsonQuery, new QueryOptions(MongoDBCollection.MULTI, true));

        // Set status to DELETED
        nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new Status(Status.DELETED), "status"), result.first());

        // Add audit
        List<Document> auditList = result.first().getList(AUDIT.key(), Document.class);
        for (ClinicalAudit clinicalAudit : clinicalAuditList) {
            auditList.add(getMongoDBDocument(clinicalAudit, "ClinicalAudit"));
        }
        result.first().put(AUDIT.key(), auditList);

        // Insert the document in the DELETE collection
        deletedClinicalCollection.insert(clientSession, result.first(), null);
        logger.debug("Inserted Clinical Analysis uid '{}' in DELETE collection", clinicalUid);

        // Remove the document from the main Clinical collection
        bsonQuery = parseQuery(new Query(QueryParams.UID.key(), clinicalUid));
        DataResult<?> remove = clinicalCollection.remove(clientSession, bsonQuery, null);
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
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    private OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(long studyUid, Query query, QueryOptions options, String user)
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
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    private DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new ClinicalAnalysisCatalogMongoDBIterator(mongoCursor, null, dbAdaptorFactory, queryOptions);
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

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
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
        qOptions = removeInnerProjections(qOptions, PROBAND.key());
        qOptions = removeInnerProjections(qOptions, FAMILY.key());
        qOptions = removeInnerProjections(qOptions, QueryParams.INTERPRETATION.key());
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
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult<>(clinicalCollection.distinct(field, bson, clazz));
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
    public OpenCGAResult insert(long studyId, ClinicalAnalysis clinicalAnalysis, List<ClinicalAudit> clinicalAuditList,
                                QueryOptions options) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting ClinicalAnalysis insert transaction for ClinicalAnalysis id '{}'", clinicalAnalysis.getId());
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
                insert(clientSession, studyId, clinicalAnalysis, clinicalAuditList);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create ClinicalAnalysis {}: {}", clinicalAnalysis.getId(), e.getMessage(), e);
            throw e;
        }
    }

    ClinicalAnalysis insert(ClientSession clientSession, long studyId, ClinicalAnalysis clinicalAnalysis, List<ClinicalAudit> clinicalAudit)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (clinicalAnalysis.getInterpretation() != null) {
            InterpretationMongoDBAdaptor interpretationDBAdaptor = dbAdaptorFactory.getInterpretationDBAdaptor();
            Interpretation interpretation = interpretationDBAdaptor.insert(clientSession, studyId, clinicalAnalysis.getInterpretation());
            clinicalAnalysis.setInterpretation(interpretation);
        }

        if (StringUtils.isEmpty(clinicalAnalysis.getId())) {
            throw new CatalogDBException("Missing ClinicalAnalysis id");
        }
        if (!get(clientSession, new Query(ID.key(), clinicalAnalysis.getId())
                .append(STUDY_UID.key(), studyId), new QueryOptions()).getResults().isEmpty()) {
            throw CatalogDBException.alreadyExists("ClinicalAnalysis", "id", clinicalAnalysis.getId());
        }

        long clinicalUid = getNewUid();

        clinicalAnalysis.setAudit(clinicalAudit);

        clinicalAnalysis.setUid(clinicalUid);
        clinicalAnalysis.setStudyUid(studyId);
        if (StringUtils.isEmpty(clinicalAnalysis.getUuid())) {
            clinicalAnalysis.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL));
        }
        if (StringUtils.isEmpty(clinicalAnalysis.getCreationDate())) {
            clinicalAnalysis.setCreationDate(TimeUtils.getTime());
        }

        Document clinicalDocument = clinicalConverter.convertToStorageType(clinicalAnalysis);
        if (StringUtils.isNotEmpty(clinicalAnalysis.getCreationDate())) {
            clinicalDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(clinicalAnalysis.getCreationDate()));
        } else {
            clinicalDocument.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        clinicalDocument.put(PRIVATE_MODIFICATION_DATE, clinicalDocument.get(PRIVATE_CREATION_DATE));
        clinicalDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting ClinicalAnalysis '{}' ({})...", clinicalAnalysis.getId(), clinicalAnalysis.getUid());
        clinicalCollection.insert(clientSession, clinicalDocument, null);
        logger.debug("ClinicalAnalysis '{}' successfully inserted", clinicalAnalysis.getId());

        return clinicalAnalysis;
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
                    case INDIVIDUAL:
                        List<Bson> queryList = new ArrayList<>();
                        addAutoOrQuery(PROBAND_UID.key(), queryParam.key(), queryCopy, PROBAND_UID.type(), queryList);
                        addAutoOrQuery(FAMILY_MEMBERS_UID.key(), queryParam.key(), queryCopy, FAMILY_MEMBERS_UID.type(), queryList);
                        andBsonList.add(Filters.or(queryList));
                        break;
                    case SAMPLE:
                        queryList = new ArrayList<>();
                        addAutoOrQuery(PROBAND_SAMPLES_UID.key(), queryParam.key(), queryCopy, PROBAND_SAMPLES_UID.type(), queryList);
                        addAutoOrQuery(FAMILY_MEMBERS_SAMPLES_UID.key(), queryParam.key(), queryCopy, FAMILY_MEMBERS_SAMPLES_UID.type(),
                                queryList);
                        andBsonList.add(Filters.or(queryList));
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(STATUS_ID.key(), queryParam.key(), queryCopy, STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(ClinicalAnalysisStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(INTERNAL_STATUS_NAME.key(), queryParam.key(), queryCopy, INTERNAL_STATUS_NAME.type(), andBsonList);
                        break;
                    case FAMILY:
                    case FAMILY_UID:
                        addAutoOrQuery(FAMILY_UID.key(), queryParam.key(), queryCopy, FAMILY_UID.type(), andBsonList);
                        break;
                    case PROBAND:
                    case PROBAND_ID:
                        addAutoOrQuery(PROBAND_ID.key(), queryParam.key(), queryCopy, PROBAND_ID.type(), andBsonList);
                        break;
                    case ANALYST:
                    case ANALYST_ID:
                        addAutoOrQuery(ANALYST_ID.key(), queryParam.key(), queryCopy, ANALYST_ID.type(), andBsonList);
                        break;
                    case FLAGS:
                    case FLAGS_ID:
                        addAutoOrQuery(FLAGS_ID.key(), queryParam.key(), queryCopy, FLAGS_ID.type(), andBsonList);
                        break;
                    case PRIORITY:
                    case PRIORITY_ID:
                        addAutoOrQuery(PRIORITY_ID.key(), queryParam.key(), queryCopy, PRIORITY_ID.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case TYPE:
                    case DUE_DATE:
                    case LOCKED:
                    case PROBAND_SAMPLES_ID:
                    case PROBAND_SAMPLES_UID:
                    case PROBAND_UID:
                    case DESCRIPTION:
                    case RELEASE:
                    case INTERNAL_STATUS_DATE:
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
