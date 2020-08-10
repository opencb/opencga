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
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.InterpretationConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.InterpretationStatus;
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
    public OpenCGAResult insert(long studyId, Interpretation interpretation, ParamUtils.SaveInterpretationAs action)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting interpretation insert transaction for interpretation id '{}'", interpretation.getId());
            Interpretation interpretation1 = insert(clientSession, studyId, interpretation);
            updateClinicalAnalysisReferences(clientSession, interpretation1, action);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create interpretation {}: {}", interpretation.getId(), e.getMessage()));
    }

    private void updateClinicalAnalysisReferences(ClientSession clientSession, Interpretation interpretation,
                                                  ParamUtils.SaveInterpretationAs action)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (action == null) {
            throw new CatalogParameterException("Missing enum to decide how to store the interpretation");
        }

        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), interpretation.getClinicalAnalysisId())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), interpretation.getStudyUid());

        OpenCGAResult<ClinicalAnalysis> clinicalAnalysisOpenCGAResult = clinicalDBAdaptor.get(clientSession, query,
                ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS);
        if (clinicalAnalysisOpenCGAResult.getNumResults() != 1) {
            throw new CatalogDBException("ClinicalAnalysis '" + interpretation.getClinicalAnalysisId() + "' not found.");
        }
        ClinicalAnalysis ca = clinicalAnalysisOpenCGAResult.first();

        switch (action) {
            case PRIMARY:
                if (ca.getInterpretation() != null && StringUtils.isNotEmpty(ca.getInterpretation().getId())) {
                    throw new CatalogDBException("Found primary interpretation '" + ca.getInterpretation().getId()
                            + "' for clinical analysis.");
                }

                // Set interpretation in ClinicalAnalysis
                ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), interpretation);

                QueryOptions options = new QueryOptions();
                for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                    if (secondaryInterpretation.getUid() == interpretation.getUid()) {
                        // Remove interpretation from secondary interpretations if it was there
                        params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                                Collections.singletonList(interpretation));
                        ObjectMap updateAction = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                                ParamUtils.UpdateAction.REMOVE);
                        options.put(Constants.ACTIONS, updateAction);

                        break;
                    }
                }

                clinicalDBAdaptor.update(clientSession, ca, params, options);
                break;
            case PRIMARY_OVERWRITE:
                if (ca.getInterpretation() != null && StringUtils.isNotEmpty(ca.getInterpretation().getId())) {
                    // Delete interpretation
                    delete(clientSession, ca.getInterpretation(), ca);
                }

                // Set interpretation in ClinicalAnalysis
                params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), interpretation);

                // Remove interpretation from secondary interpretations if it was there
                options = new QueryOptions();
                for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                    if (secondaryInterpretation.getUid() == interpretation.getUid()) {
                        params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                                Collections.singletonList(interpretation));
                        ObjectMap updateAction = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                                ParamUtils.UpdateAction.REMOVE);
                        options = new QueryOptions(Constants.ACTIONS, updateAction);
                    }
                }

                clinicalDBAdaptor.update(clientSession, ca, params, options);
                break;
            case PRIMARY_OVERWRITE_AND_SAVE:
                // Set interpretation in ClinicalAnalysis
                params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), interpretation);

                boolean isSecondary = false; // New primary interpretation is already in the array of secondary interpretations?
                boolean existsPrimary = false; // Is there any primary interpretation to be moved to the array of secondary interpretations?

                if (ca.getInterpretation() != null && StringUtils.isNotEmpty(ca.getInterpretation().getId())) {
                    existsPrimary = true;
                }
                for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                    if (secondaryInterpretation.getUid() == interpretation.getUid()) {
                        isSecondary = true;
                        break;
                    }
                }

                options = new QueryOptions();
                if (existsPrimary && isSecondary) {
                    List<Interpretation> interpretationList = new ArrayList<>(ca.getSecondaryInterpretations().size());

                    for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                        if (secondaryInterpretation.getUid() != interpretation.getUid()) {
                            interpretationList.add(secondaryInterpretation);
                        }
                    }
                    // Add current primary interpretations to list
                    interpretationList.add(ca.getInterpretation());

                    ObjectMap actions = new ObjectMap();
                    actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.UpdateAction.SET);
                    options.put(Constants.ACTIONS, actions);

                    // Set array of secondary interpretations
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), interpretationList);
                } else if (existsPrimary) {
                    ObjectMap actions = new ObjectMap();
                    actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.UpdateAction.ADD);
                    options.put(Constants.ACTIONS, actions);

                    // Move current primary interpretation to secondary interpretations
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                            Collections.singletonList(ca.getInterpretation()));
                } else if (isSecondary) {
                    ObjectMap actions = new ObjectMap();
                    actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.UpdateAction.REMOVE);
                    options.put(Constants.ACTIONS, actions);

                    // Remove current interpretation from array of secondary interpretations
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                            Collections.singletonList(interpretation));
                }

                // Update interpretation(s) in ClinicalAnalysis
                clinicalDBAdaptor.update(clientSession, ca, params, options);
                break;
            case SECONDARY:
                // Add to secondaryInterpretations array in ClinicalAnalysis
                params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                        Collections.singletonList(interpretation));
                ObjectMap actions = new ObjectMap();
                actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.UpdateAction.ADD);
                options = new QueryOptions(Constants.ACTIONS, actions);

                if (ca.getInterpretation() != null && ca.getInterpretation().getUid() == interpretation.getUid()) {
                    // New secondary interpretation is currently the primary one so we need to remove it
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), null);
                }

                clinicalDBAdaptor.update(clientSession, ca, params, options);
                break;
            default:
                throw new IllegalStateException("Unknown action " + action);
        }
    }

    Interpretation insert(ClientSession clientSession, long studyId, Interpretation interpretation)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), interpretation.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));
        filterList.add(Filters.eq(QueryParams.INTERNAL_STATUS.key(), Status.READY));

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

        return interpretation;
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
        return count(null, query);
    }

    public OpenCGAResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        logger.debug("Interpretation count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(interpretationCollection.count(clientSession, bson));
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
        return get(null, query, options);
    }

    public OpenCGAResult<Interpretation> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Interpretation> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Interpretation> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(long studyUid, Query query, QueryOptions options, String user) throws CatalogDBException {
        return nativeGet(query, options);
    }

    private UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query,
                                                        QueryOptions queryOptions) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Query tmpQuery = new Query(query);

            OpenCGAResult<Interpretation> interpretationDataResult = get(clientSession, tmpQuery, new QueryOptions());
            if (interpretationDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update interpretation: No interpretation found to be updated");
            }
            if (interpretationDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update interpretation: Cannot set the same id parameter for different interpretations");
            }

            if (!interpretationDataResult.first().getId().equals(parameters.getString(QueryParams.ID.key()))) {
                // Check that the new clinical analysis id will be unique
                long studyId = getStudyId(interpretationDataResult.first().getUid());

                tmpQuery = new Query()
                        .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                        .append(QueryParams.STUDY_UID.key(), studyId);
                OpenCGAResult<Long> count = count(clientSession, tmpQuery);
                if (count.getNumMatches() > 0) {
                    throw new CatalogDBException("Cannot set id for interpretation. A interpretation with { id: '"
                            + parameters.get(QueryParams.ID.key()) + "'} already exists.");
                }

                document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
            }
        }

        String[] acceptedParams = {QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_NAME.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_NAME.key(), parameters.get(QueryParams.INTERNAL_STATUS_NAME.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] objectAcceptedParams = {QueryParams.ANALYST.key(), QueryParams.METHODS.key()};
        filterObjectParams(parameters, document.getSet(), objectAcceptedParams);

        objectAcceptedParams = new String[]{QueryParams.COMMENTS.key()};
        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        ParamUtils.UpdateAction operation = ParamUtils.UpdateAction.from(actionMap, QueryParams.COMMENTS.key(),
                ParamUtils.UpdateAction.ADD);
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
                throw new IllegalStateException("Unknown operation " + operation);
        }

        objectAcceptedParams = new String[]{QueryParams.PRIMARY_FINDINGS.key()};
        operation = ParamUtils.UpdateAction.from(actionMap, QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
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
                throw new IllegalStateException("Unknown operation " + operation);
        }

        objectAcceptedParams = new String[]{QueryParams.SECONDARY_FINDINGS.key()};
        operation = ParamUtils.UpdateAction.from(actionMap, QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
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
                throw new IllegalStateException("Unknown operation " + operation);
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
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(id, parameters, null, queryOptions);
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, ParamUtils.SaveInterpretationAs action, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), uid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key(),
                        QueryParams.CLINICAL_ANALYSIS_ID.key()));
        OpenCGAResult<Interpretation> interpretation = get(query, options);
        if (interpretation.getNumResults() == 0) {
            throw new CatalogDBException("Could not update interpretation. Interpretation uid '" + uid + "' not found.");
        }
        String interpretationId = interpretation.first().getId();

        try {
            return runTransaction(clientSession -> update(clientSession, interpretation.first(), parameters, action, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update interpretation {}: {}", interpretationId, e.getMessage(), e);
            throw new CatalogDBException("Could not update interpretation " + interpretationId + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    OpenCGAResult<Interpretation> update(ClientSession clientSession, Interpretation interpretation, ObjectMap parameters,
                                         ParamUtils.SaveInterpretationAs action, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        long interpretationUid = interpretation.getUid();
        long studyUid = interpretation.getStudyUid();

        Query query = new Query(QueryParams.UID.key(), interpretationUid);

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(clientSession, studyUid, interpretationUid);
        }

        UpdateDocument updateDocument = parseAndValidateUpdateParams(clientSession, parameters, query, queryOptions);
        Document updateOperation = updateDocument.toFinalUpdateDocument();

        if (!updateOperation.isEmpty() || action != null) {
            if (action != null) {
                // Move interpretation
                updateClinicalAnalysisReferences(clientSession, interpretation, action);
            }

            if (!updateOperation.isEmpty()) {
                Bson bsonQuery = Filters.eq(PRIVATE_UID, interpretationUid);
                logger.debug("Update interpretation. Query: {}, Update: {}", bsonQuery.toBsonDocument(Document.class,
                        MongoClient.getDefaultCodecRegistry()), updateDocument);
                DataResult update = interpretationCollection.update(bsonQuery, updateOperation, null);

                if (update.getNumMatches() == 0) {
                    throw CatalogDBException.uidNotFound("Interpretation", interpretationUid);
                }

                return endWrite(tmpStartTime, update);
            }

            return endWrite(tmpStartTime, 1, 1, Collections.emptyList());
        }

        return OpenCGAResult.empty(Interpretation.class);
    }

    private void createNewVersion(ClientSession clientSession, long studyUid, long interpretationUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), interpretationUid);
        OpenCGAResult<Document> queryResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find interpretation '" + interpretationUid + "'");
        }

        createNewVersion(clientSession, interpretationCollection, queryResult.first());
    }

    @Override
    public OpenCGAResult delete(Interpretation interpretation) throws CatalogDBException {
        String interpretationId = interpretation.getId();
        String clinicalId = interpretation.getClinicalAnalysisId();
        try {
            return runTransaction(clientSession -> {
                Query clinicalQuery = new Query()
                        .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), interpretation.getStudyUid())
                        .append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), clinicalId);
                OpenCGAResult<ClinicalAnalysis> clinicalResult = clinicalDBAdaptor.get(clientSession, clinicalQuery,
                        ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS);
                if (clinicalResult.getNumResults() != 1) {
                    throw new CatalogDBException("Cannot find clinical analysis '" + clinicalId + "'.");
                }

                return delete(clientSession, interpretation, clinicalResult.first());
            });
        } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
            logger.error("Could not delete interpretation {}: {}", interpretationId, e.getMessage(), e);
            throw new CatalogDBException("Could not delete interpretation " + interpretation.getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        DBIterator<Interpretation> iterator = iterator(query, new QueryOptions());

        OpenCGAResult<Interpretation> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Interpretation interpretation = iterator.next();
            String interpretationId = interpretation.getId();
            String clinicalId = interpretation.getClinicalAnalysisId();
            try {
                result.append(runTransaction(clientSession -> {
                    Query clinicalQuery = new Query()
                            .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), interpretation.getStudyUid())
                            .append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), clinicalId);
                    OpenCGAResult<ClinicalAnalysis> clinicalResult = clinicalDBAdaptor.get(clientSession, clinicalQuery,
                            ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS);
                    if (clinicalResult.getNumResults() != 1) {
                        throw new CatalogDBException("Cannot find clinical analysis '" + clinicalId + "'.");
                    }

                    return delete(clientSession, interpretation, clinicalResult.first());
                }));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete interpretation {}: {}", interpretationId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, interpretationId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult delete(ClientSession clientSession, Interpretation interpretation, ClinicalAnalysis clinicalAnalysis)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        long interpretationUid = interpretation.getUid();
        long studyUid = interpretation.getStudyUid();

        ObjectMap clinicalParams = new ObjectMap();
        QueryOptions clinicalOptions = new QueryOptions();
        if (clinicalAnalysis.getInterpretation() != null && clinicalAnalysis.getInterpretation().getId().equals(interpretation.getId())) {
            // Empty primary interpretation
            clinicalParams.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), null);
        } else {
            // Remove from secondary interpretations
            clinicalParams.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                    Collections.singletonList(interpretation));
            ObjectMap actions = new ObjectMap();
            actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.UpdateAction.REMOVE);
            clinicalOptions.put(Constants.ACTIONS, actions);
        }
        clinicalDBAdaptor.update(clientSession, clinicalAnalysis, clinicalParams, clinicalOptions);

        // Obtain the native document to be deleted
        Query query = new Query()
                .append(QueryParams.UID.key(), interpretationUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);

        Document interpretationDocument = nativeGet(clientSession, query, QueryOptions.empty()).first();

        // Set status
        nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new InterpretationStatus(InterpretationStatus.DELETED), "status"),
                interpretationDocument);

        // Insert the document in the DELETE collection
        deletedInterpretationCollection.insert(clientSession, interpretationDocument, null);
        logger.debug("Inserted interpretation uid '{}' in DELETE collection", interpretation.getUid());

        // Remove the document from the main INTERPRETATION collection
        Bson bsonQuery = parseQuery(new Query(QueryParams.UID.key(), interpretation.getUid()));
        DataResult remove = interpretationCollection.remove(clientSession, bsonQuery, null);
        if (remove.getNumMatches() == 0) {
            throw new CatalogDBException("Interpretation " + interpretation.getUid() + " not found");
        }
        if (remove.getNumDeleted() == 0) {
            throw new CatalogDBException("Interpretation " + interpretation.getUid() + " could not be deleted");
        }

        logger.debug("Interpretation '{}({})' deleted from main INTERPRETATION collection", interpretation.getId(),
                interpretation.getUid());

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
    public DBIterator<Interpretation> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    public DBIterator<Interpretation> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, interpretationConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    public DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
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
        return getMongoCursor(null, query, options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        logger.debug("Interpretation query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return interpretationCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedInterpretationCollection.iterator(clientSession, bson, null, null, qOptions);
        }
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

        Query queryCopy = new Query(query);
        queryCopy.remove(SampleDBAdaptor.QueryParams.DELETED.key());
        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);

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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(Enums.ExecutionStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_NAME.key(), queryParam.key(), queryCopy,
                                QueryParams.INTERNAL_STATUS_NAME.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case CLINICAL_ANALYSIS_ID:
                    case DESCRIPTION:
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

        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
