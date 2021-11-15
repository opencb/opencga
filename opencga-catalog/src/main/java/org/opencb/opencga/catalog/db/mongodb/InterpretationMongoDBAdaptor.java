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
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.InterpretationConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.InterpretationCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.InterpretationUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.InterpretationStats;
import org.opencb.opencga.core.models.clinical.InterpretationStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor.QueryParams.STATUS_ID;
import static org.opencb.opencga.catalog.db.mongodb.ClinicalAnalysisMongoDBAdaptor.fixCommentsForRemoval;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class InterpretationMongoDBAdaptor extends MongoDBAdaptor implements InterpretationDBAdaptor {

    private final MongoDBCollection interpretationCollection;
    private final MongoDBCollection deletedInterpretationCollection;
    private final ClinicalAnalysisMongoDBAdaptor clinicalDBAdaptor;
    private InterpretationConverter interpretationConverter;

    public InterpretationMongoDBAdaptor(MongoDBCollection interpretationCollection, MongoDBCollection deletedInterpretationCollection,
                                        Configuration configuration, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(InterpretationMongoDBAdaptor.class));
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
    public OpenCGAResult insert(long studyId, Interpretation interpretation, ParamUtils.SaveInterpretationAs action,
                                List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting interpretation insert transaction for interpretation id '{}'", interpretation.getId());
            Interpretation interpretation1 = insert(clientSession, studyId, interpretation);
            updateClinicalAnalysisReferences(clientSession, interpretation1, action, clinicalAuditList);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create interpretation {}: {}", interpretation.getId(), e.getMessage()));
    }

    private void updateClinicalAnalysisReferences(ClientSession clientSession, Interpretation interpretation,
                                                  ParamUtils.SaveInterpretationAs action, List<ClinicalAudit> clinicalAuditList)
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
                // Set interpretation in ClinicalAnalysis
                ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), interpretation);

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

                QueryOptions options = new QueryOptions();
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
                    actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.BasicUpdateAction.SET);
                    options.put(Constants.ACTIONS, actions);

                    // Set array of secondary interpretations
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), interpretationList);
                } else if (existsPrimary) {
                    ObjectMap actions = new ObjectMap();
                    actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.BasicUpdateAction.ADD);
                    options.put(Constants.ACTIONS, actions);

                    // Move current primary interpretation to secondary interpretations
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                            Collections.singletonList(ca.getInterpretation()));
                } else if (isSecondary) {
                    ObjectMap actions = new ObjectMap();
                    actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.BasicUpdateAction.REMOVE);
                    options.put(Constants.ACTIONS, actions);

                    // Remove current interpretation from array of secondary interpretations
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                            Collections.singletonList(interpretation));
                }

                // Update interpretation(s) in ClinicalAnalysis
                clinicalDBAdaptor.update(clientSession, ca, params, clinicalAuditList, options);
                break;
            case SECONDARY:
                // Add to secondaryInterpretations array in ClinicalAnalysis
                params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                        Collections.singletonList(interpretation));
                ObjectMap actions = new ObjectMap();
                actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.BasicUpdateAction.ADD);
                options = new QueryOptions(Constants.ACTIONS, actions);

                if (ca.getInterpretation() != null && ca.getInterpretation().getUid() == interpretation.getUid()) {
                    // New secondary interpretation is currently the primary one so we need to remove it
                    params.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), null);
                }

                clinicalDBAdaptor.update(clientSession, ca, params, clinicalAuditList, options);
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

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = interpretationCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Cannot create interpretation. An interpretation with { id: '"
                    + interpretation.getId() + "'} already exists.");
        }

        long interpretationUid = getNewUid();
        interpretation.setUid(interpretationUid);
        interpretation.setStudyUid(studyId);
        if (StringUtils.isEmpty(interpretation.getUuid())) {
            interpretation.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.INTERPRETATION));
        }

        // Calculate stats
        InterpretationStats interpretationStats = InterpretationUtils.calculateStats(interpretation);
        interpretation.setStats(interpretationStats);

        Document interpretationObject = interpretationConverter.convertToStorageType(interpretation);
        if (StringUtils.isNotEmpty(interpretation.getCreationDate())) {
            interpretationObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(interpretation.getCreationDate()));
        } else {
            interpretationObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        interpretationObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(interpretation.getModificationDate())
                ? TimeUtils.toDate(interpretation.getModificationDate()) : TimeUtils.getDate());

        // Versioning private parameters
        interpretationObject.put(RELEASE_FROM_VERSION, Arrays.asList(interpretation.getRelease()));
        interpretationObject.put(LAST_OF_VERSION, true);
        interpretationObject.put(LAST_OF_RELEASE, true);

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
    public OpenCGAResult updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        return new OpenCGAResult(interpretationCollection.update(bson, update, queryOptions));
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

    @Override
    public OpenCGAResult<Interpretation> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing ClinicalAudit parameter");
    }

    @Override
    public OpenCGAResult<Interpretation> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing ClinicalAudit parameter");
    }

    private UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query,
                                                        QueryOptions queryOptions) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();
        Interpretation interpretation = null;

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Query tmpQuery = new Query(query);
            interpretation = getSingleInterpretation(clientSession, tmpQuery, interpretation);

            if (!interpretation.getId().equals(parameters.getString(QueryParams.ID.key()))) {
                // Check that the new clinical analysis id will be unique
                long studyId = getStudyId(interpretation.getUid());

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

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_NAME.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_NAME.key(), parameters.get(QueryParams.INTERNAL_STATUS_NAME.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] objectAcceptedParams = {QueryParams.ANALYST.key(), QueryParams.STATUS.key(), QueryParams.STATS.key()};
        filterObjectParams(parameters, document.getSet(), objectAcceptedParams);

        objectAcceptedParams = new String[]{QueryParams.COMMENTS.key()};
        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        ParamUtils.AddRemoveReplaceAction commentsOperation = ParamUtils.AddRemoveReplaceAction.from(actionMap, QueryParams.COMMENTS.key(),
                ParamUtils.AddRemoveReplaceAction.ADD);
        switch (commentsOperation) {
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            case REMOVE:
                fixCommentsForRemoval(parameters);
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case REPLACE:
                filterReplaceParams(parameters.getAsList(QueryParams.COMMENTS.key(), ClinicalComment.class), document,
                        ClinicalComment::getDate, QueryParams.COMMENTS_DATE.key());
                break;
            default:
                throw new IllegalStateException("Unknown operation " + commentsOperation);
        }

        objectAcceptedParams = new String[]{QueryParams.METHOD.key()};
        ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.METHOD.key(),
                ParamUtils.BasicUpdateAction.ADD);
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
        ParamUtils.UpdateAction findingsOperation = ParamUtils.UpdateAction.from(actionMap, QueryParams.PRIMARY_FINDINGS.key(),
                ParamUtils.UpdateAction.ADD);
        switch (findingsOperation) {
            case SET:
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case REMOVE:
                fixFindingsForRemoval(parameters, QueryParams.PRIMARY_FINDINGS.key());
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case REPLACE:
                filterReplaceParams(parameters.getAsList(QueryParams.PRIMARY_FINDINGS.key(), Map.class), document,
                        m -> String.valueOf(m.get("id")), QueryParams.PRIMARY_FINDINGS_ID.key());
                break;
            case ADD:
                if (parameters.containsKey(QueryParams.PRIMARY_FINDINGS.key())) {
                    interpretation = getSingleInterpretation(clientSession, query, interpretation);
                    checkNewFindingsDontExist(interpretation.getPrimaryFindings(),
                            parameters.getAsList(QueryParams.PRIMARY_FINDINGS.key(), Map.class));
                }
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + findingsOperation);
        }

        objectAcceptedParams = new String[]{QueryParams.SECONDARY_FINDINGS.key()};
        findingsOperation = ParamUtils.UpdateAction.from(actionMap, QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
        switch (findingsOperation) {
            case SET:
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case REMOVE:
                fixFindingsForRemoval(parameters, QueryParams.SECONDARY_FINDINGS.key());
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case REPLACE:
                filterReplaceParams(parameters.getAsList(QueryParams.SECONDARY_FINDINGS.key(), Map.class), document,
                        m -> String.valueOf(m.get("id")), QueryParams.SECONDARY_FINDINGS_ID.key());
                break;
            case ADD:
                if (parameters.containsKey(QueryParams.SECONDARY_FINDINGS.key())) {
                    interpretation = getSingleInterpretation(clientSession, query, interpretation);
                    checkNewFindingsDontExist(interpretation.getSecondaryFindings(),
                            parameters.getAsList(QueryParams.SECONDARY_FINDINGS.key(), Map.class));
                }
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + findingsOperation);
        }

        // Panels
        if (parameters.containsKey(QueryParams.PANELS.key())) {
            operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.ADD);
            String[] panelParams = {QueryParams.PANELS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), panelParams);
                    interpretationConverter.validatePanelsToUpdate(document.getSet());
                    break;
                case REMOVE:
                    fixPanelsForRemoval(parameters);
                    filterObjectParams(parameters, document.getPullAll(), panelParams);
                    interpretationConverter.validatePanelsToUpdate(document.getPullAll());
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), panelParams);
                    interpretationConverter.validatePanelsToUpdate(document.getAddToSet());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    static void fixPanelsForRemoval(ObjectMap parameters) {
        if (parameters.get(QueryParams.PANELS.key()) == null) {
            return;
        }

        List<Panel> panelParamList = new LinkedList<>();
        for (Object panel : parameters.getAsList(QueryParams.PANELS.key())) {
            if (panel instanceof Panel) {
                panelParamList.add(new Panel().setId(((Panel) panel).getId()));
            }
        }
        parameters.put(QueryParams.PANELS.key(), panelParamList);
    }

    static void fixFindingsForRemoval(ObjectMap parameters, String findingsKey) {
        if (parameters.get(findingsKey) == null) {
            return;
        }

        List<Document> findingsParamList = new LinkedList<>();
        for (Object finding : parameters.getAsList(findingsKey)) {
            if (finding instanceof Map) {
                findingsParamList.add(new Document("id", ((Map) finding).get("id")));
            }
        }
        parameters.put(findingsKey, findingsParamList);
    }

    private void checkNewFindingsDontExist(List<ClinicalVariant> currentFindings, List<Map> newFindings)
            throws CatalogDBException {
        Set<String> currentVariantIds = currentFindings.stream().map(ClinicalVariant::getId).collect(Collectors.toSet());

        for (Map<String, Object> finding : newFindings) {
            if (currentVariantIds.contains(String.valueOf(finding.get("id")))) {
                throw new CatalogDBException("Cannot add new list of findings. Variant '" + finding.get("id")
                        + "' already found in current list of findings.");
            }
        }
    }

    private Interpretation getSingleInterpretation(ClientSession clientSession, Query query, Interpretation interpretation)
            throws CatalogDBException {
        if (interpretation != null) {
            return interpretation;
        }

        Query tmpQuery = new Query(query);
        OpenCGAResult<Interpretation> interpretationDataResult = get(clientSession, tmpQuery, new QueryOptions());
        if (interpretationDataResult.getNumResults() == 0) {
            throw new CatalogDBException("No interpretation found.");
        }
        if (interpretationDataResult.getNumResults() > 1) {
            throw new CatalogDBException("More than one interpretation found");
        }

        return interpretationDataResult.first();
    }

    @Override
    public OpenCGAResult update(long id, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(id, parameters, clinicalAuditList, null, queryOptions);
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList,
                                ParamUtils.SaveInterpretationAs action, QueryOptions queryOptions)
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
            return runTransaction(clientSession -> update(clientSession, interpretation.first(), parameters, clinicalAuditList, action,
                    queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update interpretation {}: {}", interpretationId, e.getMessage(), e);
            throw new CatalogDBException("Could not update interpretation " + interpretationId + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult<Interpretation> revert(long id, int previousVersion, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                Query query = new Query(QueryParams.UID.key(), id);
                OpenCGAResult<Document> latestResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

                if (latestResult.getNumResults() == 0) {
                    throw new CatalogDBException("Could not find latest interpretation '" + id + "'");
                }

                query = new Query()
                        .append(QueryParams.UID.key(), id)
                        .append(QueryParams.VERSION.key(), previousVersion);
                OpenCGAResult<Document> versionResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

                if (versionResult.getNumResults() == 0) {
                    throw new CatalogDBException("Could not find version '" + previousVersion + "' of interpretation '" + id + "'");
                }

                Document latestInterpretation = revertToPreviousVersion(clientSession, interpretationCollection, versionResult.first(),
                        latestResult.first());

                // Update audit list from ClinicalAnalysis
                updateClinicalAnalysisInterpretationReference(clientSession,
                        interpretationConverter.convertToDataModelType(latestInterpretation), clinicalAuditList);

                return OpenCGAResult.empty(Interpretation.class).setNumUpdated(1);
            });
        } catch (CatalogDBException e) {
            logger.error("Could not revert version of interpretation {}: {}", id, e.getMessage(), e);
            CatalogDBException exception = new CatalogDBException("Could not revert version of interpretation");
            exception.addSuppressed(e);
            throw exception;
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException {
        return null;
    }

    OpenCGAResult<Interpretation> update(ClientSession clientSession, Interpretation interpretation, ObjectMap parameters,
                                         List<ClinicalAudit> clinicalAuditList, ParamUtils.SaveInterpretationAs action,
                                         QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        long interpretationUid = interpretation.getUid();
        long studyUid = interpretation.getStudyUid();

        Query query = new Query(QueryParams.UID.key(), interpretationUid);

        UpdateDocument updateDocument = parseAndValidateUpdateParams(clientSession, parameters, query, queryOptions);
        Document updateOperation = updateDocument.toFinalUpdateDocument();

        if (!updateOperation.isEmpty() || !updateDocument.getNestedUpdateList().isEmpty() || action != null) {
            if (action != null) {
                // Move interpretation
                updateClinicalAnalysisReferences(clientSession, interpretation, action, clinicalAuditList);
            }

            if (!updateOperation.isEmpty() || !updateDocument.getNestedUpdateList().isEmpty()) {
                DataResult update = DataResult.empty();

                // Increment interpretation version
                int version = createNewVersion(clientSession, studyUid, interpretationUid);
                interpretation.setVersion(version);
                updateClinicalAnalysisInterpretationReference(clientSession, interpretation, clinicalAuditList);

                if (!updateOperation.isEmpty()) {
                    Bson bsonQuery = parseQuery(new Query(QueryParams.UID.key(), interpretation.getUid()));
                    logger.debug("Update interpretation. Query: {}, Update: {}", bsonQuery.toBsonDocument(Document.class,
                            MongoClient.getDefaultCodecRegistry()), updateDocument);
                    update = interpretationCollection.update(clientSession, bsonQuery, updateOperation, null);

                    if (update.getNumMatches() == 0) {
                        throw CatalogDBException.uidNotFound("Interpretation", interpretationUid);
                    }
                }

                // Added to allow replacing a single comment
                if (!updateDocument.getNestedUpdateList().isEmpty()) {
                    for (NestedArrayUpdateDocument nestedDocument : updateDocument.getNestedUpdateList()) {

                        Bson bsonQuery = parseQuery(nestedDocument.getQuery().append(QueryParams.UID.key(), interpretation.getUid()));
                        logger.debug("Update nested element from interpretation. Query: {}, Update: {}",
                                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), nestedDocument.getSet());

                        update = interpretationCollection.update(clientSession, bsonQuery, nestedDocument.getSet(), null);

                        if (update.getNumMatches() == 0) {
                            throw CatalogDBException.uidNotFound("Interpretation", interpretationUid);
                        }
                    }
                }

                if (!updateOperation.isEmpty() || !updateDocument.getNestedUpdateList().isEmpty()) {
                    // If something was updated, we will calculate the stats of the interpretation again
                    Query iQuery = new Query()
                            .append(QueryParams.UID.key(), interpretationUid)
                            .append(QueryParams.STUDY_UID.key(), studyUid);
                    Interpretation updatedInterpretation = get(clientSession, iQuery, QueryOptions.empty()).first();
                    InterpretationStats stats = InterpretationUtils.calculateStats(updatedInterpretation);

                    Bson bsonQuery = parseQuery(new Query(QueryParams.UID.key(), interpretation.getUid()));
                    UpdateDocument updateStatsDocument = parseAndValidateUpdateParams(clientSession,
                            new ObjectMap(QueryParams.STATS.key(), stats), iQuery, QueryOptions.empty());
                    logger.debug("Update interpretation stats. Query: {}, Update: {}",
                            bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            updateStatsDocument.toFinalUpdateDocument());

                    DataResult statsUpdate = interpretationCollection.update(clientSession, bsonQuery,
                            updateStatsDocument.toFinalUpdateDocument(), null);
                    if (statsUpdate.getNumMatches() == 0) {
                        throw CatalogDBException.uidNotFound("Interpretation", interpretationUid);
                    }
                }

                return endWrite(tmpStartTime, update);
            }

            return endWrite(tmpStartTime, 1, 1, Collections.emptyList());
        }

        return OpenCGAResult.empty(Interpretation.class);
    }

    private void updateClinicalAnalysisInterpretationReference(ClientSession clientSession, Interpretation interpretation,
                                                               List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), interpretation.getClinicalAnalysisId())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), interpretation.getStudyUid());

        OpenCGAResult<ClinicalAnalysis> clinicalAnalysisOpenCGAResult = clinicalDBAdaptor.get(clientSession, query,
                ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS);
        if (clinicalAnalysisOpenCGAResult.getNumResults() != 1) {
            throw new CatalogDBException("ClinicalAnalysis '" + interpretation.getClinicalAnalysisId() + "' not found.");
        }
        ClinicalAnalysis ca = clinicalAnalysisOpenCGAResult.first();

        ObjectMap params;
        QueryOptions options = new QueryOptions();

        if (ca.getInterpretation() != null && ca.getInterpretation().getUid() == interpretation.getUid()) {
            params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), interpretation);
        } else {
            ObjectMap actions = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(),
                    ParamUtils.BasicUpdateAction.SET);
            options.put(Constants.ACTIONS, actions);

            // Interpretation must be one of the secondaries
            List<Interpretation> interpretationList = new ArrayList<>(ca.getSecondaryInterpretations().size());

            for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                if (secondaryInterpretation.getUid() == interpretation.getUid()) {
                    secondaryInterpretation.setVersion(interpretation.getVersion());
                }
                interpretationList.add(secondaryInterpretation);
            }

            params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), interpretationList);
        }

        OpenCGAResult update = clinicalDBAdaptor.update(clientSession, ca, params, clinicalAuditList, options);
        if (update.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not update interpretation reference in Clinical Analysis to new version");
        }
    }

    private int createNewVersion(ClientSession clientSession, long studyUid, long interpretationUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), interpretationUid);
        OpenCGAResult<Document> queryResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find interpretation '" + interpretationUid + "'");
        }

        createNewVersion(clientSession, interpretationCollection, queryResult.first());
        return queryResult.first().getInteger(QueryParams.VERSION.key());
    }

    @Override
    public OpenCGAResult delete(Interpretation interpretation) throws CatalogDBException {
        throw new NotImplementedException("Use other delete method passing a ClinicalAudit object");
    }

    @Override
    public OpenCGAResult delete(Interpretation interpretation, List<ClinicalAudit> clinicalAuditList) throws CatalogDBException {
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

                return delete(clientSession, interpretation, clinicalAuditList, clinicalResult.first());
            });
        } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
            logger.error("Could not delete interpretation {}: {}", interpretationId, e.getMessage(), e);
            throw new CatalogDBException("Could not delete interpretation " + interpretation.getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        throw new NotImplementedException("User other delete method passing a ClinicalAudit parameter");
    }

    @Override
    public OpenCGAResult<Interpretation> delete(Query query, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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

                    return delete(clientSession, interpretation, clinicalAuditList, clinicalResult.first());
                }));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete interpretation {}: {}", interpretationId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, interpretationId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult delete(ClientSession clientSession, Interpretation interpretation, List<ClinicalAudit> clinicalAuditList,
                         ClinicalAnalysis clinicalAnalysis)
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
            actions.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ParamUtils.BasicUpdateAction.REMOVE);
            clinicalOptions.put(Constants.ACTIONS, actions);
        }
        clinicalDBAdaptor.update(clientSession, clinicalAnalysis, clinicalParams, clinicalAuditList, clinicalOptions);

        // Obtain the native document to be deleted
        Query query = new Query()
                .append(QueryParams.ID.key(), interpretation.getId())
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(Constants.ALL_VERSIONS, true);
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, QueryOptions.empty())) {
            // Delete any documents that might have been already deleted with that id
            Bson bsonQuery = new Document()
                    .append(QueryParams.ID.key(), interpretation.getId())
                    .append(PRIVATE_STUDY_UID, studyUid);
            deletedInterpretationCollection.remove(clientSession, bsonQuery, new QueryOptions(MongoDBCollection.MULTI, true));

            while (dbIterator.hasNext()) {
                Document interpretationDocument = dbIterator.next();
                int interpretationVersion = interpretationDocument.getInteger(QueryParams.VERSION.key());

                // Set status
                nestedPut(QueryParams.INTERNAL_STATUS.key(),
                        getMongoDBDocument(new InterpretationStatus(InterpretationStatus.DELETED), "status"), interpretationDocument);

                // Insert the document in the DELETE collection
                deletedInterpretationCollection.insert(clientSession, interpretationDocument, null);
                logger.debug("Inserted interpretation uid '{}' in DELETE collection", interpretation.getUid());

                // Remove the document from the main INTERPRETATION collection
                bsonQuery = parseQuery(new Query()
                        .append(QueryParams.UID.key(), interpretationUid)
                        .append(QueryParams.VERSION.key(), interpretationVersion));
                DataResult remove = interpretationCollection.remove(clientSession, bsonQuery, null);
                if (remove.getNumMatches() == 0) {
                    throw new CatalogDBException("Interpretation " + interpretation.getUid() + " not found");
                }
                if (remove.getNumDeleted() == 0) {
                    throw new CatalogDBException("Interpretation " + interpretation.getUid() + " could not be deleted");
                }
            }
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
        return new InterpretationCatalogMongoDBIterator<>(mongoCursor, interpretationConverter, dbAdaptorFactory, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    public DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new InterpretationCatalogMongoDBIterator(mongoCursor, null, dbAdaptorFactory, queryOptions);
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

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        qOptions = filterQueryOptions(qOptions, Arrays.asList(QueryParams.ID.key(), QueryParams.UUID.key(), QueryParams.UID.key(),
                QueryParams.VERSION.key(), QueryParams.CLINICAL_ANALYSIS_ID.key()));

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
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery);

        return new OpenCGAResult<>(interpretationCollection.distinct(field, bson, clazz));
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

        if ("all".equalsIgnoreCase(queryCopy.getString(QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(QueryParams.VERSION.key());
        }

        boolean uidVersionQueryFlag = generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.ALL_VERSIONS.equals(entry.getKey())) {
                    continue;
                }
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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ANALYST:
                    case ANALYST_ID:
                        addAutoOrQuery(QueryParams.ANALYST_ID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PRIMARY_FINDINGS:
                    case PRIMARY_FINDINGS_ID:
                        addAutoOrQuery(QueryParams.PRIMARY_FINDINGS_ID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case SECONDARY_FINDINGS:
                    case SECONDARY_FINDINGS_ID:
                        addAutoOrQuery(QueryParams.SECONDARY_FINDINGS_ID.key(), queryParam.key(), queryCopy, queryParam.type(),
                                andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(STATUS_ID.key(), queryParam.key(), queryCopy, STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(Enums.ExecutionStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_NAME.key(), queryParam.key(), queryCopy,
                                QueryParams.INTERNAL_STATUS_NAME.type(), andBsonList);
                        break;
                    case METHOD:
                    case METHOD_NAME:
                        addAutoOrQuery(QueryParams.METHOD_NAME.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case PANELS_UID:
                    case RELEASE:
                    case VERSION:
                    case COMMENTS_DATE:
                    case CLINICAL_ANALYSIS_ID:
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

        // If the user doesn't look for a concrete version...
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())) {
            if (queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
                // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
                andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
            } else {
                // Otherwise, we will always look for the latest version
                andBsonList.add(Filters.eq(LAST_OF_VERSION, true));
            }
        }

        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
