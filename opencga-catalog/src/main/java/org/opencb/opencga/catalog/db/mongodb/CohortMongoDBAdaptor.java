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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.mongodb.converters.CohortConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CohortCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortAclEntry;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.api.CohortDBAdaptor.QueryParams.*;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class CohortMongoDBAdaptor extends AnnotationMongoDBAdaptor<Cohort> implements CohortDBAdaptor {

    private final MongoDBCollection cohortCollection;
    private final MongoDBCollection deletedCohortCollection;
    private CohortConverter cohortConverter;

    public CohortMongoDBAdaptor(MongoDBCollection cohortCollection, MongoDBCollection deletedCohortCollection, Configuration configuration,
                                MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(CohortMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.cohortCollection = cohortCollection;
        this.deletedCohortCollection = deletedCohortCollection;
        cohortConverter = new CohortConverter();
    }

    @Override
    protected MongoDBCollection getCollection() {
        return cohortCollection;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> cohort, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(cohort, "cohort");
        return new OpenCGAResult(cohortCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, Cohort cohort, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                long startTime = startQuery();
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                insert(clientSession, studyId, cohort, variableSetList);
                return endWrite(startTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create cohort '{}': {}", cohort.getId(), e.getMessage(), e);
            throw e;
        }
    }

    long insert(ClientSession clientSession, long studyId, Cohort cohort, List<VariableSet> variableSetList) throws CatalogDBException,
            CatalogParameterException, CatalogAuthorizationException {
        checkCohortIdExists(clientSession, studyId, cohort.getId());

        long newId = getNewUid();
        cohort.setUid(newId);
        cohort.setStudyUid(studyId);
        if (StringUtils.isEmpty(cohort.getUuid())) {
            cohort.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.COHORT));
        }
        if (StringUtils.isEmpty(cohort.getCreationDate())) {
            cohort.setCreationDate(TimeUtils.getTime());
        }
        if (CollectionUtils.isNotEmpty(cohort.getSamples())) {
            // Add Cohort reference to samples
            List<Long> sampleUids = cohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList());
            dbAdaptorFactory.getCatalogSampleDBAdaptor().updateCohortReferences(clientSession, cohort.getStudyUid(), sampleUids,
                    cohort.getId(), ParamUtils.BasicUpdateAction.ADD);
        }

        Document cohortObject = cohortConverter.convertToStorageType(cohort, variableSetList);

        cohortObject.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(cohort.getCreationDate()) ? TimeUtils.toDate(cohort.getCreationDate()) : TimeUtils.getDate());
        cohortObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(cohort.getModificationDate())
                ? TimeUtils.toDate(cohort.getModificationDate()) : TimeUtils.getDate());
        cohortObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting cohort '{}' ({})...", cohort.getId(), cohort.getUid());
        cohortCollection.insert(clientSession, cohortObject, null);
        logger.debug("Cohort '{}' successfully inserted", cohort.getId());
        return newId;
    }

    @Override
    public OpenCGAResult<Cohort> getAllInStudy(long studyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(new Query(QueryParams.STUDY_UID.key(), studyId), options);
    }

    @Override
    public OpenCGAResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();

        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }

        queryOptions.put(QueryOptions.INCLUDE, includeList);

        OpenCGAResult<Cohort> cohortDataResult = get(id, queryOptions);
        if (cohortDataResult.first().getAnnotationSets().isEmpty()) {
            return new OpenCGAResult<>(cohortDataResult.getTime(), cohortDataResult.getEvents(), 0, Collections.emptyList(), 0);
        } else {
            List<AnnotationSet> annotationSets = cohortDataResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new OpenCGAResult<>(cohortDataResult.getTime(), cohortDataResult.getEvents(), size, annotationSets, size);
        }
    }

    @Override
    public long getStudyId(long cohortId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(cohortId);
        OpenCGAResult queryResult = nativeGet(new Query(QueryParams.UID.key(), cohortId),
                new QueryOptions(QueryOptions.INCLUDE, PRIVATE_STUDY_UID));
        if (queryResult.getResults().isEmpty()) {
            throw CatalogDBException.uidNotFound("Cohort", cohortId);
        } else {
            return ((Document) queryResult.first()).getLong(PRIVATE_STUDY_UID);
        }
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(cohortCollection, studyId, permissionRuleId);
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    private OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        return endQuery(startTime, cohortCollection.count(clientSession, parseQuery(query)));
    }

    @Override
    public OpenCGAResult<Long> count(final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query, user);
    }

    private OpenCGAResult<Long> count(ClientSession clientSession, final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Cohort count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(cohortCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(long cohortId, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(cohortId, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(long cohortUid, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), cohortUid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key(),
                        QueryParams.SAMPLES.key() + "." + QueryParams.ID.key()));
        OpenCGAResult<Cohort> documentResult = get(query, options);
        if (documentResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update cohort. Cohort uid '" + cohortUid + "' not found.");
        }
        String cohortId = documentResult.first().getId();

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, documentResult.first(), parameters, variableSetList,
                    queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update cohort {}: {}", cohortId, e.getMessage(), e);
            throw new CatalogDBException("Could not update cohort " + cohortId + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single family
            if (count(query).getNumMatches() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one cohort");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key(),
                        QueryParams.SAMPLES.key() + "." + QueryParams.ID.key()));
        DBIterator<Cohort> iterator = iterator(query, options);

        OpenCGAResult<Cohort> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, cohort, parameters, variableSetList,
                        queryOptions)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update cohort {}: {}", cohort.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, cohort.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    private OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Cohort cohort, ObjectMap parameters,
                                                List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), cohort.getStudyUid())
                .append(QueryParams.UID.key(), cohort.getUid());

        DataResult result = updateAnnotationSets(clientSession, cohort.getUid(), parameters, variableSetList, queryOptions, false);
        UpdateDocument parseUpdateDocument = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery, queryOptions);
        Document cohortUpdate = parseUpdateDocument.toFinalUpdateDocument();

        if (cohortUpdate.isEmpty() && result.getNumUpdated() == 0) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        List<Event> events = new ArrayList<>();
        if (!cohortUpdate.isEmpty()) {
            Bson finalQuery = parseQuery(tmpQuery);
            logger.debug("Cohort update: query : {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    cohortUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            result = cohortCollection.update(clientSession, finalQuery, cohortUpdate, null);

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("Cohort " + cohort.getId() + " not found");
            }
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, cohort.getId(), "Cohort was already updated"));
            }

            if (parameters.containsKey(SAMPLES.key())) {
                // Update numSamples field
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(UID.key(), SAMPLES.key() + "." + UID.key()));
                MongoDBIterator<Cohort> iterator = cohortCollection.iterator(clientSession, finalQuery, null, cohortConverter, options);
                while (iterator.hasNext()) {
                    Cohort tmpCohort = iterator.next();
                    Bson bsonQuery = parseQuery(new Query(UID.key(), tmpCohort.getUid()));
                    Document updateDoc = new Document("$set", new Document(NUM_SAMPLES.key(), tmpCohort.getSamples().size()));
                    cohortCollection.update(clientSession, bsonQuery, updateDoc, QueryOptions.empty());
                }

                // Update sample references of cohort
                updateCohortReferenceInSamples(clientSession, cohort, parameters.getAsList(QueryParams.SAMPLES.key(), Sample.class),
                        (ParamUtils.BasicUpdateAction) parseUpdateDocument.getAttributes().get(SAMPLES.key()));
            }

            logger.debug("Cohort {} successfully updated", cohort.getId());
        }

        return endWrite(tmpStartTime, 1, 1, events);
    }

    private void updateCohortReferenceInSamples(ClientSession clientSession, Cohort cohort, List<Sample> samples,
                                                ParamUtils.BasicUpdateAction updateAction)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {

        switch (updateAction) {
            case ADD:
                addSamples(clientSession, cohort, samples);
                break;
            case SET:
                removeSamples(clientSession, cohort, samples, false);
                addSamples(clientSession, cohort, samples);
                break;
            case REMOVE:
                removeSamples(clientSession, cohort, samples, true);
                break;
            default:
                break;
        }
    }

    private void addSamples(ClientSession clientSession, Cohort cohort, List<Sample> samples) throws CatalogParameterException,
            CatalogDBException, CatalogAuthorizationException {
        List<Long> newSampleUids = new ArrayList<>();

        Set<Long> currentSampleUids = cohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet());

        for (Sample sample : samples) {
            long sampleUid = sample.getUid();
            if (!currentSampleUids.contains(sampleUid)) {
                newSampleUids.add(sampleUid);
            }
        }
        if (!newSampleUids.isEmpty()) {
            dbAdaptorFactory.getCatalogSampleDBAdaptor().updateCohortReferences(clientSession, cohort.getStudyUid(), newSampleUids,
                    cohort.getId(), ParamUtils.BasicUpdateAction.ADD);
        }
    }

    /**
     * @param clientSession
     * @param cohort
     * @param samples
     * @param remove        Flag to know if list of samples provided are the ones to be removed or not
     * @throws CatalogParameterException
     * @throws CatalogDBException
     * @throws CatalogAuthorizationException
     */
    private void removeSamples(ClientSession clientSession, Cohort cohort, List<Sample> samples, boolean remove)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        List<Long> sampleUidsToRemove = new ArrayList<>();

        Set<Long> finalSampleSet = samples.stream().map(Sample::getUid).collect(Collectors.toSet());

        for (Sample sample : cohort.getSamples()) {
            if (remove) {
                if (finalSampleSet.contains(sample.getUid())) {
                    sampleUidsToRemove.add(sample.getUid());
                }
            } else {
                if (!finalSampleSet.contains(sample.getUid())) {
                    sampleUidsToRemove.add(sample.getUid());
                }
            }
        }
        if (!sampleUidsToRemove.isEmpty()) {
            dbAdaptorFactory.getCatalogSampleDBAdaptor().updateCohortReferences(clientSession, cohort.getStudyUid(),
                    sampleUidsToRemove, cohort.getId(), ParamUtils.BasicUpdateAction.REMOVE);
        }
    }

    private UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query,
                                                        QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(CohortDBAdaptor.QueryParams.ID.key())) {
            // That can only be done to one cohort...
            Query tmpQuery = new Query(query);

            OpenCGAResult<Cohort> cohortDataResult = get(clientSession, tmpQuery, new QueryOptions());
            if (cohortDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update cohort: No cohort found to be updated");
            }
            if (cohortDataResult.getNumResults() > 1) {
                throw CatalogDBException.cannotUpdateMultipleEntries(QueryParams.ID.key(), "cohort");
            }

            // Check that the new sample name is still unique
            long studyId = cohortDataResult.first().getStudyUid();

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Long> count = count(clientSession, tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Cohort "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
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

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(QueryParams.TYPE.key(), Enums.CohortType.class);
        filterEnumParams(parameters, document.getSet(), acceptedEnums);

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, SAMPLES.key(),
                ParamUtils.BasicUpdateAction.ADD);
        String[] sampleObjectParams = new String[]{SAMPLES.key()};

        if (operation == ParamUtils.BasicUpdateAction.SET || !parameters.getAsList(SAMPLES.key()).isEmpty()) {
            document.getAttributes().put(SAMPLES.key(), operation);
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), sampleObjectParams);
                    cohortConverter.validateSamplesToUpdate(document.getSet());
                    break;
                case REMOVE:
                    filterObjectParams(parameters, document.getPullAll(), sampleObjectParams);
                    cohortConverter.validateSamplesToUpdate(document.getPullAll());
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), sampleObjectParams);
                    cohortConverter.validateSamplesToUpdate(document.getAddToSet());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        String[] acceptedObjectParams = {QueryParams.STATUS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_ID.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_ID.key(), parameters.get(QueryParams.INTERNAL_STATUS_ID.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_DESCRIPTION.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_DESCRIPTION.key(),
                    parameters.get(QueryParams.INTERNAL_STATUS_DESCRIPTION.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey(QueryParams.INTERNAL_STATUS.key())) {
            throw new CatalogDBException("Unable to modify cohort. Use parameter '" + QueryParams.INTERNAL_STATUS_ID.key()
                    + "' instead of '" + QueryParams.INTERNAL_STATUS.key() + "'");
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

    @Override
    public OpenCGAResult delete(Cohort cohort) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), cohort.getUid())
                    .append(QueryParams.STUDY_UID.key(), cohort.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find cohort " + cohort.getId() + " with uid " + cohort.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete cohort {}: {}", cohort.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete cohort '" + cohort.getId() + "': " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<Document> iterator = nativeIterator(query, new QueryOptions());

        OpenCGAResult<Cohort> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Document cohort = iterator.next();
            String cohortId = cohort.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, cohort)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete cohort {}: {}", cohortId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, cohortId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Cohort> privateDelete(ClientSession clientSession, Document cohortDocument) throws CatalogDBException {
        long tmpStartTime = startQuery();

        String cohortId = cohortDocument.getString(QueryParams.ID.key());
        long cohortUid = cohortDocument.getLong(PRIVATE_UID);
        long studyUid = cohortDocument.getLong(PRIVATE_STUDY_UID);

        logger.info("Deleting cohort {} ({})", cohortId, cohortUid);

        checkCohortCanBeDeleted(cohortDocument);

        // Add status DELETED
        nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new CohortStatus(InternalStatus.DELETED), "status"),
                cohortDocument);

        // Upsert the document into the DELETED collection
        Bson query = new Document()
                .append(QueryParams.ID.key(), cohortId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedCohortCollection.update(clientSession, query, new Document("$set", cohortDocument),
                new QueryOptions(MongoDBCollection.UPSERT, true));

        // Delete the document from the main COHORT collection
        query = new Document()
                .append(PRIVATE_UID, cohortUid)
                .append(PRIVATE_STUDY_UID, studyUid);
        DataResult remove = cohortCollection.remove(clientSession, query, null);
        if (remove.getNumMatches() == 0) {
            throw new CatalogDBException("Cohort " + cohortId + " not found");
        }
        if (remove.getNumDeleted() == 0) {
            throw new CatalogDBException("Cohort " + cohortId + " could not be deleted");
        }
        logger.debug("Cohort {} successfully deleted", cohortId);

        return endWrite(tmpStartTime, 1, 0, 0, 1, null);
    }

    private void checkCohortCanBeDeleted(Document cohortDocument) throws CatalogDBException {
        // Check if the cohort is different from DEFAULT_COHORT
        if (StudyEntry.DEFAULT_COHORT.equals(cohortDocument.getString(QueryParams.ID.key()))) {
            throw new CatalogDBException("Cohort " + StudyEntry.DEFAULT_COHORT + " cannot be deleted.");
        }

        if (!cohortDocument.getEmbedded(Arrays.asList(QueryParams.INTERNAL_STATUS.key(), "name"), CohortStatus.NONE)
                .equals(CohortStatus.NONE)) {
            throw new CatalogDBException("Cohort in use in storage.");
        }
    }

    @Override
    public OpenCGAResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
    }

    @Override
    public OpenCGAResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult<Cohort> get(long cohortId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.UID.key(), cohortId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(cohortId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Cohort> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return get(null, studyUid, query, options, user);
    }

    OpenCGAResult<Cohort> get(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Cohort> dbIterator = iterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Cohort> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    OpenCGAResult<Cohort> get(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Cohort> dbIterator = iterator(clientSession, query, options)) {
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
    public DBIterator<Cohort> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    DBIterator<Cohort> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CohortCatalogMongoDBIterator(mongoCursor, clientSession, cohortConverter, null,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions);
        return new CohortCatalogMongoDBIterator(mongoCursor, null, null, null, dbAdaptorFactory.getCatalogSampleDBAdaptor(), options);
    }

    @Override
    public DBIterator<Cohort> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return iterator(null, studyUid, query, options, user);
    }

    DBIterator<Cohort> iterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS.name(), CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name());

        return new CohortCatalogMongoDBIterator<>(mongoCursor, clientSession, cohortConverter, iteratorFilter,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), studyUid, user, options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, queryOptions, user);
        Document studyDocument = getStudyDocument(null, studyUid);
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS.name(), CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name());

        return new CohortCatalogMongoDBIterator(mongoCursor, null, null, iteratorFilter, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                studyUid, user, options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
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
        qOptions = removeInnerProjections(qOptions, SAMPLES.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);
        qOptions = filterOptions(qOptions, FILTER_ROUTE_COHORTS);
        fixAclProjection(qOptions);

        logger.debug("Cohort query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return cohortCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedCohortCollection.iterator(clientSession, bson, null, null, qOptions);
        }
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return rank(cohortCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(cohortCollection, bsonQuery, field, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(cohortCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(cohortCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult<>(cohortCollection.distinct(field, bson, clazz));
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<Cohort> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private void checkCohortIdExists(ClientSession clientSession, long studyId, String cohortId) throws CatalogDBException {
        DataResult<Long> count = cohortCollection.count(clientSession, Filters.and(
                Filters.eq(PRIVATE_STUDY_UID, studyId), Filters.eq(QueryParams.ID.key(), cohortId)));
        if (count.getNumMatches() > 0) {
            throw CatalogDBException.alreadyExists("Cohort", "id", cohortId);
        }
    }

    private Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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
        Document annotationDocument = null;

        if (query.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(QueryParams.STUDY_UID.key()));

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.COHORT, user,
                        configuration));
            } else {
                if (containsAnnotationQuery(query)) {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                            CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.COHORT, configuration));
                } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, CohortAclEntry.CohortPermissions.VIEW.name(),
                            Enums.Resource.COHORT, configuration));
                }
            }

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query finalQuery = new Query(query);
        finalQuery.remove(QueryParams.DELETED.key());

        for (Map.Entry<String, Object> entry : finalQuery.entrySet()) {
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
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), finalQuery, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), finalQuery, queryParam.type(), andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(finalQuery.getString(QueryParams.ANNOTATION.key()),
                                    finalQuery.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
                        break;
                    case SAMPLE_UIDS:
                        addQueryFilter(queryParam.key(), queryParam.key(), finalQuery, queryParam.type(),
                                MongoDBQueryUtils.ComparisonOperator.IN, MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), finalQuery, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), finalQuery, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(QueryParams.STATUS_ID.key(), queryParam.key(), finalQuery, QueryParams.STATUS_ID.type(),
                                andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        finalQuery.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(CohortStatus.STATUS_LIST, finalQuery.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), finalQuery,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case UUID:
                    case ID:
                    case TYPE:
                    case RELEASE:
                    case NUM_SAMPLES:
//                    case ANNOTATION_SETS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), finalQuery, queryParam.type(), andBsonList);
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
        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
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

    void removeSampleReferences(ClientSession clientSession, long studyUid, long sampleUid)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Document bsonQuery = new Document()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.SAMPLE_UIDS.key(), sampleUid);

        // We set the status of all the matching cohorts to INVALID and add the sample to be removed
        ObjectMap params = new ObjectMap()
                .append(QueryParams.INTERNAL_STATUS_ID.key(), CohortStatus.INVALID)
                .append(SAMPLES.key(), Collections.singletonList(new Sample().setUid(sampleUid)));
        // Add the the Remove action for the sample provided
        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS,
                new ObjectMap(SAMPLES.key(), ParamUtils.BasicUpdateAction.REMOVE.name()));

        Bson update = parseAndValidateUpdateParams(clientSession, params, null, queryOptions).toFinalUpdateDocument();

        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);

        logger.debug("Sample references extraction. Query: {}, update: {}",
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = cohortCollection.update(clientSession, bsonQuery, update, multi);
        logger.debug("Sample uid '" + sampleUid + "' references removed from " + result.getNumUpdated() + " out of "
                + result.getNumMatches() + " cohorts");
    }
}
