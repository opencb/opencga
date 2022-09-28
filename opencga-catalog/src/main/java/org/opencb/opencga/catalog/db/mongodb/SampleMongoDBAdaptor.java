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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.SampleCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.IndividualManager;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.*;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by hpccoll1 on 14/08/15.
 */
public class SampleMongoDBAdaptor extends AnnotationMongoDBAdaptor<Sample> implements SampleDBAdaptor {

    private static final String PRIVATE_INDIVIDUAL_UID = "_individualUid";
    private final MongoDBCollection sampleCollection;
    private final MongoDBCollection archiveSampleCollection;
    private final MongoDBCollection deletedSampleCollection;
    private final SampleConverter sampleConverter;
    private final IndividualMongoDBAdaptor individualDBAdaptor;
    private final VersionedMongoDBAdaptor versionedMongoDBAdaptor;

    public SampleMongoDBAdaptor(MongoDBCollection sampleCollection, MongoDBCollection archiveSampleCollection,
                                MongoDBCollection deletedSampleCollection, Configuration configuration,
                                MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(SampleMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.sampleCollection = sampleCollection;
        this.archiveSampleCollection = archiveSampleCollection;
        this.deletedSampleCollection = deletedSampleCollection;
        sampleConverter = new SampleConverter();
        individualDBAdaptor = dbAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.versionedMongoDBAdaptor = new VersionedMongoDBAdaptor(sampleCollection, archiveSampleCollection, deletedSampleCollection);
    }

    @Override
    protected MongoDBCollection getCollection() {
        return sampleCollection;
    }

    public MongoDBCollection getArchiveSampleCollection() {
        return archiveSampleCollection;
    }

    /*
     * Samples methods
     * ***************************
     */

    public boolean exists(ClientSession clientSession, long sampleUid)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(clientSession, new Query(QueryParams.UID.key(), sampleUid)).getNumMatches() > 0;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> sample, String userId) throws CatalogDBException {
        Document sampleDocument = getMongoDBDocument(sample, "sample");
        return new OpenCGAResult(sampleCollection.insert(sampleDocument, null));
    }

    Sample insert(ClientSession clientSession, long studyUid, Sample sample, List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyUid);

        if (StringUtils.isEmpty(sample.getId())) {
            throw new CatalogDBException("Missing sample id");
        }

        long individualUid = -1L;
        if (StringUtils.isNotEmpty(sample.getIndividualId())) {
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(IndividualDBAdaptor.QueryParams.ID.key(), sample.getIndividualId());

            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.UID.key());
            OpenCGAResult<Individual> queryResult = individualDBAdaptor.get(clientSession, query, options);

            if (queryResult.getNumResults() == 0) {
                throw new CatalogDBException("Individual " + sample.getIndividualId() + " not found");
            }

            individualUid = queryResult.first().getUid();
        }

        // Check the sample does not exist
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), sample.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyUid));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = sampleCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Sample { id: '" + sample.getId() + "'} already exists.");
        }

        long sampleUid = getNewUid();
        sample.setUid(sampleUid);
        sample.setStudyUid(studyUid);
        sample.setVersion(1);
        sample.setRelease(dbAdaptorFactory.getCatalogStudyDBAdaptor().getCurrentRelease(clientSession, studyUid));
        if (StringUtils.isEmpty(sample.getUuid())) {
            sample.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.SAMPLE));
        }
        if (sample.getFileIds() == null) {
            sample.setFileIds(Collections.emptyList());
        }

        Document sampleObject = sampleConverter.convertToStorageType(sample, variableSetList);

        // Versioning private parameters
        sampleObject.put(RELEASE_FROM_VERSION, Arrays.asList(sample.getRelease()));
        sampleObject.put(LAST_OF_VERSION, true);
        sampleObject.put(LAST_OF_RELEASE, true);
        sampleObject.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(sample.getCreationDate()) ? TimeUtils.toDate(sample.getCreationDate()) : TimeUtils.getDate());
        sampleObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(sample.getModificationDate())
                ? TimeUtils.toDate(sample.getModificationDate()) : TimeUtils.getDate());
        sampleObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        sampleObject.put(PRIVATE_INDIVIDUAL_UID, individualUid);

        logger.debug("Inserting sample '{}' ({})...", sample.getId(), sample.getUid());
        versionedMongoDBAdaptor.insert(clientSession, sampleObject);
        logger.debug("Sample '{}' successfully inserted", sample.getId());

        if (individualUid > 0) {
            updateSampleFromIndividualCollection(clientSession, sample, individualUid, ParamUtils.BasicUpdateAction.ADD);
        }

        return sample;
    }

    @Override
    public OpenCGAResult insert(long studyId, Sample sample, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting sample insert transaction for sample id '{}'", sample.getId());

            dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
            insert(clientSession, studyId, sample, variableSetList);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create sample {}: {}", sample.getId(), e.getMessage()));
    }

    @Override
    public OpenCGAResult<Sample> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId);
        return endQuery(startTime, get(query, options));
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

        OpenCGAResult<Sample> sampleDataResult = get(id, queryOptions);
        if (sampleDataResult.first().getAnnotationSets().isEmpty()) {
            return new OpenCGAResult<>(sampleDataResult.getTime(), sampleDataResult.getEvents(), 0, Collections.emptyList(), 0);
        } else {
            List<AnnotationSet> annotationSets = sampleDataResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new OpenCGAResult<>(sampleDataResult.getTime(), sampleDataResult.getEvents(), size, annotationSets, size);
        }
    }

    @Override
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), uid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key(),
                        PRIVATE_INDIVIDUAL_UID));
        OpenCGAResult<Document> documentResult = nativeGet(query, options);
        if (documentResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update sample. Sample uid '" + uid + "' not found.");
        }
        String sampleId = documentResult.first().getString(QueryParams.ID.key());

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, documentResult.first(), parameters, variableSetList,
                    queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update sample {}: {}", sampleId, e.getMessage(), e);
            throw new CatalogDBException("Could not update sample " + sampleId + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single sample
            if (count(query).getNumMatches() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one sample");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key(),
                        PRIVATE_INDIVIDUAL_UID));
        DBIterator<Document> iterator = nativeIterator(query, options);

        OpenCGAResult<Sample> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Document sampleDocument = iterator.next();
            String sampleId = sampleDocument.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, sampleDocument, parameters, variableSetList,
                        queryOptions)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update sample {}: {}", sampleId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, sampleId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Document sampleDocument, ObjectMap parameters,
                                        List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        String sampleId = sampleDocument.getString(QueryParams.ID.key());
        long sampleUid = sampleDocument.getLong(QueryParams.UID.key());
        int version = sampleDocument.getInteger(QueryParams.VERSION.key());
        long studyUid = sampleDocument.getLong(QueryParams.STUDY_UID.key());
        long individualUid = sampleDocument.getLong(PRIVATE_INDIVIDUAL_UID);

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), sampleUid);
        Bson bsonQuery = parseQuery(tmpQuery);
        return versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
            // Perform the update
            DataResult result = updateAnnotationSets(clientSession, sampleUid, parameters, variableSetList, queryOptions, true);

            UpdateDocument updateParams = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery, queryOptions);
            Document sampleUpdate = updateParams.toFinalUpdateDocument();

            if (sampleUpdate.isEmpty() && result.getNumUpdated() == 0) {
                if (!parameters.isEmpty()) {
                    logger.error("Non-processed update parameters: {}", parameters.keySet());
                }
                throw new CatalogDBException("Nothing to be updated");
            }

            List<Event> events = new ArrayList<>();
            if (!sampleUpdate.isEmpty()) {
                Bson finalQuery = parseQuery(tmpQuery);

                logger.debug("Sample update: query : {}, update: {}",
                        finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                        sampleUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                result = sampleCollection.update(clientSession, finalQuery, sampleUpdate, new QueryOptions("multi", true));

                if (updateParams.getSet().containsKey(PRIVATE_INDIVIDUAL_UID)) {
                    long newIndividualUid = updateParams.getSet().getLong(PRIVATE_INDIVIDUAL_UID);

                    // If the sample has been associated a different individual
                    if (newIndividualUid != individualUid) {
                        Sample sample = new Sample().setUid(sampleUid).setVersion(version).setStudyUid(studyUid);

                        if (newIndividualUid > 0) {
                            // Add the sample to the list of samples of new individual
                            updateSampleFromIndividualCollection(clientSession, sample, newIndividualUid, ParamUtils.BasicUpdateAction.ADD);
                        }

                        if (individualUid > 0) {
                            // Remove the sample from the individual where it was associated
                            updateSampleFromIndividualCollection(clientSession, sample, individualUid, ParamUtils.BasicUpdateAction.REMOVE);
                        }
                    }
                }

                if (result.getNumMatches() == 0) {
                    throw new CatalogDBException("Sample " + sampleId + " not found");
                }
                if (result.getNumUpdated() == 0) {
                    events.add(new Event(Event.Type.WARNING, sampleId, "Sample was already updated"));
                }
                logger.debug("Sample {} successfully updated", sampleId);
            }

            return endWrite(tmpStartTime, 1, 1, events);
        }, this::iterator, (DBIterator<Sample> iterator) -> updateReferencesAfterSampleVersionIncrement(clientSession, iterator));
    }

    private void updateReferencesAfterSampleVersionIncrement(ClientSession clientSession, DBIterator<Sample> iterator)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        while (iterator.hasNext()) {
            updateIndividualSampleReferences(clientSession, iterator.next());
        }
    }

    void updateIndividualFromSampleCollection(ClientSession clientSession, long studyId, List<Long> sampleUids, String individualId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (CollectionUtils.isEmpty(sampleUids)) {
            return;
        }

        ObjectMap params = new ObjectMap(QueryParams.INDIVIDUAL_ID.key(), individualId);
        Document update = parseAndValidateUpdateParams(clientSession, params, null, QueryOptions.empty()).toFinalUpdateDocument();
        Bson query = parseQuery(new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.UID.key(), sampleUids));

        versionedMongoDBAdaptor.update(clientSession, query, () -> {
            QueryOptions options = new QueryOptions(MongoDBCollection.MULTI, true);
            return sampleCollection.update(clientSession, query, update, options);
        }, this::iterator, (DBIterator<Sample> iterator) -> updateReferencesAfterSampleVersionIncrement(clientSession, iterator));
    }

    void updateCohortReferences(ClientSession clientSession, long studyUid, List<Long> sampleUids, String cohortId,
                                ParamUtils.BasicUpdateAction action)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {

        Bson bsonUpdate;
        switch (action) {
            case ADD:
                bsonUpdate = Updates.addToSet(SampleDBAdaptor.QueryParams.COHORT_IDS.key(), cohortId);
                break;
            case REMOVE:
                bsonUpdate = Updates.pull(SampleDBAdaptor.QueryParams.COHORT_IDS.key(), cohortId);
                break;
            case SET:
            default:
                throw new IllegalArgumentException("Unexpected action '" + action + "'");
        }

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), sampleUids);
        Bson bsonQuery = parseQuery(query);

        versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
            DataResult update = sampleCollection.update(clientSession, bsonQuery, bsonUpdate,
                    new QueryOptions(MongoDBCollection.MULTI, true));
            if (update.getNumMatches() == 0) {
                throw new CatalogDBException("Could not update cohort references in samples");
            }
            return update;
        }, this::iterator, (DBIterator<Sample> iterator) -> updateReferencesAfterSampleVersionIncrement(clientSession, iterator));
    }

    /**
     * Update Sample references from any Individual where it was used.
     *
     * @param clientSession Client session.
     * @param sample        Sample object containing the latest version.
     */
    private void updateIndividualSampleReferences(ClientSession clientSession, Sample sample)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), sample.getStudyUid())
                .append(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sample.getUid());

        List<String> include = new ArrayList<>(IndividualManager.INCLUDE_INDIVIDUAL_IDS.getAsStringList(QueryOptions.INCLUDE));
        include.add(IndividualDBAdaptor.QueryParams.SAMPLES.key());
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, include);

        DBIterator<Individual> iterator = dbAdaptorFactory.getCatalogIndividualDBAdaptor().iterator(clientSession, query, options);

        while (iterator.hasNext()) {
            Individual individual = iterator.next();

            List<Sample> samples = new ArrayList<>(individual.getSamples().size());
            for (Sample individualSample : individual.getSamples()) {
                if (individualSample.getUid() == sample.getUid()) {
                    individualSample.setVersion(sample.getVersion());
                }
                samples.add(individualSample);
            }

            ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), samples);

            ObjectMap action = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.SET);
            options = new QueryOptions()
                    .append(Constants.ACTIONS, action);

            OpenCGAResult result = dbAdaptorFactory.getCatalogIndividualDBAdaptor().privateUpdate(clientSession, individual, params, null,
                    options);
            if (result.getNumUpdated() != 1) {
                throw new CatalogDBException("Individual '" + individual.getId() + "' could not be updated to the latest sample version"
                        + " of '" + sample.getId() + "'");
            }
        }
    }

    void updateIndividualIdFromSamples(ClientSession clientSession, long studyUid, String oldIndividualId, String newIndividualId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (StringUtils.isEmpty(oldIndividualId)) {
            throw new CatalogDBException("Empty old individual ID");
        }
        if (StringUtils.isEmpty(newIndividualId)) {
            throw new CatalogDBException("Empty new individual ID");
        }

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.INDIVIDUAL_ID.key(), oldIndividualId);
        Bson bsonQuery = parseQuery(query);

        Bson update = Updates.set(QueryParams.INDIVIDUAL_ID.key(), newIndividualId);

        versionedMongoDBAdaptor.update(clientSession, bsonQuery,
                () -> sampleCollection.update(clientSession, bsonQuery, update, new QueryOptions(MongoDBCollection.MULTI, true)),
                this::iterator, (DBIterator<Sample> iterator) -> updateReferencesAfterSampleVersionIncrement(clientSession, iterator));
    }

    /**
     * Checks whether the sample that is going to be altered is in use in any locked Clinical Analysis.
     *
     * @param clientSession Client session.
     * @param sample        Sample to be updated.
     * @throws CatalogDBException CatalogDBException if the sample is in use in any Clinical Analysis.
     */
    private void checkInUseInClinicalAnalysis(ClientSession clientSession, Document sample)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        String sampleId = sample.getString(QueryParams.ID.key());
        long sampleUid = sample.getLong(QueryParams.UID.key());
        long studyUid = sample.getLong(QueryParams.STUDY_UID.key());

        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(), sampleUid);
        OpenCGAResult<ClinicalAnalysis> count = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().count(clientSession, query);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Could not delete sample '" + sampleId + "'. Sample is in use in "
                    + count.getNumMatches() + " cases");
        }
    }

    private void updateSampleFromIndividualCollection(ClientSession clientSession, Sample sample, long individualUid,
                                                      ParamUtils.BasicUpdateAction updateAction)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // Update individual information
        ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), Collections.singletonList(sample));

        QueryOptions options = new QueryOptions();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), updateAction.name());
        options.put(Constants.ACTIONS, actionMap);

        Query query = new Query(IndividualDBAdaptor.QueryParams.UID.key(), individualUid);
        Document update = individualDBAdaptor.parseAndValidateUpdateParams(clientSession, params, query, options).toFinalUpdateDocument();
        Bson bsonQuery = individualDBAdaptor.parseQuery(new Query()
                .append(IndividualDBAdaptor.QueryParams.UID.key(), individualUid)
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), sample.getStudyUid()), null);

        individualDBAdaptor.getCollection().update(clientSession, bsonQuery, update, null);
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

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

        final String[] acceptedBooleanParams = {QueryParams.SOMATIC.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        final String[] acceptedParams = {QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.SOURCE.key(), QueryParams.COLLECTION.key(), QueryParams.PROCESSING.key(),
                QueryParams.STATUS.key(), QueryParams.INTERNAL_STATUS.key(),
                QueryParams.QUALITY_CONTROL.key(), QueryParams.INTERNAL_VARIANT_INDEX.key(),
                QueryParams.INTERNAL_VARIANT_GENOTYPE_INDEX.key(), QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX.key(),
                QueryParams.INTERNAL_VARIANT_SECONDARY_INDEX.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one sample...
            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            OpenCGAResult<Sample> sampleDataResult = get(clientSession, tmpQuery,
                    new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                            QueryParams.STUDY_UID.key(), QueryParams.ID.key(), QueryParams.INTERNAL_VARIANT.key())));
            if (sampleDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update sample: No sample found to be updated");
            }
            if (sampleDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update sample: Cannot update " + QueryParams.ID.key() + " parameter. More than one sample "
                        + "found to be updated.");
            }

            Sample sample = sampleDataResult.first();
            // Check the sample is not indexed
            if (sample.getInternal().getVariant() != null && sample.getInternal().getVariant().getIndex() != null
                    && sample.getInternal().getVariant().getIndex().getStatus() != null
                    && !sample.getInternal().getVariant().getIndex().getStatus().getId().equals(IndexStatus.NONE)) {
                throw new CatalogDBException("Cannot update the '" + QueryParams.ID.key() + "' of the sample '" + sample.getId() + "'. "
                        + "The sample variant index status is '" + sample.getInternal().getVariant().getIndex().getStatus().getId() + "'.");
            }

            // Check that the new sample name is still unique
            long studyId = sample.getStudyUid();

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Sample> count = count(clientSession, tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Sample "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        if (parameters.containsKey(QueryParams.INTERNAL_RGA.key())) {
            RgaIndex rgaIndex = parameters.get(QueryParams.INTERNAL_RGA.key(), RgaIndex.class);
            rgaIndex.setDate(TimeUtils.getTime());
            document.getSet().put(QueryParams.INTERNAL_RGA.key(), getMongoDBDocument(rgaIndex, "rga"));
        }

        if (parameters.containsKey(QueryParams.INDIVIDUAL_ID.key())) {
            String individualId = parameters.getString(QueryParams.INDIVIDUAL_ID.key());

            if (StringUtils.isNotEmpty(individualId)) {
                // Look for the individual uid
                Query indQuery = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualId);
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.UID.key());
                OpenCGAResult<Individual> individualDataResult = individualDBAdaptor.get(clientSession, indQuery, options);

                if (individualDataResult.getNumResults() == 0) {
                    throw new CatalogDBException("Cannot update " + QueryParams.INDIVIDUAL_ID.key() + " for sample. Individual '"
                            + individualId + "' not found.");
                }

                document.getSet().put(QueryParams.INDIVIDUAL_ID.key(), individualId);
                document.getSet().put(PRIVATE_INDIVIDUAL_UID, individualDataResult.first().getUid());
            } else {
                // The user wants to leave the sample orphan
                document.getSet().put(QueryParams.INDIVIDUAL_ID.key(), "");
                document.getSet().put(PRIVATE_INDIVIDUAL_UID, -1L);
            }
        }

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        // Phenotypes
        if (parameters.containsKey(QueryParams.PHENOTYPES.key())) {
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.PHENOTYPES.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] phenotypesParams = {QueryParams.PHENOTYPES.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), phenotypesParams);
                    break;
                case REMOVE:
                    fixPhenotypesForRemoval(parameters);
                    filterObjectParams(parameters, document.getPull(), phenotypesParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), phenotypesParams);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(QueryParams.MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    void fixPhenotypesForRemoval(ObjectMap parameters) {
        if (parameters.get(QueryParams.PHENOTYPES.key()) == null) {
            return;
        }

        List<Document> phenotypeParamList = new LinkedList<>();
        for (Object phenotype : parameters.getAsList(QueryParams.PHENOTYPES.key())) {
            if (phenotype instanceof Phenotype) {
                phenotypeParamList.add(new Document("id", ((Phenotype) phenotype).getId()));
            } else {
                phenotypeParamList.add(new Document("id", ((Map) phenotype).get("id")));
            }
        }
        parameters.put(QueryParams.PHENOTYPES.key(), phenotypeParamList);
    }

    UpdateDocument updateFileReferences(ObjectMap parameters, QueryOptions queryOptions) {
        UpdateDocument document = new UpdateDocument();

        // Check if the tags exist.
        if (parameters.containsKey(QueryParams.FILE_IDS.key())) {
            List<String> fileIdList = parameters.getAsStringList(QueryParams.FILE_IDS.key());

            if (!fileIdList.isEmpty()) {
                Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
                ParamUtils.BasicUpdateAction operation =
                        ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.FILE_IDS.key(), ParamUtils.BasicUpdateAction.ADD);
                switch (operation) {
                    case SET:
                        document.getSet().put(QueryParams.FILE_IDS.key(), fileIdList);
                        break;
                    case REMOVE:
                        document.getPullAll().put(QueryParams.FILE_IDS.key(), fileIdList);
                        break;
                    case ADD:
                        document.getAddToSet().put(QueryParams.FILE_IDS.key(), fileIdList);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown update action " + operation);
                }
            }
        }

        return document;
    }

    @Override
    public long getStudyId(long sampleId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, sampleId);
        Bson projection = Projections.include(PRIVATE_STUDY_UID);
        DataResult<Document> queryResult = sampleCollection.find(query, projection, null);

        if (!queryResult.getResults().isEmpty()) {
            Object studyId = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Sample", sampleId);
        }
    }

    @Override
    public OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);
        return versionedMongoDBAdaptor.updateWithoutVersionIncrement(bson,
                () -> new OpenCGAResult<Sample>(sampleCollection.update(bson, update, queryOptions)));
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) {
        return unmarkPermissionRule(sampleCollection, studyId, permissionRuleId);
    }

    @Override
    public OpenCGAResult<Sample> setRgaIndexes(long studyUid, List<Long> sampleUids, RgaIndex rgaIndex)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        ObjectMap params;
        try {
            params = new ObjectMap(getDefaultObjectMapper().writeValueAsString(rgaIndex));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("Could not parse RgaIndex object", e);
        }

        Document rootDocument = new Document(QueryParams.INTERNAL_RGA.key(), params);

        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq(QueryParams.STUDY_UID.key(), studyUid));
        if (CollectionUtils.isNotEmpty(sampleUids)) {
            filters.add(Filters.in(QueryParams.UID.key(), sampleUids));
        }
        Bson query = Filters.and(filters);

        UpdateDocument updateDocument = new UpdateDocument().setSet(rootDocument);
        Document bsonUpdate = updateDocument.toFinalUpdateDocument();

        return runTransaction(
                (ClientSession clientSession) -> versionedMongoDBAdaptor.update(clientSession, query,
                        () -> new OpenCGAResult<>(sampleCollection.update(query, bsonUpdate, new QueryOptions("multi", true))),
                        this::iterator,
                        (DBIterator<Sample> iterator) -> updateReferencesAfterSampleVersionIncrement(clientSession, iterator)));

    }

    @Override
    public OpenCGAResult<Sample> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    OpenCGAResult<Sample> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(sampleCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Sample> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = new Query(query);

        // Just in case the parameter is in the query object, we attempt to remove it from the query map
        finalQuery.remove(QueryParams.INDIVIDUAL_UID.key());

        Bson bson = parseQuery(finalQuery, user);

        if (query.containsKey(QueryParams.INDIVIDUAL_UID.key())) {
            // We need to do a left join
            Bson match = Aggregates.match(bson);
            Bson lookup = Aggregates.lookup("individual", QueryParams.UID.key(), IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".uid",
                    "_individual");

            // We create the match for the individual id
            List<Bson> andBsonList = new ArrayList<>();
            addAutoOrQuery("_individual.uid", QueryParams.INDIVIDUAL_UID.key(), query, QueryParams.INDIVIDUAL_UID.type(), andBsonList);
            Bson individualMatch = Aggregates.match(andBsonList.get(0));

            Bson count = Aggregates.count("count");

            logger.debug("Sample count aggregation: {} -> {} -> {} -> {}",
                    match.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    lookup.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    individualMatch.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    count.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            DataResult<Document> aggregate = sampleCollection.aggregate(Arrays.asList(match, lookup, individualMatch, count),
                    QueryOptions.empty());
            long numResults = aggregate.getNumResults() == 0 ? 0 : ((int) aggregate.first().get("count"));
            return new OpenCGAResult<Sample>(aggregate.getTime(), Collections.emptyList(), 0, Collections.emptyList(), numResults);
        } else {
            logger.debug("Sample count query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            return new OpenCGAResult<>(sampleCollection.count(bson));
        }
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult delete(Sample sample) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), sample.getUid())
                    .append(QueryParams.STUDY_UID.key(), sample.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find sample " + sample.getId() + " with uid " + sample.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete sample {}: {}", sample.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete sample " + sample.getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        DBIterator<Document> iterator = nativeIterator(query, new QueryOptions());

        OpenCGAResult<Sample> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Document sample = iterator.next();
            String sampleId = sample.getString(QueryParams.ID.key());

            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, sample)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete sample {}: {}", sampleId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, sampleId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document sampleDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        String sampleId = sampleDocument.getString(QueryParams.ID.key());
        long sampleUid = sampleDocument.getLong(PRIVATE_UID);
        long studyUid = sampleDocument.getLong(PRIVATE_STUDY_UID);

        checkInUseInClinicalAnalysis(clientSession, sampleDocument);

        logger.debug("Deleting sample {} ({})", sampleId, sampleUid);

        Sample sample = new Sample()
                .setUid(sampleUid)
                .setUuid(sampleDocument.getString(QueryParams.UUID.key()))
                .setId(sampleId);
        removeSampleReferences(clientSession, studyUid, sample);

        // Delete sample
        Query sampleQuery = new Query()
                .append(QueryParams.UID.key(), sampleUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);
        Bson bsonQuery = parseQuery(sampleQuery);
        versionedMongoDBAdaptor.delete(clientSession, bsonQuery);
        logger.debug("Sample {}({}) deleted", sampleId, sampleUid);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    private void removeSampleReferences(ClientSession clientSession, long studyUid, Sample sample)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        dbAdaptorFactory.getCatalogFileDBAdaptor().removeSampleReferences(clientSession, studyUid, sample);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().removeSampleReferences(clientSession, studyUid, sample.getUid());
        individualDBAdaptor.removeSampleReferences(clientSession, studyUid, sample.getUid());
    }

    void removeFileReferences(ClientSession clientSession, long studyUid, String fileId)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        document.getPull().put(QueryParams.FILE_IDS.key(), fileId);
        Document updateDocument = document.toFinalUpdateDocument();

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.FILE_IDS.key(), fileId);
        Bson bsonQuery = parseQuery(query);

        logger.debug("Removing file from sample '{}' field. Query: {}, Update: {}", QueryParams.FILE_IDS.key(),
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
            DataResult<?> result = sampleCollection.update(clientSession, bsonQuery, updateDocument, new QueryOptions("multi", true));
            logger.debug("File '{}' removed from {} samples", fileId, result.getNumUpdated());
            return result;
        }, this::iterator, (DBIterator<Sample> iterator) -> updateReferencesAfterSampleVersionIncrement(clientSession, iterator));
    }

    // TODO: Check clean
    public OpenCGAResult<Sample> clean(long id) throws CatalogDBException {
        throw new UnsupportedOperationException("Clean is not yet implemented.");
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult<Sample> get(long sampleId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(sampleId);
        Query query = new Query(QueryParams.UID.key(), sampleId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(sampleId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Sample> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Sample> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Sample> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    OpenCGAResult<Sample> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Sample> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
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
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Sample> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    DBIterator<Sample> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new SampleCatalogMongoDBIterator<>(mongoCursor, clientSession, sampleConverter, null, individualDBAdaptor, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new SampleCatalogMongoDBIterator(mongoCursor, clientSession, null, null, individualDBAdaptor, options);
    }

    @Override
    public DBIterator<Sample> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        Document studyDocument = getStudyDocument(null, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SamplePermissions.VIEW_ANNOTATIONS.name());
        return new SampleCatalogMongoDBIterator<>(mongoCursor, null, sampleConverter, iteratorFilter, individualDBAdaptor, studyUid, user,
                options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    DBIterator nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SamplePermissions.VIEW_ANNOTATIONS.name());
        return new SampleCatalogMongoDBIterator<>(mongoCursor, clientSession, null, iteratorFilter, individualDBAdaptor, studyUid, user,
                options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        MongoDBIterator<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(clientSession, query, options, null);
        } catch (CatalogParameterException | CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    @Override
    public OpenCGAResult distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);
        return new OpenCGAResult<>(sampleCollection.distinct(field, bson));
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = new Query(query);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeAnnotationProjectionOptions(qOptions);
        qOptions = filterQueryOptions(qOptions, SampleManager.INCLUDE_SAMPLE_IDS.getAsStringList(QueryOptions.INCLUDE));
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);
        fixAclProjection(qOptions);

//        if (isQueryingIndividualFields(finalQuery)) {
//            OpenCGAResult<Individual> individualDataResult;
//            if (StringUtils.isEmpty(user)) {
//                individualDataResult = individualDBAdaptor.get(clientSession, getIndividualQueryFields(finalQuery),
//                        new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key()));
//            } else {
//                individualDataResult = individualDBAdaptor.get(clientSession, query.getLong(QueryParams.STUDY_UID.key()),
//                        getIndividualQueryFields(finalQuery),
//                        new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key()), user);
//            }
//            finalQuery = getSampleQueryFields(finalQuery);
//
//            // Process the whole list of sampleUids recovered from the individuals
//            Set<Long> sampleUids = new HashSet<>();
//            individualDataResult.getResults().forEach(individual ->
//                    sampleUids.addAll(individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet()))
//            );
//
//            if (sampleUids.isEmpty()) {
//                // We want not to get any result
//                finalQuery.append(QueryParams.UID.key(), -1);
//            } else {
//                finalQuery.append(QueryParams.UID.key(), sampleUids);
//            }
//        }

        Bson bson = parseQuery(finalQuery, user);
        MongoDBCollection collection = getQueryCollection(finalQuery, sampleCollection, archiveSampleCollection, deletedSampleCollection);
        logger.debug("Sample query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    private boolean isQueryingIndividualFields(Query query) {
        for (String s : query.keySet()) {
            if (s.startsWith("individual")) {
                return true;
            }
        }
        return false;
    }

    private Query getSampleQueryFields(Query query) {
        Query retQuery = new Query();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            if (!entry.getKey().startsWith("individual")) {
                retQuery.append(entry.getKey(), entry.getValue());
            }
        }
        return retQuery;
    }

    private Query getIndividualQueryFields(Query query) {
        Query retQuery = new Query();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            if (entry.getKey().startsWith("individual.")) {
                retQuery.append(entry.getKey().replace("individual.", ""), entry.getValue());
            } else if (entry.getKey().startsWith("individual")) {
                retQuery.append(entry.getKey().replace("individual", IndividualDBAdaptor.QueryParams.ID.key()), entry.getValue());
            } else if (QueryParams.STUDY_UID.key().equals(entry.getKey())) {
                retQuery.append(entry.getKey(), entry.getValue());
            }
        }
        return retQuery;
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return rank(sampleCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, field, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(sampleCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Sample> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
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
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.SAMPLE, user,
                        configuration));
            } else {
                if (containsAnnotationQuery(query)) {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                            SamplePermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.SAMPLE, configuration));
                } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, SamplePermissions.VIEW.name(),
                            Enums.Resource.SAMPLE, configuration));
                }
            }

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        if ("all".equalsIgnoreCase(queryCopy.getString(QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(QueryParams.VERSION.key());
        }

        boolean uidVersionQueryFlag = versionedMongoDBAdaptor.generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.ALL_VERSIONS.equals(entry.getKey()) || Constants.PRIVATE_ANNOTATION_PARAM_TYPES.equals(entry.getKey())) {
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
                    case PHENOTYPES:
                        addDefaultOrQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(queryCopy.getString(QueryParams.ANNOTATION.key()),
                                    queryCopy.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(QueryParams.STATUS_ID.key(), queryParam.key(), query, QueryParams.STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        query.put(queryParam.key(), InternalStatus.getPositiveStatus(InternalStatus.STATUS_LIST,
                                query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), query,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case PROCESSING_PRODUCT_ID:
                    case PROCESSING_PREPARATION_METHOD:
                    case PROCESSING_EXTRACTION_METHOD:
                    case PROCESSING_LAB_SAMPLE_ID:
                    case COLLECTION_FROM_ID:
                    case COLLECTION_METHOD:
                    case COLLECTION_TYPE:
                    case RELEASE:
                    case FILE_IDS:
                    case COHORT_IDS:
                    case VERSION:
                    case INDIVIDUAL_ID:
                    case INTERNAL_RGA_STATUS:
                    case SOMATIC:
                    case PHENOTYPES_ID:
//                    case ANNOTATION_SETS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + queryCopy.toJson(), e);
                }
            }
        }

        // If the user doesn't look for a concrete version...
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())
                && queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
            // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
            andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
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

}
