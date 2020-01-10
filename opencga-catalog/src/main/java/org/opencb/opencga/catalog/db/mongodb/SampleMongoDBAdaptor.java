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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.SampleMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by hpccoll1 on 14/08/15.
 */
public class SampleMongoDBAdaptor extends AnnotationMongoDBAdaptor<Sample> implements SampleDBAdaptor {

    private final MongoDBCollection sampleCollection;
    private final MongoDBCollection deletedSampleCollection;
    private SampleConverter sampleConverter;
    private IndividualMongoDBAdaptor individualDBAdaptor;

    private static final String PRIVATE_INDIVIDUAL_UID = "_individualUid";

    public SampleMongoDBAdaptor(MongoDBCollection sampleCollection, MongoDBCollection deletedSampleCollection,
                                MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(SampleMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.sampleCollection = sampleCollection;
        this.deletedSampleCollection = deletedSampleCollection;
        this.sampleConverter = new SampleConverter();
        this.individualDBAdaptor = dbAdaptorFactory.getCatalogIndividualDBAdaptor();
    }

    @Override
    protected AnnotableConverter<? extends Annotable> getConverter() {
        return sampleConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return sampleCollection;
    }

    /*
     * Samples methods
     * ***************************
     */

    public boolean exists(ClientSession clientSession, long sampleUid) throws CatalogDBException {
        return count(clientSession, new Query(QueryParams.UID.key(), sampleUid)).getNumMatches() > 0;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> sample, String userId) throws CatalogDBException {
        Document sampleDocument = getMongoDBDocument(sample, "sample");
        return new OpenCGAResult(sampleCollection.insert(sampleDocument, null));
    }

    Sample insert(ClientSession clientSession, long studyId, Sample sample, List<VariableSet> variableSetList) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);

        if (StringUtils.isEmpty(sample.getId())) {
            throw new CatalogDBException("Missing sample id");
        }

        long individualUid = -1;
        if (StringUtils.isNotEmpty(sample.getIndividualId())) {
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
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
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = sampleCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Sample { id: '" + sample.getId() + "'} already exists.");
        }

        long sampleId = getNewUid(clientSession);
        sample.setUid(sampleId);
        sample.setStudyUid(studyId);
        sample.setVersion(1);
        if (StringUtils.isEmpty(sample.getUuid())) {
            sample.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.SAMPLE));
        }
        if (StringUtils.isEmpty(sample.getCreationDate())) {
            sample.setCreationDate(TimeUtils.getTime());
        }

        Document sampleObject = sampleConverter.convertToStorageType(sample, variableSetList);

        // Versioning private parameters
        sampleObject.put(RELEASE_FROM_VERSION, Arrays.asList(sample.getRelease()));
        sampleObject.put(LAST_OF_VERSION, true);
        sampleObject.put(LAST_OF_RELEASE, true);
        sampleObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(sample.getCreationDate()));
        sampleObject.put(PRIVATE_MODIFICATION_DATE, sampleObject.get(PRIVATE_CREATION_DATE));
        sampleObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        sampleObject.put(PRIVATE_INDIVIDUAL_UID, individualUid);

        logger.debug("Inserting sample '{}' ({})...", sample.getId(), sample.getUid());
        sampleCollection.insert(clientSession, sampleObject, null);
        logger.debug("Sample '{}' successfully inserted", sample.getId());

        if (individualUid > 0) {
            updateSampleFromIndividualCollection(clientSession, sample, individualUid, ParamUtils.UpdateAction.ADD);
        }

        return sample;
    }

    @Override
    public OpenCGAResult insert(long studyId, Sample sample, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
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
        return endQuery(startTime, get(query, options).getResults());
    }

    @Override
    public OpenCGAResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
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
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
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
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
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
            } catch (CatalogDBException e) {
                logger.error("Could not update sample {}: {}", sampleId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, sampleId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Document sampleDocument, ObjectMap parameters,
                                     List<VariableSet> variableSetList, QueryOptions queryOptions) throws CatalogDBException {
        long tmpStartTime = startQuery();
        String sampleId = sampleDocument.getString(QueryParams.ID.key());
        long sampleUid = sampleDocument.getLong(QueryParams.UID.key());
        int version = sampleDocument.getInteger(QueryParams.VERSION.key());
        long studyUid = sampleDocument.getLong(QueryParams.STUDY_UID.key());
        long individualUid = sampleDocument.getLong(PRIVATE_INDIVIDUAL_UID);

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), sampleUid);

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(clientSession, studyUid, sampleUid);
        }

        // Perform the update
        DataResult result = updateAnnotationSets(clientSession, sampleUid, parameters, variableSetList, queryOptions, true);

        UpdateDocument updateParams = parseAndValidateUpdateParams(clientSession, tmpQuery, parameters);
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
                        updateSampleFromIndividualCollection(clientSession, sample, newIndividualUid, ParamUtils.UpdateAction.ADD);
                    }

                    if (individualUid > 0) {
                        // Remove the sample from the individual where it was associated
                        updateSampleFromIndividualCollection(clientSession, sample, individualUid, ParamUtils.UpdateAction.REMOVE);
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
    }

    private void updateSampleFromIndividualCollection(ClientSession clientSession, Sample sample, long individualUid,
                                                      ParamUtils.UpdateAction updateAction) throws CatalogDBException {
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

    private void createNewVersion(ClientSession clientSession, long studyUid, long sampleUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), sampleUid);
        OpenCGAResult<Document> queryResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find sample '" + sampleUid + "'");
        }

        createNewVersion(clientSession, sampleCollection, queryResult.first());
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, Query query, ObjectMap parameters) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        final String[] acceptedBooleanParams = {QueryParams.SOMATIC.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.SOURCE.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.TYPE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        final String[] acceptedMapParams = {QueryParams.STATS.key(), QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.PHENOTYPES.key(), QueryParams.COLLECTION.key(), QueryParams.PROCESSING.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one sample...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            OpenCGAResult<Sample> sampleDataResult = get(clientSession, tmpQuery, new QueryOptions());
            if (sampleDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update sample: No sample found to be updated");
            }
            if (sampleDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update sample: Cannot update " + QueryParams.ID.key() + " parameter. More than one sample "
                        + "found to be updated.");
            }

            // Check that the new sample name is still unique
            long studyId = sampleDataResult.first().getStudyUid();

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Long> count = count(clientSession, tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Sample "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            document.getSet().put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
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
                document.getSet().put(PRIVATE_INDIVIDUAL_UID, -1);
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
    public OpenCGAResult updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        return new OpenCGAResult(sampleCollection.update(bson, update, queryOptions));
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) {
        return unmarkPermissionRule(sampleCollection, studyId, permissionRuleId);
    }

    @Deprecated
    public void checkInUse(long sampleId) throws CatalogDBException {
        long studyId = getStudyId(sampleId);

        Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_FILES + FileDBAdaptor
                .QueryParams.UID.key(), FILTER_ROUTE_FILES + FileDBAdaptor.QueryParams.PATH.key()));
        OpenCGAResult<File> fileDataResult = dbAdaptorFactory.getCatalogFileDBAdaptor().get(query, queryOptions);
        if (fileDataResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : "
                    + fileDataResult.getResults().stream()
                    .map(file -> "{ id: " + file.getUid() + ", path: \"" + file.getPath() + "\" }")
                    .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }


        queryOptions = new QueryOptions(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId)
                .append(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.UID.key(),
                        FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.ID.key()));
        OpenCGAResult<Cohort> cohortDataResult = dbAdaptorFactory.getCatalogCohortDBAdaptor().getAllInStudy(studyId, queryOptions);
        if (cohortDataResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : "
                    + cohortDataResult.getResults().stream()
                    .map(cohort -> "{ id: " + cohort.getUid() + ", name: \"" + cohort.getId() + "\" }")
                    .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }
    }

    /**
     * To be able to delete a sample, the sample does not have to be part of any cohort.
     *
     * @param sampleId sample id.
     * @throws CatalogDBException if the sampleId is used on any cohort.
     */
    private void checkCanDelete(long sampleId) throws CatalogDBException {
        Query query = new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
        if (dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).getNumMatches() > 0) {
            List<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor()
                    .get(query, new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.UID.key())).getResults();
            throw new CatalogDBException("The sample {" + sampleId + "} cannot be deleted/removed. It is being used in "
                    + cohorts.size() + " cohorts: [" + cohorts.stream().map(Cohort::getUid).collect(Collectors.toList()).toString() + "]");
        }
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(sampleCollection.count(clientSession, bson));
    }


    @Override
    public OpenCGAResult<Long> count(long studyUid, Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        Query finalQuery = new Query(query);

        if (studyPermission == null) {
            studyPermission = StudyAclEntry.StudyPermissions.VIEW_SAMPLES;
        }

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), finalQuery.getLong(QueryParams.STUDY_UID.key()));
        OpenCGAResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + finalQuery.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Just in case the parameter is in the query object, we attempt to remove it from the query map
        finalQuery.remove(QueryParams.INDIVIDUAL_UID.key());

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getSamplePermission().name(), Enums.Resource.SAMPLE.name());
        Bson bson = parseQuery(finalQuery, queryForAuthorisedEntries);

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
            return new OpenCGAResult<>(aggregate.getTime(), Collections.emptyList(), 1, Collections.singletonList(numResults), 1);
        } else {
            logger.debug("Sample count query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            return new OpenCGAResult<>(sampleCollection.count(bson));
        }
    }

    @Override
    public OpenCGAResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult(sampleCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult delete(Sample sample) throws CatalogDBException {
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
            } catch (CatalogDBException e) {
                logger.error("Could not delete sample {}: {}", sampleId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, sampleId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document sampleDocument) throws CatalogDBException {
        long tmpStartTime = startQuery();

        String sampleId = sampleDocument.getString(QueryParams.ID.key());
        long sampleUid = sampleDocument.getLong(PRIVATE_UID);
        long studyUid = sampleDocument.getLong(PRIVATE_STUDY_UID);

        logger.debug("Deleting sample {} ({})", sampleId, sampleUid);

        removeSampleReferences(clientSession, studyUid, sampleUid);

        // Look for all the different sample versions
        Query sampleQuery = new Query()
                .append(QueryParams.UID.key(), sampleUid)
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(Constants.ALL_VERSIONS, true);
        DBIterator<Document> sampleDBIterator = nativeIterator(sampleQuery, QueryOptions.empty());

        // Delete any documents that might have been already deleted with that id
        Bson query = new Document()
                .append(QueryParams.ID.key(), sampleId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedSampleCollection.remove(clientSession, query, new QueryOptions(MongoDBCollection.MULTI, true));

        while (sampleDBIterator.hasNext()) {
            Document tmpSample = sampleDBIterator.next();

            // Set status to DELETED
            tmpSample.put(QueryParams.STATUS.key(), getMongoDBDocument(new Status(Status.DELETED), "status"));

            int sampleVersion = tmpSample.getInteger(QueryParams.VERSION.key());

            // Insert the document in the DELETE collection
            deletedSampleCollection.insert(clientSession, tmpSample, null);
            logger.debug("Inserted sample uid '{}' version '{}' in DELETE collection", sampleUid, sampleVersion);

            // Remove the document from the main SAMPLE collection
            query = parseQuery(new Query()
                    .append(QueryParams.UID.key(), sampleUid)
                    .append(QueryParams.VERSION.key(), sampleVersion));
            DataResult remove = sampleCollection.remove(clientSession, query, null);
            if (remove.getNumMatches() == 0) {
                throw new CatalogDBException("Sample " + sampleId + " not found");
            }
            if (remove.getNumDeleted() == 0) {
                throw new CatalogDBException("Sample " + sampleId + " could not be deleted");
            }

            logger.debug("Sample uid '{}' version '{}' deleted from main SAMPLE collection", sampleUid, sampleVersion);
        }

        logger.debug("Sample {}({}) deleted", sampleId, sampleUid);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    private void removeSampleReferences(ClientSession clientSession, long studyUid, long sampleUid) throws CatalogDBException {
        dbAdaptorFactory.getCatalogFileDBAdaptor().removeSampleReferences(clientSession, studyUid, sampleUid);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().removeSampleReferences(clientSession, studyUid, sampleUid);
        individualDBAdaptor.removeSampleReferences(clientSession, studyUid, sampleUid);
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
    public OpenCGAResult<Sample> get(long sampleId, QueryOptions options) throws CatalogDBException {
        checkId(sampleId);
        Query query = new Query(QueryParams.UID.key(), sampleId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(sampleId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Sample> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Sample> documentList = new ArrayList<>();
        try (DBIterator<Sample> dbIterator = iterator(studyUid, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
    }

    @Override
    public OpenCGAResult<Sample> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    OpenCGAResult<Sample> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Sample> documentList = new ArrayList<>();
        try (DBIterator<Sample> dbIterator = iterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
    }

    @Override
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        OpenCGAResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        OpenCGAResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        return endQuery(startTime, documentList);
    }

    @Override
    public DBIterator<Sample> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    DBIterator<Sample> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new SampleMongoDBIterator<>(mongoCursor, clientSession, sampleConverter, null, individualDBAdaptor, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new SampleMongoDBIterator(mongoCursor, clientSession, null, null, individualDBAdaptor, options);
    }

    @Override
    public DBIterator<Sample> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, studyUid);
        MongoCursor<Document> mongoCursor = getMongoCursor(null, query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        return new SampleMongoDBIterator<>(mongoCursor, null, sampleConverter, iteratorFilter, individualDBAdaptor, studyUid, user,
                options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    DBIterator nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(clientSession, studyUid);
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        return new SampleMongoDBIterator<>(mongoCursor, clientSession, null, iteratorFilter, individualDBAdaptor, studyUid, user, options);
    }

    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(clientSession, query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, Document studyDocument,
                                                 String user) throws CatalogDBException, CatalogAuthorizationException {
        Document queryForAuthorisedEntries = null;
        if (studyDocument != null && user != null) {
            // Get the document query needed to check the permissions as well
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name(),
                    Enums.Resource.SAMPLE.name());
        }

        Query finalQuery = new Query(query);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeAnnotationProjectionOptions(qOptions);
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);

        if (isQueryingIndividualFields(finalQuery)) {
            OpenCGAResult<Individual> individualDataResult;
            if (StringUtils.isEmpty(user)) {
                individualDataResult = individualDBAdaptor.get(clientSession, getIndividualQueryFields(finalQuery),
                        new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key()));
            } else {
                individualDataResult = individualDBAdaptor.get(clientSession, studyDocument.getLong(PRIVATE_UID),
                        getIndividualQueryFields(finalQuery),
                        new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key()), user);
            }
            finalQuery = getSampleQueryFields(finalQuery);

            // Process the whole list of sampleUids recovered from the individuals
            Set<Long> sampleUids = new HashSet<>();
            individualDataResult.getResults().forEach(individual ->
                    sampleUids.addAll(individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet()))
            );

            if (sampleUids.isEmpty()) {
                // We want not to get any result
                finalQuery.append(QueryParams.UID.key(), -1);
            } else {
                finalQuery.append(QueryParams.UID.key(), sampleUids);
            }
        }

        Bson bson = parseQuery(finalQuery, queryForAuthorisedEntries);
        logger.debug("Sample query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return sampleCollection.nativeQuery().find(clientSession, bson, qOptions).iterator();
        } else {
            return deletedSampleCollection.nativeQuery().find(clientSession, bson, qOptions).iterator();
        }
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
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(sampleCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, field, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(long studyUid, Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, studyUid);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                    SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.SAMPLE.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name(),
                    Enums.Resource.SAMPLE.name());
        }
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(sampleCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(long studyUid, Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, studyUid);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                    SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.SAMPLE.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name(),
                    Enums.Resource.SAMPLE.name());
        }
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
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

    private Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    protected Bson parseQuery(Query query, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        boolean uidVersionQueryFlag = generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam =  QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
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
                    case PHENOTYPES:
                        addOntologyQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
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
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Status.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case RELEASE:
                    case VERSION:
                    case SOURCE:
                    case DESCRIPTION:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case SOMATIC:
                    case TYPE:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
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
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())) {
            if (queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
                // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
                andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
            } else {
                // Otherwise, we will always look for the latest version
                andBsonList.add(Filters.eq(LAST_OF_VERSION, true));
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

    public MongoDBCollection getSampleCollection() {
        return sampleCollection;
    }

}
