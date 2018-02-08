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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.AnnotableMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

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
public class SampleMongoDBAdaptor extends AnnotationMongoDBAdaptor implements SampleDBAdaptor {

    private final MongoDBCollection sampleCollection;
    private SampleConverter sampleConverter;

    public SampleMongoDBAdaptor(MongoDBCollection sampleCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(SampleMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.sampleCollection = sampleCollection;
        this.sampleConverter = new SampleConverter();
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

    @Override
    public void nativeInsert(Map<String, Object> sample, String userId) throws CatalogDBException {
        Document sampleDocument = getMongoDBDocument(sample, "sample");
        sampleCollection.insert(sampleDocument, null);
    }

    @Override
    public QueryResult<Sample> insert(long studyId, Sample sample, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.NAME.key(), sample.getName()));
        filterList.add(Filters.eq(PRIVATE_STUDY_ID, studyId));
        filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        QueryResult<Long> count = sampleCollection.count(bson);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getName() + "'} already exists.");
        }

        long sampleId = getNewId();
        sample.setId(sampleId);
        sample.setVersion(1);

        Document sampleObject = sampleConverter.convertToStorageType(sample, variableSetList);
        sampleObject.put(PRIVATE_STUDY_ID, studyId);

        // Versioning private parameters
        sampleObject.put(RELEASE_FROM_VERSION, Arrays.asList(sample.getRelease()));
        sampleObject.put(LAST_OF_VERSION, true);
        sampleObject.put(LAST_OF_RELEASE, true);
        if (StringUtils.isNotEmpty(sample.getCreationDate())) {
            sampleObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(sample.getCreationDate()));
        } else {
            sampleObject.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        sampleObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        sampleCollection.insert(sampleObject, null);

        return endQuery("createSample", startTime, get(sampleId, options));
    }


    @Override
    public QueryResult<Sample> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<Sample> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<Sample> update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters, variableSetList, queryOptions);
        if (update.getNumTotalResults() != 1 && parameters.size() > 0
                && !(parameters.size() <= 3 && parameters.containsKey(QueryParams.ANNOTATION_SETS.key()))) {
            throw new CatalogDBException("Could not update sample with id " + id);
        }
        Query query = new Query()
                .append(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), "!=EMPTY");
        return endQuery("Update sample", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();

        Document sampleParameters = parseAndValidateUpdateParams(parameters, query);
        ObjectMap annotationUpdateMap = prepareAnnotationUpdate(query.getLong(QueryParams.ID.key(), -1L), parameters, variableSetList);

        if (sampleParameters.containsKey(QueryParams.STATUS_NAME.key())) {
            query.put(Constants.ALL_VERSIONS, true);
            QueryResult<UpdateResult> update = sampleCollection.update(parseQuery(query, false),
                    new Document("$set", sampleParameters), new QueryOptions("multi", true));

            applyAnnotationUpdates(query.getLong(QueryParams.ID.key(), -1L), annotationUpdateMap, true);
            return endQuery("Update sample", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        if (!queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            applyAnnotationUpdates(query.getLong(QueryParams.ID.key(), -1L), annotationUpdateMap, true);

            if (!sampleParameters.isEmpty()) {
                QueryResult<UpdateResult> update = sampleCollection.update(parseQuery(query, false),
                        new Document("$set", sampleParameters), new QueryOptions("multi", true));
                return endQuery("Update sample", startTime, Arrays.asList(update.getNumTotalResults()));
            }
        } else {
            return updateAndCreateNewVersion(query, sampleParameters, annotationUpdateMap, queryOptions);
        }

        return endQuery("Update sample", startTime, new QueryResult<>());
    }

    private QueryResult<Long> updateAndCreateNewVersion(Query query, Document sampleParameters, ObjectMap annotationUpdateMap,
                                                        QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));
        int release = queryOptions.getInt(Constants.CURRENT_RELEASE, -1);
        if (release == -1) {
            throw new CatalogDBException("Internal error. Mandatory " + Constants.CURRENT_RELEASE + " parameter not passed to update "
                    + "method");
        }

        for (Document sampleDocument : queryResult.getResult()) {
            Document updateOldVersion = new Document();

            List<Integer> supportedReleases = (List<Integer>) sampleDocument.get(RELEASE_FROM_VERSION);
            if (supportedReleases.size() > 1) {
                // If it contains several releases, it means this is the first update on the current release, so we just need to take the
                // current release number out
                supportedReleases.remove(supportedReleases.size() - 1);
            } else {
                // If it is 1, it means that the previous version being checked was made on this same release as well, so it won't be the
                // last version of the release
                updateOldVersion.put(LAST_OF_RELEASE, false);
            }
            updateOldVersion.put(RELEASE_FROM_VERSION, supportedReleases);
            updateOldVersion.put(LAST_OF_VERSION, false);

            // Perform the update on the previous version
            Document queryDocument = new Document()
                    .append(PRIVATE_STUDY_ID, sampleDocument.getLong(PRIVATE_STUDY_ID))
                    .append(QueryParams.VERSION.key(), sampleDocument.getInteger(QueryParams.VERSION.key()))
                    .append(PRIVATE_ID, sampleDocument.getLong(PRIVATE_ID));
            QueryResult<UpdateResult> updateResult = sampleCollection.update(queryDocument, new Document("$set", updateOldVersion), null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("Internal error: Could not update sample");
            }

            // We update the information for the new version of the document
            sampleDocument.put(LAST_OF_RELEASE, true);
            sampleDocument.put(LAST_OF_VERSION, true);
            sampleDocument.put(RELEASE_FROM_VERSION, Arrays.asList(release));
            sampleDocument.put(QueryParams.VERSION.key(), sampleDocument.getInteger(QueryParams.VERSION.key()) + 1);

            // We apply the updates the user wanted to apply (if any)
            mergeDocument(sampleDocument, sampleParameters);

            // TODO: Check refresh references

            // Insert the new version document
            sampleCollection.insert(sampleDocument, QueryOptions.empty());

            applyAnnotationUpdates(query.getLong(QueryParams.ID.key(), -1L), annotationUpdateMap, true);
        }

        return endQuery("Update sample", startTime, Arrays.asList(queryResult.getNumTotalResults()));
    }

    private Document parseAndValidateUpdateParams(ObjectMap parameters, Query query) throws CatalogDBException {
        Document sampleParameters = new Document();

        final String[] acceptedBooleanParams = {QueryParams.SOMATIC.key()};
        filterBooleanParams(parameters, sampleParameters, acceptedBooleanParams);

        final String[] acceptedParams = {QueryParams.SOURCE.key(), QueryParams.DESCRIPTION.key(), QueryParams.TYPE.key()};
        filterStringParams(parameters, sampleParameters, acceptedParams);

        final String[] acceptedIntParams = {QueryParams.ID.key()};
        filterLongParams(parameters, sampleParameters, acceptedIntParams);

        final String[] acceptedMapParams = {QueryParams.STATS.key(), QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, sampleParameters, acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.PHENOTYPES.key()};
        filterObjectParams(parameters, sampleParameters, acceptedObjectParams);

        if (parameters.containsKey(QueryParams.NAME.key())) {
            // That can only be done to one sample...
            QueryResult<Sample> sampleQueryResult = get(query, new QueryOptions());
            if (sampleQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update sample: No sample found to be updated");
            }
            if (sampleQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update sample: Cannot update name parameter. More than one sample found to be updated.");
            }

            // Check that the new sample name is still unique
            long studyId = getStudyId(sampleQueryResult.first().getId());

            Query tmpQuery = new Query()
                    .append(QueryParams.NAME.key(), parameters.get(QueryParams.NAME.key()))
                    .append(QueryParams.STUDY_ID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Sample { name: '" + parameters.get(QueryParams.NAME.key()) + "'} already exists.");
            }

            sampleParameters.put(QueryParams.NAME.key(), parameters.get(QueryParams.NAME.key()));
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            sampleParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            sampleParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        return sampleParameters;
    }

    @Override
    public long getStudyId(long sampleId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_ID, sampleId);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = sampleCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }
    }

    @Override
    public void updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_ID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query, false);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        sampleCollection.update(bson, update, queryOptions);
    }

    @Override
    public void unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        unmarkPermissionRule(sampleCollection, studyId, permissionRuleId);
    }

    @Deprecated
    public void checkInUse(long sampleId) throws CatalogDBException {
        long studyId = getStudyId(sampleId);

        Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_FILES + FileDBAdaptor
                .QueryParams.ID.key(), FILTER_ROUTE_FILES + FileDBAdaptor.QueryParams.PATH.key()));
        QueryResult<File> fileQueryResult = dbAdaptorFactory.getCatalogFileDBAdaptor().get(query, queryOptions);
        if (fileQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : "
                    + fileQueryResult.getResult().stream()
                    .map(file -> "{ id: " + file.getId() + ", path: \"" + file.getPath() + "\" }")
                    .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }


        queryOptions = new QueryOptions(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId)
                .append(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.ID.key(),
                        FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.NAME.key()));
        QueryResult<Cohort> cohortQueryResult = dbAdaptorFactory.getCatalogCohortDBAdaptor().getAllInStudy(studyId, queryOptions);
        if (cohortQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : "
                    + cohortQueryResult.getResult().stream()
                    .map(cohort -> "{ id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" }")
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
        if (dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).first() > 0) {
            List<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor()
                    .get(query, new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key())).getResult();
            throw new CatalogDBException("The sample {" + sampleId + "} cannot be deleted/removed. It is being used in "
                    + cohorts.size() + " cohorts: [" + cohorts.stream().map(Cohort::getId).collect(Collectors.toList()).toString() + "]");
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return sampleCollection.count(bson);
    }


    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        if (studyPermission == null) {
            studyPermission = StudyAclEntry.StudyPermissions.VIEW_SAMPLES;
        }

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.ID.key(), query.getLong(QueryParams.STUDY_ID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_ID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getSamplePermission().name());
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("Sample count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return sampleCollection.count(bson);
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return sampleCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = sampleCollection.remove(parseQuery(query, false), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("Sample");
        }
    }

    // TODO: Check clean
    public QueryResult<Sample> clean(long id) throws CatalogDBException {
        throw new UnsupportedOperationException("Clean is not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore samples", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Sample> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the sample is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The sample {" + id + "} is not deleted");
        }

        // Change the status of the sample to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore sample", startTime, get(query, null));
    }

    /***
     * Checks whether the sample id corresponds to any Individual and if it is parent of any other individual.
     * @param id Sample id that will be checked.
     * @throws CatalogDBException when the sample is parent of other individual.
     */
    @Deprecated
    public void checkSampleIsParentOfFamily(int id) throws CatalogDBException {
        Sample sample = get(id, new QueryOptions()).first();
        if (sample.getIndividual().getId() > 0) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), sample.getIndividual().getId())
                    .append(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), sample.getIndividual().getId());
            Long count = dbAdaptorFactory.getCatalogIndividualDBAdaptor().count(query).first();
            if (count > 0) {
                throw CatalogDBException.sampleIdIsParentOfOtherIndividual(id);
            }
        }
    }

    @Override
    public QueryResult<Sample> get(long sampleId, QueryOptions options) throws CatalogDBException {
        checkId(sampleId);
        Query query = new Query(QueryParams.ID.key(), sampleId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
    }

    @Override
    public QueryResult<Sample> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Sample> documentList = new ArrayList<>();
        QueryResult<Sample> queryResult;
        try (DBIterator<Sample> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_SAMPLES);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Sample> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Sample> documentList = new ArrayList<>();
        try (DBIterator<Sample> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        QueryResult<Sample> queryResult = endQuery("Get", startTime, documentList);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()
                && !query.containsKey(QueryParams.INDIVIDUAL_ID.key())) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<Sample> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new AnnotableMongoDBIterator<>(mongoCursor, sampleConverter, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new AnnotableMongoDBIterator<>(mongoCursor, options);
    }

    @Override
    public DBIterator<Sample> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        return new AnnotableMongoDBIterator<>(mongoCursor, sampleConverter, iteratorFilter, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        return new AnnotableMongoDBIterator<>(mongoCursor, iteratorFilter, options);
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
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeAnnotationProjectionOptions(qOptions);
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);

        logger.debug("Sample get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        if (query.containsKey(QueryParams.INDIVIDUAL_ID.key())) {
            // We need to do a left join
            Bson match = Aggregates.match(bson);
            Bson lookup = Aggregates.lookup("individual", QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".id",
                    "_individual");

            // We create the match for the individual id
            List<Bson> andBsonList = new ArrayList<>();
            addAutoOrQuery("_individual.id", QueryParams.INDIVIDUAL_ID.key(), query, QueryParams.INDIVIDUAL_ID.type(), andBsonList);
            Bson individualMatch = Aggregates.match(andBsonList.get(0));

            return sampleCollection.nativeQuery().aggregate(Arrays.asList(match, lookup, individualMatch), qOptions).iterator();
        } else {
            return  sampleCollection.nativeQuery().find(bson, qOptions).iterator();
        }
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.ID.key(), query.getLong(QueryParams.STUDY_ID.key()));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_ID.key()) + " not found");
        }
        return queryResult.first();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return rank(sampleCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(sampleCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(sampleCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(sampleCollection, bsonQuery, field, QueryParams.NAME.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(sampleCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
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

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        return parseQuery(query, isolated, null);
    }

    protected Bson parseQuery(Query query, boolean isolated, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam =  QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                continue;
            }
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case PHENOTYPES:
                        addOntologyQueryFilter(queryParam.key(), queryParam.key(), query, andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(query.getString(QueryParams.ANNOTATION.key()),
                                    query.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case NAME:
                    case RELEASE:
                    case VERSION:
                    case SOURCE:
                    case DESCRIPTION:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case SOMATIC:
                    case TYPE:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
//                    case ANNOTATION_SETS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + query.toJson(), e);
                }
            }
        }

        // If the user doesn't look for a concrete version...
        if (!query.getBoolean(Constants.ALL_VERSIONS) && !query.containsKey(QueryParams.VERSION.key())) {
            if (query.containsKey(QueryParams.SNAPSHOT.key())) {
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

    QueryResult<Sample> setStatus(long sampleId, String status) throws CatalogDBException {
        return update(sampleId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    private void deleteReferencesToSample(long sampleId) throws CatalogDBException {
        // Remove references from files
        Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        QueryResult<Long> result = dbAdaptorFactory.getCatalogFileDBAdaptor()
                .extractSampleFromFiles(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} files", sampleId, result.first());

        // Remove references from cohorts
        query = new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
        result = dbAdaptorFactory.getCatalogCohortDBAdaptor().extractSamplesFromCohorts(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} cohorts", sampleId, result.first());
    }

}
