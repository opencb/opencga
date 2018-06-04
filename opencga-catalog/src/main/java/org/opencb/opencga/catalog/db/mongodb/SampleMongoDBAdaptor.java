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
import com.mongodb.client.model.*;
import com.mongodb.client.model.Variable;
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
import org.opencb.commons.datastore.mongodb.MongoDBNativeQuery;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.SampleMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
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
        filterList.add(Filters.eq(QueryParams.ID.key(), sample.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_ID, studyId));
        filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        QueryResult<Long> count = sampleCollection.count(bson);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getId() + "'} already exists.");
        }

        long sampleId = getNewId();
        sample.setUid(sampleId);
        sample.setStudyUid(studyId);
        sample.setVersion(1);

        Document sampleObject = sampleConverter.convertToStorageType(sample, variableSetList);

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
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<Sample> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();
        includeList.add(QueryParams.ANNOTATION_SETS.key());
        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        QueryResult<Sample> sampleQueryResult = get(id, queryOptions);
        if (sampleQueryResult.first().getAnnotationSets().isEmpty()) {
            return new QueryResult<>("Get annotation set", sampleQueryResult.getDbTime(), 0, 0, sampleQueryResult.getWarningMsg(),
                    sampleQueryResult.getErrorMsg(), Collections.emptyList());
        } else {
            List<AnnotationSet> annotationSets = sampleQueryResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new QueryResult<>("Get annotation set", sampleQueryResult.getDbTime(), size, size, sampleQueryResult.getWarningMsg(),
                    sampleQueryResult.getErrorMsg(), annotationSets);
        }
    }

    @Override
    public QueryResult<Sample> update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.UID.key(), id), parameters, variableSetList, queryOptions);
        if (update.getNumTotalResults() != 1 && parameters.size() > 0 && !(parameters.size() <= 3
                && (parameters.containsKey(QueryParams.ANNOTATION_SETS.key()) || parameters.containsKey(Constants.DELETE_ANNOTATION_SET)
                || parameters.containsKey(Constants.DELETE_ANNOTATION)))) {
            throw new CatalogDBException("Could not update sample with id " + id);
        }
        Query query = new Query()
                .append(QueryParams.UID.key(), id)
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
        ObjectMap annotationUpdateMap = prepareAnnotationUpdate(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList);

        if (sampleParameters.containsKey(QueryParams.STATUS_NAME.key())) {
            query.put(Constants.ALL_VERSIONS, true);
            QueryResult<UpdateResult> update = sampleCollection.update(parseQuery(query, false),
                    new Document("$set", sampleParameters), new QueryOptions("multi", true));

            applyAnnotationUpdates(query.getLong(QueryParams.UID.key(), -1L), annotationUpdateMap, true);
            return endQuery("Update sample", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        if (!queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            applyAnnotationUpdates(query.getLong(QueryParams.UID.key(), -1L), annotationUpdateMap, true);

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
                    .append(PRIVATE_UID, sampleDocument.getLong(PRIVATE_UID));
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

            applyAnnotationUpdates(query.getLong(QueryParams.UID.key(), -1L), annotationUpdateMap, true);
        }

        return endQuery("Update sample", startTime, Arrays.asList(queryResult.getNumTotalResults()));
    }

    private Document parseAndValidateUpdateParams(ObjectMap parameters, Query query) throws CatalogDBException {
        Document sampleParameters = new Document();

        final String[] acceptedBooleanParams = {QueryParams.SOMATIC.key()};
        filterBooleanParams(parameters, sampleParameters, acceptedBooleanParams);

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.SOURCE.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.TYPE.key()};
        filterStringParams(parameters, sampleParameters, acceptedParams);

        final String[] acceptedIntParams = {QueryParams.UID.key()};
        filterLongParams(parameters, sampleParameters, acceptedIntParams);

        final String[] acceptedMapParams = {QueryParams.STATS.key(), QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, sampleParameters, acceptedMapParams);

        final String[] acceptedObjectParams = {QueryParams.PHENOTYPES.key()};
        filterObjectParams(parameters, sampleParameters, acceptedObjectParams);

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one sample...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            QueryResult<Sample> sampleQueryResult = get(tmpQuery, new QueryOptions());
            if (sampleQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update sample: No sample found to be updated");
            }
            if (sampleQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update sample: Cannot update " + QueryParams.ID.key() + " parameter. More than one sample "
                        + "found to be updated.");
            }

            // Check that the new sample name is still unique
            long studyId = sampleQueryResult.first().getStudyUid();

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Sample "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            sampleParameters.put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            sampleParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            sampleParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        return sampleParameters;
    }

    @Override
    public long getStudyId(long sampleId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, sampleId);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = sampleCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Sample", sampleId);
        }
    }

    @Override
    public void updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
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

        Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_FILES + FileDBAdaptor
                .QueryParams.UID.key(), FILTER_ROUTE_FILES + FileDBAdaptor.QueryParams.PATH.key()));
        QueryResult<File> fileQueryResult = dbAdaptorFactory.getCatalogFileDBAdaptor().get(query, queryOptions);
        if (fileQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : "
                    + fileQueryResult.getResult().stream()
                    .map(file -> "{ id: " + file.getUid() + ", path: \"" + file.getPath() + "\" }")
                    .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }


        queryOptions = new QueryOptions(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId)
                .append(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.UID.key(),
                        FILTER_ROUTE_COHORTS + CohortDBAdaptor.QueryParams.ID.key()));
        QueryResult<Cohort> cohortQueryResult = dbAdaptorFactory.getCatalogCohortDBAdaptor().getAllInStudy(studyId, queryOptions);
        if (cohortQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : "
                    + cohortQueryResult.getResult().stream()
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
        if (dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).first() > 0) {
            List<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor()
                    .get(query, new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.UID.key())).getResult();
            throw new CatalogDBException("The sample {" + sampleId + "} cannot be deleted/removed. It is being used in "
                    + cohorts.size() + " cohorts: [" + cohorts.stream().map(Cohort::getUid).collect(Collectors.toList()).toString() + "]");
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
        Query finalQuery = new Query(query);
        filterOutDeleted(finalQuery);

        if (studyPermission == null) {
            studyPermission = StudyAclEntry.StudyPermissions.VIEW_SAMPLES;
        }

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), finalQuery.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + finalQuery.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Just in case the parameter is in the query object, we attempt to remove it from the query map
        finalQuery.remove(QueryParams.INDIVIDUAL_UID.key());

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getSamplePermission().name());
        Bson bson = parseQuery(finalQuery, false, queryForAuthorisedEntries);

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

            QueryResult<Document> aggregate = sampleCollection.aggregate(Arrays.asList(match, lookup, individualMatch, count),
                    QueryOptions.empty());
            long numResults = aggregate.getNumResults() == 0 ? 0 : ((int) aggregate.first().get("count"));
            return new QueryResult<>(null, aggregate.getDbTime(), 1, 1, null, null, Collections.singletonList(numResults));
        } else {
            logger.debug("Sample count query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            return sampleCollection.count(bson);
        }
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
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
        Query query = new Query(QueryParams.UID.key(), id);
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
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return endQuery("Restore samples", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Sample> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the sample is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The sample {" + id + "} is not deleted");
        }

        // Change the status of the sample to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.UID.key(), id);

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
        if (sample.getIndividual().getUid() > 0) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), sample.getIndividual().getUid())
                    .append(IndividualDBAdaptor.QueryParams.MOTHER_UID.key(), sample.getIndividual().getUid());
            Long count = dbAdaptorFactory.getCatalogIndividualDBAdaptor().count(query).first();
            if (count > 0) {
                throw CatalogDBException.sampleIdIsParentOfOtherIndividual(id);
            }
        }
    }

    @Override
    public QueryResult<Sample> get(long sampleId, QueryOptions options) throws CatalogDBException {
        checkId(sampleId);
        Query query = new Query(QueryParams.UID.key(), sampleId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
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

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

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

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults() && !isQueryingIndividualFields(query)) {
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
    public DBIterator<Sample> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new SampleMongoDBIterator<>(mongoCursor, sampleConverter, null, null, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new SampleMongoDBIterator(mongoCursor, null, null, null, options);
    }

    @Override
    public DBIterator<Sample> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        Function<Document, Document> individualIteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());
        return new SampleMongoDBIterator<>(mongoCursor, sampleConverter, iteratorFilter, individualIteratorFilter, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        Function<Document, Document> individualIteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());
        return new SampleMongoDBIterator<>(mongoCursor, null, iteratorFilter, individualIteratorFilter, options);
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

        Query finalQuery = new Query(query);
        filterOutDeleted(finalQuery);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeAnnotationProjectionOptions(qOptions);
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);

        if (qOptions.getBoolean("lazy", true) && !isQueryingIndividualFields(finalQuery)) {
            Bson bson = parseQuery(finalQuery, false, queryForAuthorisedEntries);
            logger.debug("Sample query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            return sampleCollection.nativeQuery().find(bson, qOptions).iterator();
        } else {
            List<Bson> aggregationStages = new ArrayList<>();

            if (isQueryingIndividualFields(finalQuery)) {
                Query individualQuery = getIndividualQueryFields(finalQuery);
                Query sampleQuery = getSampleQueryFields(finalQuery);

                Bson sampleBson = parseQuery(sampleQuery, false, queryForAuthorisedEntries);

                queryForAuthorisedEntries = null;
                // Obtain the query to check individual permissions
                if (studyDocument != null && user != null) {
                    queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                            StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name());
                }
                Bson individualBson = dbAdaptorFactory.getCatalogIndividualDBAdaptor().parseQuery(individualQuery, false,
                        queryForAuthorisedEntries);

                // Match the individual query in first instance
                aggregationStages.add(Aggregates.match(individualBson));

                // 1st, we unwind the array of samples to be able to perform the lookup as it doesn't work over arrays
                aggregationStages.add(Aggregates.unwind("$" + IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                        new UnwindOptions().preserveNullAndEmptyArrays(true)));

                List<Bson> lookupMatchList = new ArrayList<>();

                lookupMatchList.add(
                        Filters.expr(Filters.and(Arrays.asList(
                                new Document("$eq", Arrays.asList("$" + IndividualDBAdaptor.QueryParams.UID.key(),
                                        "$$sampleUid")),
                                new Document("$eq", Arrays.asList("$" + IndividualDBAdaptor.QueryParams.VERSION.key(),
                                        "$$sampleVersion"))
                        ))));

                lookupMatchList.add(sampleBson);

                Bson lookupMatch = lookupMatchList.size() > 1
                        ? Filters.and(lookupMatchList)
                        : lookupMatchList.get(0);


                lookupMatch = Aggregates.lookup("sample",
                        Arrays.asList(
                                new Variable("sampleUid", "$" + IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key()),
                                new Variable("sampleVersion", "$" + IndividualDBAdaptor.QueryParams.SAMPLE_VERSION.key())
                        ), Collections.singletonList(Aggregates.match(lookupMatch)),
                        IndividualDBAdaptor.QueryParams.SAMPLES.key()
                );

                aggregationStages.add(lookupMatch);

                // 3rd, we unwind the samples array again to be able to move the root to a new dummy field
                aggregationStages.add(Aggregates.unwind("$" + IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                        new UnwindOptions().preserveNullAndEmptyArrays(true)));

                // 4. We copy the whole individual document in samples.attributes.individual
                aggregationStages.add(Aggregates.addFields(
                        new Field<>(IndividualDBAdaptor.QueryParams.SAMPLES.key() + "." + QueryParams.ATTRIBUTES.key() + ".individual",
                                "$$ROOT")));

                // 5. We replace the root document extracting everything from samples
                aggregationStages.add(Aggregates.replaceRoot("$" + IndividualDBAdaptor.QueryParams.SAMPLES.key()));

                // 6. We empty the array of samples from the individual
                aggregationStages.add(Aggregates.project(Projections.exclude(
                        QueryParams.ATTRIBUTES.key() + ".individual." + IndividualDBAdaptor.QueryParams.SAMPLES.key())));

                logger.debug("Sample get: lookup match from individual : {}",
                        aggregationStages.stream()
                                .map(x -> x.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()).toString())
                                .collect(Collectors.joining("; "))
                );

                return dbAdaptorFactory.getCatalogIndividualDBAdaptor().getIndividualCollection()
                        .nativeQuery().aggregate(aggregationStages, qOptions).iterator();
            } else {
                Bson bson = parseQuery(finalQuery, false, queryForAuthorisedEntries);

                // 1. Match the sample query fields
                aggregationStages.add(Aggregates.match(bson));

                MongoDBNativeQuery.parseQueryOptions(aggregationStages, qOptions);

                // 2. Match all the individuals containing the sample uid. We do this before because mongo does not support the complex
                // lookup with pipeline over arrays yet (v 3.6) but arrays are supported in simple lookups
                aggregationStages.add(Aggregates.lookup("individual", QueryParams.UID.key(),
                        IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), "_individual"));

                // 3. Unwind the resulting array of individuals
                aggregationStages.add(Aggregates.unwind("$_individual",
                        new UnwindOptions().preserveNullAndEmptyArrays(true)));

                List<Bson> lookupMatchList = new ArrayList<>();
                // 4.1. Match the individual obtained to perform the complex lookup without having to perform the query from step 4.3 over
                // all the individuals in the collection
                lookupMatchList.add(Aggregates.match(Filters.expr(
                        new Document("$eq", Arrays.asList("$" + IndividualDBAdaptor.QueryParams.UID.key(), "$$individualUid"))
                )));

                // 4.2. Unwind the array of samples from the individual
                lookupMatchList.add(Aggregates.unwind("$" + IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                        new UnwindOptions().preserveNullAndEmptyArrays(true)));

                // 4.3. Perform the complex lookup we wanted
                lookupMatchList.add(Aggregates.match(Filters.expr(
                        Filters.and(Arrays.asList(
                                new Document("$eq", Arrays.asList("$" + IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(),
                                        "$$sampleUid")),
                                new Document("$eq", Arrays.asList("$" + IndividualDBAdaptor.QueryParams.SAMPLE_VERSION.key(),
                                        "$$sampleVersion"))
                        )))));

                // Obtain the query to check individual permissions
                if (studyDocument != null && user != null) {
                    queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                            StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name());
                    if (!queryForAuthorisedEntries.isEmpty()) {
                        // 4.4. Check for permissions
                        lookupMatchList.add(Aggregates.match(queryForAuthorisedEntries));
                    }
                }

                Bson lookupMatch = Aggregates.lookup("individual",
                        Arrays.asList(
                                new Variable("sampleUid", "$" + QueryParams.UID.key()),
                                new Variable("sampleVersion", "$" + QueryParams.VERSION.key()),
                                new Variable("individualUid", "$_individual." + IndividualDBAdaptor.QueryParams.UID.key())
                        ), lookupMatchList,
                        QueryParams.ATTRIBUTES.key() + ".individual"
                );


                // 4. Perform the complex lookup
                aggregationStages.add(lookupMatch);

                logger.debug("Sample get: lookup match : {}",
                        aggregationStages.stream()
                                .map(x -> x.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()).toString())
                                .collect(Collectors.joining("; "))
                );

                return sampleCollection.nativeQuery().aggregate(aggregationStages, qOptions).iterator();
            }
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
            }
        }
        return retQuery;
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
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                    SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(sampleCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS.name(),
                    SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), SampleAclEntry.SamplePermissions.VIEW.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
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
                if (Constants.ALL_VERSIONS.equals(entry.getKey()) || Constants.PRIVATE_ANNOTATION_PARAM_TYPES.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addOrQuery(PRIVATE_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
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
                    case ID:
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
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
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
        Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleId);
        QueryResult<Long> result = dbAdaptorFactory.getCatalogFileDBAdaptor()
                .extractSampleFromFiles(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} files", sampleId, result.first());

        // Remove references from cohorts
        query = new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
        result = dbAdaptorFactory.getCatalogCohortDBAdaptor().extractSamplesFromCohorts(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} cohorts", sampleId, result.first());
    }

}
