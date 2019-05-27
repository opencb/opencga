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
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.IndividualConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.IndividualMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
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
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualMongoDBAdaptor extends AnnotationMongoDBAdaptor<Individual> implements IndividualDBAdaptor {

    private final MongoDBCollection individualCollection;
    private IndividualConverter individualConverter;

    public IndividualMongoDBAdaptor(MongoDBCollection individualCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(IndividualMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.individualCollection = individualCollection;
        this.individualConverter = new IndividualConverter();
    }

    @Override
    protected AnnotableConverter<? extends Annotable> getConverter() {
        return individualConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return individualCollection;
    }

    @Override
    public boolean exists(long individualId) {
        return individualCollection.count(new Document(PRIVATE_UID, individualId)).first() != 0;
    }

    @Override
    public void nativeInsert(Map<String, Object> individual, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(individual, "individual");
        individualCollection.insert(document, null);
    }

    @Override
    public QueryResult<Individual> insert(long studyId, Individual individual, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
        long startQuery = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        if (!get(new Query(QueryParams.ID.key(), individual.getName())
                .append(QueryParams.STUDY_UID.key(), studyId), new QueryOptions()).getResult().isEmpty()) {
            throw CatalogDBException.alreadyExists("Individual", "name", individual.getName());
        }
        if (individual.getFather() != null && individual.getFather().getUid() > 0 && !exists(individual.getFather().getUid())) {
            throw CatalogDBException.idNotFound("Individual", individual.getFather().getId());
        }
        if (individual.getMother() != null && individual.getMother().getUid() > 0 && !exists(individual.getMother().getUid())) {
            throw CatalogDBException.idNotFound("Individual", individual.getMother().getId());
        }

        long individualId = getNewId();

        individual.setUid(individualId);
        individual.setStudyUid(studyId);
        individual.setVersion(1);
        if (StringUtils.isEmpty(individual.getUuid())) {
            individual.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.INDIVIDUAL));
        }

        Document individualDocument = individualConverter.convertToStorageType(individual, variableSetList);

        // Versioning private parameters
        individualDocument.put(RELEASE_FROM_VERSION, Arrays.asList(individual.getRelease()));
        individualDocument.put(LAST_OF_VERSION, true);
        individualDocument.put(LAST_OF_RELEASE, true);
        if (StringUtils.isNotEmpty(individual.getCreationDate())) {
            individualDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(individual.getCreationDate()));
        } else {
            individualDocument.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        individualDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        individualCollection.insert(individualDocument, null);

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.UID.key(), individualId);
        return endQuery("createIndividual", startQuery, get(query, options));
    }

//    @Override
//    public QueryResult<AnnotationSet> annotate(long individualId, AnnotationSet annotationSet, boolean overwrite)
//            throws CatalogDBException {
//        long startTime = startQuery();
//
//        QueryResult<Long> count = individualCollection.count(
//                new Document("annotationSets.name", annotationSet.getName()).append(PRIVATE_UID, individualId));
//
//        if (overwrite) {
//            if (count.first() == 0) {
//                throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getName());
//            }
//        } else {
//            if (count.first() > 0) {
//                throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
//            }
//        }
//
//        Document document = getMongoDBDocument(annotationSet, "AnnotationSet");
//
//        Bson query;
//        Bson individualQuery = Filters.eq(PRIVATE_UID, individualId);
//        if (overwrite) {
////            query.put("annotationSets.id", annotationSet.getId());
//            query = Filters.and(individualQuery, Filters.eq("annotationSets.name", annotationSet.getName()));
//        } else {
////            query.put("annotationSets.id", new BasicDBObject("$ne", annotationSet.getId()));
//            query = Filters.and(individualQuery, Filters.eq("annotationSets.name", new Document("$ne", annotationSet.getName())));
//        }
//
//        Bson update;
//        if (overwrite) {
//            update = new Document("$set", new Document("annotationSets.$", document));
//        } else {
//            update = new Document("$push", new Document("annotationSets", document));
//        }
//
//        QueryResult<UpdateResult> queryResult = individualCollection.update(query, update, null);
//
//        if (queryResult.first().getModifiedCount() != 1) {
//            throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
//        }
//
//        return endQuery("", startTime, Collections.singletonList(annotationSet));
//    }
//
//    @Override
//    public QueryResult<AnnotationSet> deleteAnnotation(long individualId, String annotationId) throws CatalogDBException {
//
//        long startTime = startQuery();
//
//        Individual individual =
//                get(individualId, new QueryOptions("include", "projects.studies.individuals.annotationSets")).first();
//        AnnotationSet annotationSet = null;
//        for (AnnotationSet as : individual.getAnnotationSets()) {
//            if (as.getName().equals(annotationId)) {
//                annotationSet = as;
//                break;
//            }
//        }
//
//        if (annotationSet == null) {
//            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
//        }
//
//        Bson eq = Filters.eq(PRIVATE_UID, individualId);
//        Bson pull = Updates.pull("annotationSets", new Document("name", annotationId));
//        QueryResult<UpdateResult> update = individualCollection.update(eq, pull, null);
//        if (update.first().getModifiedCount() < 1) {
//            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
//        }
//
//        return endQuery("Delete annotation", startTime, Collections.singletonList(annotationSet));
//    }

    public void checkInUse(long individualId) throws CatalogDBException {
        long studyId = getStudyId(individualId);
        QueryResult<Individual> individuals = get(new Query(QueryParams.FATHER_UID.key(), individualId)
                .append(QueryParams.STUDY_UID.key(), studyId), new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"fatherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getUid() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        individuals = get(new Query(QueryParams.MOTHER_UID.key(), individualId)
                .append(QueryParams.STUDY_UID.key(), studyId), new QueryOptions());
        if (individuals.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"motherId\" of individual : [";
            for (Individual individual : individuals.getResult()) {
                msg += " { id: " + individual.getUid() + ", name: \"" + individual.getName() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }
        QueryResult<Sample> samples = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(
                new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), individualId), new QueryOptions());
        if (samples.getNumResults() != 0) {
            String msg = "Can't delete Individual, still in use as \"individualId\" of sample : [";
            for (Sample sample : samples.getResult()) {
                msg += " { id: " + sample.getUid() + ", name: \"" + sample.getId() + "\" },";
            }
            msg += "]";
            throw new CatalogDBException(msg);
        }

    }

    @Override
    public long getStudyId(long individualId) throws CatalogDBException {
        QueryResult<Document> result =
                individualCollection.find(new Document(PRIVATE_UID, individualId), new Document(PRIVATE_STUDY_ID, 1), null);

        if (!result.getResult().isEmpty()) {
            return (long) result.getResult().get(0).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.uidNotFound("Individual", individualId);
        }
    }

    @Override
    public void updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        individualCollection.update(bson, update, queryOptions);
    }

    @Override
    public void unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        unmarkPermissionRule(individualCollection, studyId, permissionRuleId);
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return individualCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getIndividualPermission().name(), Entity.INDIVIDUAL.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("Individual count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()));
        return individualCollection.count(bson);
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return individualCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Individual> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, null, queryOptions);
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();
        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        QueryResult<Individual> individualQueryResult = get(id, queryOptions);
        if (individualQueryResult.first().getAnnotationSets().isEmpty()) {
            return new QueryResult<>("Get annotation set", individualQueryResult.getDbTime(), 0, 0, individualQueryResult.getWarningMsg(),
                    individualQueryResult.getErrorMsg(), Collections.emptyList());
        } else {
            List<AnnotationSet> annotationSets = individualQueryResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new QueryResult<>("Get annotation set", individualQueryResult.getDbTime(), size, size,
                    individualQueryResult.getWarningMsg(), individualQueryResult.getErrorMsg(), annotationSets);
        }
    }

    @Override
    public QueryResult<Individual> update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.UID.key(), id), parameters, variableSetList, queryOptions);
        if (update.getNumTotalResults() != 1 && parameters.size() > 0 && !(parameters.size() <= 2
                && (parameters.containsKey(QueryParams.ANNOTATION_SETS.key())
                || parameters.containsKey(AnnotationSetManager.ANNOTATIONS)))) {
            throw new CatalogDBException("Could not update individual with id " + id);
        }
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), getStudyId(id))
                .append(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), "!=EMPTY");
        return endQuery("Update individual", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        if (queryOptions.getBoolean(Constants.REFRESH)) {
            updateToLastSampleVersions(query, parameters, queryOptions);
        }

        UpdateDocument updateDocument = parseAndValidateUpdateParams(parameters, query, queryOptions);
//        ObjectMap annotationUpdateMap = prepareAnnotationUpdate(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList);
        if (updateDocument.getSet().containsKey(QueryParams.STATUS_NAME.key())) {
//            applyAnnotationUpdates(query.getLong(QueryParams.UID.key(), -1L), annotationUpdateMap, true);
            updateAnnotationSets(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList, queryOptions, true);
            query.put(Constants.ALL_VERSIONS, true);

            Bson finalQuery = parseQuery(query);
            Document finalUpdateDocument = updateDocument.toFinalUpdateDocument();
            logger.debug("Individual update: query : {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    finalUpdateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            QueryResult<UpdateResult> update = individualCollection.update(finalQuery, finalUpdateDocument,
                    new QueryOptions("multi", true));
            return endQuery("Update individual", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(query);
        }

        updateAnnotationSets(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList, queryOptions, true);

        Document individualUpdate = updateDocument.toFinalUpdateDocument();
        if (!individualUpdate.isEmpty()) {
            Bson finalQuery = parseQuery(query);
            logger.debug("Individual update: query : {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    individualUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            QueryResult<UpdateResult> update = individualCollection.update(parseQuery(query), individualUpdate,
                    new QueryOptions("multi", true));
            return endQuery("Update individual", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update individual", startTime, new QueryResult<>());
    }

    /**
     * Creates a new version for all the samples matching the query.
     *
     * @param query Query object.
     */
    private void createNewVersion(Query query) throws CatalogDBException {
        QueryResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        for (Document document : queryResult.getResult()) {
            Document updateOldVersion = new Document();

            // Current release number
            int release;
            List<Integer> supportedReleases = (List<Integer>) document.get(RELEASE_FROM_VERSION);
            if (supportedReleases.size() > 1) {
                release = supportedReleases.get(supportedReleases.size() - 1);

                // If it contains several releases, it means this is the first update on the current release, so we just need to take the
                // current release number out
                supportedReleases.remove(supportedReleases.size() - 1);
            } else {
                release = supportedReleases.get(0);

                // If it is 1, it means that the previous version being checked was made on this same release as well, so it won't be the
                // last version of the release
                updateOldVersion.put(LAST_OF_RELEASE, false);
            }
            updateOldVersion.put(RELEASE_FROM_VERSION, supportedReleases);
            updateOldVersion.put(LAST_OF_VERSION, false);

            // Perform the update on the previous version
            Document queryDocument = new Document()
                    .append(PRIVATE_STUDY_ID, document.getLong(PRIVATE_STUDY_ID))
                    .append(QueryParams.VERSION.key(), document.getInteger(QueryParams.VERSION.key()))
                    .append(PRIVATE_UID, document.getLong(PRIVATE_UID));
            QueryResult<UpdateResult> updateResult = individualCollection.update(queryDocument, new Document("$set", updateOldVersion),
                    null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("Internal error: Could not update individual");
            }

            // We update the information for the new version of the document
            document.put(LAST_OF_RELEASE, true);
            document.put(LAST_OF_VERSION, true);
            document.put(RELEASE_FROM_VERSION, Arrays.asList(release));
            document.put(QueryParams.VERSION.key(), document.getInteger(QueryParams.VERSION.key()) + 1);

            // Insert the new version document
            individualCollection.insert(document, QueryOptions.empty());
        }
    }

    private void updateToLastSampleVersions(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            throw new CatalogDBException("Invalid option: Cannot update to the last version of samples and update to different samples at "
                    + "the same time.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.SAMPLES.key());
        QueryResult<Individual> queryResult = get(query, options);

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Individual not found.");
        }
        if (queryResult.getNumResults() > 1) {
            throw new CatalogDBException("Update to the last version of samples in multiple individuals at once not supported.");
        }

        Individual individual = queryResult.first();
        if (individual.getSamples() == null || individual.getSamples().isEmpty()) {
            // Nothing to do
            return;
        }

        List<Long> sampleIds = individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toList());
        Query sampleQuery = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleIds);
        options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.VERSION.key()
        ));
        QueryResult<Sample> sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(sampleQuery, options);
        parameters.put(QueryParams.SAMPLES.key(), sampleQueryResult.getResult());

        // Add SET action for samples
        queryOptions.putIfAbsent(Constants.ACTIONS, new HashMap<>());
        queryOptions.getMap(Constants.ACTIONS).put(UpdateParams.SAMPLES.key(), SET);
    }

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results from the same individual...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            QueryResult<Individual> individualQueryResult = get(tmpQuery, new QueryOptions());
            if (individualQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update individual: No individual found to be updated");
            }
            if (individualQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update individual: Cannot set the same name parameter for different individuals");
            }

            // Check that the new individual name is still unique
            long studyId = getStudyId(individualQueryResult.first().getUid());

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot set name for individual. A individual with { name: '"
                        + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.ETHNICITY.key(), QueryParams.SEX.key(),
                QueryParams.POPULATION_NAME.key(), QueryParams.POPULATION_SUBPOPULATION.key(), QueryParams.POPULATION_DESCRIPTION.key(),
                QueryParams.KARYOTYPIC_SEX.key(), QueryParams.LIFE_STATUS.key(), QueryParams.AFFECTATION_STATUS.key(),
                QueryParams.DATE_OF_BIRTH.key(), };
        filterStringParams(parameters, document.getSet(), acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap((QueryParams.SEX.key()), IndividualProperty.Sex.class);
        filterEnumParams(parameters, document.getSet(), acceptedEnums);

        String[] acceptedIntParams = {QueryParams.FATHER_UID.key(), QueryParams.MOTHER_UID.key()};
        filterLongParams(parameters, document.getSet(), acceptedIntParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] acceptedObjectParams = {UpdateParams.PHENOTYPES.key(), UpdateParams.DISORDERS.key(), UpdateParams.MULTIPLES.key(),
                UpdateParams.LOCATION.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            document.getSet().put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        //Check individualIds exist
        String[] individualIdParams = {QueryParams.FATHER_UID.key(), QueryParams.MOTHER_UID.key()};
        for (String individualIdParam : individualIdParams) {
            if (document.getSet().containsKey(individualIdParam)) {
                Long individualId1 = (Long) document.getSet().get(individualIdParam);
                if (individualId1 > 0 && !exists(individualId1)) {
                    throw CatalogDBException.uidNotFound("Individual " + individualIdParam, individualId1);
                }
            }
        }

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        String operation = (String) actionMap.getOrDefault(UpdateParams.SAMPLES.key(), "ADD");
        acceptedObjectParams = new String[]{UpdateParams.SAMPLES.key()};
        switch (operation) {
            case "SET":
                filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
                individualConverter.validateSamplesToUpdate(document.getSet());
                break;
            case "REMOVE":
                filterObjectParams(parameters, document.getPullAll(), acceptedObjectParams);
                individualConverter.validateSamplesToUpdate(document.getPullAll());
                break;
            case "ADD":
            default:
                filterObjectParams(parameters, document.getAddToSet(), acceptedObjectParams);
                individualConverter.validateSamplesToUpdate(document.getAddToSet());
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

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = individualCollection.remove(parseQuery(query), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("Individual");
        }
    }

    private QueryResult<Individual> remove(int id, boolean force) throws CatalogDBException {
        long startTime = startQuery();
        checkId(id);
        QueryResult<Individual> individual = get(id, new QueryOptions());
        Bson bson = Filters.eq(QueryParams.UID.key(), id);
        QueryResult<DeleteResult> remove = individualCollection.remove(bson, null);
        return endQuery("Delete individual", startTime, individual);
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return endQuery("Restore individuals", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Individual> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The individual {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, File.FileStatus.READY);
        query = new Query(QueryParams.UID.key(), id);

        return endQuery("Restore individual", startTime, get(query, null));
    }

    @Override
    public QueryResult<Individual> get(long individualId, QueryOptions options) throws CatalogDBException {
        checkId(individualId);
        Query query = new Query(QueryParams.UID.key(), individualId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED)
                .append(QueryParams.STUDY_UID.key(), getStudyId(individualId));
        return get(query, options);
    }

    @Override
    public QueryResult<Individual> get(long individualId, QueryOptions options, String userId)
            throws CatalogDBException, CatalogAuthorizationException {
        long studyId = getStudyId(individualId);
        Query query = new Query()
                .append(QueryParams.UID.key(), individualId)
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options, userId);
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Individual> documentList = new ArrayList<>();
        QueryResult<Individual> queryResult;
        try (DBIterator<Individual> dbIterator = iterator(query, options, user)) {
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
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Individual> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Individual> documentList = new ArrayList<>();
        QueryResult<Individual> queryResult;
        try (DBIterator<Individual> dbIterator = iterator(query, options)) {
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
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
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
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
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
    public DBIterator<Individual> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new IndividualMongoDBIterator<>(mongoCursor, individualConverter, null, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID),
                null, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new IndividualMongoDBIterator(mongoCursor, null, null, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID), null, options);
    }

    @Override
    public DBIterator<Individual> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualMongoDBIterator<>(mongoCursor, individualConverter, iteratorFilter, dbAdaptorFactory,
                query.getLong(PRIVATE_STUDY_ID), user, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions, studyDocument, user);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualMongoDBIterator(mongoCursor, null, iteratorFilter, dbAdaptorFactory, query.getLong(PRIVATE_STUDY_ID), user,
                options);
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
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name(),
                    Entity.INDIVIDUAL.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, queryForAuthorisedEntries);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        qOptions = removeInnerProjections(qOptions, QueryParams.SAMPLES.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);

        // FIXME we should be able to remove this now safely
        qOptions = filterOptions(qOptions, FILTER_ROUTE_INDIVIDUALS);

        logger.debug("Individual get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return individualCollection.nativeQuery().find(bson, qOptions).iterator();
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
        Bson bsonQuery = parseQuery(query);
        return rank(individualCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                    IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name(), Entity.INDIVIDUAL.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name(),
                    Entity.INDIVIDUAL.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(individualCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                    IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name(), Entity.INDIVIDUAL.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS.name(), IndividualAclEntry.IndividualPermissions.VIEW.name(),
                    Entity.INDIVIDUAL.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(individualCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Individual> catalogDBIterator = iterator(query, options)) {
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

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        boolean uidVersionQueryFlag = generateUidVersionQuery(queryCopy, andBsonList);

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
                        addAutoOrQuery(PRIVATE_STUDY_ID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
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
                    case DISORDERS:
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
                    case FATHER_UID:
                    case MOTHER_UID:
                    case DATE_OF_BIRTH:
                    case SEX:
                    case ETHNICITY:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case POPULATION_NAME:
                    case POPULATION_SUBPOPULATION:
                    case POPULATION_DESCRIPTION:
                    case KARYOTYPIC_SEX:
                    case LIFE_STATUS:
                    case AFFECTATION_STATUS:
                    case RELEASE:
                    case VERSION:
                    case SAMPLE_UIDS:
//                    case ANNOTATION_SETS:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
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

    public MongoDBCollection getIndividualCollection() {
        return individualCollection;
    }

    QueryResult<Individual> setStatus(long individualId, String status) throws CatalogDBException {
        return update(individualId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    /**
     * Checks whether the individualId is parent of any other individual.
     *
     * @param individualId individual id.
     * @throws CatalogDBException when the individualId is parent of other individual.
     */
    private void checkCanDelete(long individualId) throws CatalogDBException {
        // Check if the individual is father
        Query query = new Query()
                .append(QueryParams.FATHER_UID.key(), individualId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + File.FileStatus.DELETED);
        Long count = count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The individual " + individualId + " cannot be deleted/removed because it is the father of "
                    + count + " individuals. Please, consider deleting first the children or using the force parameter.");
        }

        // Check if the individual is mother
        query = new Query()
                .append(QueryParams.MOTHER_UID.key(), individualId)
                .append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + File.FileStatus.DELETED);
        count = count(query).first();
        if (count > 0) {
            throw new CatalogDBException("The individual " + individualId + " cannot be deleted/removed because it is the mother of "
                    + count + " individuals. Please, consider deleting first the children or using the force parameter.");
        }

        // Check if the individual is being used in a sample
        QueryResult<Individual> individualQueryResult = get(individualId,
                new QueryOptions(QueryOptions.INCLUDE, QueryParams.SAMPLES.key()));
        if (individualQueryResult.first().getSamples().size() > 0) {
            throw new CatalogDBException("The individual " + individualId + " cannot be deleted/removed because it is being referenced by "
                    + count + " samples.");
        }

    }

    /**
     * Remove the possible references from other individuals or samples.
     *
     * @param individualId individual Id.
     * @throws CatalogDBException when there is any kind of error.
     */
    private void deleteReferences(long individualId) throws CatalogDBException {
        Query query = new Query(QueryParams.FATHER_UID.key(), individualId);
        Long count = update(query, new ObjectMap(QueryParams.FATHER_UID.key(), -1), QueryOptions.empty()).first();
        logger.debug("Individual id {} extracted as father from {} individuals", individualId, count);

        query = new Query(QueryParams.MOTHER_UID.key(), individualId);
        count = update(query, new ObjectMap(QueryParams.MOTHER_UID.key(), -1), QueryOptions.empty()).first();
        logger.debug("Individual id {} extracted as mother from {} individuals", individualId, count);

        query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), individualId);
        count = dbAdaptorFactory.getCatalogSampleDBAdaptor()
                .update(query, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), -1), QueryOptions.empty()).first();
        logger.debug("Individual id {} extracted from {} samples", individualId, count);
    }

}
