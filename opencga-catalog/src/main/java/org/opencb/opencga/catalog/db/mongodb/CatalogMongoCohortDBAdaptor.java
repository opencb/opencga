package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.CohortConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.acls.CohortAcl;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

public class CatalogMongoCohortDBAdaptor extends CatalogMongoDBAdaptor implements CatalogCohortDBAdaptor {

    private final MongoDBCollection cohortCollection;
    private CohortConverter cohortConverter;

    public CatalogMongoCohortDBAdaptor(MongoDBCollection cohortCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoSampleDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.cohortCollection = cohortCollection;
        this.cohortConverter = new CohortConverter();
    }

    @Override
    public QueryResult<Cohort> createCohort(long studyId, Cohort cohort, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        checkCohortNameExists(studyId, cohort.getName());

        long newId = dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
        cohort.setId(newId);

        Document cohortObject = cohortConverter.convertToStorageType(cohort);
        cohortObject.append(PRIVATE_STUDY_ID, studyId);
        cohortObject.append(PRIVATE_ID, newId);

        try {
            cohortCollection.insert(cohortObject, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("Cohort from study { id:" + studyId + "}", newId);
        } catch (MongoWriteException e) {
            throw CatalogDBException.alreadyExists("Cohort from study { id:" + studyId + "}", newId);
        }

        return endQuery("createCohort", startTime, getCohort(newId, options));
    }

    @Override
    public QueryResult<Cohort> getCohort(long cohortId, QueryOptions options) throws CatalogDBException {
        return get(new Query(QueryParams.ID.key(), cohortId).append(QueryParams.STATUS_STATUS.key(), "!=" + Status.REMOVED), options);
    }

    @Override
    public QueryResult<Cohort> getAllCohorts(long studyId, QueryOptions options) throws CatalogDBException {
        return get(new Query(QueryParams.STUDY_ID.key(), studyId), options);
    }

    @Override
    public QueryResult<Cohort> modifyCohort(long cohortId, ObjectMap parameters, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        update(new Query(QueryParams.ID.key(), cohortId), parameters);
        return endQuery("Modify cohort", startTime, getCohort(cohortId, options));
    }

    @Override
    public QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions queryOptions) throws CatalogDBException {
        return delete(cohortId, queryOptions);
    }

    @Override
    public QueryResult<CohortAcl> getCohortAcl(long cohortId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();

        checkCohortId(cohortId);

        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, cohortId));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACLS.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACLS_USERS.key(), members));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACLS.key()));

        List<CohortAcl> cohortAcl = null;
        QueryResult<Document> aggregate = cohortCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        Cohort cohort = cohortConverter.convertToDataModelType(aggregate.first());

        if (cohort != null) {
            cohortAcl = cohort.getAcls();
        }

        return endQuery("get cohort Acl", startTime, cohortAcl);
    }

    @Override
    public QueryResult<CohortAcl> setCohortAcl(long cohortId, CohortAcl acl) throws CatalogDBException {
        long startTime = startQuery();

        checkCohortId(cohortId);
        long studyId = getStudyIdByCohortId(cohortId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, acl.getUsers());

        // If there are groups in acl.getUsers(), we will obtain all the users belonging to the groups and will check if any of them
        // already have permissions on its own.
        Map<String, List<String>> groups = new HashMap<>();
        Set<String> users = new HashSet<>();

        for (String member : acl.getUsers()) {
            if (member.startsWith("@")) {
                Group group = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member, Collections.emptyList()).first();
                groups.put(group.getId(), group.getUserIds());
            } else {
                users.add(member);
            }
        }
        if (groups.size() > 0) {
            // Check if any user already have permissions set on their own.
            for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
                QueryResult<CohortAcl> cohortAcl = getCohortAcl(cohortId, entry.getValue());
                if (cohortAcl.getNumResults() > 0) {
                    throw new CatalogDBException("Error when adding permissions in cohort. At least one user in " + entry.getKey()
                            + " has already defined permissions for cohort " + cohortId);
                }
            }
        }

        // Check if any of the users in the set of users also belongs to any introduced group. In that case, we will remove the user
        // because the group will be given the permission.
        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            for (String userId : entry.getValue()) {
                if (users.contains(userId)) {
                    users.remove(userId);
                }
            }
        }

        // Create the definitive list of members that will be added in the acl
        List<String> members = new ArrayList<>(users.size() + groups.size());
        members.addAll(users.stream().collect(Collectors.toList()));
        members.addAll(groups.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()));
        acl.setUsers(members);

        // Check if the members of the new acl already have some permissions set
        QueryResult<CohortAcl> cohortAcls = getCohortAcl(cohortId, acl.getUsers());
        if (cohortAcls.getNumResults() > 0) {
            Set<String> usersSet = new HashSet<>(acl.getUsers().size());
            usersSet.addAll(acl.getUsers().stream().collect(Collectors.toList()));

            List<String> usersToOverride = new ArrayList<>();
            for (CohortAcl cohortAcl : cohortAcls.getResult()) {
                for (String member : cohortAcl.getUsers()) {
                    if (usersSet.contains(member)) {
                        // Add the user to the list of users that will be taken out from the Acls.
                        usersToOverride.add(member);
                    }
                }
            }

            // Now we remove the old permissions set for the users that already existed so the permissions are overriden by the new ones.
            unsetCohortAcl(cohortId, usersToOverride);
        }

        // Append the users to the existing acl.
        List<String> permissions = acl.getPermissions().stream().map(CohortAcl.CohortPermissions::name).collect(Collectors.toList());

        // Check if the permissions found on acl already exist on cohort id
        Document queryDocument = new Document(PRIVATE_ID, cohortId);
        if (permissions.size() > 0) {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", permissions.size()).append("$all", permissions));
        } else {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", 0));
        }

        Bson update;
        if (cohortCollection.count(queryDocument).first() > 0) {
            // Append the users to the existing acl.
            update = new Document("$addToSet", new Document("acls.$.users", new Document("$each", acl.getUsers())));
        } else {
            queryDocument = new Document(PRIVATE_ID, cohortId);
            // Push the new acl to the list of acls.
            update = new Document("$push", new Document(QueryParams.ACLS.key(), getMongoDBDocument(acl, "CohortAcl")));
        }

        QueryResult<UpdateResult> updateResult = cohortCollection.update(queryDocument, update, null);
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("setCohortAcl: An error occurred when trying to share cohort " + cohortId
                    + " with other members.");
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.ACLS.key());
        Cohort cohort = cohortConverter.convertToDataModelType(cohortCollection.find(queryDocument, queryOptions).first());

        return endQuery("setCohortAcl", startTime, cohort.getAcls());
    }

    @Override
    public void unsetCohortAcl(long cohortId, List<String> members) throws CatalogDBException {
        checkCohortId(cohortId);

        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, getStudyIdByCohortId(cohortId), members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_ID, cohortId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update = new Document("$pull", new Document("acls.$.users", member));
            QueryResult<UpdateResult> updateResult = cohortCollection.update(query, update, null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("unsetCohortAcl: An error occurred when trying to stop sharing cohort " + cohortId
                        + " with other " + member + ".");
            }
        }

        // Remove possible cohortAcls that might have permissions defined but no users
        Bson queryBson = new Document(QueryParams.ID.key(), cohortId)
                .append(QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        cohortCollection.update(queryBson, update, null);
    }

    @Override
    public void unsetCohortAclsInStudy(long studyId, List<String> members) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_STUDY_ID, studyId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update = new Document("$pull", new Document("acls.$.users", member));
            cohortCollection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }

        // Remove possible CohortAcls that might have permissions defined but no users
        Bson queryBson = new Document(PRIVATE_STUDY_ID, studyId)
                .append(CatalogSampleDBAdaptor.QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        cohortCollection.update(queryBson, update, new QueryOptions(MongoDBCollection.MULTI, true));
    }

    @Override
    public long getStudyIdByCohortId(long cohortId) throws CatalogDBException {
        checkCohortId(cohortId);
        QueryResult queryResult = nativeGet(new Query(QueryParams.ID.key(), cohortId),
                new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_ID));
        if (queryResult.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            return ((Document) queryResult.first()).getLong(PRIVATE_STUDY_ID);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        long startTime = startQuery();
        return endQuery("Count cohort", startTime, cohortCollection.count(parseQuery(query)));
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return cohortCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Cohort> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_COHORTS);
        QueryResult<Cohort> cohortQueryResult = cohortCollection.find(bson, cohortConverter, qOptions);
        return endQuery("Get cohort", startTime, cohortQueryResult);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_COHORTS);
        return cohortCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Cohort> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        update(new Query(QueryParams.ID.key(), id), parameters);
        return endQuery("Update cohort", startTime, getCohort(id, new QueryOptions()));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> cohortParams = new HashMap<>();

        String[] acceptedParams = {QueryParams.DESCRIPTION.key(), QueryParams.NAME.key(), QueryParams.CREATION_DATE.key()};
        filterStringParams(parameters, cohortParams, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(QueryParams.TYPE.key(), Cohort.Type.class);
        filterEnumParams(parameters, cohortParams, acceptedEnums);

        String[] acceptedLongListParams = {QueryParams.SAMPLES.key()};
        filterLongListParams(parameters, cohortParams, acceptedLongListParams);
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            for (Long sampleId : parameters.getAsLongList(QueryParams.SAMPLES.key())) {
                if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, cohortParams, acceptedMapParams);

        //Map<String, Class<? extends Enum>> acceptedEnumParams = Collections.singletonMap(QueryParams.STATUS_STATUS.key(),
        //        Cohort.CohortStatus.class);
        //filterEnumParams(parameters, cohortParams, acceptedEnumParams);
        if (parameters.containsKey(QueryParams.STATUS_STATUS.key())) {
            cohortParams.put(QueryParams.STATUS_STATUS.key(), parameters.get(QueryParams.STATUS_STATUS.key()));
            cohortParams.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTimeMillis());
        }
        if (parameters.containsKey("status")) {
            throw new CatalogDBException("Unable to modify cohort. Use parameter \"" + QueryParams.STATUS_STATUS.key()
                    + "\" instead of \"status\"");
        }

        if (!cohortParams.isEmpty()) {
            QueryResult<UpdateResult> update = cohortCollection.update(parseQuery(query), new Document("$set", cohortParams), null);
            return endQuery("Update cohort", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update cohort", startTime, new QueryResult<>());
    }

    @Override
    public QueryResult<Cohort> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkCohortId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED + "," + Status.REMOVED);
            QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.STATUS_STATUS.key());
            Cohort cohort = get(query, options).first();
            throw new CatalogDBException("The cohort {" + id + "} was already " + cohort.getStatus().getStatus());
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), Status.DELETED);

        return endQuery("Delete cohort", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        QueryResult<Cohort> cohortQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Cohort cohort : cohortQueryResult.getResult()) {
            delete(cohort.getId(), queryOptions);
        }
        return endQuery("Delete cohort", startTime, Collections.singletonList(cohortQueryResult.getNumTotalResults()));
    }

    QueryResult<Cohort> setStatus(long cohortId, String status) throws CatalogDBException {
        return update(cohortId, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    @Override
    public QueryResult<Cohort> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
//        long startTime = startQuery();
//        if (queryOptions == null) {
//            queryOptions = new QueryOptions();
//        }
//        QueryResult<Long> remove = remove(new Query(QueryParams.ID.key(), id), queryOptions);
//        if (remove.getResult().get(0) != 1L) {
//            throw CatalogDBException.removeError("Cohort");
//        }
//        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.REMOVED);
//        return endQuery("Remove cohort", startTime, get(query, new QueryOptions()));
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
//        long startTime = startQuery();
//        query.append(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.NONE + "," + Cohort.CohortStatus.DELETED);
//
//        // First we obtain the ids of the cohorts that will be removed.
//        List<Cohort> cohorts = get(query, new QueryOptions(MongoDBCollection.INCLUDE,
//                Arrays.asList(QueryParams.ID.key(), QueryParams.STATUS_STATUS))).getResult();
//
//        QueryResult<Long> removed = update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.REMOVED));
//
//        if (removed.first() != cohorts.size()) {
//            throw CatalogDBException.removeError("Cohort");
//        }
//
//        // Remove the instances to cohort that are stored in study
//        dbAdaptorFactory.getCatalogStudyDBAdaptor().removeCohortDependencies(
//                cohorts.stream()
//                        .filter(c -> c.getStatus().getStatus() != Cohort.CohortStatus.DELETED)
//                        .map(Cohort::getId).collect(Collectors.toList())
//        );
//
//        return endQuery("Remove cohorts", startTime, Collections.singletonList(removed.first()));
    }

    @Override
    public QueryResult<Long> restore(Query query) throws CatalogDBException {
        throw new UnsupportedOperationException("Operation not yet supported.");
//        long startTime = startQuery();
//        query.append(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.DELETED);
//        QueryResult<Long> updateStatus = update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.NONE));
////        QueryResult<Long> updateStatus = updateStatus(query, Cohort.CohortStatus.NONE);
//
//        return endQuery("Restore cohorts", startTime, Collections.singletonList(updateStatus.first()));
    }

//    @Override
//    public QueryResult<Long> updateStatus(Query query, String status) throws CatalogDBException {
//        long startTime = startQuery();
//        QueryResult<UpdateResult> update = cohortCollection.update(parseQuery(query),
//                Updates.combine(
//                        Updates.set(QueryParams.STATUS_STATUS.key(), status),
//                        Updates.set(QueryParams.STATUS_DATE.key(), TimeUtils.getTimeMillis()))
//                , new QueryOptions());
//        return endQuery("Update cohort status", startTime, Collections.singletonList(update.first().getModifiedCount()));
//    }

    @Override
    public CatalogDBIterator<Cohort> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = cohortCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, cohortConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = cohortCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(cohortCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(cohortCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(cohortCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<Cohort> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private void checkCohortNameExists(long studyId, String cohortName) throws CatalogDBException {
        QueryResult<Long> count = cohortCollection.count(Filters.and(
                Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.eq(QueryParams.NAME.key(), cohortName)));
        if (count.getResult().get(0) > 0) {
            throw CatalogDBException.alreadyExists("Cohort", "name", cohortName);
        }
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        List<Bson> annotationList = new ArrayList<>();
        // We declare variableMap here just in case we have different annotation queries
        Map<String, Variable> variableMap = null;

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
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
                    case VARIABLE_SET_ID:
                        addOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    case ANNOTATION:
                        if (variableMap == null) {
                            int variableSetId = query.getInt(QueryParams.VARIABLE_SET_ID.key());
                            if (variableSetId > 0) {
                                variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                                        .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
                            }
                        }
                        addAnnotationQueryFilter(entry.getKey(), query, variableMap, annotationList);
                        break;
                    case ANNOTATION_SET_ID:
                        addOrQuery("id", queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (annotationList.size() > 0) {
            Bson projection = Projections.elemMatch(QueryParams.ANNOTATION_SETS.key(), Filters.and(annotationList));
            andBsonList.add(projection);
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

    public QueryResult<Long> extractSamplesFromCohorts(Query query, List<Long> sampleIds) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Cohort> cohortQueryResult = get(query, new QueryOptions(QueryOptions.INCLUDE, QueryParams.ID.key()));
        if (cohortQueryResult.getNumResults() > 0) {
            Bson bsonQuery = parseQuery(query);
            Bson update = new Document("$pull", new Document(QueryParams.SAMPLES.key(), new Document("$in", sampleIds)));
            QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
            QueryResult<UpdateResult> updateQueryResult = cohortCollection.update(bsonQuery, update, multi);

            // Now we set all the cohorts where a sample has been taken out to status INVALID
            List<Long> ids = cohortQueryResult.getResult().stream().map(Cohort::getId).collect(Collectors.toList());
            setStatus(new Query(QueryParams.ID.key(), ids), Cohort.CohortStatus.INVALID);

            return endQuery("Extract samples from cohorts", startTime,
                    Collections.singletonList(updateQueryResult.first().getModifiedCount()));
        }
        return endQuery("Extract samples from cohorts", startTime, Collections.singletonList(0L));
    }
}
