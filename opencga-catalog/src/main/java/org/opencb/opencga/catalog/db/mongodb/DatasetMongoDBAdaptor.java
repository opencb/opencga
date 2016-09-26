package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.DatasetDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.DatasetConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.acls.permissions.DatasetAclEntry;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 04/05/16.
 */
public class DatasetMongoDBAdaptor extends MongoDBAdaptor implements DatasetDBAdaptor {

    private final MongoDBCollection datasetCollection;
    private DatasetConverter datasetConverter;
    private AclMongoDBAdaptor<DatasetAclEntry> aclDBAdaptor;

    /***
     * CatalogMongoDatasetDBAdaptor constructor.
     *
     * @param datasetCollection MongoDB connection to the dataset collection.
     * @param dbAdaptorFactory Generic dbAdaptorFactory containing all the different collections.
     */
    public DatasetMongoDBAdaptor(MongoDBCollection datasetCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(FileMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.datasetCollection = datasetCollection;
        this.datasetConverter = new DatasetConverter();
        this.aclDBAdaptor = new AclMongoDBAdaptor<>(datasetCollection, datasetConverter, logger);
    }

    /**
     *
     * @return MongoDB connection to the file collection.
     */
    public MongoDBCollection getDatasetCollection() {
        return datasetCollection;
    }

    @Override
    public QueryResult<Dataset> insert(Dataset dataset, long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);

        Query query = new Query(QueryParams.NAME.key(), dataset.getName()).append(QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> count = count(query);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Dataset { name: \"" + dataset.getName() + "\" } already exists in the study { " + studyId
                    + " }.");
        }

        long newId = getNewId();
        dataset.setId(newId);

        Document datasetObject = datasetConverter.convertToStorageType(dataset);
        datasetCollection.insert(datasetObject, null);

        return endQuery("createDataset", startTime, get(dataset.getId(), options));
    }

    @Override
    public long getStudyIdByDatasetId(long datasetId) throws CatalogDBException {
        checkId(datasetId);
        Query query = new Query(QueryParams.ID.key(), datasetId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_ID);
        Document dataset = (Document) nativeGet(query, queryOptions).first();

        return dataset.getLong(PRIVATE_STUDY_ID);
    }

    @Override
    public QueryResult<Dataset> get(long datasetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Query query = new Query(QueryParams.ID.key(), datasetId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return endQuery("Get dataset", startTime, get(query, options));
    }

    @Override
    public QueryResult<Dataset> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson;
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get dataset: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_DATASETS);

        QueryResult<Dataset> datasetQueryResult = datasetCollection.find(bson, datasetConverter, qOptions);
        logger.debug("Dataset get: query : {}, project: {}, dbTime: {}", bson, qOptions == null ? "" : qOptions.toJson(),
                datasetQueryResult.getDbTime());
        return endQuery("Get Dataset", startTime, datasetQueryResult);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson;
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get dataset: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_DATASETS);

        return datasetCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Dataset> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        update(new Query(QueryParams.ID.key(), id), parameters);
        return endQuery("Update dataset", startTime, get(id, new QueryOptions()));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> datasetParams = new HashMap<>();

        String[] acceptedParams = {QueryParams.DESCRIPTION.key(), QueryParams.NAME.key(), QueryParams.CREATION_DATE.key()};
        filterStringParams(parameters, datasetParams, acceptedParams);

        String[] acceptedLongListParams = {QueryParams.FILES.key()};
        filterLongParams(parameters, datasetParams, acceptedLongListParams);
        if (parameters.containsKey(QueryParams.FILES.key())) {
            for (Long fileId : parameters.getAsLongList(QueryParams.FILES.key())) {
                if (!dbAdaptorFactory.getCatalogFileDBAdaptor().exists(fileId)) {
                    throw CatalogDBException.idNotFound("File", fileId);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, datasetParams, acceptedMapParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            datasetParams.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            datasetParams.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!datasetParams.isEmpty()) {
            QueryResult<UpdateResult> update = datasetCollection.update(parseQuery(query), new Document("$set", datasetParams), null);
            return endQuery("Update cohort", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update cohort", startTime, new QueryResult<>());
    }

    @Override
    public QueryResult<Dataset> delete(long id, QueryOptions queryOptions) throws CatalogDBException    {
        long startTime = startQuery();

        checkId(id);
        // Check the dataset is active
        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.READY);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED + "," + Status.DELETED);
            QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.STATUS_NAME.key());
            Dataset dataset = get(query, options).first();
            throw new CatalogDBException("The dataset {" + id + "} was already " + dataset.getStatus().getName());
        }

        // Change the status of the dataset to deleted
        setStatus(id, Status.TRASHED);

        query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), Status.TRASHED);

        return endQuery("Delete dataset", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_NAME.key(), Status.READY);
        QueryResult<Dataset> datasetQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Dataset dataset : datasetQueryResult.getResult()) {
            delete(dataset.getId(), queryOptions);
        }
        return endQuery("Delete dataset", startTime, Collections.singletonList(datasetQueryResult.getNumTotalResults()));
    }

    QueryResult<Dataset> setStatus(long datasetId, String status) throws CatalogDBException {
        return update(datasetId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    @Override
    public QueryResult<Dataset> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore datasets", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Dataset> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The dataset {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore dataset", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> insertFilesIntoDatasets(Query query, List<Long> fileIds) throws CatalogDBException {
        long startTime = startQuery();
        Bson bsonQuery = parseQuery(query);
        Bson update = new Document("$push", new Document(QueryParams.FILES.key(), new Document("$each", fileIds)));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        QueryResult<UpdateResult> updateQueryResult = datasetCollection.update(bsonQuery, update, multi);
        return endQuery("Insert files into datasets", startTime, Collections.singletonList(updateQueryResult.first().getModifiedCount()));
    }

    @Override
    public QueryResult<Long> extractFilesFromDatasets(Query query, List<Long> fileIds) throws CatalogDBException {
        long startTime = startQuery();
        Bson bsonQuery = parseQuery(query);
        Bson update = new Document("$pull", new Document(QueryParams.FILES.key(), new Document("$in", fileIds)));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        QueryResult<UpdateResult> updateQueryResult = datasetCollection.update(bsonQuery, update, multi);
        return endQuery("Extract files from datasets", startTime, Collections.singletonList(updateQueryResult.first().getModifiedCount()));
    }

//    @Override
//    public QueryResult<DatasetAclEntry> getDatasetAcl(long datasetId, List<String> members) throws CatalogDBException {
//        long startTime = startQuery();
//
//        checkId(datasetId);
//
//        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, datasetId));
//        Bson unwind = Aggregates.unwind("$" + QueryParams.ACL.key());
//        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACL_MEMBER.key(), members));
//        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACL.key()));
//
//        List<DatasetAclEntry> datasetAcl = null;
//        QueryResult<Document> aggregate = datasetCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
//        Dataset dataset = datasetConverter.convertToDataModelType(aggregate.first());
//
//        if (dataset != null) {
//            datasetAcl = dataset.getAcl();
//        }
//
//        return endQuery("get dataset Acl", startTime, datasetAcl);
//    }

//    @Override
//    public QueryResult<DatasetAclEntry> setDatasetAcl(long datasetId, DatasetAclEntry acl, boolean override) throws CatalogDBException {
//        long startTime = startQuery();
//        long studyId = getStudyIdByDatasetId(datasetId);
//
//        String member = acl.getMember();
//
//        // If there is a group in acl.getMember(), we will obtain all the users belonging to the groups and will check if any of them
//        // already have permissions on its own.
//        if (member.startsWith("@")) {
//            Group group = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member, Collections.emptyList()).first();
//
//            // Check if any user already have permissions set on their own.
//            QueryResult<DatasetAclEntry> datasetAcl = getDatasetAcl(datasetId, group.getUserIds());
//            if (datasetAcl.getNumResults() > 0) {
//                throw new CatalogDBException("Error when adding permissions in dataset. At least one user in " + group.getName()
//                        + " has already defined permissions for dataset " + datasetId);
//            }
//        } else {
//            // Check if the members of the new acl already have some permissions set
//            QueryResult<DatasetAclEntry> datasetAcls = getDatasetAcl(datasetId, acl.getMember());
//
//            if (datasetAcls.getNumResults() > 0 && override) {
//                unsetDatasetAcl(datasetId, Arrays.asList(member), Collections.emptyList());
//            } else if (datasetAcls.getNumResults() > 0 && !override) {
//                throw new CatalogDBException("setDatasetAcl: " + member + " already had an Acl set. If you "
//                        + "still want to set a new Acl and remove the old one, please use the override parameter.");
//            }
//        }
//
//        // Push the new acl to the list of acls.
//        Document queryDocument = new Document(PRIVATE_ID, datasetId);
//        Document update = new Document("$push", new Document(QueryParams.ACL.key(), getMongoDBDocument(acl, "DatasetAcl")));
//        QueryResult<UpdateResult> updateResult = datasetCollection.update(queryDocument, update, null);
//
//        if (updateResult.first().getModifiedCount() == 0) {
//            throw new CatalogDBException("setDatasetAcl: An error occurred when trying to share file " + datasetId + " with " + member);
//        }
//
//        return endQuery("setDatasetAcl", startTime, Arrays.asList(acl));
//    }

//    @Override
//    public void unsetDatasetAcl(long datasetId, List<String> members, List<String> permissions) throws CatalogDBException {
//        // Check that all the members (users) are correct and exist.
//        checkMembers(dbAdaptorFactory, getStudyIdByDatasetId(datasetId), members);
//
//        // Remove the permissions the members might have had
//        for (String member : members) {
//            Document query = new Document(PRIVATE_ID, datasetId).append(QueryParams.ACL_MEMBER.key(), member);
//            Bson update;
//            if (permissions.size() == 0) {
//                update = new Document("$pull", new Document("acl", new Document("member", member)));
//            } else {
//                update = new Document("$pull", new Document("acl.$.permissions", new Document("$in", permissions)));
//            }
//            QueryResult<UpdateResult> updateResult = datasetCollection.update(query, update, null);
//            if (updateResult.first().getModifiedCount() == 0) {
//                throw new CatalogDBException("unsetDatasetAcl: An error occurred when trying to stop sharing dataset " + datasetId
//                        + " with other " + member + ".");
//            }
//        }
//
//        // Remove possible datasetAcls that might have permissions defined but no users
////        Bson queryBson = new Document(QueryParams.ID.key(), datasetId)
////                .append(QueryParams.ACL_MEMBER.key(),
////                        new Document("$exists", true).append("$eq", Collections.emptyList()));
////        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
////        datasetCollection.update(queryBson, update, null);
//    }

//    @Override
//    public void unsetDatasetAclsInStudy(long studyId, List<String> members) throws CatalogDBException {
//        // Check that all the members (users) are correct and exist.
//        checkMembers(dbAdaptorFactory, studyId, members);
//
//        // Remove the permissions the members might have had
//        for (String member : members) {
//            Document query = new Document(PRIVATE_STUDY_ID, studyId).append(QueryParams.ACL_MEMBER.key(), member);
//            Bson update = new Document("$pull", new Document("acl", new Document("member", member)));
//            datasetCollection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
//        }
//
////        // Remove possible DatasetAcls that might have permissions defined but no users
////        Bson queryBson = new Document(PRIVATE_STUDY_ID, studyId)
////                .append(CatalogSampleDBAdaptor.QueryParams.ACL_MEMBER.key(),
////                        new Document("$exists", true).append("$eq", Collections.emptyList()));
////        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
////        datasetCollection.update(queryBson, update, new QueryOptions(MongoDBCollection.MULTI, true));
//    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return datasetCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return datasetCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public DBIterator<Dataset> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = datasetCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, datasetConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = datasetCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(datasetCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(datasetCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(datasetCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        DBIterator<Dataset> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null
                    ? QueryParams.getParam(entry.getKey())
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
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    @Override
    public QueryResult<DatasetAclEntry> createAcl(long id, DatasetAclEntry acl) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.createAcl(id, acl, datasetCollection, "DatasetAcl");
        return endQuery("create dataset Acl", startTime, Arrays.asList(aclDBAdaptor.createAcl(id, acl)));
    }

    @Override
    public QueryResult<DatasetAclEntry> getAcl(long id, List<String> members) throws CatalogDBException {
        long startTime = startQuery();
//
//        List<DatasetAclEntry> acl = null;
//        QueryResult<Document> aggregate = CatalogMongoDBUtils.getAcl(id, members, datasetCollection, logger);
//        Dataset dataset = datasetConverter.convertToDataModelType(aggregate.first());
//
//        if (dataset != null) {
//            acl = dataset.getAcl();
//        }

        return endQuery("get dataset Acl", startTime, aclDBAdaptor.getAcl(id, members));
    }

    @Override
    public void removeAcl(long id, String member) throws CatalogDBException {
//        CatalogMongoDBUtils.removeAcl(id, member, datasetCollection);
        aclDBAdaptor.removeAcl(id, member);
    }

    @Override
    public QueryResult<DatasetAclEntry> setAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.setAclsToMember(id, member, permissions, datasetCollection);
        return endQuery("Set Acls to member", startTime, Arrays.asList(aclDBAdaptor.setAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<DatasetAclEntry> addAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.addAclsToMember(id, member, permissions, datasetCollection);
        return endQuery("Add Acls to member", startTime, Arrays.asList(aclDBAdaptor.addAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<DatasetAclEntry> removeAclsFromMember(long id, String member, List<String> permissions) throws CatalogDBException {
//        CatalogMongoDBUtils.removeAclsFromMember(id, member, permissions, datasetCollection);
        long startTime = startQuery();
        return endQuery("Remove Acls from member", startTime, Arrays.asList(aclDBAdaptor.removeAclsFromMember(id, member, permissions)));
    }

    public void removeAclsFromStudy(long studyId, String member) throws CatalogDBException {
        aclDBAdaptor.removeAclsFromStudy(studyId, member);
    }
}
