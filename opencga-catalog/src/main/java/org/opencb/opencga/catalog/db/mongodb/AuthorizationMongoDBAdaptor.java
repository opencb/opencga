package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.*;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationDBAdaptor;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.mongodb.converters.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.AbstractAcl;
import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory.*;

/**
 * Created by pfurio on 20/04/17.
 */
public class AuthorizationMongoDBAdaptor extends MongoDBAdaptor implements AuthorizationDBAdaptor {

    private MongoDataStore mongoDataStore;

    private Map<String, MongoDBCollection> dbCollectionMap = new HashMap<>();

    private StudyConverter studyConverter;
    private CohortConverter cohortConverter;
    private DatasetConverter datasetConverter;
    private FileConverter fileConverter;
    private IndividualConverter individualConverter;
    private JobConverter jobConverter;
    private SampleConverter sampleConverter;
    private PanelConverter panelConverter;

    public AuthorizationMongoDBAdaptor(Configuration configuration) throws CatalogDBException {
        super(LoggerFactory.getLogger(AuthorizationMongoDBAdaptor.class));
        initMongoDatastore(configuration);
        initCollectionConnections();
        initConverters();
    }

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        ACL("acl", TEXT_ARRAY, ""),
        MEMBER("member", TEXT, ""),
        PERMISSIONS("permissions", TEXT_ARRAY, ""),
        ACL_MEMBER("acl.member", TEXT_ARRAY, ""),
        ACL_PERMISSIONS("acl.permissions", TEXT_ARRAY, "");

        private static Map<String, QueryParams> map = new HashMap<>();
        static {
            for (QueryParams param : QueryParams.values()) {
                map.put(param.key(), param);
            }
        }

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    private void initConverters() {
        this.studyConverter = new StudyConverter();
        this.cohortConverter = new CohortConverter();
        this.datasetConverter = new DatasetConverter();
        this.fileConverter = new FileConverter();
        this.individualConverter = new IndividualConverter();
        this.jobConverter = new JobConverter();
        this.sampleConverter = new SampleConverter();
        this.panelConverter = new PanelConverter();
    }

    private void initCollectionConnections() {
        this.dbCollectionMap.put(STUDY_COLLECTION, mongoDataStore.getCollection(STUDY_COLLECTION));
        this.dbCollectionMap.put(COHORT_COLLECTION, mongoDataStore.getCollection(COHORT_COLLECTION));
        this.dbCollectionMap.put(DATASET_COLLECTION, mongoDataStore.getCollection(DATASET_COLLECTION));
        this.dbCollectionMap.put(FILE_COLLECTION, mongoDataStore.getCollection(FILE_COLLECTION));
        this.dbCollectionMap.put(INDIVIDUAL_COLLECTION, mongoDataStore.getCollection(INDIVIDUAL_COLLECTION));
        this.dbCollectionMap.put(JOB_COLLECTION, mongoDataStore.getCollection(JOB_COLLECTION));
        this.dbCollectionMap.put(SAMPLE_COLLECTION, mongoDataStore.getCollection(SAMPLE_COLLECTION));
        this.dbCollectionMap.put(PANEL_COLLECTION, mongoDataStore.getCollection(PANEL_COLLECTION));
    }

    private void initMongoDatastore(Configuration configuration) throws CatalogDBException {
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", configuration.getCatalog().getDatabase().getUser())
                .add("password", configuration.getCatalog().getDatabase().getPassword())
                .add("authenticationDatabase", configuration.getCatalog().getDatabase().getOptions().get("authenticationDatabase"))
                .build();

        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : configuration.getCatalog().getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }

        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);
        mongoDataStore = mongoManager.get(getCatalogDatabase(configuration), mongoDBConfiguration);
        if (mongoDataStore == null) {
            throw new CatalogDBException("Unable to connect to MongoDB");
        }
    }

    private String getCatalogDatabase(Configuration configuration) {
        String database;
        if (StringUtils.isNotEmpty(configuration.getDatabasePrefix())) {
            if (!configuration.getDatabasePrefix().endsWith("_")) {
                database = configuration.getDatabasePrefix() + "_catalog";
            } else {
                database = configuration.getDatabasePrefix() + "catalog";
            }
        } else {
            database = "opencga_catalog";
        }
        return database;
    }

    private void validateCollection(String collection) throws CatalogDBException {
        switch (collection) {
            case STUDY_COLLECTION:
            case COHORT_COLLECTION:
            case INDIVIDUAL_COLLECTION:
            case DATASET_COLLECTION:
            case JOB_COLLECTION:
            case FILE_COLLECTION:
            case SAMPLE_COLLECTION:
            case PANEL_COLLECTION:
                return;
            default:
                throw new CatalogDBException("Unexpected parameter received. " + collection + " has been received.");
        }
    }

    private GenericDocumentComplexConverter<? extends AbstractAcl> getConverter(String collection) throws CatalogException {
        switch (collection) {
            case STUDY_COLLECTION:
                return studyConverter;
            case COHORT_COLLECTION:
                return cohortConverter;
            case INDIVIDUAL_COLLECTION:
                return individualConverter;
            case DATASET_COLLECTION:
                return datasetConverter;
            case JOB_COLLECTION:
                return jobConverter;
            case FILE_COLLECTION:
                return fileConverter;
            case SAMPLE_COLLECTION:
                return sampleConverter;
            case PANEL_COLLECTION:
                return panelConverter;
            default:
                throw new CatalogException("Unexpected parameter received. " + collection + " has been received.");
        }
    }

    @Override
    public <E extends AbstractAclEntry> QueryResult<E> get(long resourceId, List<String> members, String entity) throws CatalogException {
        validateCollection(entity);
        long startTime = startQuery();
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, resourceId)));
        aggregation.add(Aggregates.project(
                Projections.include(QueryParams.ID.key(), QueryParams.ACL.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.ACL.key()));

        List<Bson> filters = new ArrayList<>();
        if (members != null && members.size() > 0) {
            filters.add(Filters.in(QueryParams.ACL_MEMBER.key(), members));
        }

        if (filters.size() > 0) {
            Bson filter = filters.size() == 1 ? filters.get(0) : Filters.and(filters);
            aggregation.add(Aggregates.match(filter));
        }

        for (Bson bson : aggregation) {
            logger.debug("Get Acl: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        }

        QueryResult<Document> aggregate = dbCollectionMap.get(entity).aggregate(aggregation, null);

        List<E> retList = new ArrayList<>();

        if (aggregate.getNumResults() > 0) {
            for (Document document : aggregate.getResult()) {
                retList.add((E) getConverter(entity).convertToDataModelType(document).getAcl().get(0));
            }
        }

        return endQuery(Long.toString(resourceId), startTime, retList);
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> get(List<Long> resourceIds, List<String> members, String entity)
            throws CatalogException {
        List<QueryResult<E>> retList = new ArrayList<>(resourceIds.size());
        for (Long resourceId : resourceIds) {
            retList.add(get(resourceId, members, entity));
        }
        return retList;
    }

    @Override
    public void removeFromStudy(long studyId, String member, String entity) throws CatalogException {
        validateCollection(entity);
        Document query = new Document()
                .append(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.ACL_MEMBER.key(), member);
        Bson update = new Document("$pull", new Document("acl", new Document("member", member)));
        dbCollectionMap.get(entity).update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));

        logger.debug("Remove all the Acls for member {} in study {}", member, studyId);
    }

    @Override
    public void setToMembers(List<Long> resourceIds, List<String> members, List<String> permissions, String entity)
            throws CatalogDBException {
        validateCollection(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);
        for (String member : members) {
            logger.debug("Setting ACLs for {}", member);

            Document query = new Document()
                    .append("$isolated", 1)
                    .append(PRIVATE_ID, new Document("$in", resourceIds))
                    .append(QueryParams.ACL_MEMBER.key(), member);
            Document update = new Document("$set", new Document("acl.$.permissions", permissions));

            logger.debug("Set Acls (set): Query {}, Push {}",
                    query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            QueryResult<UpdateResult> queryResult = collection.update(query, update, new QueryOptions("multi", true));
            logger.debug("{} out of {} acls added to {}", queryResult.first().getModifiedCount(), queryResult.first().getMatchedCount(),
                    member);

            // Try to do the same but only for resources where the member was not given any permissions
            query.put(QueryParams.ACL_MEMBER.key(), new Document("$ne", member));

            // Create the ACL entry
            Document aclEntry = new Document()
                    .append(QueryParams.MEMBER.key(), member)
                    .append(QueryParams.PERMISSIONS.key(), permissions);
            update = new Document("$push", new Document(QueryParams.ACL.key(), aclEntry));
            logger.debug("Set Acls (Push): Query {}, Push {}",
                    query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            queryResult = collection.update(query, update, new QueryOptions("multi", true));
            logger.debug("{} out of {} acls created for {}", queryResult.first().getModifiedCount(), queryResult.first().getMatchedCount(),
                    member);
        }
    }

    @Override
    public void addToMembers(List<Long> resourceIds, List<String> members, List<String> permissions, String entity)
            throws CatalogDBException {
        validateCollection(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);
        for (String member : members) {
            logger.debug("Adding ACLs for {}", member);

            // Try to update and add the new permissions (if the member already had those permissions set)
            Document queryDocument = new Document()
                    .append("$isolated", 1)
                    .append(PRIVATE_ID, new Document("$in", resourceIds))
                    .append(QueryParams.ACL_MEMBER.key(), member);

            Document update = new Document("$addToSet", new Document("acl.$.permissions", new Document("$each", permissions)));
            logger.debug("Add Acls (addToSet): Query {}, Push {}",
                    queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            QueryResult<UpdateResult> pushUpdate = collection.update(queryDocument, update, new QueryOptions("multi", true));
            logger.debug("{} out of {} acls added to {}", pushUpdate.first().getModifiedCount(), pushUpdate.first().getMatchedCount(),
                    member);

            // Try to do the same but only for resources where the member was not given any permissions
            queryDocument.put(QueryParams.ACL_MEMBER.key(), new Document("$ne", member));

            // Create the ACL entry
            Document aclEntry = new Document()
                    .append(QueryParams.MEMBER.key(), member)
                    .append(QueryParams.PERMISSIONS.key(), permissions);
            update = new Document("$push", new Document(QueryParams.ACL.key(), aclEntry));
            logger.debug("Add Acls (Push): Query {}, Push {}",
                    queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            pushUpdate = collection.update(queryDocument, update, new QueryOptions("multi", true));
            logger.debug("{} out of {} acls created for {}", pushUpdate.first().getModifiedCount(), pushUpdate.first().getMatchedCount(),
                    member);
        }
    }

    @Override
    public void removeFromMembers(List<Long> resourceIds, List<String> members, @Nullable List<String> permissions, String entity)
            throws CatalogDBException {
        validateCollection(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);
        if (permissions == null || permissions.size() == 0) {
            // Remove the members from the acl table
            Document queryDocument = new Document()
                    .append("$isolated", 1)
                    .append(PRIVATE_ID, new Document("$in", resourceIds));

            Document update = new Document("$pull", new Document(QueryParams.ACL.key(),
                    new Document(QueryParams.MEMBER.key(), new Document("$in", members))));

            QueryResult<UpdateResult> updateResult = collection.update(queryDocument, update, new QueryOptions("multi", true));

            logger.debug("Remove Acl: {} out of {} removed for members {}", updateResult.first().getModifiedCount(),
                    updateResult.first().getMatchedCount(), members);
        } else {
            // Remove the selected permissions from the array of permissions of each member
            for (String member : members) {
                Document queryDocument = new Document()
                        .append("$isolated", 1)
                        .append(PRIVATE_ID, new Document("$in", resourceIds))
                        .append(QueryParams.ACL_MEMBER.key(), member);

                Bson update = Updates.pullAll("acl.$.permissions", permissions);
                logger.debug("Remove Acl: Query {}, Pull {}",
                        queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                        update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

                QueryResult<UpdateResult> pullUpdate = collection.update(queryDocument, update, new QueryOptions("multi", true));

                logger.debug("Remove Acl: {} out of {} acls removed from {}", pullUpdate.first().getModifiedCount(),
                        pullUpdate.first().getMatchedCount(), members);
            }
        }
    }
}
