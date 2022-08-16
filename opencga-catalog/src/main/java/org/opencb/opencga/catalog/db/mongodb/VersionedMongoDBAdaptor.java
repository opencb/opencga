package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.getMongoDBDocument;

public class VersionedMongoDBAdaptor {

    private final Logger logger;
    private final MongoDBCollection collection;
    private final MongoDBCollection archiveCollection;
    private final MongoDBCollection deletedCollection;

    private static final String PRIVATE_TRANSACTION_ID = "_transactionId";

    public VersionedMongoDBAdaptor(MongoDBCollection collection, MongoDBCollection archiveCollection, MongoDBCollection deletedCollection) {
        this.collection = collection;
        this.archiveCollection = archiveCollection;
        this.deletedCollection = deletedCollection;
        logger = LoggerFactory.getLogger(VersionedMongoDBAdaptor.class);
    }


    /**
     * Generate complex query where [{id - version}, {id2 - version2}] pairs will be queried.
     *
     * @param query         Query object.
     * @param bsonQueryList Final bson query object.
     * @return a boolean indicating whether the complex query was generated or not.
     * @throws CatalogDBException If the size of the array of ids does not match the size of the array of version.
     */
    boolean generateUidVersionQuery(Query query, List<Bson> bsonQueryList) throws CatalogDBException {
        if (!query.containsKey(VERSION) || query.getAsIntegerList(VERSION).size() == 1) {
            return false;
        }
        if (!query.containsKey(PRIVATE_UID) && !query.containsKey(ID) && !query.containsKey(PRIVATE_UUID)) {
            return false;
        }
        int numIds = 0;
        numIds += query.containsKey(ID) ? 1 : 0;
        numIds += query.containsKey(PRIVATE_UID) ? 1 : 0;
        numIds += query.containsKey(PRIVATE_UUID) ? 1 : 0;

        if (numIds > 1) {
            List<Integer> versionList = query.getAsIntegerList(VERSION);
            if (versionList.size() > 1) {
                throw new CatalogDBException("Cannot query by more than one version when more than one id type is being queried");
            }
            return false;
        }

        String idQueried = PRIVATE_UID;
        idQueried = query.containsKey(ID) ? ID : idQueried;
        idQueried = query.containsKey(PRIVATE_UUID) ? PRIVATE_UUID : idQueried;

        List<?> idList;
        if (PRIVATE_UID.equals(idQueried)) {
            idList = query.getAsLongList(PRIVATE_UID);
        } else {
            idList = query.getAsStringList(idQueried);
        }
        List<Integer> versionList = query.getAsIntegerList(VERSION);

        if (versionList.size() > 1 && idList.size() > 1 && versionList.size() != idList.size()) {
            throw new CatalogDBException("The size of the array of versions should match the size of the array of ids to be queried");
        }

        List<Bson> bsonQuery = new ArrayList<>();
        for (int i = 0; i < versionList.size(); i++) {
            Document docQuery = new Document(VERSION, versionList.get(i));
            if (idList.size() == 1) {
                docQuery.put(idQueried, idList.get(0));
            } else {
                docQuery.put(idQueried, idList.get(i));
            }
            bsonQuery.add(docQuery);
        }

        if (!bsonQuery.isEmpty()) {
            bsonQueryList.add(Filters.or(bsonQuery));

            query.remove(idQueried);
            query.remove(VERSION);

            return true;
        }

        return false;
    }

    private String getClientSessionUuid(ClientSession session) {
        UUID sessionUUID = session.getServerSession().getIdentifier().getBinary("id").asUuid();
        long transactionNumber = session.getServerSession().getTransactionNumber();

        // Generate new UUID with sessionId + transactionNumber
        return new UUID(sessionUUID.getMostSignificantBits(), sessionUUID.getLeastSignificantBits() + transactionNumber).toString();
    }

    public interface VersionedModelExecution<T> {
        T execute() throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;
    }

    public interface ReferenceModelExecution<T> {
        void execute(DBIterator<T> iterator) throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;
    }

    @FunctionalInterface
    public interface PostVersionIncrementIterator<T> {
        DBIterator<T> iterator(ClientSession session, Query query, QueryOptions options)
                throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
    }

    protected void insert(ClientSession session, Document document) {
        String uuid = getClientSessionUuid(session);
        document.put(PRIVATE_TRANSACTION_ID, uuid);
        collection.insert(session, document, QueryOptions.empty());
        archiveCollection.insert(session, document, QueryOptions.empty());
    }

    protected <T, E> T update(ClientSession session, Bson sourceQuery, VersionedModelExecution<T> update,
                              PostVersionIncrementIterator<E> postVersionIncrementIterator,
                              ReferenceModelExecution<E> postVersionIncrementExecution)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(session, sourceQuery, update, Collections.emptyList(), postVersionIncrementIterator, postVersionIncrementExecution);
    }

    protected <T, E> T update(ClientSession session, Bson sourceQuery, VersionedModelExecution<T> update,
                           List<String> postVersionIncrementAdditionalIncludeFields, PostVersionIncrementIterator<E> dbIterator,
                           ReferenceModelExecution<E> postVersionIncrementExecution)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        String uuid = getClientSessionUuid(session);

        // 1. Increment version
        // 1.1 Only increase version of those documents not already increased by same transaction id
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(PRIVATE_UID, VERSION, RELEASE_FROM_VERSION, PRIVATE_TRANSACTION_ID));
        List<Long> allUids = new LinkedList<>();
        List<Long> uidsChanged = new LinkedList<>();
        try (MongoDBIterator<Document> iterator = collection.iterator(session, sourceQuery, null, null, options)) {
            while (iterator.hasNext()) {
                Document result = iterator.next();

                long uid = result.get(PRIVATE_UID, Number.class).longValue();
                int version = result.get(VERSION, Number.class).intValue();
                String transactionId = result.getString(PRIVATE_TRANSACTION_ID);
                allUids.add(uid);

                if (!uuid.equals(transactionId)) {
                    // If the version hasn't been incremented yet in this transaction
                    uidsChanged.add(uid);

                    Document collectionUpdate = new Document();
                    Document archiveCollectionUpdate = new Document();
                    processReleaseSnapshotChanges(result, collectionUpdate, archiveCollectionUpdate);

                    Bson bsonQuery = Filters.and(
                            Filters.eq(PRIVATE_UID, uid),
                            Filters.eq(VERSION, version)
                    );
                    // Update previous version
                    logger.debug("Updating previous version: query : {}, update: {}",
                            bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            archiveCollectionUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                    archiveCollection.update(session, bsonQuery, new Document("$set", archiveCollectionUpdate), QueryOptions.empty());

                    // Add current transaction id to the document so we don't enter here twice in the same transaction
                    collectionUpdate.put(PRIVATE_TRANSACTION_ID, uuid);
                    // Update current version
                    logger.debug("Updating current version: query : {}, update: {}",
                            bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            collectionUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                    collection.update(session, bsonQuery, new Document("$set", collectionUpdate), QueryOptions.empty());
                }
            }
        }

        // 2. Execute main update
        T executionResult = update.execute();

        // 3. Fetch document containing update and copy into the archive collection
        Bson bsonQuery = Filters.in(PRIVATE_UID, allUids);
        options = new QueryOptions(MongoDBCollection.NO_CURSOR_TIMEOUT, true);
        QueryOptions upsertOptions = new QueryOptions()
                .append(MongoDBCollection.REPLACE, true)
                .append(MongoDBCollection.UPSERT, true);
        try (MongoDBIterator<Document> iterator = collection.iterator(session, bsonQuery, null, null, options)) {
            while (iterator.hasNext()) {
                Document result = iterator.next();
                result.remove(PRIVATE_MONGO_ID);
                result.put(PRIVATE_TRANSACTION_ID, uuid);

                // Some annotations have "." as part of their keys. Mongo does not support that so we call this method to replace them.
                Document fixedResult = GenericDocumentComplexConverter.replaceDots(result);

                // Insert/replace in archive collection
                Bson tmpBsonQuery = Filters.and(
                        Filters.eq(PRIVATE_UID, fixedResult.get(PRIVATE_UID)),
                        Filters.eq(VERSION, fixedResult.get(VERSION))
                );
                logger.debug("Copying current document to archive: query : {}",
                        tmpBsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                archiveCollection.update(session, tmpBsonQuery, fixedResult, upsertOptions);
            }
        }

        // 4. Perform any additional reference checks/updates over those that have increased its version in this call
        if (!uidsChanged.isEmpty()) {
            Query query = new Query(PRIVATE_UID, uidsChanged);
            if (postVersionIncrementExecution != null) {
                List<String> includeList = new ArrayList<>(Arrays.asList(PRIVATE_UID, PRIVATE_UUID, ID, PRIVATE_STUDY_UID, VERSION));
                if (postVersionIncrementExecution != null) {
                    includeList.addAll(postVersionIncrementAdditionalIncludeFields);
                }

                logger.debug("Executing react code after incrementing version: query : {}={}", PRIVATE_UID, uidsChanged);
                options = new QueryOptions()
                        .append(QueryOptions.INCLUDE, includeList)
                        .append(MongoDBCollection.NO_CURSOR_TIMEOUT, true);
                try (DBIterator<E> iterator = dbIterator.iterator(session, query, options)) {
                    postVersionIncrementExecution.execute(iterator);
                }
//                try (MongoDBIterator<Document> iterator = collection.iterator(session, query, null, null, options)) {
//                    postVersionIncrementExecution.execute(iterator);
//                }
            }
        }

        return executionResult;
    }

    protected <T> T updateWithoutVersionIncrement(Bson sourceQuery, VersionedModelExecution<T> update)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // Execute main update
        T executionResult = update.execute();

        // Fetch document containing update and copy into the archive collection
        QueryOptions options = new QueryOptions(MongoDBCollection.NO_CURSOR_TIMEOUT, true);
        QueryOptions upsertOptions = new QueryOptions()
                .append(MongoDBCollection.REPLACE, true)
                .append(MongoDBCollection.UPSERT, true);
        try (MongoDBIterator<Document> iterator = collection.iterator(sourceQuery, options)) {
            while (iterator.hasNext()) {
                Document result = iterator.next();
                result.remove(PRIVATE_MONGO_ID);

                // Insert/replace in archive collection
                Bson tmpBsonQuery = Filters.and(
                        Filters.eq(PRIVATE_UID, result.get(PRIVATE_UID)),
                        Filters.eq(VERSION, result.get(VERSION))
                );
                archiveCollection.update(tmpBsonQuery, result, upsertOptions);
            }
        }

        return executionResult;
    }

    /**
     * Revert to a previous version.
     *
     * @param clientSession ClientSession for transactional operations.
     * @param uid           UID of the element to be recovered.
     * @param version       Version to be recovered.
     * @return the new latest document that will be written in the database.
     * @throws CatalogDBException in case of any issue.
     */
    protected Document revertToVersion(ClientSession clientSession, long uid, int version) throws CatalogDBException {
        Bson query = Filters.and(
                Filters.eq(PRIVATE_UID, uid),
                Filters.eq(VERSION, version)
        );
        DataResult<Document> result = archiveCollection.find(clientSession, query, EXCLUDE_MONGO_ID);
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Could not find version '" + version + "'");
        }
        Document document = result.first();

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(RELEASE_FROM_VERSION, VERSION, LAST_OF_RELEASE));
        // Find out latest version available
        result = collection.find(clientSession, Filters.eq(PRIVATE_UID, uid), options);
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Unexpected error. Could not find 'uid': " + uid);
        }
        int lastVersion = result.first().getInteger(VERSION);

        // Delete previous version from active collection
        collection.remove(clientSession, Filters.eq(PRIVATE_UID, uid), QueryOptions.empty());

        // Edit previous version from archive collection
        query = Filters.and(
                Filters.eq(PRIVATE_UID, uid),
                Filters.eq(VERSION, lastVersion)
        );
        archiveCollection.update(clientSession, query, Updates.set(LAST_OF_RELEASE, false), QueryOptions.empty());

        // Edit private fields from document to be restored
        document.put(VERSION, lastVersion + 1);
        document.put(RELEASE_FROM_VERSION, result.first().get(RELEASE_FROM_VERSION));
        document.put(LAST_OF_RELEASE, result.first().get(LAST_OF_RELEASE));

        // Add restored element to main and archive collection
        collection.insert(clientSession, document, QueryOptions.empty());
        archiveCollection.insert(clientSession, document, QueryOptions.empty());

        return document;
    }

    protected void delete(ClientSession session, Bson query) {
        // Remove any old documents from the "delete" collection matching the criteria
        deletedCollection.remove(session, query, QueryOptions.empty());

        // Remove document from main collection
        collection.remove(session, query, QueryOptions.empty());

        // Add versioned documents to "delete" collection
        InternalStatus internalStatus = new InternalStatus(InternalStatus.DELETED);
        Document status;
        try {
            status = getMongoDBDocument(internalStatus, "status");
        } catch (CatalogDBException e) {
            status = new Document("id", InternalStatus.DELETED);
        }
        for (Document document : archiveCollection.find(session, query, QueryOptions.empty()).getResults()) {
            Document internal = document.get("internal", Document.class);
            internal.put("status", status);

            deletedCollection.insert(session, document, QueryOptions.empty());
        }

        // Remove documents from versioned collection
        archiveCollection.remove(session, query, QueryOptions.empty());
    }

    /**
     * Given the current document, it puts in collectionUpdate and archiveCollectionUpdate the changes that need to be applied.
     * It takes care of the private fields that allow performing queries by snapshot:
     * LAST_OF_VERSION, LAST_OF_RELEASE, RELEASE_FROM_VERSION
     *
     * @param document                Current document.
     * @param collectionUpdate        Empty document where we will put the changes to be applied to the main collection.
     * @param archiveCollectionUpdate Empty document where we will put the changes to be applied to the archive collection.
     */
    private void processReleaseSnapshotChanges(Document document, Document collectionUpdate, Document archiveCollectionUpdate) {
        int version = document.get(VERSION, Number.class).intValue();
        List<Integer> releaseFromVersion = document.getList(RELEASE_FROM_VERSION, Integer.class);

        // Current release number
        int release;
        if (releaseFromVersion.size() > 1) {
            release = releaseFromVersion.get(releaseFromVersion.size() - 1);

            // If it contains several releases, it means this is the first update on the current release, so we just need to take the
            // current release number out
            releaseFromVersion.remove(releaseFromVersion.size() - 1);
        } else {
            release = releaseFromVersion.get(0);

            // If it is 1, it means that the previous version being checked was made on this same release as well, so it won't be the
            // last version of the release
            archiveCollectionUpdate.put(LAST_OF_RELEASE, false);
        }
        archiveCollectionUpdate.put(RELEASE_FROM_VERSION, releaseFromVersion);
        archiveCollectionUpdate.put(LAST_OF_VERSION, false);

        // We update the information for the new version of the document
        collectionUpdate.put(RELEASE_FROM_VERSION, Collections.singletonList(release));
        collectionUpdate.put(VERSION, version + 1);
    }

}
