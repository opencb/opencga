package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
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
    boolean generateIdVersionQuery(Query query, List<Bson> bsonQueryList) throws CatalogDBException {
        if (!query.containsKey(VERSION) || query.getAsIntegerList(VERSION).size() == 1) {
            return false;
        }
        if (!query.containsKey(ID)) {
            return false;
        }

        List<?> idList = query.getAsStringList(ID);
        List<Integer> versionList = query.getAsIntegerList(VERSION);

        if (versionList.size() > 1 && idList.size() > 1 && versionList.size() != idList.size()) {
            throw new CatalogDBException("The size of the array of versions should match the size of the array of ids to be queried");
        }

        List<Bson> bsonQuery = new ArrayList<>();
        for (int i = 0; i < versionList.size(); i++) {
            Document docQuery = new Document(VERSION, versionList.get(i));
            if (idList.size() == 1) {
                docQuery.put(ID, idList.get(0));
            } else {
                docQuery.put(ID, idList.get(i));
            }
            bsonQuery.add(docQuery);
        }

        if (!bsonQuery.isEmpty()) {
            bsonQueryList.add(Filters.or(bsonQuery));

            query.remove(ID);
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
        T execute(List<Document> entryList) throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;
    }

    public interface NonVersionedModelExecution<T> {
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
        document.put(VERSION, 1);
        document.put(LAST_OF_VERSION, true);
        collection.insert(session, document, QueryOptions.empty());
        archiveCollection.insert(session, document, QueryOptions.empty());
    }

    protected <T> T update(ClientSession session, Bson sourceQuery, VersionedModelExecution<T> update)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(session, sourceQuery, Collections.emptyList(), update, Collections.emptyList(), null, null);
    }

    protected <T> T update(ClientSession session, Bson sourceQuery, List<String> includeFields, VersionedModelExecution<T> update)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(session, sourceQuery, includeFields, update, Collections.emptyList(), null, null);
    }

    protected <T, E> T update(ClientSession session, Bson sourceQuery, VersionedModelExecution<T> update,
                              PostVersionIncrementIterator<E> postVersionIncrementIterator,
                              ReferenceModelExecution<E> postVersionIncrementExecution)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(session, sourceQuery, Collections.emptyList(), update, Collections.emptyList(), postVersionIncrementIterator,
                postVersionIncrementExecution);
    }

    protected <T, E> T update(ClientSession session, Bson sourceQuery, List<String> includeFields, VersionedModelExecution<T> update,
                           List<String> postVersionIncrementAdditionalIncludeFields, PostVersionIncrementIterator<E> dbIterator,
                           ReferenceModelExecution<E> postVersionIncrementExecution)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        String uuid = getClientSessionUuid(session);

        // 1. Increment version
        // 1.1 Only increase version of those documents not already increased by same transaction id
        Set<String> toInclude = new HashSet<>(Arrays.asList(ID, VERSION, PRIVATE_TRANSACTION_ID));
        if (CollectionUtils.isNotEmpty(includeFields)) {
            toInclude.addAll(includeFields);
        }
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, new ArrayList<>(toInclude));
        List<String> allIds = new LinkedList<>();
        List<String> idsChanged = new LinkedList<>();
        List<Document> entryList = new LinkedList<>();
        try (MongoDBIterator<Document> iterator = collection.iterator(session, sourceQuery, null, null, options)) {
            while (iterator.hasNext()) {
                Document result = iterator.next();
                entryList.add(result);

                String id = result.getString(ID);
                int version = result.get(VERSION, Number.class).intValue();
                String transactionId = result.getString(PRIVATE_TRANSACTION_ID);
                allIds.add(id);

                if (!uuid.equals(transactionId)) {
                    // If the version hasn't been incremented yet in this transaction
                    idsChanged.add(id);

                    Document collectionUpdate = new Document();
                    Document archiveCollectionUpdate = new Document();
                    processLastOfVersionChanges(result, collectionUpdate, archiveCollectionUpdate);

                    Bson bsonQuery = Filters.and(
                            Filters.eq(ID, id),
                            Filters.eq(VERSION, version)
                    );
                    // Update previous version
                    logger.debug("Updating previous version: query : {}, update: {}", bsonQuery.toBsonDocument(),
                            archiveCollectionUpdate.toBsonDocument());
                    archiveCollection.update(session, bsonQuery, new Document("$set", archiveCollectionUpdate), QueryOptions.empty());

                    // Add current transaction id to the document so we don't enter here twice in the same transaction
                    collectionUpdate.put(PRIVATE_TRANSACTION_ID, uuid);
                    // Update current version
                    logger.debug("Updating current version: query : {}, update: {}", bsonQuery.toBsonDocument(),
                            collectionUpdate.toBsonDocument());
                    collection.update(session, bsonQuery, new Document("$set", collectionUpdate), QueryOptions.empty());
                }
            }
        }

        // 2. Execute main update
        T executionResult = update.execute(entryList);

        // 3. Fetch document containing update and copy into the archive collection
        Bson bsonQuery = Filters.in(ID, allIds);
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
                        Filters.eq(ID, fixedResult.get(ID)),
                        Filters.eq(VERSION, fixedResult.get(VERSION))
                );
                logger.debug("Copying current document to archive: query : {}", tmpBsonQuery.toBsonDocument());
                archiveCollection.update(session, tmpBsonQuery, fixedResult, upsertOptions);
            }
        }

        // 4. Perform any additional reference checks/updates over those that have increased its version in this call
        if (!idsChanged.isEmpty()) {
            Query query = new Query(ID, idsChanged);
            if (postVersionIncrementExecution != null) {
                List<String> includeList = new ArrayList<>(Arrays.asList(ID, VERSION));
                if (postVersionIncrementExecution != null) {
                    includeList.addAll(postVersionIncrementAdditionalIncludeFields);
                }

                logger.debug("Executing react code after incrementing version: query : {}={}", ID, idsChanged);
                options = new QueryOptions()
                        .append(QueryOptions.INCLUDE, includeList)
                        .append(MongoDBCollection.NO_CURSOR_TIMEOUT, true);
                try (DBIterator<E> iterator = dbIterator.iterator(session, query, options)) {
                    postVersionIncrementExecution.execute(iterator);
                }
            }
        }

        return executionResult;
    }

    protected <T> T updateWithoutVersionIncrement(Bson sourceQuery, NonVersionedModelExecution<T> update)
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
                        Filters.eq(ID, result.get(ID)),
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
     * @param id            ID of the element to be recovered.
     * @param version       Version to be recovered.
     * @return the new latest document that will be written in the database.
     * @throws CatalogDBException in case of any issue.
     */
    protected Document revertToVersion(ClientSession clientSession, String id, int version) throws CatalogDBException {
        Bson query = Filters.and(
                Filters.eq(ID, id),
                Filters.eq(VERSION, version)
        );
        DataResult<Document> result = archiveCollection.find(clientSession, query, EXCLUDE_MONGO_ID);
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Could not find version '" + version + "'");
        }
        Document document = result.first();

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, VERSION);
        // Find out latest version available
        result = collection.find(clientSession, Filters.eq(ID, id), options);
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Unexpected error. Could not find 'id': " + id);
        }
        int lastVersion = result.first().getInteger(VERSION);

        // Delete previous version from active collection
        collection.remove(clientSession, Filters.eq(ID, id), QueryOptions.empty());

        // Edit previous version from archive collection
        query = Filters.and(
                Filters.eq(ID, id),
                Filters.eq(VERSION, lastVersion)
        );
        archiveCollection.update(clientSession, query, Updates.set(LAST_OF_VERSION, false), QueryOptions.empty());

        // Edit private fields from document to be restored
        document.put(VERSION, lastVersion + 1);

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
     * Given the current document, it writes the changes that need to be applied in collectionUpdate and archiveCollectionUpdate.
     * It takes care of the private fields: LAST_OF_VERSION
     *
     * @param document                Current document.
     * @param collectionUpdate        Empty document where we will put the changes to be applied to the main collection.
     * @param archiveCollectionUpdate Empty document where we will put the changes to be applied to the archive collection.
     */
    private void processLastOfVersionChanges(Document document, Document collectionUpdate, Document archiveCollectionUpdate) {
        int version = document.get(VERSION, Number.class).intValue();

        // And set the document in archive with the flag LAST_OF_VERSION to false
        archiveCollectionUpdate.put(LAST_OF_VERSION, false);

        // We update the information for the new version of the document
        collectionUpdate.put(VERSION, version + 1);
    }

}
