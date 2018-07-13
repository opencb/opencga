package org.opencb.opencga.storage.mongodb.metadata;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.ProjectMetadataAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoLock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.opencb.commons.datastore.mongodb.MongoDBCollection.UPSERT;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBProjectMetadataDBAdaptor implements ProjectMetadataAdaptor {

    public static final String ID = "META";
    public static final Document QUERY = new Document("_id", ID);
    public static final String COUNTERS_FIELD = "counters";
    private final MongoLock mongoLock;
    private final MongoDBCollection collection;
    private GenericDocumentComplexConverter<ProjectMetadata> converter;

    public MongoDBProjectMetadataDBAdaptor(MongoDataStore db, String collectionName) {
        converter = new GenericDocumentComplexConverter<>(ProjectMetadata.class);
        this.collection = db.getCollection(collectionName)
                .withReadPreference(ReadPreference.primary())
                .withWriteConcern(WriteConcern.ACKNOWLEDGED);
        mongoLock = new MongoLock(collection, "_lock");
    }

    @Override
    public long lockProject(long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        return mongoLock.lock(ID, lockDuration, timeout);
    }

    @Override
    public void unLockProject(long lockId) {
        mongoLock.unlock(ID, lockId);
    }

    @Override
    public QueryResult<ProjectMetadata> getProjectMetadata() {
        return collection.find(QUERY, new Document(), converter, new QueryOptions());
    }

    @Override
    public QueryResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters) {
        Document mongo = new GenericDocumentComplexConverter<>(ProjectMetadata.class).convertToStorageType(projectMetadata);

        // Update field by field, instead of replacing the whole object to preserve existing fields like "_lock"
        List<Bson> updates = new ArrayList<>(mongo.size());
        mongo.forEach((s, o) -> {
            // Do not update counters
            if (updateCounters || !s.equals(COUNTERS_FIELD)) {
                updates.add(new Document("$set", new Document(s, o)));
            }
        });

        return collection.update(QUERY, Updates.combine(updates), new QueryOptions(UPSERT, true));
    }

    @Override
    public int generateId(StudyConfiguration studyConfiguration, String idType) throws StorageEngineException {
        // Ignore study configuration. Same ID counter for all studies in the same database
        return generateId(idType, true);
    }

    private int generateId(String idType, boolean retry) throws StorageEngineException {
        String field = COUNTERS_FIELD + '.' + idType;
        Document projection = new Document(field, true);
        Bson inc = Updates.inc(field, 1);
        QueryOptions queryOptions = new QueryOptions("returnNew", true);
        QueryResult<Document> result = collection.findAndUpdate(QUERY, projection, null, inc, queryOptions);
        if (result.first() == null) {
            if (retry) {
                ensureProjectMetadataExists();
                return generateId(idType, false);
            } else {
                throw new StorageEngineException("Error creating new ID. Project Metadata not found");
            }
        } else {
            Document document = result.getResult().get(0);
            Document counters = document.get(COUNTERS_FIELD, Document.class);
            Integer id = counters.getInteger(idType);
//            System.out.println("New ID " + idType + " : " + id);
            return id;
        }
    }

    protected void ensureProjectMetadataExists() throws StorageEngineException {
        try {
            long lock = lockProject(100, 1000);
            if (getProjectMetadata().first() == null) {
                updateProjectMetadata(new ProjectMetadata(), false);
            }
            unLockProject(lock);
        } catch (InterruptedException | TimeoutException e) {
            throw new StorageEngineException("Unable to get lock over project", e);
        }
    }
}
