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
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
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
    private final MongoLock mongoLock;
    private final MongoDBCollection collection;

    public MongoDBProjectMetadataDBAdaptor(MongoDataStore db, String collectionName) {
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
        return collection.find(new Document("_id", ID), new Document(), new GenericDocumentComplexConverter<>(ProjectMetadata.class),
                new QueryOptions());
    }

    @Override
    public QueryResult updateProjectMetadata(ProjectMetadata projectMetadata) {
        Document mongo = new GenericDocumentComplexConverter<>(ProjectMetadata.class).convertToStorageType(projectMetadata);

        // Update field by field, instead of replacing the whole object to preserve existing fields like "_lock"
        Document query = new Document("_id", ID);
        List<Bson> updates = new ArrayList<>(mongo.size());
        mongo.forEach((s, o) -> updates.add(new Document("$set", new Document(s, o))));

        return collection.update(query, Updates.combine(updates), new QueryOptions(UPSERT, true));
    }

}
