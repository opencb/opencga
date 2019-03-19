package org.opencb.opencga.storage.mongodb.metadata;

import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.metadata.adaptors.TaskMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;

import java.util.Iterator;

/**
 * Created by jacobo on 20/01/19.
 */
public class MongoDBTaskMetadataDBAdaptor extends AbstractMongoDBAdaptor<TaskMetadata>
        implements TaskMetadataDBAdaptor {

    public MongoDBTaskMetadataDBAdaptor(MongoDataStore db, String collectionName) {
        super(db, collectionName, TaskMetadata.class);
    }

    @Override
    public TaskMetadata getTask(int studyId, int taskId, Long timeStamp) {
        return get(studyId, taskId, null);
    }

    @Override
    public Iterator<TaskMetadata> taskIterator(int studyId, boolean reversed) {
        QueryOptions options = new QueryOptions(QueryOptions.ORDER, reversed ? QueryOptions.ASCENDING : QueryOptions.DESCENDING);
        Bson query = buildQuery(studyId);
        return iterator(query, options);
    }

    @Override
    public void updateTask(int studyId, TaskMetadata task, Long timeStamp) {
        update(studyId, task.getId(), task);
    }
}
