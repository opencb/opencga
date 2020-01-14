package org.opencb.opencga.storage.mongodb.metadata;

import com.google.common.collect.Iterators;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.metadata.adaptors.TaskMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

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
    public Iterator<TaskMetadata> taskIterator(int studyId, List<TaskMetadata.Status> statusFilter, boolean reversed) {
        QueryOptions options = new QueryOptions(QueryOptions.ORDER, reversed ? QueryOptions.ASCENDING : QueryOptions.DESCENDING);
        Bson query = buildQuery(studyId);
        if (statusFilter == null) {
            return iterator(query, options);
        } else {
            EnumSet<TaskMetadata.Status> set = EnumSet.copyOf(statusFilter);
            return Iterators.filter(iterator(query, options), t -> set.contains(t.currentStatus()));
        }
    }

    @Override
    public void updateTask(int studyId, TaskMetadata task, Long timeStamp) {
        update(studyId, task.getId(), task);
    }
}
