package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.core.metadata.adaptors.TaskMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.util.HashSet;
import java.util.Iterator;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created by jacobo on 20/01/19.
 */
public class HBaseTaskMetadataDBAdaptor extends AbstractHBaseDBAdaptor implements TaskMetadataDBAdaptor {

    public HBaseTaskMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
    }

    @Override
    public TaskMetadata getTask(int studyId, int taskId, Long timeStamp) {
        return readValue(getTaskRowKey(studyId, taskId), TaskMetadata.class, timeStamp);
    }

    @Override
    public Iterator<TaskMetadata> taskIterator(int studyId, boolean reversed) {
        return iterator(getTaskRowKeyPrefix(studyId), TaskMetadata.class, reversed);
    }

    @Override
    public void updateTask(int studyId, TaskMetadata task, Long timeStamp) {
        putValue(getTaskRowKey(studyId, task.getId()), Type.TASK, task, timeStamp);

        TaskMetadata.Status currentStatus = task.currentStatus();
        HashSet<TaskMetadata.Status> allStatus = new HashSet<>(task.getStatus().values());
        for (TaskMetadata.Status status : allStatus) {
            if (currentStatus.equals(status)) {
                putValue(getTaskStatusIndexRowKey(studyId, currentStatus, task.getId()), Type.INDEX, task.getId(), timeStamp);
            } else {
                deleteRow(getTaskStatusIndexRowKey(studyId, status, task.getId()));
            }
        }
    }

}
