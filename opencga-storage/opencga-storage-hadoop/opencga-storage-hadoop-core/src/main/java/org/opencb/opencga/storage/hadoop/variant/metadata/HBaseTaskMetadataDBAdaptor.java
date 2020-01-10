package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Locked;
import org.opencb.opencga.storage.core.metadata.adaptors.TaskMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.util.*;

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
    public Iterator<TaskMetadata> taskIterator(int studyId, List<TaskMetadata.Status> statusFilter, boolean reversed) {
        if (statusFilter == null) {
            return iterator(getTaskRowKeyPrefix(studyId), TaskMetadata.class, reversed);
        } else if (statusFilter.contains(TaskMetadata.Status.READY)) {
            EnumSet<TaskMetadata.Status> set = EnumSet.copyOf(statusFilter);
            return Iterators.filter(iterator(getTaskRowKeyPrefix(studyId), TaskMetadata.class, reversed),
                    t -> set.contains(t.currentStatus()));
        } else {
            List<Iterator<Integer>> idsIterators = new ArrayList<>(statusFilter.size());
            for (TaskMetadata.Status status : statusFilter) {
                idsIterators.add(iterator(getTaskStatusIndexRowKeyPrefix(studyId, status), Integer.class));
            }
            Iterator<Integer> allIds = Iterators.concat(idsIterators.iterator());
            Iterator<byte[]> rowKeys = Iterators.transform(allIds, id -> getTaskRowKey(studyId, id));

            return iterator(rowKeys, TaskMetadata.class, getValueColumn());
        }
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

    @Override
    public Locked lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
        return lock(getTaskRowKey(studyId, id), lockDuration, timeout);
    }

}
