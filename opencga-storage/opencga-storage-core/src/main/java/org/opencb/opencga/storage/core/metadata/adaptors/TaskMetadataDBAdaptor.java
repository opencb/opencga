package org.opencb.opencga.storage.core.metadata.adaptors;

import com.google.common.collect.Iterators;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Locked;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jacobo on 20/01/19.
 */
public interface TaskMetadataDBAdaptor {

    TaskMetadata getTask(int studyId, int taskId, Long timeStamp);

    Iterator<TaskMetadata> taskIterator(int studyId, List<TaskMetadata.Status> statusFilter, boolean reversed);

    void updateTask(int studyId, TaskMetadata task, Long timeStamp);

    default Iterable<TaskMetadata> getRunningTasks(int studyId) {
        return () -> Iterators.filter(taskIterator(studyId, Collections.singletonList(TaskMetadata.Status.RUNNING), false),
                t -> t.currentStatus().equals(TaskMetadata.Status.RUNNING));
    }

    Locked lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException;
}
