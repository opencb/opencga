package org.opencb.opencga.storage.core.metadata.adaptors;

import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;

import java.util.Iterator;

/**
 * Created by jacobo on 20/01/19.
 */
public interface TaskMetadataDBAdaptor {

    TaskMetadata getTask(int studyId, int taskId, Long timeStamp);

    Iterator<TaskMetadata> taskIterator(int studyId, boolean reversed);

    void updateTask(int studyId, TaskMetadata task, Long timeStamp);

}
